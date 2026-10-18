//! The newest record of each key of the memtables a kept index covers, in one table a get reads
//! where the key's hash points (research/63): a record that fits its slot is answered by that one
//! read, where the index of memtable and offset it replaces read the entry after the slot, two
//! reads in a row; one larger keeps its key's head in the slot and the rest in the table's spill.
//!
//! Linear probing with Robin Hood displacement (Celis, 1986): each slot holds its record's
//! displacement from its home slot, and a lookup stops at the first slot whose displacement is
//! below the probe's. The table is kept at most 2/3 full, where a successful search reads
//! 1/2 (1 + 1/(1 - a)) = 2 slots expected, the second the next line pair (Knuth, TAOCP vol. 3,
//! §6.4), and grows by half again when a merge would pass that.
//!
//! Its slots live on a mapping Linux backs with huge pages, faulted in for writing when made
//! ([`crate::port::mmap::HugeZeroed`]): on 4 KiB pages a probe walks the page tables, and a 10M
//! get gained nothing from the one read (research/63 §4).

use crate::branch::Op;
use crate::error::{Error, Malformed};
use crate::port::mmap::HugeZeroed;

/// A slot's header: the memtable's number's low 16 bits (2 bytes; [`older16`]), the displacement
/// plus one (0: empty), the flags, the record's place (4: the key's 32-bit filter hash, which names
/// its home slot, kept so a rebuild places it without hashing the key again), and the key's
/// length (2). An inline record's value length follows (2); a spilled one's spill offset (4) and
/// value length (4), then the key's head. A 16-byte key and a 100-byte value fill a 128-byte slot.
const HEADER: usize = 10;
const INLINE_HEAD: usize = HEADER + 2;
const SPILL_HEAD: usize = HEADER + 8;
/// The flags: the operation is a put; the record is in the spill.
const PUT: u8 = 1;
const SPILLED: u8 = 2;
/// The displacement a slot records at most, its byte's range less the empty mark; a table whose
/// probe would go further grows.
const MOST_DISPLACED: usize = 254;
/// The bytes a cache line holds, the unit slots are sized in.
const LINE: usize = 64;
/// A record list's header: the 32-bit hash (4), the operation (1), the key's length (4) and the
/// value's (4).
const LIST_HEAD: usize = 13;

/// What a table answers for a key.
#[derive(Debug, PartialEq, Eq)]
pub enum Answer<'a> {
    /// Its newest record among the memtables the table covers: the operation and the value.
    Record(Op, &'a [u8]),
    /// No memtable the table covers holds it.
    Absent,
    /// Its record's memtable was given back: the trunk answers.
    Unknown,
}

/// A record bound for a table: its key's 32-bit filter hash ([`crate::branch::filter::hash32`],
/// which a packing's sort keeps for each key), operation, key and value.
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    pub hash32: u32,
    pub op: Op,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// One memtable's records as its packing walks them, each key's newest entry once, in one buffer:
/// what a kept index takes the memtable's records from ([`RecordTable::merge`]).
#[derive(Debug, Default)]
pub struct RecordList {
    bytes: Vec<u8>,
    count: usize,
}

impl RecordList {
    /// Empties the list, keeping its buffer for the next memtable's walk.
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.count = 0;
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The bytes its buffer holds, at its capacity.
    pub fn bytes(&self) -> usize {
        self.bytes.capacity()
    }

    /// Room for `bytes` more bytes of records.
    pub fn reserve(&mut self, bytes: usize) -> Result<(), Error> {
        self.bytes
            .try_reserve(bytes)
            .map_err(|_| Error::LimitExceeded {
                what: "a kept memtable's record list",
                limit: u64::try_from(bytes).unwrap_or(u64::MAX),
            })
    }

    /// Adds a record.
    pub fn push(&mut self, r: &Record<'_>) -> Result<(), Error> {
        let too_large = || Error::LimitExceeded {
            what: "a record a kept index can name",
            limit: u64::from(u32::MAX),
        };
        let klen = u32::try_from(r.key.len()).map_err(|_| too_large())?;
        let vlen = u32::try_from(r.value.len()).map_err(|_| too_large())?;
        let need = LIST_HEAD
            .checked_add(r.key.len())
            .and_then(|n| n.checked_add(r.value.len()))
            .ok_or_else(too_large)?;
        self.reserve(need)?;
        self.bytes.extend_from_slice(&r.hash32.to_le_bytes());
        self.bytes.push(r.op.byte());
        self.bytes.extend_from_slice(&klen.to_le_bytes());
        self.bytes.extend_from_slice(&vlen.to_le_bytes());
        self.bytes.extend_from_slice(r.key);
        self.bytes.extend_from_slice(r.value);
        self.count = self.count.saturating_add(1);
        Ok(())
    }

    /// Its records, in the order they were added.
    pub fn records(&self) -> impl Iterator<Item = Result<Record<'_>, Error>> {
        let run = self.bytes.as_slice();
        let mut at = 0usize;
        std::iter::from_fn(move || {
            if at >= run.len() {
                return None;
            }
            let read = (|| {
                let head = run.get(at..at.checked_add(LIST_HEAD)?)?;
                let hash32 = u32::from_le_bytes(head.get(..4)?.try_into().ok()?);
                let op = match *head.get(4)? {
                    b if b == Op::Put.byte() => Op::Put,
                    b if b == Op::Delete.byte() => Op::Delete,
                    _ => return None,
                };
                let klen =
                    usize::try_from(u32::from_le_bytes(head.get(5..9)?.try_into().ok()?)).ok()?;
                let vlen =
                    usize::try_from(u32::from_le_bytes(head.get(9..13)?.try_into().ok()?)).ok()?;
                let kstart = at.checked_add(LIST_HEAD)?;
                let vstart = kstart.checked_add(klen)?;
                let end = vstart.checked_add(vlen)?;
                let r = Record {
                    hash32,
                    op,
                    key: run.get(kstart..vstart)?,
                    value: run.get(vstart..end)?,
                };
                Some((r, end))
            })();
            Some(match read {
                Some((r, end)) => {
                    at = end;
                    Ok(r)
                }
                None => {
                    at = run.len();
                    Err(corrupt())
                }
            })
        })
    }

    /// The slot size that holds its records in the fewest bytes: a record's expected share at a
    /// load of 2/3 (3/2 of its slot) and the spill's bytes for those larger than the slot, over
    /// every whole number of lines up to the one that holds the largest record.
    pub fn slot_size(&self) -> usize {
        let mut sizes: Vec<usize> = Vec::new();
        if sizes.try_reserve(self.count).is_err() {
            return LINE;
        }
        for r in self.records().flatten() {
            sizes.push(r.key.len().saturating_add(r.value.len()));
        }
        let largest = sizes.iter().copied().max().unwrap_or(0);
        let most = INLINE_HEAD
            .saturating_add(largest)
            .div_ceil(LINE)
            .saturating_mul(LINE)
            .max(LINE);
        let mut best = (usize::MAX, most);
        let mut slot = LINE;
        while slot <= most {
            let fits = slot.saturating_sub(INLINE_HEAD);
            let spilled = sizes
                .iter()
                .filter(|&&s| s > fits)
                .fold(0usize, |acc, &s| acc.saturating_add(s));
            let cost = slot
                .saturating_mul(3)
                .saturating_mul(sizes.len())
                .saturating_div(2)
                .saturating_add(spilled);
            if cost < best.0 {
                best = (cost, slot);
            }
            slot = slot.saturating_add(LINE);
        }
        best.1
    }
}

/// The records: `slots` slots of `slot` bytes on a huge-page mapping, and the spill of those too
/// large for a slot.
pub struct RecordTable {
    map: HugeZeroed,
    slot: usize,
    slots: usize,
    live: usize,
    spill: Vec<u8>,
    /// A record's slot image as it is placed, reused.
    scratch: Vec<u8>,
}

impl std::fmt::Debug for RecordTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordTable")
            .field("slot", &self.slot)
            .field("slots", &self.slots)
            .field("live", &self.live)
            .field("spill", &self.spill.len())
            .finish()
    }
}

impl Default for RecordTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordTable {
    /// An empty table, its slot size taken from the first records merged into it.
    pub fn new() -> Self {
        Self {
            map: HugeZeroed::empty(),
            slot: 0,
            slots: 0,
            live: 0,
            spill: Vec::new(),
            scratch: Vec::new(),
        }
    }

    /// The records it holds, those of memtables given back included until a sweep drops them.
    pub fn len(&self) -> usize {
        self.live
    }

    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// The bytes it holds.
    pub fn bytes(&self) -> usize {
        self.map.len().saturating_add(self.spill.capacity())
    }

    /// The home slot of a place: scaled to the slot count (Lemire's reduction of a 32-bit value).
    fn home(&self, place: u32) -> usize {
        let scaled = u64::from(place).wrapping_mul(self.slots as u64) >> 32;
        usize::try_from(scaled).unwrap_or(0)
    }

    fn at(&self, i: usize) -> Option<&[u8]> {
        let start = i.checked_mul(self.slot)?;
        self.map.bytes().get(start..start.checked_add(self.slot)?)
    }

    fn at_mut(&mut self, i: usize) -> Option<&mut [u8]> {
        let start = i.checked_mul(self.slot)?;
        let end = start.checked_add(self.slot)?;
        self.map.bytes_mut().get_mut(start..end)
    }

    fn next(&self, i: usize) -> usize {
        let n = i.wrapping_add(1);
        if n >= self.slots { 0 } else { n }
    }

    /// Reads `hash32`'s home slot's first byte, so a lookup after finds its line cached: probes of
    /// several tables or memtables made together overlap their misses.
    pub fn touch(&self, hash32: u32) -> u8 {
        if self.slots == 0 {
            return 0;
        }
        self.at(self.home(hash32))
            .and_then(|s| s.first().copied())
            .unwrap_or(0)
    }

    /// Whether slot `s`, at displacement `d` from the probe's home, holds `key` of place `place`.
    fn holds(&self, s: &[u8], d: usize, place: u32, key: &[u8]) -> bool {
        if usize::from(disp_of(s)) != d.wrapping_add(1) || place_of(s) != Some(place) {
            return false;
        }
        let klen = klen_of(s);
        if klen != key.len() {
            return false;
        }
        if flags_of(s) & SPILLED == 0 {
            return s.get(INLINE_HEAD..INLINE_HEAD.saturating_add(klen)) == Some(key);
        }
        // The key's head in the slot, its whole in the spill.
        let head = klen.min(self.slot.saturating_sub(SPILL_HEAD));
        if s.get(SPILL_HEAD..SPILL_HEAD.saturating_add(head)) != key.get(..head) {
            return false;
        }
        self.spilled(s).is_some_and(|(k, _)| k == key)
    }

    /// A spilled slot's key and value, read from the spill.
    fn spilled<'a>(&'a self, s: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
        let klen = klen_of(s);
        let at = usize::try_from(u32::from_le_bytes(s.get(10..14)?.try_into().ok()?)).ok()?;
        let vlen = usize::try_from(u32::from_le_bytes(s.get(14..18)?.try_into().ok()?)).ok()?;
        let mid = at.checked_add(klen)?;
        let end = mid.checked_add(vlen)?;
        Some((self.spill.get(at..mid)?, self.spill.get(mid..end)?))
    }

    /// The newest record of `key`, of 32-bit filter hash `hash32`: unknown when its memtable is
    /// older than `oldest`, which was given back.
    pub fn get(&self, hash32: u32, key: &[u8], oldest: u32) -> Answer<'_> {
        if self.slots == 0 {
            return Answer::Absent;
        }
        let mut i = self.home(hash32);
        // At a load of 2/3 a hit reads two slots expected: the next one's first line started
        // with the home slot's, so the second read overlaps the first rather than following it
        // (two slots read together cost one read's latency: research/63 §1).
        std::hint::black_box(self.at(self.next(i)).and_then(|s| s.first().copied()));
        for d in 0..=MOST_DISPLACED {
            let Some(s) = self.at(i) else {
                return Answer::Absent;
            };
            let disp = usize::from(disp_of(s));
            if disp == 0 || disp.wrapping_sub(1) < d {
                return Answer::Absent;
            }
            if self.holds(s, d, hash32, key) {
                return self.answer(s, oldest);
            }
            i = self.next(i);
        }
        Answer::Absent
    }

    fn answer<'a>(&'a self, s: &'a [u8], oldest: u32) -> Answer<'a> {
        if older16(number_of(s), oldest) {
            return Answer::Unknown;
        }
        let flags = flags_of(s);
        let op = if flags & PUT != 0 {
            Op::Put
        } else {
            Op::Delete
        };
        let value = if flags & SPILLED != 0 {
            self.spilled(s).map(|(_, v)| v)
        } else {
            let start = INLINE_HEAD.saturating_add(klen_of(s));
            u16_at(s, 10)
                .map(usize::from)
                .and_then(|vlen| s.get(start..start.saturating_add(vlen)))
        };
        match value {
            Some(v) => Answer::Record(op, v),
            None => Answer::Unknown,
        }
    }

    /// Lays `r`, of memtable `number` and place `place`, into `scratch` as its slot holds it at
    /// displacement 0, its key and value into the spill when they do not fit.
    fn image(&mut self, number: u32, place: u32, r: &Record<'_>) -> Result<(), Error> {
        let too_large = || Error::LimitExceeded {
            what: "a record a kept index can name",
            limit: u64::from(u16::MAX),
        };
        let klen = u16::try_from(r.key.len()).map_err(|_| too_large())?;
        let mut s = std::mem::take(&mut self.scratch);
        s.clear();
        s.resize(self.slot, 0);
        let flags = if r.op == Op::Put { PUT } else { 0 };
        let inline = INLINE_HEAD
            .checked_add(r.key.len())
            .and_then(|n| n.checked_add(r.value.len()))
            .is_some_and(|n| n <= self.slot)
            && u16::try_from(r.value.len()).is_ok();
        let laid = (|| {
            s.get_mut(..2)?
                .copy_from_slice(&low16(number).to_le_bytes());
            *s.get_mut(2)? = 1;
            s.get_mut(4..8)?.copy_from_slice(&place.to_le_bytes());
            s.get_mut(8..10)?.copy_from_slice(&klen.to_le_bytes());
            if inline {
                *s.get_mut(3)? = flags;
                let vlen = u16::try_from(r.value.len()).ok()?;
                s.get_mut(10..12)?.copy_from_slice(&vlen.to_le_bytes());
                let mid = INLINE_HEAD.checked_add(r.key.len())?;
                s.get_mut(INLINE_HEAD..mid)?.copy_from_slice(r.key);
                s.get_mut(mid..mid.checked_add(r.value.len())?)?
                    .copy_from_slice(r.value);
            } else {
                *s.get_mut(3)? = flags | SPILLED;
                let at = u32::try_from(self.spill.len()).ok()?;
                let vlen = u32::try_from(r.value.len()).ok()?;
                let grows = r.key.len().checked_add(r.value.len())?;
                self.spill.try_reserve(grows).ok()?;
                self.spill.extend_from_slice(r.key);
                self.spill.extend_from_slice(r.value);
                s.get_mut(10..14)?.copy_from_slice(&at.to_le_bytes());
                s.get_mut(14..18)?.copy_from_slice(&vlen.to_le_bytes());
                let head = r.key.len().min(self.slot.saturating_sub(SPILL_HEAD));
                s.get_mut(SPILL_HEAD..SPILL_HEAD.checked_add(head)?)?
                    .copy_from_slice(r.key.get(..head)?);
            }
            Some(())
        })();
        self.scratch = s;
        laid.ok_or_else(too_large)
    }

    /// Places the record in `scratch` (displacement 0) from its home slot, replacing the key's
    /// record if the table holds one. False when the probe would pass [`MOST_DISPLACED`]: the
    /// table then still holds every record it held, some moved along the run.
    fn place(&mut self, place: u32, key: &[u8]) -> bool {
        let mut cur = std::mem::take(&mut self.scratch);
        let mut i = self.home(place);
        let mut d = 0usize;
        let mut carrying_new = true;
        let placed = loop {
            if d > MOST_DISPLACED {
                break false;
            }
            let Some(s) = self.at(i) else {
                break false;
            };
            let disp = usize::from(disp_of(s));
            let replaces = disp != 0 && carrying_new && self.holds(s, d, place, key);
            if disp == 0 || replaces {
                if let Some(b) = cur.get_mut(2) {
                    *b = u8::try_from(d.wrapping_add(1)).unwrap_or(u8::MAX);
                }
                if let Some(dst) = self.at_mut(i) {
                    dst.copy_from_slice(&cur);
                }
                if disp == 0 {
                    self.live = self.live.saturating_add(1);
                }
                break true;
            }
            let sd = disp.wrapping_sub(1);
            if sd < d {
                // Robin Hood: the record carried takes the slot, and the one there moves on.
                if let Some(b) = cur.get_mut(2) {
                    *b = u8::try_from(d.wrapping_add(1)).unwrap_or(u8::MAX);
                }
                if let Some(dst) = self.at_mut(i) {
                    dst.swap_with_slice(&mut cur);
                }
                d = sd;
                carrying_new = false;
            }
            i = self.next(i);
            d = d.wrapping_add(1);
        };
        self.scratch = cur;
        placed
    }

    /// Rebuilt with room for `records` at a load of at most 2/3, holding only the records of
    /// memtables no older than `oldest`.
    fn rebuild(&mut self, records: usize, oldest: u32) -> Result<(), Error> {
        let too_large = || Error::LimitExceeded {
            what: "a kept index's records",
            limit: u64::try_from(records).unwrap_or(u64::MAX),
        };
        let slots = records
            .checked_add(records / 2)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(too_large)?;
        let len = slots.checked_mul(self.slot).ok_or_else(too_large)?;
        let mut fresh = RecordTable {
            map: HugeZeroed::allocate(len)?,
            slot: self.slot,
            slots,
            live: 0,
            spill: Vec::new(),
            scratch: Vec::new(),
        };
        for i in 0..self.slots {
            let Some(s) = self.at(i) else {
                return Err(corrupt());
            };
            if disp_of(s) == 0 || older16(number_of(s), oldest) {
                continue;
            }
            let (Some((key, value, op)), Some(place)) = (self.record_of(s), place_of(s)) else {
                return Err(corrupt());
            };
            // A record's place is what puts it in a slot: the slot kept it.
            let r = Record {
                hash32: place,
                op,
                key,
                value,
            };
            fresh.image(u32::from(number_of(s)), place, &r)?;
            if !fresh.place(place, key) {
                return Err(corrupt());
            }
        }
        *self = fresh;
        Ok(())
    }

    /// A slot's key, value and operation.
    fn record_of<'a>(&'a self, s: &'a [u8]) -> Option<(&'a [u8], &'a [u8], Op)> {
        let flags = flags_of(s);
        let op = if flags & PUT != 0 {
            Op::Put
        } else {
            Op::Delete
        };
        if flags & SPILLED != 0 {
            let (k, v) = self.spilled(s)?;
            return Some((k, v, op));
        }
        let klen = klen_of(s);
        let vlen = usize::from(u16_at(s, 10)?);
        let mid = INLINE_HEAD.checked_add(klen)?;
        Some((
            s.get(INLINE_HEAD..mid)?,
            s.get(mid..mid.checked_add(vlen)?)?,
            op,
        ))
    }

    /// Takes memtable `number`'s records, each key once, after every record already held: a key's
    /// record here is replaced by its newer one. The first list a table takes sets its slot size
    /// ([`RecordList::slot_size`]). A table that must grow drops, as it rebuilds, the records of
    /// memtables older than `oldest`.
    pub fn merge(&mut self, number: u32, list: &RecordList, oldest: u32) -> Result<(), Error> {
        if self.slot == 0 {
            self.slot = list.slot_size();
        }
        let need = self.live.checked_add(list.len()).ok_or_else(corrupt)?;
        // At most 2/3 full: grown by half again past the need, so a table grows in steps that
        // move each record a bounded number of times.
        if need.saturating_mul(3) > self.slots.saturating_mul(2) {
            self.rebuild(need.saturating_add(need / 2), oldest)?;
        }
        for r in list.records() {
            let r = r?;
            self.image(number, r.hash32, &r)?;
            if !self.place(r.hash32, r.key) {
                // A run too long: grown, and placed again.
                let room = self.live.saturating_add(self.live / 2).saturating_add(1);
                self.rebuild(room, oldest)?;
                self.image(number, r.hash32, &r)?;
                if !self.place(r.hash32, r.key) {
                    return Err(corrupt());
                }
            }
        }
        Ok(())
    }

    /// Drops the records of memtables older than `oldest`, given back, the table rebuilt to its
    /// records' size.
    pub fn sweep(&mut self, oldest: u32) -> Result<(), Error> {
        if self.slots == 0 {
            return Ok(());
        }
        let live = (0..self.slots)
            .filter_map(|i| self.at(i))
            .filter(|s| disp_of(s) != 0 && !older16(number_of(s), oldest))
            .count();
        self.rebuild(live, oldest)
    }
}

fn corrupt() -> Error {
    Error::Corruption {
        what: "a kept index's record table",
        why: Malformed::CountMismatch,
    }
}

/// A memtable number's low 16 bits, as a slot keeps it.
fn low16(number: u32) -> u16 {
    u16::try_from(number & 0xFFFF).unwrap_or(0)
}

/// Whether memtable `a`, of which a slot keeps the low 16 bits, was numbered before `b`: as
/// [`crate::kept::older`] decides it modulo 2^32, modulo 2^16, which holds while the memtables a
/// table's records came from span fewer than 2^15 numbers (the shard sweeps a table before its
/// oldest record's memtable is that far behind).
fn older16(a: u16, b: u32) -> bool {
    let d = low16(b).wrapping_sub(a);
    d != 0 && d < 1 << 15
}

/// The little-endian 16 bits at `at`, if the slot holds them.
fn u16_at(s: &[u8], at: usize) -> Option<u16> {
    s.get(at..at.checked_add(2)?)
        .and_then(|b| b.try_into().ok())
        .map(u16::from_le_bytes)
}

fn number_of(s: &[u8]) -> u16 {
    u16_at(s, 0).unwrap_or(0)
}

fn disp_of(s: &[u8]) -> u8 {
    s.get(2).copied().unwrap_or(0)
}

fn flags_of(s: &[u8]) -> u8 {
    s.get(3).copied().unwrap_or(0)
}

fn place_of(s: &[u8]) -> Option<u32> {
    s.get(4..8)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_le_bytes)
}

fn klen_of(s: &[u8]) -> usize {
    u16_at(s, 8).map_or(0, usize::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn h(k: &[u8]) -> u32 {
        crate::branch::filter::hash32(crate::branch::filter::hash(k))
    }

    fn list_of(batch: &BTreeMap<Vec<u8>, (Op, Vec<u8>)>) -> RecordList {
        let mut list = RecordList::default();
        for (k, (op, v)) in batch {
            list.push(&Record {
                hash32: h(k),
                op: *op,
                key: k,
                value: v,
            })
            .unwrap();
        }
        list
    }

    /// Memtables merged oldest first, some given back and swept, against a model of each key's
    /// newest record: a key whose newest record is in a memtable still kept answers it, one whose
    /// newest record was given back never does, keys never written are absent, and records larger
    /// than a slot answer through the spill.
    #[test]
    fn it_answers_each_key_s_newest_kept_record_through_merges_growth_sweeps_and_give_backs() {
        let mut t = RecordTable::new();
        let mut model: BTreeMap<Vec<u8>, (u32, Op, Vec<u8>)> = BTreeMap::new();
        let mut oldest = 0u32;
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut rand = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for number in 0u32..12 {
            let mut batch: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
            for _ in 0..700 {
                let k = rand(3000);
                let key = if rand(20) == 0 {
                    format!("{k:0200}").into_bytes()
                } else {
                    format!("{k:016}").into_bytes()
                };
                let op = if rand(9) == 0 { Op::Delete } else { Op::Put };
                let vlen: usize = if rand(15) == 0 { 400 } else { 100 };
                batch.insert(key, (op, vec![b'a' + (number % 26) as u8; vlen]));
            }
            t.merge(number, &list_of(&batch), oldest).unwrap();
            for (k, (op, v)) in batch {
                model.insert(k, (number, op, v));
            }
            if number % 4 == 3 {
                oldest = number - 1;
            }
            if number % 5 == 4 {
                t.sweep(oldest).unwrap();
            }
            for (k, (n, op, v)) in &model {
                let got = t.get(h(k), k, oldest);
                if crate::kept::older(*n, oldest) {
                    assert!(
                        matches!(got, Answer::Unknown | Answer::Absent),
                        "{k:?} of memtable {n}, given back, answered {got:?}"
                    );
                } else {
                    assert_eq!(got, Answer::Record(*op, v), "{k:?} of memtable {n}");
                }
            }
            for k in 3000u64..3200 {
                let key = format!("{k:016}").into_bytes();
                assert_eq!(t.get(h(&key), &key, oldest), Answer::Absent);
            }
        }
    }

    #[test]
    fn keys_of_one_hash_keep_their_own_records() {
        let mut t = RecordTable::new();
        let mut list = RecordList::default();
        for (k, v) in [(b"a", b"1"), (b"b", b"2")] {
            list.push(&Record {
                hash32: 7,
                op: Op::Put,
                key: k,
                value: v,
            })
            .unwrap();
        }
        t.merge(0, &list, 0).unwrap();
        let mut newer = RecordList::default();
        newer
            .push(&Record {
                hash32: 7,
                op: Op::Put,
                key: b"a",
                value: b"3",
            })
            .unwrap();
        t.merge(1, &newer, 0).unwrap();
        assert_eq!(t.get(7, b"a", 0), Answer::Record(Op::Put, b"3"));
        assert_eq!(t.get(7, b"b", 0), Answer::Record(Op::Put, b"2"));
        assert_eq!(t.get(7, b"c", 0), Answer::Absent);
    }

    /// The slot size fits a list of one size in a whole number of lines, and a list whose records
    /// are mostly small with a few large keeps the small slot and spills the large.
    #[test]
    fn the_slot_size_is_the_one_that_holds_the_records_in_the_fewest_bytes() {
        let mut list = RecordList::default();
        for i in 0..100u32 {
            let key = format!("{i:016}").into_bytes();
            list.push(&Record {
                hash32: i,
                op: Op::Put,
                key: &key,
                value: &[0u8; 100],
            })
            .unwrap();
        }
        assert_eq!(list.slot_size(), 128);
        for i in 0..3u32 {
            let key = format!("{i:016}").into_bytes();
            list.push(&Record {
                hash32: i,
                op: Op::Put,
                key: &key,
                value: &[0u8; 4000],
            })
            .unwrap();
        }
        assert_eq!(list.slot_size(), 128);
    }
}
