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
/// Records a merge places together, their home slots read first, back to back, so their misses
/// are out at once rather than each in turn (group prefetching: Chen, Ailamaki, Gibbons and
/// Mowry, ICDE 2004).
const GROUP: usize = 16;

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

/// The part of an index split by the top `bits` bits of a place that `place` goes to.
pub fn part_of(place: u32, bits: u32) -> usize {
    if bits == 0 {
        return 0;
    }
    usize::try_from(place.wrapping_shr(32u32.saturating_sub(bits))).unwrap_or(0)
}

/// One memtable's records split by the top `bits` bits of their places, a list for each part of
/// the index they go to, so a part's merge reads only its own records ([`crate::kept`]).
#[derive(Debug)]
pub struct PartedList {
    parts: Vec<RecordList>,
    bits: u32,
}

impl Default for PartedList {
    fn default() -> Self {
        Self::new(0)
    }
}

impl PartedList {
    /// Empty lists for `2^bits` parts.
    pub fn new(bits: u32) -> Self {
        let parts = 1usize.checked_shl(bits).unwrap_or(1);
        Self {
            parts: (0..parts).map(|_| RecordList::default()).collect(),
            bits,
        }
    }

    /// The top bits of a place that choose its part.
    pub fn bits(&self) -> u32 {
        self.bits
    }

    /// Empties every part, keeping the buffers for the next memtable's walk.
    pub fn clear(&mut self) {
        self.parts.iter_mut().for_each(RecordList::clear);
    }

    pub fn len(&self) -> usize {
        self.parts
            .iter()
            .map(RecordList::len)
            .fold(0, usize::saturating_add)
    }

    pub fn is_empty(&self) -> bool {
        self.parts.iter().all(RecordList::is_empty)
    }

    /// The bytes its buffers hold, at their capacities.
    pub fn bytes(&self) -> usize {
        self.parts
            .iter()
            .map(RecordList::bytes)
            .fold(0, usize::saturating_add)
    }

    /// Room for `bytes` more bytes of records, an even share in each part: places are a filter
    /// hash's, spread evenly over the parts.
    pub fn reserve(&mut self, bytes: usize) -> Result<(), Error> {
        let share = bytes.div_ceil(self.parts.len().max(1));
        self.parts.iter_mut().try_for_each(|p| p.reserve(share))
    }

    /// Adds a record to its part.
    pub fn push(&mut self, r: &Record<'_>) -> Result<(), Error> {
        self.parts
            .get_mut(part_of(r.hash32, self.bits))
            .ok_or_else(corrupt)?
            .push(r)
    }

    /// Part `part`'s list, taken out for its merge; an empty one is left until it comes back.
    pub fn take(&mut self, part: usize) -> RecordList {
        self.parts
            .get_mut(part)
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Part `part`'s list, back from its merge.
    pub fn put(&mut self, part: usize, list: RecordList) {
        if let Some(p) = self.parts.get_mut(part) {
            *p = list;
        }
    }

    /// Part `part`'s list.
    pub fn part(&self, part: usize) -> Option<&RecordList> {
        self.parts.get(part)
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
    /// The top bits of a place that chose this table among an index's parts ([`part_of`]): its
    /// records share them, so a home is reduced from the bits below.
    shift: u32,
}

impl std::fmt::Debug for RecordTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordTable")
            .field("slot", &self.slot)
            .field("slots", &self.slots)
            .field("live", &self.live)
            .field("spill", &self.spill.len())
            .field("shift", &self.shift)
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
        Self::part(0)
    }

    /// An empty table for one part of an index split by the top `bits` bits of a place.
    pub fn part(bits: u32) -> Self {
        Self {
            map: HugeZeroed::empty(),
            slot: 0,
            slots: 0,
            live: 0,
            spill: Vec::new(),
            scratch: Vec::new(),
            shift: bits,
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

    /// The home slot of a place: its bits below the part's, scaled to the slot count (Lemire's
    /// reduction of a 32-bit value).
    fn home(&self, place: u32) -> usize {
        let local = place.wrapping_shl(self.shift);
        let scaled = u64::from(local).wrapping_mul(self.slots as u64) >> 32;
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

    /// Places the record whose slot image (displacement 0) is `image` from its home slot,
    /// replacing the key's record if the table holds one. Robin Hood keeps a run's records in the order of their
    /// homes, so the record goes where the first record nearer its own home sits, and the rest of
    /// the run moves along one slot, each a step further from its home: one move of the run's
    /// bytes, where carrying each displaced record on to the next slot read and wrote every slot
    /// twice (128-byte slots: an insert spent 104-131 ns, `kept_merge`). False, the table
    /// unchanged, when the record or a record it moves would pass [`MOST_DISPLACED`], or no slot is
    /// empty: nothing moves before every check passes.
    fn place(&mut self, place: u32, key: &[u8], image: &[u8]) -> bool {
        if self.slots == 0 {
            return false;
        }
        let mut i = self.home(place);
        let mut d = 0usize;
        loop {
            if d > MOST_DISPLACED {
                return false;
            }
            let Some(s) = self.at(i) else {
                return false;
            };
            let disp = usize::from(disp_of(s));
            if disp == 0 || disp.wrapping_sub(1) < d {
                break;
            }
            if self.holds(s, d, place, key) {
                return self.lay(i, d, image);
            }
            i = self.next(i);
            d = d.wrapping_add(1);
        }
        // The run from `i` to the first empty slot moves along one: each record there must
        // still have a displacement to give.
        let mut end = i;
        let mut moved = 0usize;
        loop {
            let Some(s) = self.at(end) else {
                return false;
            };
            let disp = usize::from(disp_of(s));
            if disp == 0 {
                break;
            }
            if disp > MOST_DISPLACED {
                return false;
            }
            end = self.next(end);
            moved = moved.saturating_add(1);
            if moved >= self.slots {
                return false;
            }
        }
        if !self.shift(i, end) {
            return false;
        }
        self.live = self.live.saturating_add(1);
        self.lay(i, d, image)
    }

    /// Writes `image` into slot `i` at displacement `d`.
    fn lay(&mut self, i: usize, d: usize, image: &[u8]) -> bool {
        let Ok(disp) = u8::try_from(d.saturating_add(1)) else {
            return false;
        };
        if image.len() != self.slot {
            return false;
        }
        let Some(dst) = self.at_mut(i) else {
            return false;
        };
        dst.copy_from_slice(image);
        dst.get_mut(2).map(|b| *b = disp).is_some()
    }

    /// Places the record laid in `scratch` ([`Self::place`]).
    fn place_scratch(&mut self, place: u32, key: &[u8]) -> bool {
        let image = std::mem::take(&mut self.scratch);
        let placed = self.place(place, key, &image);
        self.scratch = image;
        placed
    }

    /// Moves the records of slots `from` up to the empty slot `end` along one slot, around the
    /// table's end if the run wraps, each a step further from its home. False, nothing moved, when
    /// a range falls outside the table.
    fn shift(&mut self, from: usize, end: usize) -> bool {
        if from == end {
            return true;
        }
        let slot = self.slot;
        let (Some(from_at), Some(end_at), Some(len)) = (
            from.checked_mul(slot),
            end.checked_mul(slot),
            self.slots.checked_mul(slot),
        ) else {
            return false;
        };
        let bytes = self.map.bytes_mut();
        if bytes.len() < len || from_at >= len || end_at >= len {
            return false;
        }
        if from < end {
            // [from, end) to [from + 1, end]: `end` is in the table, so is `end_at + slot`.
            bytes.copy_within(from_at..end_at, from_at.saturating_add(slot));
        } else {
            // The run is [from, slots) then [0, end): the start's records move first, then the
            // last slot's to slot 0, then the rest of the run's tail.
            let last = len.saturating_sub(slot);
            bytes.copy_within(0..end_at, slot);
            bytes.copy_within(last..len, 0);
            bytes.copy_within(from_at..last, from_at.saturating_add(slot));
        }
        let mut k = self.next(from);
        loop {
            if let Some(b) = self.at_mut(k).and_then(|s| s.get_mut(2)) {
                *b = b.saturating_add(1);
            }
            if k == end {
                return true;
            }
            k = self.next(k);
        }
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
            shift: self.shift,
        };
        for i in 0..self.slots {
            let Some(s) = self.at(i) else {
                return Err(corrupt());
            };
            if disp_of(s) == 0 || older16(number_of(s), oldest) {
                continue;
            }
            // A record's place is what puts it in a slot: the slot kept it.
            let Some(place) = place_of(s) else {
                return Err(corrupt());
            };
            let placed = if flags_of(s) & SPILLED == 0 {
                // An inline slot moves as it is, copied once from where it lies, its displacement
                // set where it lands; the old table's slot order is its homes' order in the new
                // one but where a run wrapped, so each lands at or just after the last.
                let key = s
                    .get(INLINE_HEAD..INLINE_HEAD.saturating_add(klen_of(s)))
                    .ok_or_else(corrupt)?;
                fresh.place(place, key, s)
            } else {
                let Some((key, value, op)) = self.record_of(s) else {
                    return Err(corrupt());
                };
                let r = Record {
                    hash32: place,
                    op,
                    key,
                    value,
                };
                fresh.image(u32::from(number_of(s)), place, &r)?;
                fresh.place_scratch(place, key)
            };
            if !placed {
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

    /// Sets the slot size of a table that has none from the first records it will take
    /// ([`RecordList::slot_size`]), once.
    pub fn settle_slot(&mut self, list: &RecordList) {
        if self.slot == 0 && !list.is_empty() {
            self.slot = list.slot_size();
        }
    }

    /// The bytes of the table that a sweep, with `sweep`, then merging lists of `lengths` records,
    /// oldest first, grows this one to at its last growth, as [`Self::sweep`] and [`Self::merge`]
    /// rebuild it, every record counted new and none dropped: what a trip holds beside this table
    /// while it rebuilds, at most; none when nothing would rebuild it. A table's slot size must be
    /// settled first ([`Self::settle_slot`]).
    pub fn growth(&self, sweep: bool, lengths: impl Iterator<Item = usize>) -> usize {
        let (mut live, mut slots, mut grown) = (self.live, self.slots, 0usize);
        if sweep && slots > 0 {
            // A sweep rebuilds to the records it keeps, at most every one.
            slots = live.saturating_add(live / 2).saturating_add(1);
            grown = slots.saturating_mul(self.slot);
        }
        for len in lengths {
            let need = live.saturating_add(len);
            if need.saturating_mul(3) > slots.saturating_mul(2) {
                let records = need.saturating_add(need / 2);
                slots = records.saturating_add(records / 2).saturating_add(1);
                grown = slots.saturating_mul(self.slot);
            }
            live = need;
        }
        grown
    }

    /// Takes memtable `number`'s records, each key once, after every record already held: a key's
    /// record here is replaced by its newer one. The first list a table takes sets its slot size
    /// ([`RecordList::slot_size`]). A table that must grow drops, as it rebuilds, the records of
    /// memtables older than `oldest`.
    pub fn merge(&mut self, number: u32, list: &RecordList, oldest: u32) -> Result<(), Error> {
        self.settle_slot(list);
        let need = self.live.checked_add(list.len()).ok_or_else(corrupt)?;
        // At most 2/3 full: grown by half again past the need, so a table grows in steps that
        // move each record a bounded number of times.
        if need.saturating_mul(3) > self.slots.saturating_mul(2) {
            self.rebuild(need.saturating_add(need / 2), oldest)?;
        }
        let mut records = list.records();
        loop {
            let mut group: [Option<Record<'_>>; GROUP] = [None; GROUP];
            let mut taken = 0usize;
            for slot in &mut group {
                match records.next() {
                    Some(r) => *slot = Some(r?),
                    None => break,
                }
                taken = taken.saturating_add(1);
            }
            if taken == 0 {
                return Ok(());
            }
            let touched = group
                .iter()
                .flatten()
                .fold(0u8, |t, r| t ^ self.touch(r.hash32));
            std::hint::black_box(touched);
            for r in group.iter().flatten() {
                self.image(number, r.hash32, r)?;
                if !self.place_scratch(r.hash32, r.key) {
                    // A run too long: grown, and placed again.
                    let room = self.live.saturating_add(self.live / 2).saturating_add(1);
                    self.rebuild(room, oldest)?;
                    self.image(number, r.hash32, r)?;
                    if !self.place_scratch(r.hash32, r.key) {
                        return Err(corrupt());
                    }
                }
            }
        }
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

    /// Places crowded into the top half of their range put every home in the table's last half, so
    /// runs wrap past its end, and several keys share each place, so a record placed in a run
    /// moves the rest of it along, around the end too: through rounds that write every key again
    /// and add new ones, growing the table, each key answers its newest record.
    #[test]
    fn records_moved_along_runs_that_wrap_the_table_s_end_keep_their_places() {
        let place = |k: u32| u32::MAX - (k % 97) * (u32::MAX / 200);
        let mut t = RecordTable::new();
        let mut model: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        for round in 0u32..8 {
            let mut list = RecordList::default();
            for k in 0..60 * (round + 1) {
                let key = format!("key{k:05}").into_bytes();
                let value = vec![u8::try_from(round).unwrap(); 30 + (k % 9) as usize];
                list.push(&Record {
                    hash32: place(k),
                    op: Op::Put,
                    key: &key,
                    value: &value,
                })
                .unwrap();
                model.insert(k, value);
            }
            t.merge(round, &list, 0).unwrap();
            assert_eq!(t.len(), model.len());
            for (k, v) in &model {
                let key = format!("key{k:05}").into_bytes();
                assert_eq!(
                    t.get(place(*k), &key, 0),
                    Answer::Record(Op::Put, v.as_slice()),
                    "key {k} after round {round}"
                );
            }
            let absent = format!("key{:05}", 60 * (round + 1)).into_bytes();
            assert_eq!(t.get(place(60 * (round + 1)), &absent, 0), Answer::Absent);
            // Every home is in the table's last half: a record in its first slot wrapped there.
            assert_ne!(
                disp_of(t.at(0).unwrap()),
                0,
                "no run wrapped the table's end"
            );
        }
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
