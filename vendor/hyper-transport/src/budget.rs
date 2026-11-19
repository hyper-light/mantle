//! The bytes a node may hold for the transport, reserved through a trait each project implements
//! over its own accountant (mantle note 32 §3.1, "Budgets by trait"): focal's `MemoryBudget`,
//! slates' `mem::budget`, mantle's admission authorities. A reservation is a value its holder gives
//! back; nothing is reference counted.

use crate::Refusal;

/// What a reservation is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane<K> {
    /// A connection's receive window: the bytes QUIC may buffer for the peer before the
    /// application reads them (quinn: "upper bound proportional to receive_window", node.md §3.3);
    /// and the connections' own state beside it (docs/transport.md §4x). Accounting only;
    /// such a reservation carries no buffer.
    Window,
    /// A message of this class: its head, a piece of its body, or a lane's frame.
    Class(K),
}

/// Bytes the node may hold for the transport, implemented over each project's accountant.
pub trait Budget<K> {
    /// `bytes` for `lane`, or the refusal the accountant gives.
    fn reserve(&mut self, bytes: u64, lane: Lane<K>) -> Result<Reservation, Refusal>;
    /// A reservation given back, its bytes free again.
    fn release(&mut self, reservation: Reservation);
}

/// Bytes granted by a [`Budget`], with the buffer they are read into when they hold data.
#[derive(Debug, Default)]
pub struct Reservation {
    granted: u64,
    buffer: Vec<u8>,
    /// The accountant's own mark, given back with the reservation: what it needs to release the
    /// bytes where it took them (focal funds the most urgent class from a reserve apart from the
    /// rest). The endpoint never reads it.
    tag: u8,
}

impl Reservation {
    /// `granted` bytes, read into `buffer` (cleared here); an accountant that recycles buffers
    /// hands back one whose capacity already holds `granted`, so that reading into it allocates
    /// nothing.
    pub fn new(granted: u64, mut buffer: Vec<u8>) -> Self {
        buffer.clear();
        Self {
            granted,
            buffer,
            tag: 0,
        }
    }
    /// [`Reservation::new`], marked with the accountant's `tag` ([`Reservation::tag`]).
    pub fn tagged(granted: u64, buffer: Vec<u8>, tag: u8) -> Self {
        Self {
            tag,
            ..Self::new(granted, buffer)
        }
    }
    /// The accountant's mark: zero unless it was made [`Reservation::tagged`].
    pub fn tag(&self) -> u8 {
        self.tag
    }
    /// The bytes granted.
    pub fn granted(&self) -> u64 {
        self.granted
    }
    /// The bytes read into the reservation so far.
    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }
    /// How many more bytes the reservation takes.
    pub fn room(&self) -> u64 {
        let filled = u64::try_from(self.buffer.len()).unwrap_or(u64::MAX);
        self.granted.saturating_sub(filled)
    }
    /// Empty the reservation, keeping its grant, to read into it again.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
    /// `other`'s grant taken into this one, to be given back with it: how an owner that charges a
    /// sum as one reservation (an endpoint's connections' state, docs/transport.md §4x) grows it.
    /// Both must be of one accountant's mark and hold no buffer (accounting only); one that is not
    /// so, or whose grant the sum cannot hold, is handed back as it was.
    pub fn absorb(&mut self, other: Reservation) -> Result<(), Reservation> {
        if other.tag != self.tag || other.buffer.capacity() > 0 || self.buffer.capacity() > 0 {
            return Err(other);
        }
        match self.granted.checked_add(other.granted) {
            Some(granted) => {
                self.granted = granted;
                Ok(())
            }
            None => Err(other),
        }
    }
    /// `bytes` of this grant split off as a reservation of their own, of the same mark, to be given
    /// back apart; none past the grant, or of a reservation holding a buffer. An accountant gives
    /// back what each release carries, so a reservation split and absorbed gives back, in all, what
    /// was granted.
    pub fn split(&mut self, bytes: u64) -> Option<Reservation> {
        if self.buffer.capacity() > 0 {
            return None;
        }
        self.granted = self.granted.checked_sub(bytes)?;
        Some(Self {
            granted: bytes,
            buffer: Vec::new(),
            tag: self.tag,
        })
    }
    /// The buffer, for an accountant that recycles it.
    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }
    pub(crate) fn fill(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }
}

/// The classes the spare buffers are kept in: a buffer's capacity in sixty-fourths of its power of
/// two, TLSF's two-level index (Masmano, Ripoll, Crespo and Real, "TLSF: a New Dynamic Memory
/// Allocator for Real-Time Systems", ECRTS 2004, §3), so the class a size is in, and the first class
/// past it that holds a buffer, are each found in constant time. A power of two's classes are the
/// bits of one word of the occupancy map.
const CLASS_BITS: u32 = u64::BITS.trailing_zeros();
/// The classes of one power of two: one word's bits.
const PER_POWER: usize = 1 << CLASS_BITS;
/// Every class: a word's for each of a `usize`'s powers of two.
const CLASSES: usize = PER_POWER << CLASS_BITS;
/// The occupancy map's words, a bit for each class.
const WORDS: usize = CLASSES / PER_POWER;
/// The end of a list of slots: above every slot, there being fewer than `u32::MAX`.
const NONE: u32 = u32::MAX;

/// The class of a buffer of `capacity` bytes: below [`PER_POWER`], its capacity; from it, the power
/// of two it is in and the sixty-fourth of it. A larger capacity is in the same class or a later
/// one, so a buffer of a later class than a size's holds that size.
fn class(capacity: usize) -> usize {
    if capacity < PER_POWER {
        return capacity;
    }
    let power = usize::BITS
        .saturating_sub(1)
        .saturating_sub(capacity.leading_zeros());
    let part = capacity
        .checked_shr(power.saturating_sub(CLASS_BITS))
        .unwrap_or(0)
        % PER_POWER;
    usize::try_from(power)
        .unwrap_or(0)
        .saturating_mul(PER_POWER)
        .saturating_add(part)
        .min(CLASSES.saturating_sub(1))
}

/// A budget of a fixed number of bytes, recycling the buffers it hands out: the accountant the
/// tests, the benchmarks and a single process use. Reservations past `capacity` are refused.
///
/// A reservation takes the spare buffer last given back of its size's class if that holds it, else
/// one of the first later class holding any, else the largest class's, grown: constant time
/// whatever the spares number. mantle: it searched every spare buffer for the smallest that held
/// the grant, and a node keeps one for each peer and client it may serve, so a range leader's first
/// shard spent 6.7% of itself in the search (2026-11-19, vendor/UPSTREAM.md).
#[derive(Debug)]
pub struct Fixed {
    capacity: u64,
    used: u64,
    /// The spare buffers, each in a slot: at most `max_spare` slots.
    slots: Vec<Vec<u8>>,
    /// Each slot's next: in its class's list while it holds a buffer, in the free slots' after.
    next: Vec<u32>,
    /// Each class's last given back, [`CLASSES`] of them.
    heads: Vec<u32>,
    /// Which classes hold a buffer.
    occupied: [u64; WORDS],
    /// The first slot holding no buffer.
    free: u32,
    /// The spare buffers held.
    spare: usize,
    max_spare: usize,
    refused: u64,
}

impl Fixed {
    /// A budget of `capacity` bytes that keeps at most `max_spare` buffers for reuse.
    pub fn new(capacity: u64, max_spare: usize) -> Self {
        Self {
            capacity,
            used: 0,
            slots: Vec::new(),
            next: Vec::new(),
            heads: vec![NONE; CLASSES],
            occupied: [0; WORDS],
            free: NONE,
            spare: 0,
            max_spare,
            refused: 0,
        }
    }
    /// The bytes reserved and not yet given back.
    pub fn used(&self) -> u64 {
        self.used
    }
    /// The reservations refused so far.
    pub fn refused(&self) -> u64 {
        self.refused
    }

    fn mark(&mut self, class: usize, held: bool) {
        let bit = 1u64
            .checked_shl(u32::try_from(class % PER_POWER).unwrap_or(0))
            .unwrap_or(0);
        if let Some(word) = self.occupied.get_mut(class / PER_POWER) {
            if held {
                *word |= bit;
            } else {
                *word &= !bit;
            }
        }
    }

    /// The first class from `from` on that holds a buffer.
    fn first_from(&self, from: usize) -> Option<usize> {
        let mut mask = u64::MAX
            .checked_shl(u32::try_from(from % PER_POWER).unwrap_or(0))
            .unwrap_or(0);
        for word in from / PER_POWER..WORDS {
            let bits = self.occupied.get(word)? & mask;
            if bits != 0 {
                let bit = usize::try_from(bits.trailing_zeros()).ok()?;
                return word.checked_mul(PER_POWER)?.checked_add(bit);
            }
            mask = u64::MAX;
        }
        None
    }

    /// The last class up to `to` that holds a buffer.
    fn last_to(&self, to: usize) -> Option<usize> {
        let top = to.min(CLASSES.saturating_sub(1));
        let highest = u64::BITS.saturating_sub(1);
        let below = highest.saturating_sub(u32::try_from(top % PER_POWER).unwrap_or(highest));
        let mut mask = u64::MAX.checked_shr(below).unwrap_or(0);
        for word in (0..=top / PER_POWER).rev() {
            let bits = self.occupied.get(word)? & mask;
            if bits != 0 {
                let bit = usize::try_from(highest.saturating_sub(bits.leading_zeros())).ok()?;
                return word.checked_mul(PER_POWER)?.checked_add(bit);
            }
            mask = u64::MAX;
        }
        None
    }

    /// Whether the buffer class `class` gives next holds `wanted` bytes.
    fn head_holds(&self, class: usize, wanted: usize) -> bool {
        self.heads
            .get(class)
            .and_then(|slot| usize::try_from(*slot).ok())
            .and_then(|slot| self.slots.get(slot))
            .is_some_and(|buffer| buffer.capacity() >= wanted)
    }

    /// The buffer class `class` gives next, out of its list.
    fn take(&mut self, class: usize) -> Option<Vec<u8>> {
        let slot = *self.heads.get(class)?;
        let at = usize::try_from(slot).ok()?;
        let buffer = std::mem::take(self.slots.get_mut(at)?);
        let next = std::mem::replace(self.next.get_mut(at)?, self.free);
        self.free = slot;
        if let Some(head) = self.heads.get_mut(class) {
            *head = next;
        }
        if next == NONE {
            self.mark(class, false);
        }
        self.spare = self.spare.saturating_sub(1);
        Some(buffer)
    }

    /// Keeps `buffer` for reuse, first in its class's list, if fewer than `max_spare` are kept.
    fn keep(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() == 0 || self.spare >= self.max_spare {
            return;
        }
        let class = class(buffer.capacity());
        let Some(head) = self.heads.get(class).copied() else {
            return;
        };
        let slot = if self.free == NONE {
            let Some(slot) = u32::try_from(self.slots.len())
                .ok()
                .filter(|slot| *slot != NONE)
            else {
                return;
            };
            if self.slots.try_reserve(1).is_err() || self.next.try_reserve(1).is_err() {
                return;
            }
            self.slots.push(Vec::new());
            self.next.push(NONE);
            slot
        } else {
            self.free
        };
        let Some(at) = usize::try_from(slot).ok() else {
            return;
        };
        let (Some(place), Some(link)) = (self.slots.get_mut(at), self.next.get_mut(at)) else {
            return;
        };
        if slot == self.free {
            self.free = *link;
        }
        *place = buffer;
        *link = head;
        if let Some(first) = self.heads.get_mut(class) {
            *first = slot;
        }
        self.mark(class, true);
        self.spare = self.spare.saturating_add(1);
    }
}

impl<K> Budget<K> for Fixed {
    fn reserve(&mut self, bytes: u64, lane: Lane<K>) -> Result<Reservation, Refusal> {
        let used = self
            .used
            .checked_add(bytes)
            .filter(|used| *used <= self.capacity);
        let wanted = usize::try_from(bytes).ok();
        let (Some(used), Some(wanted)) = (used, wanted) else {
            self.refused = self.refused.saturating_add(1);
            return Err(Refusal::Budget);
        };
        self.used = used;
        let buffer = match lane {
            Lane::Window => Vec::new(),
            Lane::Class(_) => {
                // The last given back of its class if it holds the grant, else one of the first
                // later class, which does, else the largest class's: a buffer that already holds
                // the grant is never grown.
                let at = class(wanted);
                let from = if self.head_holds(at, wanted) {
                    Some(at)
                } else {
                    self.first_from(at.saturating_add(1))
                        .or_else(|| self.last_to(at))
                };
                let mut buffer = from.and_then(|from| self.take(from)).unwrap_or_default();
                buffer.clear();
                buffer.reserve_exact(wanted);
                buffer
            }
        };
        Ok(Reservation::new(bytes, buffer))
    }

    fn release(&mut self, reservation: Reservation) {
        self.used = self.used.saturating_sub(reservation.granted);
        self.keep(reservation.into_buffer());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fixed_budget_refuses_past_its_capacity_and_recycles_buffers() {
        let mut budget = Fixed::new(100, 1);
        let first = Budget::<()>::reserve(&mut budget, 60, Lane::Class(())).unwrap();
        assert_eq!(
            Budget::<()>::reserve(&mut budget, 41, Lane::Class(())).err(),
            Some(Refusal::Budget)
        );
        assert_eq!((budget.used(), budget.refused()), (60, 1));
        let pointer = first.bytes().as_ptr();
        Budget::<()>::release(&mut budget, first);
        assert_eq!(budget.used(), 0);
        let second = Budget::<()>::reserve(&mut budget, 40, Lane::Class(())).unwrap();
        assert_eq!(second.bytes().as_ptr(), pointer, "the buffer was reused");
        let window = Budget::<()>::reserve(&mut budget, 60, Lane::Window).unwrap();
        assert_eq!((window.granted(), window.room()), (60, 60));
    }

    /// A reservation takes a spare buffer that holds it without growing one, and of the smallest
    /// class that holds it: the spares of other classes are not visited.
    #[test]
    fn a_reservation_takes_a_spare_of_the_first_class_that_holds_it() {
        let mut budget = Fixed::new(1 << 30, 8);
        let reserve = |budget: &mut Fixed, bytes: u64| {
            Budget::<()>::reserve(budget, bytes, Lane::Class(())).unwrap()
        };
        let made: Vec<Reservation> = [64, 4_096, 65_536]
            .into_iter()
            .map(|bytes| reserve(&mut budget, bytes))
            .collect();
        let pointers: Vec<*const u8> = made.iter().map(|each| each.bytes().as_ptr()).collect();
        for each in made {
            Budget::<()>::release(&mut budget, each);
        }
        let head = reserve(&mut budget, 1_000);
        assert_eq!(head.bytes().as_ptr(), pointers[1], "the 4,096-byte spare");
        let piece = reserve(&mut budget, 65_536);
        assert_eq!(piece.bytes().as_ptr(), pointers[2], "the 65,536-byte spare");
        let small = reserve(&mut budget, 64);
        assert_eq!(small.bytes().as_ptr(), pointers[0], "the 64-byte spare");
        Budget::<()>::release(&mut budget, small);
        // None holds 70,000: the largest is taken and grown.
        let grown = reserve(&mut budget, 70_000);
        let grown = grown.into_buffer();
        assert!(grown.capacity() >= 70_000);
        assert_eq!(budget.spare, 0);
    }

    /// Whatever the sizes, every buffer handed out holds its grant, the bytes reserved are counted
    /// exactly, and no more than `max_spare` buffers are kept.
    #[test]
    fn spares_are_bounded_and_every_buffer_holds_its_grant() {
        let mut budget = Fixed::new(1 << 40, 16);
        let mut held = Vec::new();
        let mut size = 1u64;
        for round in 0..2_000u64 {
            size = size
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let bytes = (size >> 33) % (1 << (round % 20 + 1));
            let reservation = Budget::<()>::reserve(&mut budget, bytes, Lane::Class(())).unwrap();
            assert_eq!(reservation.room(), bytes);
            let buffer = reservation.into_buffer();
            assert!(
                buffer.capacity() as u64 >= bytes,
                "{bytes} in {}",
                buffer.capacity()
            );
            held.push(Reservation::new(bytes, buffer));
            if round % 3 == 0 {
                for each in held.drain(..) {
                    Budget::<()>::release(&mut budget, each);
                }
            }
            let kept = budget
                .slots
                .iter()
                .filter(|slot| slot.capacity() > 0)
                .count();
            assert_eq!(kept, budget.spare);
            assert!(budget.spare <= 16 && budget.slots.len() <= 16);
        }
        let total: u64 = held.iter().map(Reservation::granted).sum();
        assert_eq!(budget.used(), total);
    }

    #[test]
    fn a_tag_comes_back_with_its_reservation_and_survives_the_fill() {
        let mut tagged = Reservation::tagged(8, vec![9; 4], 3);
        assert_eq!(
            (tagged.tag(), tagged.granted(), tagged.bytes()),
            (3, 8, &[][..])
        );
        tagged.fill(&[1, 2]);
        tagged.clear();
        assert_eq!((tagged.tag(), tagged.room()), (3, 8));
        assert_eq!(Reservation::new(8, Vec::new()).tag(), 0);
    }
}
