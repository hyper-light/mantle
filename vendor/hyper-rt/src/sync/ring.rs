//! A bounded ring of values with no lock on any path, and the block a primitive's ends share it through
//! (docs/runtime.md §8; mantle `docs/design/event-loop.md` D4). The synchronization primitives carried their
//! values through `std::sync::mpsc::sync_channel`, whose blocking receive and send register in a waiter
//! list behind a pthread mutex (std `sync/mpmc/waker.rs`, `SyncWaker`): mantle's fill profile caught threads
//! in `__psynch_mutexwait` under `SyncWaker::register` while senders held it to notify
//! (`benchmark-results/mantle-boxed-request-ab-20261010/fill5m-sample.txt`). Here the ring is only atomics
//! and the slots' values, and waiting is the cell's waiter word (`crate::handoff`), so neither end ever
//! waits for the other to leave a critical section.
//!
//! **The ring** is Vyukov's bounded queue [C: Dmitry Vyukov, "Bounded MPMC queue", 1024cores.net]: each
//! slot carries a stamp, the position it is free for (`position`) or full at (`position + 1`); a producer
//! claims a position by compare-and-swap on the tail and publishes by storing the stamp, a consumer claims by
//! compare-and-swap on the head and releases the slot for the next lap by storing `position + lap`. A
//! position is a lap above an index (`lap | index`), one lap the capacity rounded up to a power of two and at
//! least two, as Rust's std encodes its array channel (`sync/mpmc/array.rs`), so the capacity is exact and
//! "full at this lap" never reads as "free for the next" (slates' word ring refused a capacity of one for
//! exactly that collision, AUD-29-33).
//!
//! **Closing.** A ring closes to producers by a mark bit in its tail, between the index and the lap, as std's
//! array channel marks a disconnect (`array.rs`, `mark_bit`): the compare-and-swap that claims a position
//! fails on a marked tail, so whether a push got in before the close is decided by that one instruction. A
//! channel whose receiver had set a flag and then drained let a sender that had read the flag clear take the
//! room the drain made, and its send succeeded into a channel no one would read (mantle's suite caught it,
//! `tests/sync.rs`, at round 2380). A push that claimed its position before the close may publish after the
//! closer's drain has looked; std's drain spins until it does, and here the producer drops what the drain
//! missed instead ([`Ring::drain_if_closed`], after a `SeqCst` fence that follows its push, as the closer
//! fences between its close and its drain).
//!
//! **No spin.** A slot the consumer has claimed but not yet released reads as full, and a position a
//! producer has claimed but not yet published reads as empty: the call returns at once and the waiter
//! protocol carries progress (the consumer grants room after it releases; a producer wakes the consumer
//! after it publishes). Neither end waits on a peer that the scheduler has preempted mid-operation, which a
//! spin on those two states would do. A compare-and-swap retries only after another end's claim of the same
//! position succeeded, and a view of the tail or the head left behind advances past positions others
//! completed or reads the latest value by a read-modify-write once, so every retry follows another end's
//! progress (lock-free). A view that still makes no sense after that read, a slot's stamp laps older than the
//! latest tail or head, is reported full or empty rather than read again until it changes: nothing bounds how
//! long a plain load may return an old value (loom explores exactly that, and a loop on it never ended), while
//! the waiting protocols fence before their second look, which rules such a stamp out.
//!
//! **The shared block** ([`Shared`]) is the state a primitive's ends share, freed by the last end: its count
//! is the primitive's cell's handle count (`crate::sync::cell`), so the block lives exactly as long as the
//! cell's handles, the existing ownership of every primitive here, and nothing is reference counted twice.
#![allow(unsafe_code)]

use std::mem::MaybeUninit;
use std::ptr::NonNull;

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

use super::cell::CellRef;
use crate::error::RtError;

#[cfg(loom)]
use loom::cell::UnsafeCell;

/// std's cell behind loom's interface, so the ring's code is one text under both.
#[cfg(not(loom))]
#[derive(Debug)]
struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

/// Shape: keeps the head and the tail on separate cache lines (the largest line we target, Apple
/// silicon's 128 bytes): producers write the tail, the consumer the head.
#[repr(align(128))]
#[derive(Debug)]
struct Padded<T>(T);

/// One slot: the position it is free for or full at, and its value while full.
#[derive(Debug)]
struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded ring of `T` for any number of producers and consumers (the primitives here have one consumer).
#[derive(Debug)]
pub(crate) struct Ring<T> {
    head: Padded<AtomicUsize>,
    tail: Padded<AtomicUsize>,
    /// The tail's bit once the ring is closed to producers: the capacity rounded up to a power of two, at
    /// least two, so it sits above every index.
    mark: usize,
    /// One lap of positions: twice the mark, so a lap's bits sit above it.
    one_lap: usize,
    slots: Box<[Slot<T>]>,
}

/// Why a push handed its value back.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refused<T> {
    /// The ring is full (or its slot is still being taken from a lap ago).
    Full(T),
    /// The ring is closed to producers.
    Closed(T),
}

impl<T> Refused<T> {
    /// The value handed back.
    pub(crate) fn into_value(self) -> T {
        match self {
            Self::Full(value) | Self::Closed(value) => value,
        }
    }
}

// SAFETY: a value moves into a slot on one thread and out of it on another, exactly once each, ordered by the
// slot's stamp (Release by the writer, Acquire by the reader); a `T` that may move between threads makes the
// ring safe to move and to share. No `&T` is ever handed out, so `T: Sync` is not required.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: as for `Send`: every access to a value is the one claim of its position, so shared references to
// the ring never alias a value.
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    /// A ring holding at most `capacity` values. Refused `BadConfig` for no capacity or one whose positions
    /// do not fit a word beside a lap.
    pub(crate) fn new(capacity: usize) -> Result<Self, RtError> {
        let refused = RtError::BadConfig {
            what: "a ring of no capacity, or one too large for its positions",
        };
        if capacity == 0 {
            return Err(refused);
        }
        let mark = capacity
            .checked_add(1)
            .and_then(usize::checked_next_power_of_two)
            .filter(|mark| mark.checked_mul(4).is_some())
            .ok_or(refused.clone())?;
        let one_lap = mark.checked_mul(2).ok_or(refused.clone())?;
        // The slots' allocation must be a valid layout, or collecting them would abort on the overflow.
        std::alloc::Layout::array::<Slot<T>>(capacity).map_err(|_| refused)?;
        let slots = (0..capacity)
            .map(|position| Slot {
                stamp: AtomicUsize::new(position),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Ok(Self {
            head: Padded(AtomicUsize::new(0)),
            tail: Padded(AtomicUsize::new(0)),
            mark,
            one_lap,
            slots,
        })
    }

    /// The position after `position`: the next index of its lap, or the first of the next lap.
    fn next(&self, position: usize) -> usize {
        let index = position & self.mark.wrapping_sub(1);
        if index.saturating_add(1) < self.slots.len() {
            position.wrapping_add(1)
        } else {
            (position & !self.one_lap.wrapping_sub(1)).wrapping_add(self.one_lap)
        }
    }

    /// The slot of `position`.
    fn slot(&self, position: usize) -> Option<&Slot<T>> {
        self.slots.get(position & self.mark.wrapping_sub(1))
    }

    /// Adds `value` at the tail; hands it back `Full` when the ring is full, including while the slot the
    /// tail reaches is still being taken from a lap ago, and `Closed` once the ring is closed.
    pub(crate) fn push(&self, value: T) -> Result<(), Refused<T>> {
        let mut tail = self.tail.0.load(Ordering::Relaxed);
        let mut reread = false;
        loop {
            if tail & self.mark != 0 {
                return Err(Refused::Closed(value));
            }
            let Some(slot) = self.slot(tail) else {
                return Err(Refused::Full(value));
            };
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == tail {
                match self.tail.0.compare_exchange(
                    tail,
                    self.next(tail),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: the claim of `tail` is this call's alone (the compare-and-swap), and the stamp
                        // it read (Acquire) says the last lap's value was taken and released, so no one reads or
                        // writes this slot until the stamp below publishes it.
                        slot.value
                            .with_mut(|cell| unsafe { cell.write(MaybeUninit::new(value)) });
                        slot.stamp.store(tail.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(seen) => tail = seen,
                }
            } else if stamp.wrapping_add(self.one_lap) == tail
                || stamp.wrapping_add(self.one_lap) == tail.wrapping_add(1)
            {
                // The slot still holds the last lap's position, claimed and not yet published or published and
                // not yet taken: the ring is full.
                return Err(Refused::Full(value));
            } else if stamp == tail.wrapping_add(1) {
                // Another producer claimed and published `tail`: the tail is past it.
                tail = self.next(tail);
            } else if !reread {
                // This view of the tail, or of the slot, is a lap or more behind: read the latest tail once (a
                // read-modify-write reads the last value in the tail's order, where a plain load may keep
                // returning an old one).
                reread = true;
                tail = self.tail.0.fetch_add(0, Ordering::Relaxed);
            } else {
                // Still a lap apart with the latest tail: the slot's stamp read is the old one. Reported full
                // rather than read again until it changes, which nothing bounds; a sender that must wait fences
                // before it looks again (`sync::room`), and no stamp that old survives the fence.
                return Err(Refused::Full(value));
            }
        }
    }

    /// Takes the value at the head, if one is published there.
    pub(crate) fn pop(&self) -> Option<T> {
        let mut head = self.head.0.load(Ordering::Relaxed);
        let mut reread = false;
        loop {
            let slot = self.slot(head)?;
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == head.wrapping_add(1) {
                match self.head.0.compare_exchange(
                    head,
                    self.next(head),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: the stamp read (Acquire) says a producer wrote this slot and published it at
                        // `head`, and the claim of `head` is this call's alone, so the value is initialized and
                        // read once; the store below releases the slot for the next lap only after the read.
                        let value = slot.value.with(|cell| unsafe { cell.read().assume_init() });
                        slot.stamp
                            .store(head.wrapping_add(self.one_lap), Ordering::Release);
                        return Some(value);
                    }
                    Err(seen) => head = seen,
                }
            } else if stamp == head {
                return None;
            } else if stamp == head.wrapping_add(self.one_lap) {
                // Another consumer took and released `head`: the head is past it.
                head = self.next(head);
            } else if !reread {
                // This view of the head, or of the slot, is a lap or more behind: read the latest head once
                // (as for the tail).
                reread = true;
                head = self.head.0.fetch_add(0, Ordering::Relaxed);
            } else {
                // The slot's stamp read is the old one: reported empty (as for a push); a receiver that must
                // wait fences before it looks again (`crate::handoff`).
                return None;
            }
        }
    }

    /// Closes the ring to producers: every push from now on is refused `Closed` (the mark is in the word their
    /// claims compare-and-swap). The caller fences (`SeqCst`) before it drains, so that a push it does not see
    /// published sees the mark ([`Ring::drain_if_closed`]).
    pub(crate) fn close(&self) {
        self.tail.0.fetch_or(self.mark, Ordering::AcqRel);
    }

    /// Drops the values published at the head, up to the first position not yet published.
    pub(crate) fn drain(&self) {
        while self.pop().is_some() {}
    }

    /// For a producer whose push succeeded, after a `SeqCst` fence that follows the push: drops what the ring
    /// holds if it has been closed. The closer fences between its close and its drain, so of the two fences the
    /// later side's read sees the earlier side's write: the closer's drain sees this push published, or this
    /// read sees the mark. A drain stops at the first position not yet published, whose producer then drains
    /// on from it.
    pub(crate) fn drain_if_closed(&self) {
        if self.tail.0.load(Ordering::Relaxed) & self.mark != 0 {
            self.drain();
        }
    }
}

impl<T> Drop for Ring<T> {
    /// Drops the values still in the ring: with `&mut self` no end can reach it.
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

/// A block a primitive's ends share, freed by the last of them: the count is the primitive's cell's handle
/// count, so an end holds the cell and the block together and gives both back at once, when it drops (after
/// the end's own `Drop`, which may still read the block and wake through the cell).
#[derive(Debug)]
pub(crate) struct Shared<S> {
    block: NonNull<S>,
    cell: CellRef,
}

// SAFETY: an end reaches the block only through `&S`, and the block is freed only by the end whose release
// was the cell's last handle, after every other end has given its handle back; so a `Shared` may move to and
// be used from any thread when `S` may be shared between threads.
unsafe impl<S: Send + Sync> Send for Shared<S> {}
// SAFETY: as for `Send`.
unsafe impl<S: Send + Sync> Sync for Shared<S> {}

impl<S> Shared<S> {
    /// The first end of `state`, holding the one handle `cell` was claimed with.
    pub(crate) fn new(state: S, cell: CellRef) -> Self {
        Self {
            block: NonNull::from(Box::leak(Box::new(state))),
            cell,
        }
    }

    /// Another end of the same block, holding a handle of the cell it takes now.
    pub(crate) fn share(&self) -> Self {
        self.cell.retain();
        Self {
            block: self.block,
            cell: self.cell,
        }
    }

    /// The shared state.
    pub(crate) fn get(&self) -> &S {
        // SAFETY: this end holds one of the cell's handles, so the block is not freed (only the release of the
        // last handle frees it, below) for as long as `self` lives.
        unsafe { self.block.as_ref() }
    }

    /// The primitive's cell.
    pub(crate) fn cell(&self) -> CellRef {
        self.cell
    }
}

impl<S> Drop for Shared<S> {
    /// Gives this end's handle back; the last frees the block.
    fn drop(&mut self) {
        if self.cell.release_last() {
            // SAFETY: the block came from `Box::leak` in `new`, and this was the cell's last handle: every
            // other end has dropped, none can reach the block again, and this is its one free.
            drop(unsafe { Box::from_raw(self.block.as_ptr()) });
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn a_ring_is_fifo_bounded_exactly_and_reuses_its_slots_across_laps() {
        for capacity in [1usize, 2, 3, 4, 7] {
            let ring = Ring::new(capacity).unwrap();
            assert_eq!(ring.pop(), None);
            for lap in 0..5u64 {
                for value in 0..capacity {
                    ring.push((lap, value)).unwrap();
                }
                assert_eq!(
                    ring.push((lap, capacity)),
                    Err(Refused::Full((lap, capacity))),
                    "full at {capacity}"
                );
                for value in 0..capacity {
                    assert_eq!(ring.pop(), Some((lap, value)));
                }
                assert_eq!(ring.pop(), None);
            }
        }
        assert!(Ring::<u8>::new(0).is_err());
        assert!(Ring::<u8>::new(usize::MAX).is_err());
    }

    /// Drops counted in a test's own static, so a dropped value's count is exact.
    static DROPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// A value that counts its drop.
    struct Counted;

    impl Drop for Counted {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Values left in a ring are dropped with it, each once.
    #[test]
    fn a_dropped_ring_drops_its_values() {
        let before = DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        let ring = Ring::new(3).unwrap();
        ring.push(Counted).ok().unwrap();
        ring.push(Counted).ok().unwrap();
        assert_eq!(DROPPED.load(std::sync::atomic::Ordering::Relaxed), before);
        drop(ring);
        assert_eq!(
            DROPPED.load(std::sync::atomic::Ordering::Relaxed),
            before + 2
        );
    }

    /// A closed ring refuses every push, room or not, at every capacity and lap; what it held is dropped by a
    /// drain, and a producer's `drain_if_closed` drops it too, while an open ring's leaves it.
    #[test]
    fn a_closed_ring_refuses_every_push_and_its_drains_drop_what_it_held() {
        for capacity in [1usize, 2, 3, 4, 7] {
            for laps in 0..3usize {
                let ring = Ring::new(capacity).unwrap();
                for _ in 0..laps * capacity + 1 {
                    ring.push(0u8).unwrap();
                    assert_eq!(ring.pop(), Some(0));
                }
                ring.push(1u8).unwrap();
                ring.drain_if_closed();
                assert_eq!(ring.pop(), Some(1), "open: drain_if_closed left it");
                ring.push(2u8).unwrap();
                ring.close();
                assert_eq!(ring.push(3u8), Err(Refused::Closed(3)), "room, but closed");
                ring.drain_if_closed();
                assert_eq!(ring.pop(), None, "closed: drain_if_closed dropped it");
                assert_eq!(ring.push(4u8), Err(Refused::Closed(4)));
            }
        }
        let before = DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        let ring = Ring::new(2).unwrap();
        ring.push(Counted).ok().unwrap();
        ring.push(Counted).ok().unwrap();
        ring.close();
        ring.drain();
        assert_eq!(
            DROPPED.load(std::sync::atomic::Ordering::Relaxed),
            before + 2,
            "a drain drops each value once"
        );
        assert!(matches!(ring.push(Counted), Err(Refused::Closed(_))));
    }

    /// Many producers and one consumer: every value arrives once, in each producer's own order.
    #[test]
    fn concurrent_producers_deliver_every_value_once_in_their_order() {
        const PRODUCERS: u64 = 4;
        const EACH: u64 = 20_000;
        let ring = Ring::new(8).unwrap();
        let mut last = [None::<u64>; PRODUCERS as usize];
        let mut received = 0u64;
        std::thread::scope(|scope| {
            for producer in 0..PRODUCERS {
                let ring = &ring;
                scope.spawn(move || {
                    for sequence in 0..EACH {
                        let mut value = (producer, sequence);
                        while let Err(back) = ring.push(value) {
                            value = back.into_value();
                            std::hint::spin_loop();
                        }
                    }
                });
            }
            while received < PRODUCERS * EACH {
                if let Some((producer, sequence)) = ring.pop() {
                    let seen = &mut last[producer as usize];
                    assert!(seen.is_none_or(|previous| previous + 1 == sequence));
                    *seen = Some(sequence);
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        assert_eq!(ring.pop(), None);
    }
}

#[cfg(test)]
#[cfg(loom)]
mod loom_tests {
    use super::*;
    use crate::mem::loom_bounds;

    /// Two producers each push once and a consumer pops once, all three spawned, on a ring of one slot; then the
    /// main thread, having joined them, drains what is left: every accepted value comes out once and only
    /// accepted values come out. The pushes race for one position, a push meets the slot published or mid-take,
    /// and a take lets the next lap's push in. The consumer is a thread of its own, not the main thread before
    /// its joins: loom's reduction remembers only an object's last access, so a pop the main thread made before
    /// the producers ever ran would be overwritten by a producer's own load and never reordered after a push
    /// (the first draft of this model never popped a published value in any interleaving, which the mutations
    /// recorded in `benchmark-results/hyper-rt-vs-tokio-20261010/tests/loom-channel.txt` showed). Loom's cell
    /// reports a value read that the stamp did not order after its write, and a write over a value still being
    /// read. No step retries, so the model has no spin of its own.
    #[test]
    fn two_producers_and_a_consumer_move_each_value_once_through_one_slot() {
        loom_bounds::explore("ring: two producers, one consumer, one slot", || {
            let ring: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
            let producers: Vec<_> = (0..2u32)
                .map(|value| loom::thread::spawn(move || ring.push(value).is_ok()))
                .collect();
            let consumer = loom::thread::spawn(move || ring.pop());
            let accepted: Vec<u32> = producers
                .into_iter()
                .zip(0..2u32)
                .filter_map(|(producer, value)| producer.join().unwrap().then_some(value))
                .collect();
            let mut out: Vec<u32> = consumer.join().unwrap().into_iter().collect();
            while let Some(value) = ring.pop() {
                out.push(value);
            }
            out.sort_unstable();
            assert_eq!(out, accepted, "out {out:?}, accepted {accepted:?}");
            assert!(!accepted.is_empty(), "an empty slot refused both pushes");
        });
    }

    /// Counts the drops of the values a model makes, in loom's own atomic.
    struct Counted(&'static loom::sync::atomic::AtomicUsize);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A producer's push and a closer's close and drain, as a channel's send (its wake fences, then it looks
    /// for the close) and its receiver's drop make them.
    fn race_a_push_and_a_close(ring: &'static Ring<Counted>, value: Counted) -> bool {
        let producer = loom::thread::spawn(move || match ring.push(value) {
            Ok(()) => {
                loom::sync::atomic::fence(Ordering::SeqCst);
                ring.drain_if_closed();
                true
            }
            Err(_) => false,
        });
        let closer = loom::thread::spawn(move || {
            ring.close();
            loom::sync::atomic::fence(Ordering::SeqCst);
            ring.drain();
        });
        let pushed = producer.join().unwrap();
        closer.join().unwrap();
        pushed
    }

    /// A push races the close of a ring whose one slot is full: it never gets in, since the only room is what
    /// the close's drain makes, after the close; and both values are dropped once. With the close a flag the
    /// push did not compare-and-swap, the push took the drained slot (the channel's send that succeeded into a
    /// dropped receiver, `tests/sync.rs` round 2380).
    #[test]
    fn a_push_never_takes_the_room_a_closing_drain_makes() {
        loom_bounds::explore(
            "ring: a push against a close and drain, the slot full",
            || {
                let drops: &'static loom::sync::atomic::AtomicUsize =
                    Box::leak(Box::new(loom::sync::atomic::AtomicUsize::new(0)));
                let ring: &'static Ring<Counted> = Box::leak(Box::new(Ring::new(1).unwrap()));
                assert!(ring.push(Counted(drops)).is_ok());
                assert!(
                    !race_a_push_and_a_close(ring, Counted(drops)),
                    "a push got in after the close"
                );
                assert_eq!(drops.load(Ordering::Relaxed), 2);
            },
        );
    }

    /// A push races the close of an empty ring: whether it got in or was refused, its value is dropped by the
    /// time both are done, by the closer's drain or, when it published after the drain looked, by its own.
    #[test]
    fn a_value_pushed_before_a_close_is_dropped_by_one_drain_or_the_other() {
        loom_bounds::explore(
            "ring: a push against a close and drain, the slot empty",
            || {
                let drops: &'static loom::sync::atomic::AtomicUsize =
                    Box::leak(Box::new(loom::sync::atomic::AtomicUsize::new(0)));
                let ring: &'static Ring<Counted> = Box::leak(Box::new(Ring::new(1).unwrap()));
                race_a_push_and_a_close(ring, Counted(drops));
                assert_eq!(
                    drops.load(Ordering::Relaxed),
                    1,
                    "the value was stranded or dropped twice"
                );
            },
        );
    }
}
