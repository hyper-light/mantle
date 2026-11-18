//! The local run queue: a ring of ready slot indices with one pending flag per slot, so a task woken
//! many times between polls appears once and the ring never holds more than the arena's slots (slates'
//! `queue.rs`; §4.3 "intrusive run queue"; no allocation on the wake path).
//!
//! Single-threaded by construction: only the owning shard's thread pushes and pops. The rings are of
//! `Cell`s (docs/runtime.md §3.4), so nothing is borrowed and nothing is refused: slates' version kept its
//! lists in `RefCell`s and counted the re-entrant accesses it refused.
//!
//! A step takes at most its configured poll budget, oldest first. Wakes that arrive while the phase
//! runs join the same FIFO behind tasks already ready and may use its remaining budget. A task waking
//! itself cannot extend that budget, so the phase never drains an unbounded ready set. `batch` can
//! still capture the number ready now for callers that need a fixed snapshot.
//!
//! A task that wakes itself in its own poll (a yield, [`crate::futures::yield_now`], or any future with
//! more to do) is listed apart, and joins the FIFO only when the loop has next read its inputs: its
//! control, wakes from other threads, timers, pollers and the driver's completions go ahead of it.
//! Re-listed in the FIFO at once, such a task took the rest of its step's budget, poll after poll, while
//! those inputs waited for the step to end (`tests/yield_defers.rs`; tokio defers its `yield_now` past
//! its driver's poll the same way, tokio 1.23 #5223). A slot is in one list at most (its flag), and each
//! list holds one entry per slot.
//!
//! Each task has a class ([`Class`]: Critical, Normal, the default, or Background), and each class its own pair
//! of lists, made as a task of the shard first takes it. Each class with tasks listed gets an equal share of the
//! shard's time, and the most urgent goes first while it has run no more than the shard's quantum past the
//! least-run class with work ([`LocalQueue::pop_within`]): Kiln's `kiln-rt` `queue.rs` (its goal 6, plan
//! 01a1a59c2abe step 3, after Seastar's scheduling groups and Google's criticalities), taken onto this queue's
//! rings. A shard whose tasks take no class other than Normal makes none of it and runs Normal's lists alone.

use std::cell::{Cell, OnceCell};

use crate::coop::Class;

/// A first-in-first-out ring of slot numbers, one entry per slot at most: [`crate::cells::CellRing`]'s
/// shape with the numbers held bare, since a slot is read only between the head and the length, so no entry
/// needs a mark of its own.
struct SlotRing {
    slots: Box<[Cell<u32>]>,
    /// The index of the oldest number.
    head: Cell<usize>,
    /// Numbers held.
    len: Cell<usize>,
}

impl SlotRing {
    /// A ring that holds at most `capacity` numbers.
    fn new(capacity: usize) -> Self {
        Self {
            slots: (0..capacity).map(|_| Cell::new(0)).collect(),
            head: Cell::new(0),
            len: Cell::new(0),
        }
    }

    fn len(&self) -> usize {
        self.len.get()
    }

    fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    /// Appends `slot`, or hands it back when the ring is full.
    #[inline]
    fn push(&self, slot: u32) -> Result<(), u32> {
        let len = self.len.get();
        let capacity = self.slots.len();
        if len >= capacity {
            return Err(slot);
        }
        // `head` and `len` are each below the capacity, so their sum is below twice it: one subtraction wraps it.
        let end = self.head.get().wrapping_add(len);
        let at = if end >= capacity {
            end.wrapping_sub(capacity)
        } else {
            end
        };
        let Some(cell) = self.slots.get(at) else {
            return Err(slot);
        };
        cell.set(slot);
        self.len.set(len.wrapping_add(1));
        Ok(())
    }

    /// Takes the oldest number.
    #[inline]
    fn pop(&self) -> Option<u32> {
        let len = self.len.get();
        if len == 0 {
            return None;
        }
        let head = self.head.get();
        let slot = self.slots.get(head)?.get();
        // `head` is below the capacity, so the next index is at most the capacity: it wraps by a comparison.
        let next = head.wrapping_add(1);
        self.head
            .set(if next >= self.slots.len() { 0 } else { next });
        self.len.set(len.wrapping_sub(1));
        Some(slot)
    }
}

/// A class's two lists, each holding one entry per slot: `lists[ready]` the ready FIFO, the other the slots of the
/// class that woke themselves since the loop last released them. A release onto an empty ready FIFO swaps the
/// lists' roles rather than moving each entry, the case of a round of yields whose batch drained the FIFO before
/// reading its inputs again.
struct Lists {
    lists: [SlotRing; 2],
    /// Which of `lists` is the ready FIFO: 0 or 1.
    ready: Cell<usize>,
}

impl Lists {
    fn new(capacity: usize) -> Self {
        Self {
            lists: [SlotRing::new(capacity), SlotRing::new(capacity)],
            ready: Cell::new(0),
        }
    }

    /// The ready FIFO.
    #[inline]
    fn ready_list(&self) -> &SlotRing {
        let [first, second] = &self.lists;
        if self.ready.get() == 0 { first } else { second }
    }

    /// The slots that woke themselves, waiting for their release.
    #[inline]
    fn yielded_list(&self) -> &SlotRing {
        let [first, second] = &self.lists;
        if self.ready.get() == 0 { second } else { first }
    }

    /// Whether the class has a slot ready or waiting for its release.
    fn holds(&self) -> bool {
        !self.ready_list().is_empty() || !self.yielded_list().is_empty()
    }

    fn len(&self) -> usize {
        self.ready_list()
            .len()
            .saturating_add(self.yielded_list().len())
    }
}

/// The classes, most urgent first: the order a pop prefers among classes that have run alike.
const CLASSES: [Class; 3] = [Class::Critical, Class::Normal, Class::Background];

/// What the queue keeps once a task of its shard first takes a class other than [`Class::Normal`]: each slot's
/// class, the other classes' lists, each made as its first slot takes it, and the time each class has run. A shard
/// whose tasks never take another class makes none of it and runs Normal's lists alone, as it did.
struct Kept {
    /// Each slot's class, as its task or its spawn last set it: the lists it joins at its next listing.
    class: Box<[Cell<Class>]>,
    critical: OnceCell<Lists>,
    background: OnceCell<Lists>,
    /// The poll time each class has run while it had tasks listed, nanoseconds, by `Class as usize`.
    ran_ns: [Cell<u64>; 3],
    /// The shard's virtual time: after each poll, the least time run of the class polled and the classes listed,
    /// never falling (the minimum virtual runtime a fair scheduler keeps, Linux's CFS `min_vruntime`). A class that
    /// starts listing is raised to it, or to the least of the classes listed ([`LocalQueue::activate`]).
    vtime_ns: Cell<u64>,
}

impl Kept {
    fn new(capacity: usize) -> Self {
        Self {
            class: (0..capacity).map(|_| Cell::new(Class::Normal)).collect(),
            critical: OnceCell::new(),
            background: OnceCell::new(),
            ran_ns: [Cell::new(0), Cell::new(0), Cell::new(0)],
            vtime_ns: Cell::new(0),
        }
    }

    fn ran(&self, class: Class) -> &Cell<u64> {
        let [background, normal, critical] = &self.ran_ns;
        match class {
            Class::Background => background,
            Class::Normal => normal,
            Class::Critical => critical,
        }
    }
}

/// The queue.
pub struct LocalQueue {
    pending: Box<[Cell<bool>]>,
    /// The Normal class's lists: the default, and every task's until it takes another.
    normal: Lists,
    /// The classes, made at the first task that takes one other than Normal.
    kept: OnceCell<Box<Kept>>,
    /// Slots `push` has listed, a count the loop reads twice to tell whether polls between listed any.
    listed: Cell<u64>,
    overflow: Cell<u64>,
}

impl std::fmt::Debug for LocalQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalQueue")
            .field("capacity", &self.pending.len())
            .field("listed", &self.len())
            .field("classes", &self.kept.get().is_some())
            .finish()
    }
}

/// `slot` as an index, `usize::MAX` (past any arena) where it does not fit.
fn index(slot: u32) -> usize {
    usize::try_from(slot).unwrap_or(usize::MAX)
}

impl LocalQueue {
    /// A queue for an arena of `capacity` slots.
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: (0..capacity).map(|_| Cell::new(false)).collect(),
            normal: Lists::new(capacity),
            kept: OnceCell::new(),
            listed: Cell::new(0),
            overflow: Cell::new(0),
        }
    }

    fn kept(&self) -> Option<&Kept> {
        self.kept.get().map(|kept| &**kept)
    }

    /// `class`'s own lists, where they are made: Normal's always, another class's once a slot took it.
    fn own_lists(&self, class: Class) -> Option<&Lists> {
        match class {
            Class::Normal => Some(&self.normal),
            Class::Critical => self.kept()?.critical.get(),
            Class::Background => self.kept()?.background.get(),
        }
    }

    /// The lists a slot of `class` joins, with the class whose they are: Normal's where `class`'s are not made.
    fn lists(&self, class: Class) -> (Class, &Lists) {
        match self.own_lists(class) {
            Some(lists) => (class, lists),
            None => (Class::Normal, &self.normal),
        }
    }

    /// Whether `class` has a slot listed or waiting for its release.
    fn holds(&self, class: Class) -> bool {
        self.own_lists(class).is_some_and(Lists::holds)
    }

    /// The class of the task in `slot`: Normal for a slot past the arena, and on a shard whose tasks took no other.
    pub fn class_of(&self, slot: u32) -> Class {
        self.kept()
            .and_then(|kept| kept.class.get(index(slot)))
            .map_or(Class::Normal, Cell::get)
    }

    /// Sets the class of the task in `slot`, from its next listing; the class it had. The first slot of a class
    /// other than Normal makes the queue's classes, and that class's lists; Normal on a shard with none made sets
    /// nothing, as a spawn's default does.
    pub fn set_class(&self, slot: u32, class: Class) -> Class {
        let capacity = self.pending.len();
        let kept = match (self.kept(), class) {
            (Some(kept), _) => kept,
            (None, Class::Normal) => return Class::Normal,
            (None, _) => self.kept.get_or_init(|| Box::new(Kept::new(capacity))),
        };
        let Some(cell) = kept.class.get(index(slot)) else {
            return Class::Normal;
        };
        match class {
            Class::Critical => {
                let _ = kept.critical.get_or_init(|| Lists::new(capacity));
            }
            Class::Background => {
                let _ = kept.background.get_or_init(|| Lists::new(capacity));
            }
            Class::Normal => {}
        }
        cell.replace(class)
    }

    /// The least time run of the classes `among` takes, `None` with none.
    fn least_run(kept: &Kept, among: impl Fn(Class) -> bool) -> Option<u64> {
        CLASSES
            .into_iter()
            .filter(|class| among(*class))
            .map(|class| kept.ran(class).get())
            .min()
    }

    /// A class with nothing listed starts listing: its time run is raised to the least of the classes with tasks
    /// listed, or with none listed to the shard's virtual time, so time it spent idle is no credit against them (a
    /// class's share is of the time it wants the shard, as a weighted fair queue's virtual time is). Without the
    /// virtual time, a class that ran alone kept all that time against the next class to list while it rested, and
    /// was shut out for as long once it listed again (Kiln's `queue.rs`).
    fn activate(&self, class: Class) {
        let Some(kept) = self.kept() else {
            return;
        };
        let least = Self::least_run(kept, |other| other != class && self.holds(other));
        // A listed class has run at least the virtual time (it was in the least that set it, or raised to it), so
        // this is the least listed's where one is listed.
        let floor = least.unwrap_or(0).max(kept.vtime_ns.get());
        let ran = kept.ran(class);
        ran.set(ran.get().max(floor));
    }

    /// Marks a slot ready; a slot already pending is not listed twice. A slot beyond the arena is counted as
    /// overflow and ignored (a stale or foreign word).
    #[inline]
    pub fn push(&self, slot: u32) {
        let Some(flag) = self.pending.get(index(slot)) else {
            self.overflow.set(self.overflow.get().saturating_add(1));
            return;
        };
        if flag.replace(true) {
            return;
        }
        let (class, lists) = self.lists(self.class_of(slot));
        let idle = !lists.holds();
        // A slot is in the ring at most once (its flag), and the ring holds one entry per slot, so it never
        // refuses; were it to, the flag is cleared so the next wake lists the slot again.
        if lists.ready_list().push(slot).is_err() {
            flag.set(false);
            self.overflow.set(self.overflow.get().saturating_add(1));
            return;
        }
        if idle {
            self.activate(class);
        }
        // Compared only with an earlier reading: 2^64 listings between two readings cannot happen.
        self.listed.set(self.listed.get().wrapping_add(1));
    }

    /// The slots `push` has listed so far: two readings differ when it listed any between them.
    pub fn listed(&self) -> u64 {
        self.listed.get()
    }

    /// Marks a slot ready behind the loop's next reading of its inputs: the task woke itself in its own
    /// poll. A slot already pending keeps its place; one beyond the arena is counted as overflow and
    /// ignored.
    #[inline]
    pub fn defer(&self, slot: u32) {
        let Some(flag) = self.pending.get(index(slot)) else {
            self.overflow.set(self.overflow.get().saturating_add(1));
            return;
        };
        if flag.replace(true) {
            return;
        }
        let (class, lists) = self.lists(self.class_of(slot));
        let idle = !lists.holds();
        // Never refused, as for `push`: one entry per slot, and the list holds one for every slot.
        if lists.yielded_list().push(slot).is_err() {
            flag.set(false);
            self.overflow.set(self.overflow.get().saturating_add(1));
            return;
        }
        if idle {
            self.activate(class);
        }
    }

    /// Lists every slot that woke itself behind the slots ready now in its class, oldest first: the loop calls it
    /// once its inputs are in the queue.
    pub fn release_yielded(&self) {
        for class in CLASSES {
            if let Some(lists) = self.own_lists(class) {
                self.release(lists);
            }
        }
    }

    /// [`LocalQueue::release_yielded`] for one class's lists.
    fn release(&self, lists: &Lists) {
        // Behind nothing: the lists swap roles, the released slots in the order they yielded.
        if lists.ready_list().is_empty() {
            lists.ready.set(if lists.ready.get() == 0 { 1 } else { 0 });
            return;
        }
        let (ready, yielded) = (lists.ready_list(), lists.yielded_list());
        while let Some(slot) = yielded.pop() {
            // Never refused: a slot is in one list at most, so both hold no more than the arena's slots.
            if ready.push(slot).is_err() {
                if let Some(flag) = self.pending.get(index(slot)) {
                    flag.set(false);
                }
                self.overflow.set(self.overflow.get().saturating_add(1));
            }
        }
    }

    /// Whether a slot that woke itself waits for its release.
    pub fn has_yielded(&self) -> bool {
        CLASSES.into_iter().any(|class| {
            self.own_lists(class)
                .is_some_and(|lists| !lists.yielded_list().is_empty())
        })
    }

    /// How many slots the step about to run may take: at most `limit`, and only those ready now, so a wake
    /// during the batch waits for the next step.
    pub fn batch(&self, limit: usize) -> usize {
        CLASSES
            .into_iter()
            .filter_map(|class| self.own_lists(class))
            .fold(0usize, |ready, lists| {
                ready.saturating_add(lists.ready_list().len())
            })
            .min(limit)
    }

    /// [`LocalQueue::pop_within`] with no lead: urgency only breaks ties between classes that have run alike.
    #[inline]
    pub fn pop(&self) -> Option<u32> {
        self.pop_within(0)
    }

    /// Takes the next ready slot, clearing its pending flag so a wake during its poll lists it again: the oldest of
    /// Normal's while no task took another class; else the oldest of the most urgent class with a slot ready that
    /// has run no more than `lead_ns` past the least-run class with a slot ready. The shard's loop gives its quantum
    /// as the lead, the most one task's turn holds the shard anyway (`crate::coop`), so the less urgent wait at most
    /// that much longer, and each class's lag is bounded by the lead and a poll: the shares stay equal over time
    /// (Kiln's `kiln-rt` `queue.rs`, its goal 6, plan 01a1a59c2abe step 3). With no lead, a Critical task of more
    /// than one poll went behind the lower classes' polls at each poll after its first, its first poll's time having
    /// put it past them.
    #[inline]
    pub fn pop_within(&self, lead_ns: u64) -> Option<u32> {
        let lists = match self.kept() {
            None => &self.normal,
            Some(kept) => self.pick(kept, lead_ns)?,
        };
        let slot = lists.ready_list().pop()?;
        if let Some(flag) = self.pending.get(index(slot)) {
            flag.set(false);
        }
        Some(slot)
    }

    /// The lists to take from: of the classes with a slot ready, the first in priority order that has run no more
    /// than `lead_ns` past the least-run of them.
    fn pick(&self, kept: &Kept, lead_ns: u64) -> Option<&Lists> {
        let ready = |class: Class| {
            self.own_lists(class)
                .filter(|lists| !lists.ready_list().is_empty())
        };
        let within = Self::least_run(kept, |class| ready(class).is_some())?.saturating_add(lead_ns);
        CLASSES
            .into_iter()
            .find_map(|class| ready(class).filter(|_| kept.ran(class).get() <= within))
    }

    /// Counts `ns` of poll time to the class of the task in `slot`, while classes are kept, and moves the shard's
    /// virtual time on to the least time run of that class and the classes listed.
    #[inline]
    pub fn charge(&self, slot: u32, ns: u64) {
        let Some(kept) = self.kept() else {
            return;
        };
        let (class, _) = self.lists(self.class_of(slot));
        let ran = kept.ran(class);
        ran.set(ran.get().saturating_add(ns));
        let least = Self::least_run(kept, |other| other == class || self.holds(other)).unwrap_or(0);
        kept.vtime_ns.set(kept.vtime_ns.get().max(least));
    }

    /// Whether a slot of a class more urgent than `class` is listed, or woke itself and waits for its release: what
    /// a task that lowered its class gives way to (`crate::coop::give_way`). False on a shard whose tasks took no
    /// class other than Normal.
    pub fn waiting_above(&self, class: Class) -> bool {
        self.kept().is_some()
            && CLASSES
                .into_iter()
                .any(|other| other > class && self.holds(other))
    }

    /// Whether nothing is ready, yielded tasks included.
    pub fn is_empty(&self) -> bool {
        !CLASSES.into_iter().any(|class| self.holds(class))
    }

    /// Ready entries, yielded tasks included.
    pub fn len(&self) -> usize {
        CLASSES
            .into_iter()
            .filter_map(|class| self.own_lists(class))
            .fold(0usize, |listed, lists| listed.saturating_add(lists.len()))
    }

    /// Whether a task of the shard took a class other than Normal, so the queue keeps classes: what arrives may
    /// then go ahead of what is listed.
    pub fn has_classes(&self) -> bool {
        self.kept.get().is_some()
    }

    /// Wakes for slots beyond the arena, ignored.
    pub fn overflow(&self) -> u64 {
        self.overflow.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take(q: &LocalQueue, limit: usize) -> Vec<u32> {
        let n = q.batch(limit);
        (0..n).filter_map(|_| q.pop()).collect()
    }

    #[test]
    fn duplicates_collapse_and_drain_returns_in_order() {
        let q = LocalQueue::new(4);
        q.push(2);
        q.push(0);
        q.push(2);
        q.push(3);
        assert_eq!(q.len(), 3);
        assert_eq!(take(&q, usize::MAX), vec![2, 0, 3]);
        assert!(q.is_empty());
        q.push(2);
        assert_eq!(take(&q, usize::MAX), vec![2]);
    }

    #[test]
    fn a_slot_beyond_the_arena_is_counted_not_listed() {
        let q = LocalQueue::new(2);
        q.push(9);
        assert_eq!(q.overflow(), 1);
        assert!(q.is_empty());
    }

    #[test]
    fn a_wake_during_a_drain_lands_in_the_next_batch() {
        let q = LocalQueue::new(3);
        q.push(1);
        let n = q.batch(usize::MAX);
        assert_eq!(n, 1);
        assert_eq!(q.pop(), Some(1));
        q.push(1);
        assert_eq!(take(&q, usize::MAX), vec![1]);
    }

    /// A yielded slot waits for its release, then goes behind every slot listed before it, and a wake while it
    /// waits does not list it twice. Do: yield 1, wake 2 and 1, release, wake 3. Expect: 2, 1, 3.
    #[test]
    fn a_yielded_slot_goes_behind_what_was_ready_at_its_release() {
        let q = LocalQueue::new(4);
        q.defer(1);
        assert!(!q.is_empty(), "a yielded slot is work");
        assert_eq!(q.pop(), None, "but not before its release");
        q.push(2);
        q.push(1);
        assert_eq!(q.len(), 2, "the wake of a yielded slot collapsed");
        q.release_yielded();
        q.push(3);
        assert_eq!(take(&q, usize::MAX), vec![2, 1, 3]);
        assert!(q.is_empty());
    }

    /// §4.3 bounded work: a drain hands out at most its batch, oldest first; the slots left behind keep
    /// their places ahead of any wake that arrives while the batch runs, and a wake for one of them is
    /// still collapsed. Do: queue five, take two, wake one of the left behind and a new one. Expect:
    /// the first two, then the other three in order, then the new one.
    #[test]
    fn a_drain_takes_at_most_its_batch_and_the_rest_keep_their_places() {
        let q = LocalQueue::new(8);
        for slot in 0..5 {
            q.push(slot);
        }
        assert_eq!(take(&q, 2), vec![0, 1]);
        assert_eq!(q.len(), 3, "the other three wait, not re-queued");
        q.push(3);
        q.push(7);
        assert_eq!(
            take(&q, 8),
            vec![2, 3, 4, 7],
            "FIFO; the repeated wake collapsed"
        );
    }

    /// A shard whose tasks take no class other than Normal makes no classes: Normal set on a slot sets nothing.
    #[test]
    fn s5a_normal_alone_makes_no_classes() {
        let q = LocalQueue::new(4);
        assert_eq!(q.set_class(1, Class::Normal), Class::Normal);
        assert!(q.kept.get().is_none(), "no classes made");
        q.push(1);
        assert!(
            !q.waiting_above(Class::Background),
            "no classes: nothing outranks"
        );
        assert_eq!(q.pop(), Some(1));
    }

    /// Kiln's goal 6, step 3 (its `g6_s3_classes_share_the_shard_and_the_most_urgent_goes_first`): with every class
    /// listed and none having run, Critical goes first, then Normal, then Background; each class's poll time counts
    /// to its share, so a class that ran more waits for the others to run as much. Do: list one slot of each class,
    /// take one, charge it; list again. Expect: Critical, Normal, Background in turn as they run alike, the least-run
    /// first after.
    #[test]
    fn s5a_classes_share_the_shard_and_the_most_urgent_goes_first() {
        let q = LocalQueue::new(8);
        q.set_class(1, Class::Critical);
        q.set_class(3, Class::Background);
        for slot in [3, 2, 1] {
            q.push(slot);
        }
        assert_eq!(q.pop(), Some(1), "critical first");
        q.charge(1, 100);
        assert_eq!(q.pop(), Some(2), "then normal");
        q.charge(2, 100);
        assert_eq!(q.pop(), Some(3), "then background");
        q.charge(3, 100);
        // All three have run 100 ns: listed again, priority order.
        for slot in [3, 2, 1] {
            q.push(slot);
        }
        assert_eq!(q.pop(), Some(1));
        q.charge(1, 1_000);
        // Critical ran 1,100 ns against 100: Normal and Background take their turns before it again.
        q.push(1);
        assert_eq!(q.pop(), Some(2));
        q.charge(2, 50);
        assert_eq!(q.pop(), Some(3));
        q.charge(3, 50);
        assert_eq!(q.pop(), Some(1));
        assert!(q.is_empty());
    }

    /// An idle class takes no credit (Kiln's `g6_s3_an_idle_class_takes_no_credit_for_its_idle_time`): a Background
    /// slot listed after Normal ran a long while alone starts level with Normal, not behind it by all that time, so
    /// Normal is not shut out while Background catches up.
    #[test]
    fn s5a_an_idle_class_takes_no_credit_for_its_idle_time() {
        let q = LocalQueue::new(8);
        q.set_class(5, Class::Background);
        q.push(2);
        assert_eq!(q.pop(), Some(2));
        q.charge(2, 1_000_000);
        q.push(2);
        q.push(5);
        // Background was raised to Normal's time as it listed: level, so Normal (higher) first, then Background.
        assert_eq!(q.pop(), Some(2));
        q.charge(2, 10);
        assert_eq!(q.pop(), Some(5));
    }

    /// A class that ran alone owes nothing once it rests (Kiln's
    /// `g6_s3_a_class_that_ran_alone_owes_nothing_once_it_rests`): Normal runs a long while alone and its work ends;
    /// a Background slot lists with nothing else listed, then Normal's lists again. Expect: the two level
    /// (Background was raised to the shard's virtual time, Normal's run), so Normal goes first and is not shut out
    /// while Background makes up the time Normal ran alone.
    #[test]
    fn s5a_a_class_that_ran_alone_owes_nothing_once_it_rests() {
        let q = LocalQueue::new(8);
        q.set_class(5, Class::Background);
        q.push(2);
        assert_eq!(q.pop(), Some(2));
        q.charge(2, 1_000_000);
        assert!(q.is_empty(), "Normal's work ended");
        q.push(5);
        q.push(2);
        assert_eq!(q.pop(), Some(2), "level: Normal first, by urgency");
        q.charge(2, 10);
        assert_eq!(q.pop(), Some(5));
        q.charge(5, 10);
        // Both ran 1,000,010 ns: Normal's next wake goes ahead again rather than behind a million of Background's.
        q.push(5);
        q.push(2);
        assert_eq!(q.pop(), Some(2));
    }

    /// Kiln's `g6_s3_a_critical_slot_overtakes_queued_normal_and_background_work`: a Critical slot woken behind
    /// queued Normal and Background work is taken next, ahead of all of it; and, within the lead (a quantum), its
    /// next polls are too, where with no lead each went behind a poll of a lower class. Past the lead, the lower
    /// classes take their turns: the shares hold. Do: four Normal and four Background slots queued, one of each
    /// polled a quantum; a Critical slot woken; it polls a fifth of a quantum and is woken again, twice; then it runs
    /// a quantum. Expect: Critical three times (where no lead would have put Normal first), then Normal and
    /// Background before Critical again.
    #[test]
    fn s5a_a_woken_critical_slot_overtakes_queued_normal_and_background_work() {
        // A quantum of the shape the tests' runtimes take; the order depends only on the ratios to it.
        const QUANTUM: u64 = 250_000;
        let q = LocalQueue::new(16);
        for slot in 5..=8 {
            q.set_class(slot, Class::Background);
        }
        q.set_class(9, Class::Critical);
        for slot in [1, 5, 2, 6, 3, 7, 4, 8] {
            q.push(slot);
        }
        // A poll of each lower class, a quantum each: they have run alike.
        assert_eq!(q.pop(), Some(1));
        q.charge(1, QUANTUM);
        assert_eq!(q.pop(), Some(5));
        q.charge(5, QUANTUM);
        // Six slots queued ahead of it, the Critical one is woken last, and taken first.
        q.push(9);
        assert_eq!(
            q.pop_within(QUANTUM),
            Some(9),
            "critical overtakes the queued work"
        );
        q.charge(9, QUANTUM / 5);
        // Woken again (a handshake's next round trip, a reply its handler awaited): still first.
        q.push(9);
        assert_eq!(
            q.pop_within(QUANTUM),
            Some(9),
            "its second poll, a fifth of a quantum ahead: still first"
        );
        q.charge(9, QUANTUM / 5);
        q.push(9);
        assert_eq!(
            q.pop(),
            Some(2),
            "with no lead, a Normal poll first: Critical ran past the least"
        );
        q.charge(2, 1);
        assert_eq!(
            q.pop_within(QUANTUM),
            Some(9),
            "within the lead: first again"
        );
        // Past its lead, the lower classes go first: Normal (within its own lead of Background), then Background;
        // then Critical, within the lead of the least again.
        q.charge(9, QUANTUM);
        q.push(9);
        assert_eq!(
            q.pop_within(QUANTUM),
            Some(3),
            "Normal, Critical past its lead"
        );
        q.charge(3, QUANTUM);
        assert_eq!(q.pop_within(QUANTUM), Some(6), "Background, the least run");
        q.charge(6, QUANTUM);
        assert_eq!(
            q.pop_within(QUANTUM),
            Some(9),
            "Critical within the lead of the least again"
        );
    }

    /// Kiln's `g6_s3_the_lead_keeps_the_shares_equal_over_time`, no starvation under a busy class: three classes
    /// always busy (two slots each, so none ever rests), each poll a fifth of the quantum. Expect: at every round
    /// the classes have run alike within the lead and a poll, the most urgent first within that (Critical takes its
    /// lead first: six polls, five make a quantum and the sixth starts at it), and over the run each class's polls
    /// equal within a lead's worth.
    #[test]
    fn s5a_the_lead_keeps_the_shares_equal_and_starves_no_class() {
        const QUANTUM: u64 = 250_000;
        const POLL: u64 = QUANTUM / 5;
        let q = LocalQueue::new(8);
        for (slot, class) in [
            (1, Class::Critical),
            (4, Class::Critical),
            (3, Class::Background),
            (6, Class::Background),
        ] {
            q.set_class(slot, class);
        }
        for slot in 1..=6 {
            q.push(slot);
        }
        let class_of = |slot: u32| match q.class_of(slot) {
            Class::Background => 0,
            Class::Normal => 1,
            Class::Critical => 2,
        };
        let (mut ran, mut polls) = ([0u64; 3], [0u64; 3]);
        let mut first = Vec::new();
        for round in 0..3_000 {
            let slot = q
                .pop_within(QUANTUM)
                .expect("six slots, each pushed back as it is taken");
            if round < 6 {
                first.push(slot);
            }
            q.charge(slot, POLL);
            ran[class_of(slot)] += POLL;
            polls[class_of(slot)] += 1;
            q.push(slot);
            let (most, least) = (
                ran.iter().max().copied().unwrap_or(0),
                ran.iter().min().copied().unwrap_or(0),
            );
            assert!(
                most - least <= QUANTUM + POLL,
                "round {round}: {ran:?} apart by more than the lead and a poll"
            );
        }
        assert!(
            first.iter().all(|slot| [1, 4].contains(slot)),
            "the most urgent first, within its lead: {first:?}"
        );
        let (most, least) = (
            polls.iter().max().copied().unwrap_or(0),
            polls.iter().min().copied().unwrap_or(0),
        );
        assert!(
            most - least <= QUANTUM / POLL + 1,
            "equal shares over time: {polls:?} polls"
        );
    }

    /// A class's slots that woke themselves wait for the release with the rest, and go behind their own class's
    /// ready slots; a lowered task waits behind a more urgent class's yielded slot (`waiting_above` counts it).
    #[test]
    fn s5a_a_classs_yielded_slots_wait_for_the_release_and_count_as_waiting() {
        let q = LocalQueue::new(8);
        q.set_class(1, Class::Critical);
        q.defer(1);
        assert!(q.has_yielded());
        assert!(
            q.waiting_above(Class::Normal),
            "a yielded Critical slot waits"
        );
        assert!(!q.waiting_above(Class::Critical));
        q.push(2);
        assert_eq!(
            q.pop(),
            Some(2),
            "the yielded Critical slot waits for its release"
        );
        q.release_yielded();
        assert_eq!(q.pop(), Some(1));
        assert!(q.is_empty() && !q.has_yielded());
    }
}
