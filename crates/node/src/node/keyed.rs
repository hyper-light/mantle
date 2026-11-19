//! Lists the node's task finds an entry of by a key it holds: its exchanges' bodies going out and
//! coming in, its chunk asks out and its native replies' halves, each found by its exchange at
//! every transport event on it, with thousands in flight on a loaded node. Each entry's slot is
//! kept in a hash map in step with the list as an entry is pushed and swap-removed, as the host
//! keeps its jobs' (`host::Slots`), so an event finds its entry in one probe rather than a search
//! of every entry.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use hyper_transport::ExchangeId;

/// What an entry of a [`Keyed`] list is found by: one key an entry, no two entries of a list
/// holding the same.
pub(super) trait Key {
    type Of: Copy + Eq + Hash + std::fmt::Debug;

    fn key(&self) -> Self::Of;
}

/// An answer kept for its asker, found by its ask (`Node::chunks_done`, `commands_done`,
/// `reads_done`).
impl<T> Key for (u64, T) {
    type Of = u64;

    fn key(&self) -> u64 {
        self.0
    }
}

/// What the node's task holds for an exchange, found by it (`Node::replies`, a native reply's half).
impl<T> Key for (ExchangeId, T) {
    type Of = ExchangeId;

    fn key(&self) -> ExchangeId {
        self.0
    }
}

/// Entries in a vector, each found by its key in one probe of the map of their slots, kept in step
/// as each is pushed and swap-removed: the entry moved into a removed one's slot is re-pointed.
/// Bound: its owner's, each entry's key once.
pub(super) struct Keyed<T: Key> {
    entries: Vec<T>,
    slots: HashMap<T::Of, usize>,
}

impl<T: Key> Default for Keyed<T> {
    /// None, which allocates nothing (`Vec::new`, `HashMap::new`).
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            slots: HashMap::new(),
        }
    }
}

impl<T: Key> Keyed<T> {
    /// Room for `entries` made at once (`Vec::with_capacity`, `HashMap::with_capacity`), for an
    /// owner that holds at most that many: none grows past it.
    pub(super) fn with_capacity(entries: usize) -> Self {
        Self {
            entries: Vec::with_capacity(entries),
            slots: HashMap::with_capacity(entries),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Its entries, in no order a caller may rely on: a removal moves the last into its slot.
    pub(super) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.entries.iter()
    }

    /// The entry at slot `at`, for a walk of every entry that removes some as it goes
    /// ([`Keyed::take`]): its slots are `0..len`, a removal moving the last into the slot taken.
    pub(super) fn at(&self, at: usize) -> Option<&T> {
        self.entries.get(at)
    }

    /// The entry at slot `at`, to change all but its key, which its slot is kept by.
    pub(super) fn at_mut(&mut self, at: usize) -> Option<&mut T> {
        self.entries.get_mut(at)
    }

    /// The slot of the entry holding `key`.
    pub(super) fn slot(&self, key: T::Of) -> Option<usize> {
        #[cfg(test)]
        tests::PROBES.with(|probes| probes.set(probes.get().saturating_add(1)));
        self.slots.get(&key).copied()
    }

    pub(super) fn contains(&self, key: T::Of) -> bool {
        self.slot(key).is_some()
    }

    pub(super) fn get(&self, key: T::Of) -> Option<&T> {
        self.slot(key).and_then(|at| self.entries.get(at))
    }

    pub(super) fn get_mut(&mut self, key: T::Of) -> Option<&mut T> {
        self.slot(key).and_then(|at| self.entries.get_mut(at))
    }

    /// Holds `entry`, last. An entry already holding its key, which no caller pushes (each holds
    /// its own exchange or ask), is replaced in its slot and given back, so the map stays exact.
    pub(super) fn push(&mut self, entry: T) -> Option<T> {
        let key = entry.key();
        let displaced = match self.slots.get(&key).copied() {
            Some(at) => self
                .entries
                .get_mut(at)
                .map(|held| std::mem::replace(held, entry)),
            None => {
                self.slots.insert(key, self.entries.len());
                self.entries.push(entry);
                None
            }
        };
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// The entry holding `key` taken out, the last moved into its slot.
    pub(super) fn remove(&mut self, key: T::Of) -> Option<T> {
        let at = self.slot(key)?;
        self.take(at)
    }

    /// The entry at `at` taken out, the last moved into its slot and re-pointed.
    pub(super) fn take(&mut self, at: usize) -> Option<T> {
        if at >= self.entries.len() {
            return None;
        }
        let entry = self.entries.swap_remove(at);
        self.slots.remove(&entry.key());
        if let Some(moved) = self.entries.get(at) {
            self.slots.insert(moved.key(), at);
        }
        #[cfg(test)]
        self.audit();
        Some(entry)
    }

    /// Keeps the entries `keep` holds to, each other taken out as [`Keyed::remove`] takes one: a
    /// walk of every entry, for what no key finds (a peer's chunk reads, its connection suspected).
    /// Each step removes an entry or passes one, so it ends within the entries' count.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut at = 0;
        while let Some(entry) = self.entries.get(at) {
            if keep(entry) {
                at = at.saturating_add(1);
            } else {
                self.take(at);
            }
        }
    }
}

/// What an entry of a [`KeyedOn`] list is also found by while it holds one: a read out's exchange,
/// while it is out on one (`Node::read_on`).
pub(super) trait On: Key {
    type By: Copy + Eq + Hash + std::fmt::Debug;

    fn on(&self) -> Option<Self::By>;

    /// What its list's owner finds a set of entries by, many an entry of: a command's and a read's
    /// range group, let go a group at a time as the range stops leading (`Node::commanded`,
    /// `Node::reading`), and a written-off node's own request's old identity, of which one is held
    /// at a time (`Node::enrollments`). None for the other lists, and an entry of none.
    fn class(&self) -> Option<u128> {
        None
    }
}

/// A [`Keyed`] list whose entries are also found by a second key while each holds one, in one
/// probe of `by`, the entry's own key there, and by their class, where they name one, in one probe
/// of `classes`: kept in step as an entry is pushed, changed and removed, a change made only
/// through [`KeyedOn::change`] or [`KeyedOn::change_at`], which read its second key and class
/// before and after. Bound: one `by` an entry, so at most its entries'; a class held while an entry
/// names it, its set put aside once it empties for the next class to take, so a class that comes
/// again takes no allocation: sets held and aside at most the most entries held at once.
pub(super) struct KeyedOn<T: On> {
    keyed: Keyed<T>,
    by: HashMap<T::By, T::Of>,
    classes: HashMap<u128, Marked<T::Of>>,
    aside: Vec<Marked<T::Of>>,
}

impl<T: On> Default for KeyedOn<T> {
    /// None, which allocates nothing (`Keyed::default`, `HashMap::new`, `Vec::new`).
    fn default() -> Self {
        Self {
            keyed: Keyed::default(),
            by: HashMap::new(),
            classes: HashMap::new(),
            aside: Vec::new(),
        }
    }
}

impl<T: On> KeyedOn<T> {
    pub(super) fn len(&self) -> usize {
        self.keyed.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.keyed.is_empty()
    }

    /// Its entries, in no order a caller may rely on, as [`Keyed::iter`].
    pub(super) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.keyed.iter()
    }

    /// The entry at slot `at`, as [`Keyed::at`].
    pub(super) fn at(&self, at: usize) -> Option<&T> {
        self.keyed.at(at)
    }

    pub(super) fn get(&self, key: T::Of) -> Option<&T> {
        self.keyed.get(key)
    }

    /// The key of the entry holding `by` as its second key.
    pub(super) fn on(&self, by: T::By) -> Option<T::Of> {
        #[cfg(test)]
        tests::PROBES.with(|probes| probes.set(probes.get().saturating_add(1)));
        self.by.get(&by).copied()
    }

    /// The key of an entry of `class`, the last its set holds: none once none is held. One probe
    /// of the classes, so taking each out in turn visits that class's entries alone.
    pub(super) fn of_class(&self, class: u128) -> Option<T::Of> {
        #[cfg(test)]
        tests::PROBES.with(|probes| probes.set(probes.get().saturating_add(1)));
        self.classes.get(&class).and_then(Marked::last)
    }

    /// Holds `entry`, last, as [`Keyed::push`]: an entry it displaces given back, its second key
    /// and class let go.
    pub(super) fn push(&mut self, entry: T) -> Option<T> {
        let (key, on, class) = (entry.key(), entry.on(), entry.class());
        let displaced = self.keyed.push(entry);
        if let Some(gone) = &displaced {
            if let Some(by) = gone.on() {
                self.by.remove(&by);
            }
            self.classed(key, gone.class(), None);
        }
        if let Some(by) = on {
            self.by.insert(by, key);
        }
        self.classed(key, None, class);
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// The entry holding `key` changed by `change`, all but its key, its second key and class
    /// kept.
    pub(super) fn change<R>(&mut self, key: T::Of, change: impl FnOnce(&mut T) -> R) -> Option<R> {
        let entry = self.keyed.get_mut(key)?;
        let before = (entry.on(), entry.class());
        let changed = change(entry);
        let after = (entry.on(), entry.class());
        self.moved(key, before, after);
        Some(changed)
    }

    /// The entry at slot `at` changed by `change`, all but its key, its second key and class kept.
    pub(super) fn change_at<R>(
        &mut self,
        at: usize,
        change: impl FnOnce(&mut T) -> R,
    ) -> Option<R> {
        let entry = self.keyed.at_mut(at)?;
        let (key, before) = (entry.key(), (entry.on(), entry.class()));
        let changed = change(entry);
        let after = (entry.on(), entry.class());
        self.moved(key, before, after);
        Some(changed)
    }

    /// The entry holding `key` went from second key and class `before` to `after`.
    fn moved(
        &mut self,
        key: T::Of,
        (before, was): (Option<T::By>, Option<u128>),
        (after, is): (Option<T::By>, Option<u128>),
    ) {
        if before != after {
            if let Some(by) = before {
                self.by.remove(&by);
            }
            if let Some(by) = after {
                self.by.insert(by, key);
            }
        }
        self.classed(key, was, is);
        #[cfg(test)]
        self.audit();
    }

    /// The entry holding `key` went from class `was` to `is`: a class whose set empties let go,
    /// its set put aside, and a class that comes takes one put aside where one is.
    fn classed(&mut self, key: T::Of, was: Option<u128>, is: Option<u128>) {
        if was == is {
            return;
        }
        if let Some(class) = was
            && let Some(held) = self.classes.get_mut(&class)
        {
            held.remove(key);
            if held.is_empty()
                && let Some(empty) = self.classes.remove(&class)
            {
                self.aside.push(empty);
            }
        }
        if let Some(class) = is {
            let held = match self.classes.entry(class) {
                std::collections::hash_map::Entry::Occupied(held) => held.into_mut(),
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(self.aside.pop().unwrap_or_default())
                }
            };
            held.insert(key);
        }
    }

    /// The entry holding `key` taken out, as [`Keyed::remove`], its second key let go.
    pub(super) fn remove(&mut self, key: T::Of) -> Option<T> {
        let entry = self.keyed.remove(key)?;
        self.let_go(&entry);
        Some(entry)
    }

    /// The entry at slot `at` taken out, as [`Keyed::take`], its second key let go.
    pub(super) fn take(&mut self, at: usize) -> Option<T> {
        let entry = self.keyed.take(at)?;
        self.let_go(&entry);
        Some(entry)
    }

    /// Keeps the entries `keep` holds to, each other taken out as [`KeyedOn::take`] takes one: a
    /// walk of every entry, for what neither key finds (an entry this node asked itself, by its
    /// own number). Each step removes an entry or passes one, so it ends within the entries' count.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut at = 0;
        while let Some(entry) = self.keyed.at(at) {
            if keep(entry) {
                at = at.saturating_add(1);
            } else {
                self.take(at);
            }
        }
    }

    fn let_go(&mut self, entry: &T) {
        if let Some(by) = entry.on() {
            self.by.remove(&by);
        }
        self.classed(entry.key(), entry.class(), None);
        #[cfg(test)]
        self.audit();
    }
}

/// What the parts of a [`Waiting`] list's entries wait on: each part, while it waits, on one thing
/// an answer names, a command on its group, session and serial until it is answered
/// (`Node::commanded`). Its entries' keys are ordered, as the parts waiting on one thing are.
pub(super) trait Waits: Key<Of: Ord> + On {
    type For: Copy + Eq + Hash + std::fmt::Debug;
    type Part;

    /// Its parts, each at its place for as long as the entry is held.
    fn parts(&self) -> &[Self::Part];

    fn parts_mut(&mut self) -> &mut [Self::Part];

    /// What `part`, one of this entry's, waits on: none once it waits no longer.
    fn waits(&self, part: &Self::Part) -> Option<Self::For>;
}

/// A [`KeyedOn`] list whose entries' parts are each also found by what they wait on, while they
/// wait, in one probe of `waiting` ([`Waiting::first`]), and whose entries none of whose parts
/// waits are marked `answered` ([`Waiting::answered`]): kept in step as an entry is pushed and
/// removed, and as a part changes, which it does only through [`Waiting::change_part`], which
/// reads what the part waits on before and after and hands the change that part alone. Bound:
/// one waiter a part waiting, so at most the entries' parts; one mark an entry.
pub(super) struct Waiting<T: Waits> {
    on: KeyedOn<T>,
    waiting: HashMap<T::For, Waiters<T::Of>>,
    answered: Marked<T::Of>,
}

/// The parts waiting on one thing, each by its entry's key and its place there: the least first,
/// which the next answer to it reaches, and any others, usually none (a command asked again before
/// the first was answered), in no order.
struct Waiters<K> {
    first: (K, usize),
    more: Vec<(K, usize)>,
}

impl<T: Waits> Default for Waiting<T> {
    /// None, which allocates nothing (`KeyedOn::default`, `HashMap::new`, `Marked::default`).
    fn default() -> Self {
        Self {
            on: KeyedOn::default(),
            waiting: HashMap::new(),
            answered: Marked::default(),
        }
    }
}

impl<T: Waits> Waiting<T> {
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.on.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.on.is_empty()
    }

    /// Its entries, in no order a caller may rely on, as [`Keyed::iter`].
    pub(super) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.on.iter()
    }

    /// The entry at slot `at`, as [`Keyed::at`].
    #[cfg(test)]
    pub(super) fn at(&self, at: usize) -> Option<&T> {
        self.on.at(at)
    }

    pub(super) fn get(&self, key: T::Of) -> Option<&T> {
        self.on.get(key)
    }

    /// The key of the entry holding `by` as its second key, as [`KeyedOn::on`].
    pub(super) fn on(&self, by: T::By) -> Option<T::Of> {
        self.on.on(by)
    }

    /// The key of an entry of `class`, as [`KeyedOn::of_class`].
    pub(super) fn of_class(&self, class: u128) -> Option<T::Of> {
        self.on.of_class(class)
    }

    /// The keys of the entries none of whose parts waits, added to `out` in the order they were
    /// marked, the first answered first: no other entry read.
    pub(super) fn answered_into(&self, out: &mut Vec<T::Of>) {
        out.extend_from_slice(self.answered.keys());
    }

    /// The part waiting on `waits` that an answer to it reaches first: its entry's key and its
    /// place there, the least of any waiting on it.
    pub(super) fn first(&self, waits: T::For) -> Option<(T::Of, usize)> {
        #[cfg(test)]
        tests::PROBES.with(|probes| probes.set(probes.get().saturating_add(1)));
        self.waiting.get(&waits).map(|waiters| waiters.first)
    }

    /// Holds `entry`, last, as [`KeyedOn::push`], each part of it waiting found by what it waits
    /// on, and it marked answered where none does: an entry it displaces given back, its parts
    /// and its mark let go.
    pub(super) fn push(&mut self, entry: T) -> Option<T> {
        let key = entry.key();
        let displaced = self.on.push(entry);
        if let Some(gone) = &displaced {
            went(&mut self.waiting, gone);
            self.answered.remove(key);
        }
        // The entry pushed is the last, or in the slot of the one it displaced.
        let keyed = &self.on.keyed;
        let held = match displaced {
            None => keyed.entries.last(),
            Some(_) => keyed.slots.get(&key).and_then(|&at| keyed.entries.get(at)),
        };
        if let Some(held) = held
            && !came(&mut self.waiting, held)
        {
            self.answered.insert(key);
        }
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// Part `place` of the entry holding `key` changed by `change`, that part alone, what it
    /// waits on kept, and the entry marked answered once none of its parts waits, unmarked once
    /// one waits again.
    pub(super) fn change_part<R>(
        &mut self,
        key: T::Of,
        place: usize,
        change: impl FnOnce(&mut T::Part) -> R,
    ) -> Option<R> {
        let (before, changed, after, other) = self
            .on
            .change(key, |entry| {
                let before = entry.parts().get(place).and_then(|part| entry.waits(part));
                let changed = change(entry.parts_mut().get_mut(place)?);
                let after = entry.parts().get(place).and_then(|part| entry.waits(part));
                // Where this part stopped waiting, whether another still does: a walk of the
                // entry's parts from its last, which its answers reach last (an entry's commands
                // apply in its order), so it stops at the first it reads but at the last answer.
                let other = before.is_some()
                    && after.is_none()
                    && entry
                        .parts()
                        .iter()
                        .rev()
                        .any(|part| entry.waits(part).is_some());
                Some((before, changed, after, other))
            })
            .flatten()?;
        if before != after {
            if let Some(waits) = before {
                unwait(&mut self.waiting, waits, (key, place));
            }
            if let Some(waits) = after {
                wait(&mut self.waiting, waits, (key, place));
            }
        }
        match (before, after) {
            (Some(_), None) if !other => {
                self.answered.insert(key);
            }
            (None, Some(_)) => {
                self.answered.remove(key);
            }
            _ => {}
        }
        #[cfg(test)]
        self.audit();
        Some(changed)
    }

    /// The entry holding `key` taken out, as [`KeyedOn::remove`], its parts and its mark let go.
    pub(super) fn remove(&mut self, key: T::Of) -> Option<T> {
        let entry = self.on.remove(key)?;
        went(&mut self.waiting, &entry);
        self.answered.remove(key);
        #[cfg(test)]
        self.audit();
        Some(entry)
    }

    /// The entry at slot `at` taken out, as [`KeyedOn::take`], its parts and its mark let go.
    pub(super) fn take(&mut self, at: usize) -> Option<T> {
        let entry = self.on.take(at)?;
        went(&mut self.waiting, &entry);
        self.answered.remove(entry.key());
        #[cfg(test)]
        self.audit();
        Some(entry)
    }

    /// Keeps the entries `keep` holds to, each other taken out as [`Waiting::take`] takes one, as
    /// [`KeyedOn::retain`]: each step removes an entry or passes one, so it ends within the
    /// entries' count.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut at = 0;
        while let Some(entry) = self.on.at(at) {
            if keep(entry) {
                at = at.saturating_add(1);
            } else {
                self.take(at);
            }
        }
    }
}

/// Each part of `entry` that waits found by what it waits on: a walk of its parts, at most an
/// entry's. Whether any part waits.
fn came<T: Waits>(waiting: &mut HashMap<T::For, Waiters<T::Of>>, entry: &T) -> bool {
    let key = entry.key();
    let mut any = false;
    for (place, part) in entry.parts().iter().enumerate() {
        if let Some(waits) = entry.waits(part) {
            wait(waiting, waits, (key, place));
            any = true;
        }
    }
    any
}

/// Each part of `entry` that waits let go: a walk of its parts, at most an entry's.
fn went<T: Waits>(waiting: &mut HashMap<T::For, Waiters<T::Of>>, entry: &T) {
    let key = entry.key();
    for (place, part) in entry.parts().iter().enumerate() {
        if let Some(waits) = entry.waits(part) {
            unwait(waiting, waits, (key, place));
        }
    }
}

/// `waiter` waits on `waits`: first where it is the least of those waiting on it.
fn wait<F: Eq + Hash, K: Ord>(waiting: &mut HashMap<F, Waiters<K>>, waits: F, waiter: (K, usize)) {
    match waiting.entry(waits) {
        std::collections::hash_map::Entry::Vacant(vacant) => {
            vacant.insert(Waiters {
                first: waiter,
                more: Vec::new(),
            });
        }
        std::collections::hash_map::Entry::Occupied(mut held) => {
            let waiters = held.get_mut();
            let other = if waiter < waiters.first {
                std::mem::replace(&mut waiters.first, waiter)
            } else {
                waiter
            };
            waiters.more.push(other);
        }
    }
}

/// `waiter` waits on `waits` no longer: where it was first, the least of the others is, found by a
/// walk of them, usually none.
fn unwait<F: Eq + Hash, K: Ord + Copy>(
    waiting: &mut HashMap<F, Waiters<K>>,
    waits: F,
    waiter: (K, usize),
) {
    let Some(waiters) = waiting.get_mut(&waits) else {
        return;
    };
    let at = if waiters.first == waiter {
        let least = waiters
            .more
            .iter()
            .enumerate()
            .min_by_key(|(_, other)| **other)
            .map(|(at, _)| at);
        let Some(least) = least else {
            waiting.remove(&waits);
            return;
        };
        least
    } else {
        match waiters.more.iter().position(|other| *other == waiter) {
            Some(at) => at,
            None => return,
        }
    };
    if at < waiters.more.len() {
        let other = waiters.more.swap_remove(at);
        if waiters.first == waiter {
            waiters.first = other;
        }
    }
}

/// Keys marked once each, in a vector, each found in one probe of the map of their slots, kept in
/// step as each is marked and let go as [`Keyed`] keeps its entries': the set of a list's entries
/// something made ready, read last first, in no order a caller may rely on. Bound: its owner's,
/// each key once.
pub(super) struct Marked<K> {
    keys: Vec<K>,
    slots: HashMap<K, usize>,
}

impl<K> Default for Marked<K> {
    /// None, which allocates nothing (`Vec::new`, `HashMap::new`).
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            slots: HashMap::new(),
        }
    }
}

impl<K: Copy + Eq + Hash> Marked<K> {
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[cfg(test)]
    pub(super) fn contains(&self, key: K) -> bool {
        self.slots.contains_key(&key)
    }

    /// The key marked last of those held.
    pub(super) fn last(&self) -> Option<K> {
        self.keys.last().copied()
    }

    /// The keys held, in the order they were marked but where a key let go moved the last into
    /// its slot.
    pub(super) fn keys(&self) -> &[K] {
        &self.keys
    }

    /// Marks `key`, last, where it is not already: whether it was not.
    pub(super) fn insert(&mut self, key: K) -> bool {
        if self.slots.contains_key(&key) {
            return false;
        }
        self.slots.insert(key, self.keys.len());
        self.keys.push(key);
        #[cfg(test)]
        self.audit();
        true
    }

    /// `key` let go, the last moved into its slot and re-pointed: whether it was marked.
    pub(super) fn remove(&mut self, key: K) -> bool {
        let Some(at) = self.slots.remove(&key) else {
            return false;
        };
        if at < self.keys.len() {
            self.keys.swap_remove(at);
            if let Some(&moved) = self.keys.get(at) {
                self.slots.insert(moved, at);
            }
        }
        #[cfg(test)]
        self.audit();
        true
    }

    /// The key marked last taken out.
    pub(super) fn pop(&mut self) -> Option<K> {
        let key = self.keys.pop()?;
        self.slots.remove(&key);
        #[cfg(test)]
        self.audit();
        Some(key)
    }

    /// Every key let go, the storage kept.
    pub(super) fn clear(&mut self) {
        self.keys.clear();
        self.slots.clear();
    }
}

/// A [`Keyed`] list with the set of its entries something made ready ([`Marked`]), each marked at
/// every event that can let it move and visited at the next turn, where a walk of every entry
/// visited thousands that could not move (`Node::write_replies`). An entry
/// is marked as it is pushed, since nothing looked at it yet, and its mark let go with it. A turn
/// takes the marks to visit ([`Readied::begin_visits`]), so an entry marked again while it is
/// visited is visited at the next turn, not again in this one. Bound: its owner's, each entry's key
/// marked once and taken to visit once a turn.
pub(super) struct Readied<T: Key> {
    entries: Keyed<T>,
    ready: Marked<T::Of>,
    /// The marks a turn took to visit, the next last; and the one visited now.
    visits: Vec<T::Of>,
    visiting: Option<T::Of>,
}

impl<T: Key> Default for Readied<T> {
    /// None, which allocates nothing (`Keyed::default`, `Marked::default`, `Vec::new`).
    fn default() -> Self {
        Self {
            entries: Keyed::default(),
            ready: Marked::default(),
            visits: Vec::new(),
            visiting: None,
        }
    }
}

impl<T: Key> Readied<T> {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Its entries, in no order a caller may rely on, as [`Keyed::iter`].
    pub(super) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.entries.iter()
    }

    pub(super) fn at(&self, at: usize) -> Option<&T> {
        self.entries.at(at)
    }

    pub(super) fn at_mut(&mut self, at: usize) -> Option<&mut T> {
        self.entries.at_mut(at)
    }

    pub(super) fn slot(&self, key: T::Of) -> Option<usize> {
        self.entries.slot(key)
    }

    pub(super) fn contains(&self, key: T::Of) -> bool {
        self.entries.contains(key)
    }

    /// Holds `entry`, last, marked, as [`Keyed::push`].
    pub(super) fn push(&mut self, entry: T) -> Option<T> {
        let key = entry.key();
        let displaced = self.entries.push(entry);
        self.ready.insert(key);
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// The entry at `at` taken out, its mark with it, as [`Keyed::take`]. A key taken to visit is
    /// passed over as its visit comes ([`Readied::next_visit`]).
    pub(super) fn take(&mut self, at: usize) -> Option<T> {
        let entry = self.entries.take(at)?;
        self.ready.remove(entry.key());
        if self.visiting == Some(entry.key()) {
            self.visiting = None;
        }
        #[cfg(test)]
        self.audit();
        Some(entry)
    }

    /// Marks the entry holding `key`, where one does: whether it was not marked.
    pub(super) fn mark(&mut self, key: T::Of) -> bool {
        if !self.entries.contains(key) {
            return false;
        }
        let marked = self.ready.insert(key);
        #[cfg(test)]
        self.audit();
        marked
    }

    /// Marks every entry: a walk of each, for a word that names none (`host::Marks`, a mark
    /// the node's marks had no room for).
    pub(super) fn mark_all(&mut self) {
        for entry in self.entries.iter() {
            self.ready.insert(entry.key());
        }
        #[cfg(test)]
        self.audit();
    }

    /// The marks taken to visit at a turn, the set left empty for what marks entries meanwhile:
    /// how many.
    pub(super) fn begin_visits(&mut self) -> usize {
        self.visits.clear();
        self.visits.extend_from_slice(self.ready.keys());
        self.ready.clear();
        self.visiting = None;
        self.visits.len()
    }

    /// The next entry to visit of those taken, still held: none once each was.
    pub(super) fn next_visit(&mut self) -> Option<T::Of> {
        self.visiting = None;
        while let Some(key) = self.visits.pop() {
            if self.entries.contains(key) {
                self.visiting = Some(key);
                return Some(key);
            }
        }
        None
    }
}

/// Entries in the order they came, each found by its key in one probe of the map of their places:
/// a queue whose order its owner depends on (`Node::parked`, taken first come first), which a
/// swap-remove would break, and whose entries leave from anywhere as their exchanges end. Each
/// entry's place is the count of pushes before it, the entries a B-tree by place, its first the
/// oldest. Bound: its owner's, each entry's key once.
pub(super) struct Queued<T: Key> {
    entries: BTreeMap<u64, T>,
    places: HashMap<T::Of, u64>,
    next: u64,
}

impl<T: Key> Default for Queued<T> {
    /// None, which allocates nothing (`BTreeMap::new`, `HashMap::new`).
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            places: HashMap::new(),
            next: 0,
        }
    }
}

impl<T: Key> Queued<T> {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Its entries, oldest first.
    pub(super) fn iter(&self) -> std::collections::btree_map::Values<'_, u64, T> {
        self.entries.values()
    }

    /// Its oldest entry.
    pub(super) fn front(&self) -> Option<&T> {
        self.entries.first_key_value().map(|(_, entry)| entry)
    }

    /// Its oldest entry taken out.
    pub(super) fn pop_front(&mut self) -> Option<T> {
        let (_, entry) = self.entries.pop_first()?;
        self.places.remove(&entry.key());
        #[cfg(test)]
        self.audit();
        Some(entry)
    }

    /// Holds `entry`, last. An entry already holding its key, which no caller pushes (each holds
    /// its own exchange), is taken out and given back, so the map stays exact. Places count up
    /// from the last renumbering; one past a `u64`'s, which no node's pushes reach, renumbers the
    /// entries held from 0 in their order first.
    pub(super) fn push_back(&mut self, entry: T) -> Option<T> {
        let displaced = self.remove(entry.key());
        let place = match self.next.checked_add(1) {
            Some(next) => std::mem::replace(&mut self.next, next),
            None => self.renumbered(),
        };
        self.places.insert(entry.key(), place);
        self.entries.insert(place, entry);
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// The entries held renumbered from 0 in their order: the place the next entry takes.
    fn renumbered(&mut self) -> u64 {
        let entries = std::mem::take(&mut self.entries);
        self.places.clear();
        let mut place = 0;
        for entry in entries.into_values() {
            self.places.insert(entry.key(), place);
            self.entries.insert(place, entry);
            place = place.saturating_add(1);
        }
        self.next = place.saturating_add(1);
        place
    }

    /// The entry holding `key` taken out, wherever it is: one probe of its place, and the B-tree's
    /// removal there.
    pub(super) fn remove(&mut self, key: T::Of) -> Option<T> {
        #[cfg(test)]
        tests::PROBES.with(|probes| probes.set(probes.get().saturating_add(1)));
        let place = self.places.remove(&key)?;
        let entry = self.entries.remove(&place);
        #[cfg(test)]
        self.audit();
        entry
    }
}

impl<T: Key> Extend<T> for Keyed<T> {
    /// Each pushed, as [`Keyed::push`] holds one.
    fn extend<I: IntoIterator<Item = T>>(&mut self, entries: I) {
        for entry in entries {
            self.push(entry);
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::{Key, Keyed, KeyedOn, Marked, On, Queued, Readied, Waiting, Waits};

    thread_local! {
        /// The probes this thread made of a keyed list's slots ([`Keyed::slot`]).
        pub(in crate::node) static PROBES: std::cell::Cell<usize> =
            const { std::cell::Cell::new(0) };
    }

    /// The probes made on this thread since the last call.
    pub(in crate::node) fn probes() -> usize {
        PROBES.with(|probes| probes.replace(0))
    }

    impl<T: Key> Keyed<T> {
        /// The oracle, after each change: its slots agree with a full recount, each entry's key
        /// at its own slot, and no other key held.
        pub(in crate::node) fn audit(&self) {
            for (at, entry) in self.entries.iter().enumerate() {
                let key = entry.key();
                assert_eq!(self.slots.get(&key), Some(&at), "{key:?}");
            }
            assert_eq!(
                self.slots.len(),
                self.entries.len(),
                "a key's slot no entry holds, or two entries holding one key"
            );
        }
    }

    impl<T: On> KeyedOn<T> {
        /// The oracle, after each change: its list's own slots exact ([`Keyed::audit`]), and its
        /// second keys agree with a full recount, each entry holding one found by it, no other.
        pub(in crate::node) fn audit(&self) {
            self.keyed.audit();
            let mut on = 0;
            for entry in self.keyed.iter() {
                if let Some(by) = entry.on() {
                    assert_eq!(self.by.get(&by), Some(&entry.key()), "{by:?}");
                    on += 1;
                }
            }
            assert_eq!(
                self.by.len(),
                on,
                "a second key no entry holds, or two entries holding one"
            );
            let mut classed = 0;
            for entry in self.keyed.iter() {
                if let Some(class) = entry.class() {
                    let held = self.classes.get(&class).expect("each class held");
                    assert!(held.contains(entry.key()), "{class:x}: {:?}", entry.key());
                    classed += 1;
                }
            }
            for held in self.classes.values() {
                held.audit();
                assert!(!held.is_empty(), "a class held that no entry names");
            }
            for aside in &self.aside {
                assert!(aside.is_empty(), "a set put aside holding a key");
            }
            let held: usize = self.classes.values().map(Marked::len).sum();
            assert_eq!(held, classed, "a class holding an entry gone or of another");
        }
    }

    impl<T: Key> Readied<T> {
        /// The oracle, after each change: its slots exact (`Keyed::audit`), its marks' (`Marked::audit`),
        /// and each key marked, taken to visit or visited one an entry holds.
        pub(in crate::node) fn audit(&self) {
            self.entries.audit();
            self.ready.audit();
            for key in self.ready.keys().iter().chain(self.visiting.iter()) {
                assert!(self.entries.contains(*key), "{key:?} marked, held by none");
            }
        }

        /// Whether the entry holding `key` is marked, taken to visit at this turn, or visited now:
        /// what its owner's audit asks of an entry that could move (`host::Writes::audit`).
        pub(in crate::node) fn covered(&self, key: T::Of) -> bool {
            self.ready.contains(key) || self.visiting == Some(key) || self.visits.contains(&key)
        }

        pub(in crate::node) fn marked(&self) -> usize {
            self.ready.len()
        }
    }

    impl<K: Copy + Eq + std::hash::Hash> Marked<K> {
        /// The oracle, after each change: its slots agree with a full recount, each key at its own
        /// slot, and no other key held.
        pub(in crate::node) fn audit(&self) {
            for (at, key) in self.keys.iter().enumerate() {
                assert_eq!(self.slots.get(key), Some(&at), "each key at its own slot");
            }
            assert_eq!(
                self.slots.len(),
                self.keys.len(),
                "a key's slot no key holds, or one key marked twice"
            );
        }
    }

    impl<T: Waits> Waiting<T> {
        /// The oracle, after each change: its list's own maps exact ([`KeyedOn::audit`]), and what
        /// its parts wait on agrees with a full recount: each part waiting held by what it waits
        /// on, none before the first there, and as many held as wait, so no other is held and
        /// the first is the least of those waiting on it.
        pub(in crate::node) fn audit(&self) {
            self.on.audit();
            let mut parts_waiting = 0;
            for entry in self.on.iter() {
                for (place, part) in entry.parts().iter().enumerate() {
                    let Some(waits) = entry.waits(part) else {
                        continue;
                    };
                    parts_waiting += 1;
                    let waiter = (entry.key(), place);
                    let held = self.waiting.get(&waits).expect("each thing waited on held");
                    assert!(
                        held.first == waiter || held.more.contains(&waiter),
                        "{waits:?}: {waiter:?}"
                    );
                    assert!(held.first <= waiter, "{waits:?}: the least first");
                }
            }
            let held: usize = self
                .waiting
                .values()
                .map(|waiters| 1 + waiters.more.len())
                .sum();
            assert_eq!(held, parts_waiting, "a waiter held that waits no longer");
            self.answered.audit();
            let mut answered = 0;
            for entry in self.on.iter() {
                let waits = entry.parts().iter().any(|part| entry.waits(part).is_some());
                assert_eq!(
                    self.answered.contains(entry.key()),
                    !waits,
                    "{:?}: marked answered exactly when none of its parts waits",
                    entry.key()
                );
                answered += usize::from(!waits);
            }
            assert_eq!(self.answered.len(), answered, "a mark no entry holds");
        }
    }

    impl<T: Key> Queued<T> {
        /// The oracle, after each change: its places agree with a full recount, each entry's key
        /// at its own place, every place before the next, and no other key held.
        pub(in crate::node) fn audit(&self) {
            for (place, entry) in &self.entries {
                let key = entry.key();
                assert_eq!(self.places.get(&key), Some(place), "{key:?}");
                assert!(
                    *place < self.next,
                    "{key:?} at {place}, the next {}",
                    self.next
                );
            }
            assert_eq!(
                self.places.len(),
                self.entries.len(),
                "a key's place no entry holds, or two entries holding one key"
            );
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Entry(u64, &'static str);

    impl Key for Entry {
        type Of = u64;

        fn key(&self) -> u64 {
            self.0
        }
    }

    /// Pushes, removals from every place, a displacing push and a retain each keep the slots
    /// exact (the audit after each change), and every entry left is found by its key in one probe.
    #[test]
    fn a_keyed_lists_slots_follow_each_push_and_removal() {
        let mut keyed = Keyed::default();
        for key in 0..64 {
            assert_eq!(keyed.push(Entry(key, "first")), None);
        }
        assert_eq!(keyed.remove(0), Some(Entry(0, "first")), "the first");
        assert_eq!(
            keyed.remove(63),
            Some(Entry(63, "first")),
            "moved into the first's slot"
        );
        assert_eq!(keyed.remove(62), Some(Entry(62, "first")), "the last");
        assert_eq!(keyed.remove(0), None, "gone");
        assert_eq!(keyed.push(Entry(5, "again")), Some(Entry(5, "first")));
        keyed.retain(|entry| entry.0 % 3 != 0);
        let left: Vec<u64> = (1..62).filter(|key| key % 3 != 0).collect();
        assert_eq!(keyed.len(), left.len());
        probes();
        for key in &left {
            assert!(keyed.get(*key).is_some(), "{key}");
        }
        assert_eq!(probes(), left.len(), "one probe a lookup");
        assert_eq!(keyed.get(5).map(|entry| entry.1), Some("again"));
        assert!(!keyed.contains(3) && !keyed.contains(64));
        keyed.audit();
    }

    /// The entries a search of the list visits for `key`, as the node's task searched it before
    /// (`iter().position`): each one up to the one holding it, or every one where none does.
    fn searched<T: Key>(keyed: &Keyed<T>, key: T::Of) -> usize {
        keyed
            .iter()
            .position(|entry| entry.key() == key)
            .map_or(keyed.len(), |at| at + 1)
    }

    /// The node's task's events on its exchanges, each finding its chunk ask out, its body going
    /// out and its body arriving in a probe or two of their lists' slots (`Node::chunks_out`,
    /// `writing`, `receiving`): 2,048 chunk puts out writing their bodies and 2,048 bodies
    /// arriving, every third of each ended as an event ends one, then a `Writable` on each put
    /// left (its body's entry, then its chunk ask's, as a piece goes), a `Reply` on each (its
    /// chunk ask's, twice: `Event::Reply`'s owner, then `chunk_replied`'s), and a `Refused` on
    /// each body arriving (`Event::Refused`: its body's, its chunk ask's, `give_back`'s, then each
    /// list's removal). Before, each lookup searched its list.
    #[test]
    fn an_exchanges_entries_are_found_with_no_scan() {
        const IN_FLIGHT: u64 = 2048;
        let (mut chunks_out, mut writing, mut receiving) =
            (Keyed::default(), Keyed::default(), Keyed::default());
        for exchange in 0..IN_FLIGHT {
            chunks_out.push(Entry(exchange, "chunk"));
            writing.push(Entry(exchange, "put"));
            receiving.push(Entry(IN_FLIGHT + exchange, "arriving"));
        }
        for exchange in (0..IN_FLIGHT).filter(|exchange| exchange % 3 == 0) {
            chunks_out.remove(exchange);
            writing.remove(exchange);
            receiving.remove(IN_FLIGHT + exchange);
        }
        let puts: Vec<u64> = chunks_out.iter().map(|entry| entry.0).collect();
        let arriving: Vec<u64> = receiving.iter().map(|entry| entry.0).collect();
        let (mut before, mut after) = ([0; 3], [0; 3]);
        probes();
        for &exchange in &puts {
            before[0] += searched(&writing, exchange) + searched(&chunks_out, exchange);
            assert!(writing.get_mut(exchange).is_some() && chunks_out.get(exchange).is_some());
        }
        after[0] = probes();
        for &exchange in &puts {
            before[1] += 2 * searched(&chunks_out, exchange);
            assert!(chunks_out.contains(exchange));
            assert!(chunks_out.remove(exchange).is_some());
        }
        after[1] = probes();
        for &exchange in &arriving {
            // `Event::Refused` read every body arriving twice (its own and `give_back`'s filter),
            // searched the chunk asks out, and kept the bodies both ways but its own (`retain`).
            before[2] += 3 * receiving.len() + chunks_out.len() + writing.len();
            assert!(receiving.get(exchange).is_some() && chunks_out.remove(exchange).is_none());
            assert!(receiving.get(exchange).is_some());
            assert!(receiving.remove(exchange).is_some() && writing.remove(exchange).is_none());
        }
        after[2] = probes();
        println!(
            "{} puts, {} bodies arriving: Writable {} probes ({} visited by the searches), Reply \
             {} ({}), Refused {} ({})",
            puts.len(),
            arriving.len(),
            after[0],
            before[0],
            after[1],
            before[1],
            after[2],
            before[2]
        );
        assert_eq!(after[0], 2 * puts.len(), "two probes a Writable");
        assert_eq!(after[1], 2 * puts.len(), "two probes a Reply");
        assert_eq!(after[2], 5 * arriving.len(), "five probes a Refused");
        assert!(chunks_out.len() + receiving.len() == 0 && writing.len() == puts.len());
    }

    /// A driving shard's lanes take the node's answers to their asks at each serve by reading the
    /// answers waiting, each probing the asks held (`Lanes::serve`), and each answer taken is one
    /// probe of its done list (`take_done`): 2,048 asks held, 16 of them answered. Before, each
    /// ask held searched the done list at every serve (`node.outcome` for each), and `host_due`
    /// and `drop_abandoned` likewise searched it for each ask let go.
    #[test]
    fn a_serve_reads_the_answers_waiting_not_the_asks_held() {
        const HELD: u64 = 2048;
        const ANSWERED: u64 = 16;
        let asked: std::collections::HashMap<u64, u64> =
            (0..HELD).map(|ask| (ask, ask + HELD)).collect();
        let answers = || {
            let mut done: Keyed<(u64, ())> = Keyed::default();
            done.extend((HELD - ANSWERED..HELD).map(|ask| (ask, ())));
            done
        };
        // Before: each ask held, in order, searched the answers, each found taken out.
        let mut done = answers();
        let mut searched = 0;
        for ask in 0..HELD {
            let at = done.iter().position(|(answered, _)| *answered == ask);
            searched += at.map_or(done.len(), |at| at + 1);
            if at.is_some() {
                done.remove(ask);
            }
        }
        // Now: each answer waiting probes the asks held, and each found is taken in one probe.
        let mut done = answers();
        let found: Vec<u64> = done
            .iter()
            .map(|(ask, _)| *ask)
            .filter(|ask| asked.contains_key(ask))
            .collect();
        probes();
        for ask in &found {
            assert!(done.remove(*ask).is_some());
        }
        let visited = found.len() + probes();
        println!(
            "{HELD} asks held, {ANSWERED} answered: {visited} entries visited, {searched} by a \
             search of the answers for each ask held"
        );
        assert_eq!(visited, 2 * usize::try_from(ANSWERED).unwrap());
        assert!(done.is_empty());
    }

    /// The node's task's events on exchanges, each asking whether the host holds it (`Node::hosts`):
    /// its job's slot (`Host::job_of`, a probe of its own), then, where no job holds it, its native
    /// reply's half in a probe of the replies' slots (`Node::replies`). 2,048 native requests in
    /// flight, every third ended as `write_replies` and `job_lost` end one; then a `BodyReady` and
    /// a `Writable` on each of 2,048 chunk asks out, which no reply holds, and on each native
    /// request left, its job ended and its reply writing; then each request's own lookups: its
    /// fields read (`read_fields`), its body fed whole (`feed_bodies`), and its exchange lost
    /// (`job_lost`, whose `extract_if` read every reply). Before, each searched the replies.
    #[test]
    fn a_replys_half_is_found_by_its_exchange_with_no_scan() {
        const IN_FLIGHT: u64 = 2048;
        let mut replies: Keyed<(u64, ())> = Keyed::default();
        for exchange in 0..IN_FLIGHT {
            assert!(replies.push((exchange, ())).is_none());
        }
        for exchange in (0..IN_FLIGHT).filter(|exchange| exchange % 3 == 0) {
            assert!(replies.remove(exchange).is_some());
        }
        let native: Vec<u64> = replies.iter().map(|(exchange, _)| *exchange).collect();
        let chunks: Vec<u64> = (IN_FLIGHT..2 * IN_FLIGHT).collect();
        let (mut before, mut after) = ([0; 2], [0; 2]);
        probes();
        for &exchange in chunks.iter().chain(&native) {
            before[0] += 2 * searched(&replies, exchange);
            let held = exchange < IN_FLIGHT;
            assert_eq!(replies.contains(exchange), held, "BodyReady");
            assert_eq!(replies.contains(exchange), held, "Writable");
        }
        after[0] = probes();
        for &exchange in &native {
            before[1] += 2 * searched(&replies, exchange) + replies.len();
            assert!(replies.get_mut(exchange).is_some(), "its fields read");
            assert!(replies.get_mut(exchange).is_some(), "its body fed whole");
            assert!(replies.remove(exchange).is_some(), "its exchange lost");
        }
        after[1] = probes();
        println!(
            "{} native requests in flight, {} chunk asks out: BodyReady and Writable {} probes ({} \
             visited by the searches), each request's own lookups {} ({})",
            native.len(),
            chunks.len(),
            after[0],
            before[0],
            after[1],
            before[1]
        );
        assert_eq!(
            after[0],
            2 * (chunks.len() + native.len()),
            "a probe an event"
        );
        assert_eq!(after[1], 3 * native.len(), "a probe a lookup");
        assert!(replies.is_empty());
    }

    /// A read out as `ReadOut` holds one: its ask, and the exchange it is out on, if any (`via`).
    #[derive(Debug, PartialEq, Eq)]
    struct Out(u64, Option<u64>);

    impl Key for Out {
        type Of = u64;

        fn key(&self) -> u64 {
            self.0
        }
    }

    impl On for Out {
        type By = u64;

        fn on(&self) -> Option<u64> {
            self.1
        }
    }

    /// Pushes, each change of a second key (asked on an exchange, let go of it, asked on another),
    /// removals by key and by slot and a displacing push each keep both maps exact (the audit after
    /// each change), and every second key held finds its entry in one probe.
    #[test]
    fn a_keyed_on_lists_second_keys_follow_each_change() {
        let mut reads = KeyedOn::default();
        for ask in 0..64 {
            assert!(reads.push(Out(ask, None)).is_none());
            assert_eq!(reads.change(ask, |out| out.1 = Some(1000 + ask)), Some(()));
        }
        for ask in (0..64).filter(|ask| ask % 2 == 0) {
            reads.change(ask, |out| out.1 = None);
        }
        for ask in (0..64).filter(|ask| ask % 4 == 0) {
            reads.change(ask, |out| out.1 = Some(2000 + ask));
        }
        assert_eq!(reads.remove(1), Some(Out(1, Some(1001))));
        let first = reads.at(0).map(|out| out.0);
        assert_eq!(reads.take(0).map(|out| out.0), first);
        assert_eq!(reads.push(Out(5, None)), Some(Out(5, Some(1005))));
        reads.change_at(0, |out| out.1 = Some(3000));
        assert_eq!(reads.change(64, |_| ()), None, "none holds it");
        probes();
        assert_eq!(reads.on(3000), reads.at(0).map(|out| out.0));
        assert_eq!(reads.on(1005), None, "let go with the entry it displaced");
        assert_eq!(reads.on(1003), Some(3));
        assert_eq!(reads.on(1002), None, "let go");
        assert_eq!(reads.on(2004), Some(4));
        assert_eq!(reads.on(1001), None, "removed");
        assert_eq!(probes(), 6, "one probe a lookup");
        assert_eq!(reads.len(), 62);
        reads.audit();
    }

    /// The node's task's events on the exchanges of the reads it asks itself (`Node::reads_out`),
    /// each found by its exchange in a probe of their second keys (`KeyedOn::on`): 1,024 reads out
    /// on exchanges, every third answered and taken out; a `Reply` on each read left
    /// (`Event::Reply`'s `read_on`, then `read_replied`'s and its read's), a `Refused` on each of
    /// 2,048 exchanges no read is out on (`ask_lost`'s), and each read's answer whole
    /// (`read_came`'s, then its removal). Before, each lookup searched the reads out.
    #[test]
    fn a_reads_exchange_is_found_with_no_scan() {
        const READS: u64 = 1024;
        let mut reads = KeyedOn::default();
        for ask in 0..READS {
            reads.push(Out(ask, None));
            reads.change(ask, |out| out.1 = Some(READS + ask));
        }
        for ask in (0..READS).filter(|ask| ask % 3 == 0) {
            assert!(reads.remove(ask).is_some());
        }
        let out: Vec<u64> = reads.iter().filter_map(|read| read.1).collect();
        let others: Vec<u64> = (4 * READS..4 * READS + 2048).collect();
        let searched = |reads: &KeyedOn<Out>, exchange: u64| {
            reads
                .iter()
                .position(|read| read.1 == Some(exchange))
                .map_or(reads.len(), |at| at + 1)
        };
        let (mut before, mut after) = ([0; 3], [0; 3]);
        probes();
        for &exchange in &out {
            before[0] += 2 * searched(&reads, exchange);
            assert!(reads.on(exchange).is_some(), "Event::Reply's");
            let ask = reads.on(exchange).unwrap();
            assert!(reads.get(ask).is_some(), "read_replied's");
        }
        after[0] = probes();
        for &exchange in &others {
            before[1] += searched(&reads, exchange);
            assert!(reads.on(exchange).is_none(), "ask_lost's");
        }
        after[1] = probes();
        for &exchange in &out {
            before[2] += searched(&reads, exchange);
            let ask = reads.on(exchange).unwrap();
            assert!(reads.remove(ask).is_some(), "read_came's");
        }
        after[2] = probes();
        println!(
            "{} reads out: Reply {} probes ({} visited by the searches), Refused on {} other \
             exchanges {} ({}), answers whole {} ({})",
            out.len(),
            after[0],
            before[0],
            others.len(),
            after[1],
            before[1],
            after[2],
            before[2]
        );
        assert_eq!(after[0], 3 * out.len(), "three probes a Reply");
        assert_eq!(after[1], others.len(), "a probe a Refused");
        assert_eq!(after[2], 2 * out.len(), "two probes an answer");
        assert_eq!(reads.len(), 0);
    }

    /// `Event::Refused` lets go of what a peer asked this node's members on the exchange that
    /// ended (`Node::commanded`, `Node::reading`), each found by its exchange in one probe
    /// (`KeyedOn::on`): 1,024 commands and 1,024 reads peers asked on exchanges, and 64 of each
    /// this node asked itself, which no exchange finds; a `Refused` on each of 2,048 exchanges
    /// neither holds (a chunk's, a native request's), then on each command's and each read's.
    /// Before, each `Refused` walked both lists whole (`retain`).
    #[test]
    fn a_refused_exchange_finds_its_command_and_read_with_no_scan() {
        const ASKED: u64 = 1024;
        const OWN: u64 = 64;
        let (mut commanded, mut reading) = (KeyedOn::default(), KeyedOn::default());
        for number in 0..ASKED + OWN {
            let on = (number < ASKED).then(|| 10_000 + number);
            commanded.push(Out(number, on));
            reading.push(Out(number, on.map(|exchange| exchange + ASKED)));
        }
        let others: Vec<u64> = (100_000..100_000 + 2048).collect();
        let (mut before, mut after) = ([0; 2], [0; 2]);
        probes();
        for &exchange in &others {
            before[0] += commanded.len() + reading.len();
            assert!(commanded.on(exchange).is_none() && reading.on(exchange).is_none());
        }
        after[0] = probes();
        for exchange in 10_000..10_000 + ASKED {
            before[1] += commanded.len() + reading.len();
            let command = commanded.on(exchange).unwrap();
            assert!(commanded.remove(command).is_some());
            let read = reading.on(exchange + ASKED).unwrap();
            assert!(reading.remove(read).is_some());
        }
        after[1] = probes();
        println!(
            "{ASKED} commands and {ASKED} reads asked by peers, {OWN} of each its own: Refused on \
             {} other exchanges {} probes ({} visited by the walks), on each command's and read's \
             {} ({})",
            others.len(),
            after[0],
            before[0],
            after[1],
            before[1]
        );
        assert_eq!(after[0], 2 * others.len(), "two probes a Refused");
        assert_eq!(after[1], 4 * usize::try_from(ASKED).unwrap(), "two a list");
        let own = usize::try_from(OWN).unwrap();
        assert_eq!((commanded.len(), reading.len()), (own, own));
    }

    /// A command waiting as `Commanded` holds one: its ask, the exchange a peer asked it on, if
    /// any, its group, and its commands' sessions and serials, each with its answer once applied.
    struct Asked {
        ask: u64,
        on: Option<u64>,
        group: u128,
        commands: Vec<(u64, u64, Option<u64>)>,
    }

    impl Key for Asked {
        type Of = u64;

        fn key(&self) -> u64 {
            self.ask
        }
    }

    impl On for Asked {
        type By = u64;

        fn on(&self) -> Option<u64> {
            self.on
        }

        fn class(&self) -> Option<u128> {
            Some(self.group)
        }
    }

    impl Waits for Asked {
        type For = (u128, u64, u64);
        type Part = (u64, u64, Option<u64>);

        fn parts(&self) -> &[Self::Part] {
            &self.commands
        }

        fn parts_mut(&mut self) -> &mut [Self::Part] {
            &mut self.commands
        }

        fn waits(&self, part: &Self::Part) -> Option<(u128, u64, u64)> {
            match part.2 {
                None => Some((self.group, part.0, part.1)),
                Some(_) => None,
            }
        }
    }

    /// An entry of `ask` on `group`, asked on `on`, of a command of each `(session, serial)`.
    fn asked(ask: u64, on: Option<u64>, group: u128, commands: &[(u64, u64)]) -> Asked {
        let commands = commands
            .iter()
            .map(|&(session, serial)| (session, serial, None))
            .collect();
        Asked {
            ask,
            on,
            group,
            commands,
        }
    }

    /// Pushes, an answer to each part, a command asked again in two entries and in one entry
    /// twice, removals by key and by slot, a retain and a displacing push each keep what the parts
    /// wait on exact (the audit after each change); each thing waited on finds its least waiter in
    /// one probe, the next once that one is answered.
    #[test]
    fn a_waiting_lists_parts_follow_each_change() {
        let mut commanded = Waiting::default();
        for ask in 0..16 {
            let on = (ask % 2 == 0).then(|| 100 + ask);
            commanded.push(asked(ask, on, 7, &[(ask, 1), (ask, 2), (ask, 3)]));
        }
        // Asked again: by a later entry, and twice in one, each after the first.
        commanded.push(asked(20, None, 7, &[(3, 2), (3, 2)]));
        assert_eq!(commanded.first((7, 3, 2)), Some((3, 1)));
        assert_eq!(commanded.change_part(3, 1, |c| c.2 = Some(9)), Some(()));
        assert_eq!(commanded.first((7, 3, 2)), Some((20, 0)));
        commanded.change_part(20, 0, |c| c.2 = Some(9));
        assert_eq!(commanded.first((7, 3, 2)), Some((20, 1)));
        assert_eq!(commanded.remove(20).map(|a| a.ask), Some(20));
        assert_eq!(commanded.first((7, 3, 2)), None, "answered, then let go");
        // An answer taken back makes its part wait again; one past the parts changes nothing.
        commanded.change_part(3, 1, |c| c.2 = None);
        assert_eq!(commanded.first((7, 3, 2)), Some((3, 1)));
        assert_eq!(commanded.change_part(3, 3, |c| c.2 = Some(9)), None);
        assert_eq!(commanded.change_part(64, 0, |c| c.2 = Some(9)), None);
        // The same session and serial in another group waits apart.
        commanded.push(asked(21, None, 8, &[(3, 2)]));
        assert_eq!(commanded.first((8, 3, 2)), Some((21, 0)));
        assert_eq!(commanded.first((7, 3, 2)), Some((3, 1)));
        let first = commanded.at(0).map(|a| a.ask);
        assert_eq!(commanded.take(0).map(|a| a.ask), first);
        commanded.retain(|a| a.ask % 3 != 0);
        assert_eq!(commanded.first((7, 3, 2)), None, "its entry let go");
        let displaced = commanded.push(asked(4, Some(104), 7, &[(4, 9)]));
        assert_eq!(displaced.map(|a| a.commands.len()), Some(3));
        assert_eq!(commanded.first((7, 4, 1)), None, "the displaced entry's");
        assert_eq!(commanded.first((7, 4, 9)), Some((4, 0)));
        assert_eq!(commanded.on(104), Some(4));
        commanded.audit();
    }

    /// The node's answers to the commands it proposed (`answer_command`), each found by its
    /// range's applied answer in one probe of what the commands wait on (`Waiting::first`) and
    /// answered in one of its entry's slot (`Waiting::change_part`): 2,048 puts in flight, each an
    /// entry of three commands in one range; their answers applied in the order they were
    /// proposed, each entry answered whole taken out (`reply_commands`) and another put proposed
    /// in its place, until 2,048 more were answered. Before, each answer searched the entries in
    /// the list's order, and each command of the range's entries, for the first command of its
    /// session and serial waiting, then its entry's commands again to answer it.
    #[test]
    fn an_applied_answer_finds_its_command_with_no_scan() {
        const IN_FLIGHT: u64 = 2048;
        const GROUP: u128 = 7;
        let put = |ask: u64| {
            asked(
                ask,
                Some(10_000 + ask),
                GROUP,
                &[(ask, 1), (ask, 2), (ask, 3)],
            )
        };
        let mut commanded = Waiting::default();
        for ask in 0..IN_FLIGHT {
            commanded.push(put(ask));
        }
        // What `answer_command`'s search visited: each entry up to the one found, and each
        // command of each of the range's, then the found entry's commands up to its answer's.
        let searched = |commanded: &Waiting<Asked>, session: u64, serial: u64| {
            let mut visited = 0;
            for entry in commanded.iter() {
                visited += 1;
                if entry.group != GROUP {
                    continue;
                }
                for (at, &(s, n, answer)) in entry.commands.iter().enumerate() {
                    visited += 1;
                    if s == session && n == serial && answer.is_none() {
                        return visited + at + 1;
                    }
                }
            }
            visited
        };
        let (mut answered, mut before, mut after) = (0, 0, 0);
        probes();
        for ask in 0..IN_FLIGHT {
            for serial in 1..=3 {
                before += searched(&commanded, ask, serial);
                let (key, place) = commanded.first((GROUP, ask, serial)).unwrap();
                assert_eq!(
                    commanded.change_part(key, place, |c| c.2 = Some(serial)),
                    Some(())
                );
                answered += 1;
            }
            after += probes();
            assert!(commanded.remove(ask).is_some());
            commanded.push(put(IN_FLIGHT + ask));
            probes();
        }
        println!(
            "{IN_FLIGHT} puts in flight, three commands each: {answered} answers applied, {after} \
             probes ({before} entries and commands visited by the search), {} and {} an answer",
            after / answered,
            before / answered
        );
        assert_eq!(after, 2 * answered, "two probes an answer");
        assert_eq!(commanded.len(), usize::try_from(IN_FLIGHT).unwrap());
    }

    /// Marks, a mark made twice, removals from every slot and pops keep the slots exact (the audit
    /// after each change), and each key is marked once.
    #[test]
    fn a_marked_sets_slots_follow_each_mark_and_removal() {
        let mut marked = Marked::default();
        for key in 0..64_u64 {
            assert!(marked.insert(key));
        }
        assert!(!marked.insert(5), "marked once");
        assert!(marked.remove(0) && marked.remove(62) && marked.remove(63));
        assert!(!marked.remove(0), "gone");
        // 63 moved into 0's slot, then 61, the last, into 63's.
        assert_eq!(marked.pop(), Some(60), "the last held");
        assert!(marked.insert(0));
        assert_eq!(marked.last(), Some(0));
        assert_eq!(marked.len(), 61);
        marked.clear();
        assert_eq!((marked.len(), marked.pop()), (0, None));
    }

    /// The node's replies to the commands it proposed (`Node::reply_commands`), each turn
    /// visiting the commands answered whole (`Waiting::answered_into`), first answered first, each
    /// group the turn drove read
    /// once (`Node::checks`), and a group's commands through the group (`KeyedOn::of_class`) once
    /// the node leads it no longer: 2,048 puts in flight over four ranges, each an entry of three
    /// commands; at each of 8 turns the 64 oldest answered whole and replied, another proposed
    /// in each one's place and every range driven; then one range stops leading. Before, each turn
    /// walked every command held, reading each one's answers and its range's leadership, and a
    /// range that stopped leading was found by that walk.
    #[test]
    fn a_turns_replies_visit_what_moved_not_every_command() {
        const IN_FLIGHT: u64 = 2048;
        const GROUPS: u64 = 4;
        const ANSWERED: u64 = 64;
        const TURNS: u64 = 8;
        let group = |ask: u64| u128::from(ask % GROUPS);
        let put = |ask: u64| {
            asked(
                ask,
                Some(10_000 + ask),
                group(ask),
                &[(ask, 1), (ask, 2), (ask, 3)],
            )
        };
        let mut commanded = Waiting::default();
        for ask in 0..IN_FLIGHT {
            commanded.push(put(ask));
        }
        let mut checks = Marked::default();
        let (mut oldest, mut next) = (0, IN_FLIGHT);
        let (mut before, mut after, mut read) = (0, 0, 0);
        for _ in 0..TURNS {
            for ask in oldest..oldest + ANSWERED {
                for serial in 1..=3 {
                    let (key, place) = commanded.first((group(ask), ask, serial)).unwrap();
                    commanded.change_part(key, place, |c| c.2 = Some(serial));
                }
            }
            oldest += ANSWERED;
            for driven in 0..GROUPS {
                checks.insert(u128::from(driven));
            }
            before += commanded.len();
            // Each group driven read once: led still, none of its commands visited.
            while let Some(driven) = checks.pop() {
                assert!(commanded.of_class(driven).is_some());
                read += 1;
            }
            let mut replied = 0;
            let mut replying = Vec::new();
            commanded.answered_into(&mut replying);
            for ask in replying.drain(..) {
                let entry = commanded.remove(ask).unwrap();
                assert!(entry.commands.iter().all(|c| c.2.is_some()), "{ask}");
                assert_eq!(ask, oldest - ANSWERED + replied, "the first answered first");
                replied += 1;
            }
            assert_eq!(replied, ANSWERED);
            after += replied;
            for _ in 0..ANSWERED {
                commanded.push(put(next));
                next += 1;
            }
        }
        println!(
            "{IN_FLIGHT} puts in flight over {GROUPS} ranges, {ANSWERED} answered a turn: {after} \
             commands visited over {TURNS} turns and {read} groups read ({before} visited before)"
        );
        assert_eq!(before, usize::try_from(TURNS * IN_FLIGHT).unwrap());
        assert_eq!(after, TURNS * ANSWERED, "the commands answered alone");
        assert_eq!(read, TURNS * GROUPS, "each range driven read once a turn");
        // Range 1 stops leading: its commands alone are taken, through the group.
        let lost = 1;
        let mut taken = 0;
        while let Some(ask) = commanded.of_class(lost) {
            assert_eq!(commanded.remove(ask).map(|c| c.group), Some(lost));
            taken += 1;
        }
        assert_eq!(
            taken,
            IN_FLIGHT / GROUPS,
            "the lost range's commands, not every one"
        );
        assert!(commanded.iter().all(|c| c.group != lost));
        let mut replying = Vec::new();
        commanded.answered_into(&mut replying);
        assert!(replying.is_empty());
        commanded.audit();
    }

    /// A written-off node's request, by its own exchange, its old identity's class where it is the
    /// node's own and not a member's forward (`enrollments::Enrollment`).
    struct Request(u64, u64, bool);

    impl Key for Request {
        type Of = u64;

        fn key(&self) -> u64 {
            self.0
        }
    }

    impl On for Request {
        type By = u64;

        fn on(&self) -> Option<u64> {
            None
        }

        fn class(&self) -> Option<u128> {
            (!self.2).then(|| u128::from(self.1))
        }
    }

    /// A new request's check for a second of one node's (`Node::enrollment_read`), found by its
    /// old identity in one probe of the classes (`KeyedOn::of_class`): 1,024 requests held, every
    /// other one a member's forward, which no check finds; then 1,024 new ones checked, half a
    /// second of a node's held. Each answered request lets its class go, its set put aside for the
    /// next class, so as many come and go as are held and no set more is made. Before, each check
    /// walked every request held.
    #[test]
    fn a_second_request_of_one_node_is_found_with_no_scan() {
        const HELD: u64 = 1024;
        let mut enrollments = KeyedOn::default();
        for exchange in 0..HELD {
            enrollments.push(Request(exchange, 50_000 + exchange, exchange % 2 == 1));
        }
        let sets = enrollments.classes.len() + enrollments.aside.len();
        let (mut before, mut seconds) = (0, 0);
        probes();
        for old in 0..HELD {
            // A second of each node held, or one no request names.
            let asked = if old % 2 == 0 {
                50_000 + old
            } else {
                90_000 + old
            };
            before += enrollments.len();
            seconds += usize::from(enrollments.of_class(u128::from(asked)).is_some());
        }
        let after = probes();
        assert_eq!(
            seconds,
            usize::try_from(HELD / 2).unwrap(),
            "each node's own held"
        );
        assert_eq!(after, usize::try_from(HELD).unwrap(), "one probe a check");
        // Answered and taken out, a new node's request in each one's place.
        for exchange in 0..HELD {
            assert!(enrollments.remove(exchange).is_some());
            enrollments.push(Request(
                HELD + exchange,
                70_000 + exchange,
                exchange % 2 == 1,
            ));
        }
        assert_eq!(
            enrollments.classes.len() + enrollments.aside.len(),
            sets,
            "each class gone put its set aside for the next"
        );
        println!(
            "{HELD} requests held: {HELD} checks, {after} probes ({before} requests visited before)"
        );
        enrollments.audit();
    }

    /// A lost exchange (`Event::Refused`, a forward's bytes refused) lets go of the written-off
    /// node's request whose own exchange it was and puts one forwarded on it back to waiting
    /// (`Node::enrollment_lost`), each found in one probe (`Node::enrollments`, by its own exchange
    /// and by its forward's): 1,024 requests held, every other one forwarded to the leader; a loss
    /// on each of 2,048 exchanges neither holds, then on each forward's, then on each request's
    /// own. Before, each loss walked the requests for its own until none was left, then every
    /// request for a forward on it.
    #[test]
    fn a_lost_exchange_finds_its_enrollment_with_no_scan() {
        const HELD: u64 = 1024;
        let mut enrollments = KeyedOn::default();
        for exchange in 0..HELD {
            let forward = (exchange % 2 == 0).then(|| 10_000 + exchange);
            enrollments.push(Out(exchange, forward));
        }
        // What `enrollment_lost` visited: each request up to the one whose own exchange it was,
        // and again past it to find no other, then every request for a forward on it.
        let walked = |enrollments: &KeyedOn<Out>, exchange: u64| {
            let held = enrollments.len();
            match enrollments.iter().position(|e| e.0 == exchange) {
                Some(at) => at + 1 + 2 * (held - 1),
                None => 2 * held,
            }
        };
        let others: Vec<u64> = (100_000..100_000 + 2048).collect();
        let forwards: Vec<u64> = (0..HELD).step_by(2).map(|e| 10_000 + e).collect();
        let (mut before, mut after) = ([0; 3], [0; 3]);
        probes();
        for &exchange in &others {
            before[0] += walked(&enrollments, exchange);
            assert!(enrollments.remove(exchange).is_none() && enrollments.on(exchange).is_none());
        }
        after[0] = probes();
        for &exchange in &forwards {
            before[1] += walked(&enrollments, exchange);
            assert!(enrollments.remove(exchange).is_none());
            let own = enrollments.on(exchange).unwrap();
            assert_eq!(enrollments.change(own, |e| e.1 = None), Some(()), "waiting");
        }
        after[1] = probes();
        for exchange in 0..HELD {
            before[2] += walked(&enrollments, exchange);
            assert!(enrollments.remove(exchange).is_some() && enrollments.on(exchange).is_none());
        }
        after[2] = probes();
        println!(
            "{HELD} requests held, every other forwarded: a loss on {} other exchanges {} probes \
             ({} visited by the walks), on each forward's {} ({}), on each request's own {} ({})",
            others.len(),
            after[0],
            before[0],
            after[1],
            before[1],
            after[2],
            before[2]
        );
        assert_eq!(after[0], 2 * others.len(), "two probes a loss");
        assert_eq!(after[1], 3 * forwards.len(), "three, its request changed");
        assert_eq!(
            after[2],
            2 * usize::try_from(HELD).unwrap(),
            "two probes a loss"
        );
        assert!(enrollments.is_empty());
    }

    /// Pushes, removals from the front, the middle and the end, a displacing push and a
    /// renumbering at a `u64`'s end each keep the places exact (the audit after each change), and
    /// the entries left come out in the order they came.
    #[test]
    fn a_queues_places_keep_its_order_through_each_removal() {
        let mut queued = Queued::default();
        for key in 0..64 {
            assert_eq!(queued.push_back(Entry(key, "first")), None);
        }
        assert_eq!(queued.pop_front(), Some(Entry(0, "first")));
        assert_eq!(queued.remove(31), Some(Entry(31, "first")), "the middle");
        assert_eq!(queued.remove(63), Some(Entry(63, "first")), "the last");
        assert_eq!(queued.remove(31), None, "gone");
        assert_eq!(queued.push_back(Entry(5, "again")), Some(Entry(5, "first")));
        queued.next = u64::MAX;
        assert_eq!(queued.push_back(Entry(64, "renumbered")), None);
        assert_eq!(queued.next, 62, "the 61 held, then the one pushed");
        let order: Vec<u64> = queued.iter().map(|entry| entry.0).collect();
        let mut expected: Vec<u64> = (1..63).filter(|key| *key != 31 && *key != 5).collect();
        expected.extend([5, 64]);
        assert_eq!(order, expected);
        assert_eq!(queued.front(), Some(&Entry(1, "first")));
        assert_eq!(queued.len(), expected.len());
        while queued.pop_front().is_some() {}
        assert!(queued.is_empty());
    }

    /// An exchange lost lets go of its job's parked request and its body's feeding
    /// (`Node::job_lost`), each found by its exchange in one probe (`Queued::remove`,
    /// `Keyed::remove`): 1,024 requests parked, in the order they came, and 2,048 put bodies
    /// feeding; a loss on each feeding's exchange, then on each parked one's. Before, each loss
    /// walked every parked request (`retain`) and searched the feedings.
    #[test]
    fn a_lost_exchanges_parked_request_and_feeding_are_found_with_no_scan() {
        const PARKED: u64 = 1024;
        const FEEDING: u64 = 2048;
        let (mut parked, mut feeding) = (Queued::default(), Keyed::default());
        for exchange in 0..FEEDING {
            feeding.push(Entry(exchange, "feeding"));
        }
        for exchange in 10_000..10_000 + PARKED {
            parked.push_back(Entry(exchange, "parked"));
        }
        let (mut before, mut after) = ([0; 2], [0; 2]);
        probes();
        for exchange in 0..FEEDING {
            before[0] += parked.len() + searched(&feeding, exchange);
            assert!(parked.remove(exchange).is_none() && feeding.remove(exchange).is_some());
        }
        after[0] = probes();
        for exchange in (10_000..10_000 + PARKED).rev() {
            before[1] += parked.len() + feeding.len();
            assert!(parked.remove(exchange).is_some() && feeding.remove(exchange).is_none());
        }
        after[1] = probes();
        println!(
            "{PARKED} parked, {FEEDING} feeding: a loss on each feeding's exchange {} probes ({} \
             visited by the walks), on each parked one's {} ({})",
            after[0], before[0], after[1], before[1]
        );
        assert_eq!(
            after[0],
            2 * usize::try_from(FEEDING).unwrap(),
            "two probes a loss"
        );
        assert_eq!(
            after[1],
            2 * usize::try_from(PARKED).unwrap(),
            "two probes a loss"
        );
        assert!(parked.is_empty() && feeding.is_empty());
    }
}
