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
    fn slot(&self, key: T::Of) -> Option<usize> {
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
}

/// A [`Keyed`] list whose entries are also found by a second key while each holds one, in one
/// probe of `by`, the entry's own key there: kept in step as an entry is pushed, changed and
/// removed, a change made only through [`KeyedOn::change`] or [`KeyedOn::change_at`], which read
/// its second key before and after. Bound: one `by` an entry, so at most its entries'.
pub(super) struct KeyedOn<T: On> {
    keyed: Keyed<T>,
    by: HashMap<T::By, T::Of>,
}

impl<T: On> Default for KeyedOn<T> {
    /// None, which allocates nothing (`Keyed::default`, `HashMap::new`).
    fn default() -> Self {
        Self {
            keyed: Keyed::default(),
            by: HashMap::new(),
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

    /// Holds `entry`, last, as [`Keyed::push`]: an entry it displaces given back, its second key
    /// let go.
    pub(super) fn push(&mut self, entry: T) -> Option<T> {
        let (key, on) = (entry.key(), entry.on());
        let displaced = self.keyed.push(entry);
        if let Some(by) = displaced.as_ref().and_then(On::on) {
            self.by.remove(&by);
        }
        if let Some(by) = on {
            self.by.insert(by, key);
        }
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// The entry holding `key` changed by `change`, all but its key, its second key kept.
    pub(super) fn change<R>(&mut self, key: T::Of, change: impl FnOnce(&mut T) -> R) -> Option<R> {
        let entry = self.keyed.get_mut(key)?;
        let before = entry.on();
        let changed = change(entry);
        let after = entry.on();
        self.moved(key, before, after);
        Some(changed)
    }

    /// The entry at slot `at` changed by `change`, all but its key, its second key kept.
    pub(super) fn change_at<R>(
        &mut self,
        at: usize,
        change: impl FnOnce(&mut T) -> R,
    ) -> Option<R> {
        let entry = self.keyed.at_mut(at)?;
        let (key, before) = (entry.key(), entry.on());
        let changed = change(entry);
        let after = entry.on();
        self.moved(key, before, after);
        Some(changed)
    }

    /// The entry holding `key` went from second key `before` to `after`.
    fn moved(&mut self, key: T::Of, before: Option<T::By>, after: Option<T::By>) {
        if before != after {
            if let Some(by) = before {
                self.by.remove(&by);
            }
            if let Some(by) = after {
                self.by.insert(by, key);
            }
        }
        #[cfg(test)]
        self.audit();
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
        #[cfg(test)]
        self.audit();
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
    use super::{Key, Keyed, KeyedOn, On, Queued};

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
