//! Lists the node's task finds an entry of by a key it holds: its exchanges' bodies going out and
//! coming in and its chunk asks out, each found by its exchange at every transport event on it,
//! with thousands in flight on a loaded node. Each entry's slot is kept in a hash map in step with
//! the list as an entry is pushed and swap-removed, as the host keeps its jobs' (`host::Slots`), so
//! an event finds its entry in one probe rather than a search of every entry.

use std::collections::HashMap;
use std::hash::Hash;

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
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Its entries, in no order a caller may rely on: a removal moves the last into its slot.
    pub(super) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.entries.iter()
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
    fn take(&mut self, at: usize) -> Option<T> {
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
    use super::{Key, Keyed};

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
    fn searched(keyed: &Keyed<Entry>, key: u64) -> usize {
        keyed
            .iter()
            .position(|entry| entry.0 == key)
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
}
