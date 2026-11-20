//! The peers' chunk puts that came while the node had no room for their bytes, each held on its
//! own exchange, its body unread, first come first served, and read as room frees: a volume's
//! write answered gives back its room, and the oldest held put is read in that same turn. Refused
//! at once instead, as they were, a put's asker heard nothing when the room came back, and asked
//! again only at its next beat, a probe timeout, about a second (`host::Host::step`); a cell
//! writing 10,000 small puts a second keeps its volumes' queues full, so each refusal cost its put
//! up to a beat. A held put's asker is told at every half beat that it is held, as the asker of a
//! chunk a volume holds is (`Node::vouch_held`, research/77 D2): the vouch rests on the writes in
//! flight that hold the room, since a put is held only once its room was refused, which
//! `Storage::take_room` does only while some is taken. It is refused as before once the time its
//! request says it waits has passed, at most the stock patience (`waits_until`), or let go if its
//! exchange ends.
//!
//! Bound: at most the chunk exchanges the volumes take at once (`Storage::transfers`), and their
//! bodies at most the room the node takes chunks into (`Storage::room`), a body larger than the
//! room held alone as it is taken alone: an unread body is held by the transport, in its stream's
//! window, so the bytes held are bounded as the bytes read are. Past either a put is refused
//! `Budget` as it came, as every put was before.

use std::collections::VecDeque;

/// A put held for room: its exchange (`K`, the transport's `ExchangeId`), the bytes its body takes
/// of the room, when its request stops waiting on the shard's clock, and what it is read as once
/// it has room.
#[derive(Debug)]
pub(super) struct Held<K, T> {
    pub(super) exchange: K,
    pub(super) body: u64,
    pub(super) until_ns: u64,
    pub(super) what: T,
}

/// The held puts, in the order they came.
pub(super) struct Holds<K, T> {
    queue: VecDeque<Held<K, T>>,
    /// The bytes the held puts' bodies take of the room.
    bytes: u64,
    /// The earliest of their patiences, `None` while none is held.
    until_ns: Option<u64>,
    /// The storage's count of room given back as the held puts were last offered it
    /// (`Storage::freed`): they are offered it again only once it moved.
    freed: u64,
}

impl<K, T> Default for Holds<K, T> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            bytes: 0,
            until_ns: None,
            freed: 0,
        }
    }
}

impl<K: Copy + Eq, T> Holds<K, T> {
    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Holds `put` behind the others, within `(count, room)`, the most held at once and the bytes
    /// their bodies may take: `Err` with it past either, for its exchange to be refused.
    pub(super) fn hold(
        &mut self,
        put: Held<K, T>,
        (count, room): (usize, u64),
    ) -> Result<(), Held<K, T>> {
        let bytes = self.bytes.checked_add(put.body);
        let fits = bytes.is_some_and(|bytes| bytes <= room) || self.queue.is_empty();
        if self.queue.len() >= count || !fits {
            return Err(put);
        }
        self.bytes = bytes.unwrap_or(u64::MAX);
        self.until_ns = Some(self.until_ns.map_or(put.until_ns, |u| u.min(put.until_ns)));
        self.queue.push_back(put);
        Ok(())
    }

    /// Whether the held puts have anything to be offered at `now`: room given back since they
    /// were last offered it (`freed`, the storage's count of it), or the earliest patience passed.
    /// Nothing held, or nothing changed, nothing is visited.
    pub(super) fn due(&self, freed: u64, now: u64) -> bool {
        !self.queue.is_empty()
            && (freed != self.freed || self.until_ns.is_some_and(|until| now >= until))
    }

    /// When the earliest held put's request stops waiting, the node's wait bounded by it.
    pub(super) fn until_ns(&self) -> Option<u64> {
        self.until_ns
    }

    /// Takes out the held puts whose requests waited past `now`, in the order they came, each
    /// given to `refuse` for its exchange to be refused.
    pub(super) fn expire(&mut self, now: u64, mut refuse: impl FnMut(Held<K, T>)) {
        if self.until_ns.is_none_or(|until| now < until) {
            return;
        }
        let mut at = 0;
        while let Some(put) = self.queue.get(at) {
            if now < put.until_ns {
                at = at.saturating_add(1);
                continue;
            }
            if let Some(put) = self.queue.remove(at) {
                self.bytes = self.bytes.saturating_sub(put.body);
                refuse(put);
            }
        }
        self.until_ns = self.queue.iter().map(|put| put.until_ns).min();
    }

    /// The oldest held put, taken out where `take` takes the room its body needs: the one after it
    /// waits behind it, first come first served. `freed` is the storage's count of room given back,
    /// recorded as the room these were last offered.
    pub(super) fn admit(
        &mut self,
        freed: u64,
        take: impl FnOnce(u64) -> bool,
    ) -> Option<Held<K, T>> {
        self.freed = freed;
        let body = self.queue.front()?.body;
        if !take(body) {
            return None;
        }
        let put = self.queue.pop_front()?;
        self.bytes = self.bytes.saturating_sub(put.body);
        if self.until_ns == Some(put.until_ns) {
            self.until_ns = self.queue.iter().map(|put| put.until_ns).min();
        }
        Some(put)
    }

    /// Lets go of the put held on `exchange`, its exchange ended.
    pub(super) fn remove(&mut self, exchange: K) -> Option<Held<K, T>> {
        let at = self.queue.iter().position(|put| put.exchange == exchange)?;
        let put = self.queue.remove(at)?;
        self.bytes = self.bytes.saturating_sub(put.body);
        self.until_ns = self.queue.iter().map(|put| put.until_ns).min();
        Some(put)
    }

    /// The held puts' exchanges whose requests still wait at `now`: their askers are told they
    /// are held.
    pub(super) fn waiting(&self, now: u64) -> impl Iterator<Item = K> + '_ {
        self.queue
            .iter()
            .filter(move |put| put.until_ns > now)
            .map(|put| put.exchange)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(n: u64, body: u64, until_ns: u64) -> Held<u64, u64> {
        Held {
            exchange: n,
            body,
            until_ns,
            what: n,
        }
    }

    /// Puts are held within the count and the room, a body larger than the room alone, and past
    /// either refused.
    #[test]
    fn held_puts_are_bounded_by_the_exchanges_and_the_room_the_volumes_take() {
        let mut holds = Holds::default();
        let bounds = (3, 100);
        assert!(holds.hold(put(1, 60, 10), bounds).is_ok());
        assert!(holds.hold(put(2, 50, 10), bounds).is_err(), "past the room");
        assert!(holds.hold(put(3, 40, 10), bounds).is_ok());
        assert!(holds.hold(put(4, 0, 10), bounds).is_ok());
        assert!(holds.hold(put(5, 0, 10), bounds).is_err(), "past the count");
        assert_eq!(holds.waiting(0).count(), 3);
        let mut alone = Holds::default();
        assert!(alone.hold(put(6, 500, 10), bounds).is_ok(), "larger, alone");
        assert!(alone.hold(put(7, 1, 10), bounds).is_err());
    }

    /// Room given back offers the oldest held put first, and the ones behind it wait for it; with
    /// no room given back and no patience passed, nothing is due and nothing is visited.
    #[test]
    fn held_puts_are_read_first_come_first_served_as_room_frees() {
        let mut holds = Holds::default();
        for n in 1..=3 {
            holds.hold(put(n, 10, 1_000 + n), (8, 100)).unwrap();
        }
        assert!(!holds.due(0, 0), "nothing changed");
        assert!(holds.due(1, 0), "room given back");
        let mut asked = Vec::new();
        assert!(
            holds
                .admit(1, |body| {
                    asked.push(body);
                    false
                })
                .is_none()
        );
        assert_eq!(asked, [10], "the oldest alone is asked for its room");
        assert!(!holds.due(1, 0), "offered once for each room given back");
        let room = std::cell::Cell::new(15);
        let take = |body: u64| {
            let fits = body <= room.get();
            if fits {
                room.set(room.get() - body);
            }
            fits
        };
        assert_eq!(holds.admit(2, take).map(|p| p.what), Some(1));
        assert_eq!(holds.admit(2, take).map(|p| p.what), None);
        room.set(100);
        assert_eq!(holds.admit(3, take).map(|p| p.what), Some(2));
        assert_eq!(holds.admit(3, take).map(|p| p.what), Some(3));
        assert!(holds.is_empty() && !holds.due(4, u64::MAX));
    }

    /// A held put is refused once its own patience passes, the others kept in order; one whose
    /// exchange ended is let go; the earliest patience left bounds the node's wait.
    #[test]
    fn a_held_put_ends_at_its_patience_or_its_exchange() {
        let mut holds = Holds::default();
        holds.hold(put(1, 10, 50), (8, 100)).unwrap();
        holds.hold(put(2, 10, 20), (8, 100)).unwrap();
        holds.hold(put(3, 10, 80), (8, 100)).unwrap();
        assert_eq!(holds.until_ns(), Some(20));
        assert!(!holds.due(0, 19));
        assert!(holds.due(0, 20));
        let mut out = Vec::new();
        holds.expire(19, |put| out.push(put));
        assert!(out.is_empty());
        holds.expire(50, |put| out.push(put));
        assert_eq!(out.iter().map(|p| p.what).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(holds.until_ns(), Some(80));
        assert_eq!(holds.waiting(60).collect::<Vec<_>>(), [3]);
        assert_eq!(holds.remove(3).map(|p| p.what), Some(3));
        assert!(holds.is_empty() && holds.until_ns().is_none());
        assert!(holds.remove(3).is_none());
        // Its room is the room its body took, given back as it ends.
        holds.hold(put(4, 100, 90), (8, 100)).unwrap();
        assert!(holds.hold(put(5, 1, 90), (8, 100)).is_err());
        holds.remove(4);
        assert!(holds.hold(put(5, 100, 90), (8, 100)).is_ok());
    }
}
