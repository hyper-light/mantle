//! The peers a node's configuration names by a host name rather than an address, and when each is
//! looked up again (node.md §1.7 step 5; research/78, "Open"). A name stands for whatever address
//! its holder has now: a Kubernetes pod made again comes back at another address under the same
//! name, so a name is looked up as the node starts and again whenever the node's link to its peer
//! ends, or, while it does not resolve, at the peer's backoff, the one the node dials it again at
//! ([`bounds::backed_off`], [`bounds::pause`]). One lookup runs at a time, through hyper-rt's
//! resolver off the node's shards; a changed address is taken as `Node::learn` takes one.

use std::net::SocketAddr;
use std::time::Duration;

use mantle_protocol::bounds;

use crate::process::Endpoint;

/// One peer named by a host name: where its line says it listens, the addresses its name last
/// gave, and when it is next looked up.
#[derive(Debug)]
struct Named {
    id: u64,
    at: (Endpoint, Endpoint),
    known: Option<(SocketAddr, SocketAddr)>,
    /// When its next lookup may start, in the runtime's nanoseconds; none while it needs none.
    due: Option<u64>,
    /// The pause its next failed lookup doubles.
    backoff: Duration,
    /// Its failed lookups since its name last resolved, which draws each pause.
    attempt: u32,
}

/// A peer named by a host name as the node starts: its id, where its line says it listens (QUIC's
/// endpoint and the plane's), and what its name gave then, none where it did not resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Start {
    pub id: u64,
    pub at: (Endpoint, Endpoint),
    pub known: Option<(SocketAddr, SocketAddr)>,
}

/// What a lookup's answer changes.
#[derive(Debug, PartialEq, Eq)]
pub enum Answered {
    /// The name gives other addresses than it last did, or its first: the node dials them.
    Moved(SocketAddr, SocketAddr),
    /// The name gives the addresses it last did.
    Same,
    /// The name did not resolve, why; it is looked up again at the peer's backoff.
    Unresolved(String),
}

/// The node's named peers and the lookup in flight, at most one.
#[derive(Debug)]
pub struct Names {
    peers: Vec<Named>,
    looking: Option<u64>,
    /// The node's own id, whose draws jitter its pauses as its other backoffs' are.
    local: u64,
}

impl Names {
    /// The named peers as the node starts, at `now`: each with the addresses its name gave then,
    /// none for one that did not resolve, which is looked up again after a first pause.
    pub fn new(local: u64, named: Vec<Start>, now: u64) -> Self {
        let initial = bounds::initial_pto();
        let peers = named
            .into_iter()
            .map(|Start { id, at, known }| Named {
                id,
                at,
                due: match known {
                    Some(_) => None,
                    None => Some(after(now, bounds::pause(initial, local, 0))),
                },
                known,
                backoff: initial,
                attempt: 0,
            })
            .collect();
        Self {
            peers,
            looking: None,
            local,
        }
    }

    /// Whether `id` is a named peer.
    pub fn names(&self, id: u64) -> bool {
        self.peers.iter().any(|p| p.id == id)
    }

    /// The node's link to `id` ended at `now`: its name is looked up again now, unless a lookup of it
    /// is in flight or already due sooner.
    pub fn ended(&mut self, id: u64, now: u64) {
        if self.looking == Some(id) {
            return;
        }
        if let Some(peer) = self.peers.iter_mut().find(|p| p.id == id) {
            peer.due = Some(peer.due.map_or(now, |due| due.min(now)));
        }
    }

    /// The next lookup to start, when none is in flight: the peer due soonest, where its line says it
    /// listens, and how long the lookup waits before it asks, its due time less `now`.
    pub fn next(&mut self, now: u64) -> Option<(u64, (Endpoint, Endpoint), u64)> {
        if self.looking.is_some() {
            return None;
        }
        let peer = self
            .peers
            .iter()
            .filter(|p| p.due.is_some())
            .min_by_key(|p| p.due)?;
        let wait = peer.due.map_or(0, |due| due.saturating_sub(now));
        self.looking = Some(peer.id);
        Some((peer.id, peer.at.clone(), wait))
    }

    /// The lookup of `id` came back at `now`, `found`: what it changes. A name that resolves needs
    /// no lookup until the node's link to its peer ends again; one that does not is looked up again
    /// at the peer's backoff, doubled.
    pub fn answered(
        &mut self,
        id: u64,
        found: Result<(SocketAddr, SocketAddr), String>,
        now: u64,
    ) -> Answered {
        if self.looking == Some(id) {
            self.looking = None;
        }
        let local = self.local;
        let Some(peer) = self.peers.iter_mut().find(|p| p.id == id) else {
            return Answered::Same;
        };
        match found {
            Ok(addresses) => {
                peer.due = None;
                peer.backoff = bounds::initial_pto();
                peer.attempt = 0;
                if peer.known == Some(addresses) {
                    Answered::Same
                } else {
                    peer.known = Some(addresses);
                    Answered::Moved(addresses.0, addresses.1)
                }
            }
            Err(why) => {
                peer.attempt = peer.attempt.saturating_add(1);
                peer.backoff = bounds::backed_off(peer.backoff);
                peer.due = Some(after(now, bounds::pause(peer.backoff, local, peer.attempt)));
                Answered::Unresolved(why)
            }
        }
    }
}

/// `pause` past `now`, in the runtime's nanoseconds; the clock's end where it would pass that, a
/// time never reached.
fn after(now: u64, pause: Duration) -> u64 {
    now.saturating_add(u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(host: &str) -> (Endpoint, Endpoint) {
        (
            Endpoint {
                host: host.to_owned(),
                port: 7000,
            },
            Endpoint {
                host: host.to_owned(),
                port: 7001,
            },
        )
    }

    fn start(id: u64, known: Option<(SocketAddr, SocketAddr)>) -> Start {
        Start {
            id,
            at: at(&format!("peer-{id}")),
            known,
        }
    }

    fn addresses(last: u8) -> (SocketAddr, SocketAddr) {
        (
            SocketAddr::from(([10, 0, 0, last], 7000)),
            SocketAddr::from(([10, 0, 0, last], 7001)),
        )
    }

    fn nanos(d: Duration) -> u64 {
        u64::try_from(d.as_nanos()).unwrap()
    }

    #[test]
    fn a_name_that_resolved_at_the_start_is_looked_up_again_only_when_its_link_ends() {
        let mut names = Names::new(1, vec![start(2, Some(addresses(2)))], 0);
        assert_eq!(names.next(0), None, "nothing is due");
        names.ended(2, 5);
        let (id, _, wait) = names.next(5).unwrap();
        assert_eq!((id, wait), (2, 0));
        // One lookup at a time: no second while the first is in flight.
        names.ended(2, 6);
        assert_eq!(names.next(6), None);
        // The pod came back at another address: the node dials it there.
        let moved = names.answered(2, Ok(addresses(9)), 7);
        assert_eq!(moved, Answered::Moved(addresses(9).0, addresses(9).1));
        assert_eq!(
            names.next(7),
            None,
            "resolved: due again only when its link ends"
        );
        names.ended(2, 8);
        names.next(8).unwrap();
        assert_eq!(names.answered(2, Ok(addresses(9)), 9), Answered::Same);
    }

    #[test]
    fn a_name_that_does_not_resolve_is_asked_again_at_a_backoff_that_doubles_to_its_bound() {
        let begun = 1_000;
        let mut names = Names::new(1, vec![start(2, None)], begun);
        let (_, _, first) = names.next(begun).unwrap();
        let initial = nanos(bounds::initial_pto());
        // The first pause is drawn between half the initial backoff and all of it.
        assert!(first >= initial / 2 && first <= initial, "{first}");
        let mut now = begun;
        let mut backoff = bounds::initial_pto();
        for _ in 0..8 {
            now = now.saturating_add(first);
            let answered = names.answered(2, Err("NXDOMAIN".into()), now);
            assert_eq!(answered, Answered::Unresolved("NXDOMAIN".into()));
            backoff = bounds::backed_off(backoff);
            let (id, _, wait) = names.next(now).unwrap();
            assert_eq!(id, 2);
            assert!(
                wait >= nanos(backoff) / 2 && wait <= nanos(backoff),
                "{wait} against {backoff:?}"
            );
            assert!(backoff <= bounds::idle_timeout());
        }
        assert_eq!(backoff, bounds::idle_timeout(), "doubled to its bound");
        // Once it resolves the backoff starts over and nothing more is due.
        let found = names.answered(2, Ok(addresses(3)), now);
        assert_eq!(found, Answered::Moved(addresses(3).0, addresses(3).1));
        assert_eq!(names.next(now), None);
    }

    #[test]
    fn the_peer_due_soonest_is_looked_up_first_and_an_ended_link_comes_before_a_pause() {
        let mut names = Names::new(1, vec![start(2, None), start(3, Some(addresses(3)))], 0);
        assert!(names.names(2) && names.names(3) && !names.names(4));
        names.ended(3, 0);
        let (id, _, wait) = names.next(0).unwrap();
        assert_eq!(
            (id, wait),
            (3, 0),
            "an ended link is due now, before a first pause"
        );
        assert_eq!(names.answered(3, Ok(addresses(3)), 1), Answered::Same);
        let (id, _, _) = names.next(1).unwrap();
        assert_eq!(id, 2);
        // An answer for a peer no line names changes nothing.
        assert_eq!(names.answered(4, Ok(addresses(4)), 2), Answered::Same);
    }
}
