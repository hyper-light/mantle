//! Which peer a connection names (docs/design/node.md §3.6): the cell's nodes and gateways this node
//! knows, each by the certificate its authority issued it and the name a dial checks that
//! certificate against, and the clients the authority issued certificates for the client role.
//! Where a peer listens is the node's to learn (`crate::node::Node::learn`). A connection whose
//! certificate names no peer is refused at its handshake, charged to no identity (hyper-transport
//! `Directory::identify`).
//!
//! A client is pinned nowhere: the handshake verified its certificate's chain to the authority
//! (rustls's client verifier, for clientAuth), and its names say which client it is and the account
//! it acts for (`mantle_cert::client_names`). The directory keeps each admitted client's account,
//! the principal its object operations act for, while the transport holds a connection of it: at
//! most as many as the transport admits identities and handshakes, the rest pruned once their
//! connections go ([`Peers::keep_clients`]).
//!
//! A node of a cell made by grants is known by its name under the authority rather than by a pin
//! (docs/design/membership.md §2): a certificate the authority issued under the node profile, its
//! chain verified at the handshake, names `node-ID`, and the node admits it when the cell's root
//! range, as this node's member of it last applied it, holds `ID` serving or leaving under that
//! certificate's fingerprint, or holds no member of `ID` and the certificate's grant is new by
//! the grant window ([`Peers::set_members`]). A pinned node is judged by the same rows once the
//! root range holds any: refused once its member is gone, or removed and kept as a learner no
//! more, or its grant is spent and the root range holds no member of its id. The pinned table
//! still names the gateways and the founders `mantle init` pinned, in a cell made before grants
//! or by them. A certificate the cell's issuer signed (membership.md §5.1), its issuer not the
//! authority, is a node's alone, admitted only as the member the root range holds under its
//! fingerprint, or before it joins at the fingerprint a committed re-grant names (rule 4,
//! [`Peers::authority`]).
//!
//! A node the cell wrote off asks for a new identity on the enrollment protocol
//! (`mantle_protocol::classes::ENROLL`, membership.md §5.1 step 3) under its old certificate, which
//! its handshake proved it holds the key of (RFC 8446 §4.4.3): admitted there alone, as
//! `Role::Enrolling`, where that certificate's fingerprint is a ledger entry's not banned and the
//! cell's setting is not `off`, and kept with the fingerprint and key the handshake proved
//! ([`Peers::enrolling`]), never ones the request names.

use std::net::SocketAddr;

use hyper_transport::{Authenticated, Directory, PeerId};
use mantle_meta::record::{FINGERPRINT, Grants, Ledgered, PUBLIC_KEY, Rejoin};
use mantle_meta::root;
use mantle_protocol::classes::{ENROLL, Role};

/// A peer as the directory holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerId,
    pub role: Role,
    /// Its certificate, DER, as its authority issued it.
    pub certificate: Vec<u8>,
}

/// A member of the cell as the root range holds it (docs/design/membership.md §1): its id, its
/// certificate's fingerprint, and whether its identity is admitted, serving or leaving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admitted {
    pub id: PeerId,
    pub fingerprint: [u8; FINGERPRINT],
    pub admitted: bool,
}

/// A written-off node admitted on the enrollment protocol (membership.md §5.1 step 3): its old id,
/// and its old certificate's fingerprint and the key it certifies, as its handshake proved them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Enrolling {
    pub old: PeerId,
    pub fingerprint: [u8; FINGERPRINT],
    pub key: [u8; PUBLIC_KEY],
}

/// A pinned peer as the directory holds it.
#[derive(Debug)]
struct Pinned {
    /// The peer, its certificate the one its handshakes must present byte for byte.
    peer: Peer,
    /// The name a dial checks its certificate against.
    name: String,
    /// The grant its certificate names, if it is a node's that names one.
    grant: Option<u64>,
    /// Its certificate's SHA-256, what its member's row is compared to.
    fingerprint: [u8; FINGERPRINT],
}

/// The node's peers.
#[derive(Debug, Default)]
pub struct Peers {
    /// Each pinned peer.
    peers: Vec<Pinned>,
    /// The clients admitted, each its id and the account it acts for, sorted by id.
    clients: Vec<(PeerId, String)>,
    /// The most clients held at once: the identities and handshakes the transport admits, or none
    /// where the node admits no client.
    most_clients: usize,
    /// The cell's members as this node's root range last held them, sorted by id: at most the
    /// cell's bound on nodes.
    members: Vec<Admitted>,
    /// The grant window as the root range last held it, and its width, the cell's bound on nodes;
    /// none in a cell made before grants, which admits no node it does not pin.
    window: Option<(Grants, u32)>,
    /// The name each node this node dials and pins nowhere is checked against, `node-ID`: its
    /// seeds and the members it learned, at most the cell's bound on nodes and its seeds.
    named: Vec<(PeerId, String)>,
    /// The nodes admitted by the grant window alone, granted and not yet members, sorted: at most
    /// the window's width, each kept while the transport holds a connection of it.
    joining: Vec<PeerId>,
    /// A granted node's seeds, sorted: the members its grant names, admitted by their names under
    /// the authority before this node's root range holds any row of them (membership.md §3 step
    /// 2), as it then admits them by their rows.
    seeds: Vec<PeerId>,
    /// The cell's authority's certificate, which tells a certificate it signed from one the cell's
    /// issuer under it signed ([`Peers::authority`]); none in a directory not told it, which takes
    /// every chain its handshake verified as the authority's.
    authority: Option<Vec<u8>>,
    /// The identities the root range re-granted and has yet to see join, sorted by their new ids:
    /// each its new id and its certificate's fingerprint, an entry of the ledger not banned (rule
    /// 4, membership.md §5.1). At most the ledger's entries, which the cell's nodes bound.
    regranted: Vec<(PeerId, [u8; FINGERPRINT])>,
    /// The written-off identities that may ask for a new one, sorted by their old ids: each its old
    /// id and its certificate's fingerprint, an entry of the ledger not banned, none where the
    /// cell's setting is `off`. At most the ledger's entries.
    ledger: Vec<(PeerId, [u8; FINGERPRINT])>,
    /// The written-off nodes admitted on the enrollment protocol, sorted by their old ids: one an
    /// entry of `ledger` at most, each kept while the transport holds a connection of it.
    enrolling: Vec<Enrolling>,
    /// The most enrolling nodes the node serves at once ([`Peers::enrollments_at_once`]): past
    /// them, one is refused once its connection is up.
    most_enrolling: usize,
}

impl Peers {
    /// The directory of `peers`, each known by its node name (`mantle_protocol::node_name`), which
    /// admits at most `most_clients` clients at once.
    pub fn new(peers: Vec<Peer>, most_clients: usize) -> Self {
        Self {
            peers: peers
                .into_iter()
                .map(|peer| {
                    let name = mantle_protocol::node_name(peer.id);
                    let grant = match peer.role {
                        Role::Node => mantle_cert::node_grant(&peer.certificate).ok().flatten(),
                        Role::Gateway | Role::Client | Role::Enrolling => None,
                    };
                    let fingerprint = mantle_cert::fingerprint(&peer.certificate);
                    Pinned {
                        peer,
                        name,
                        grant,
                        fingerprint,
                    }
                })
                .collect(),
            clients: Vec::new(),
            most_clients,
            members: Vec::new(),
            window: None,
            named: Vec::new(),
            joining: Vec::new(),
            seeds: Vec::new(),
            authority: None,
            regranted: Vec::new(),
            ledger: Vec::new(),
            enrolling: Vec::new(),
            most_enrolling: 0,
        }
    }

    /// Serves at most `most` enrolling nodes at once: each is answered by a command of the root
    /// range the node asks for it (membership.md §5.1 step 3), so the commands the node holds at
    /// once bound them.
    pub fn enrollments_at_once(&mut self, most: usize) {
        self.most_enrolling = most;
    }

    /// Takes `certificate` as the cell's authority's (membership.md §2 rule 4, §5.1): a node's
    /// certificate whose issuer is not its subject, the cell's issuer's, is admitted only as the
    /// member the root range holds under its fingerprint, never by a grant window or as a seed, and
    /// no client's certificate the issuer signed is admitted.
    pub fn authority(&mut self, certificate: Vec<u8>) {
        self.authority = Some(certificate);
    }

    /// Whether `certificate`, whose chain the handshake verified to the authority, was signed by
    /// the cell's issuer rather than the authority; unreadable, it is taken as the issuer's, which
    /// admits least.
    fn by_issuer(&self, certificate: &[u8]) -> bool {
        self.authority.as_ref().is_some_and(|authority| {
            !mantle_cert::issuer_is(certificate, authority).unwrap_or(false)
        })
    }

    /// Names `id` as a seed of this node's grant: a node it dials, admitted by its name under the
    /// authority until the root range's rows say otherwise.
    pub fn seed(&mut self, id: PeerId) {
        self.name(id);
        if let Err(at) = self.seeds.binary_search(&id) {
            self.seeds.insert(at, id);
        }
    }

    /// Whether `peer` is a node this directory admits: pinned as one and not refused, a member
    /// serving or leaving, or granted and joining.
    pub fn is_node(&self, peer: PeerId) -> bool {
        self.peers
            .iter()
            .find(|p| p.peer.id == peer)
            .is_some_and(|p| p.peer.role == Role::Node && !self.pinned_refused(p))
            || self
                .members
                .binary_search_by_key(&peer, |m| m.id)
                .ok()
                .and_then(|at| self.members.get(at))
                .is_some_and(|m| m.admitted)
            || self.joining.binary_search(&peer).is_ok()
            || self.seeds.binary_search(&peer).is_ok()
            || self
                .regranted
                .binary_search_by_key(&peer, |(id, _)| *id)
                .is_ok()
    }

    /// The pinned peer named `id`.
    pub fn get(&self, id: PeerId) -> Option<&Peer> {
        self.iter().find(|p| p.id == id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.iter().map(|p| &p.peer)
    }

    /// The cell's members and grant window as this node's root range holds them now, `width` the
    /// window's, the cell's bound on nodes (docs/design/membership.md §2), and its ledger of
    /// written-off identities and its `rejoin` setting (§5.1). The members whose identity this
    /// takes away, admitted before and not now, are given back, for the node to close their
    /// connections: a change applies to the handshakes after it, and a connection already
    /// identified keeps its peer until its owner closes it (hyper-transport
    /// `Endpoint::directory_mut`). So are the enrolling nodes whose entry no longer admits them.
    pub fn set_members(
        &mut self,
        mut members: Vec<Admitted>,
        (window, width): (Grants, u32),
        (ledger, rejoin): (&[(u64, Ledgered)], Rejoin),
    ) -> Vec<PeerId> {
        members.sort_unstable_by_key(|m| m.id);
        let pinned_refused = |peers: &Self| -> Vec<PeerId> {
            peers
                .peers
                .iter()
                .filter(|p| peers.pinned_refused(p))
                .map(|p| p.peer.id)
                .collect()
        };
        let refused_before = pinned_refused(self);
        let was = std::mem::take(&mut self.members);
        let was_regranted = std::mem::take(&mut self.regranted);
        self.members = members;
        self.window = Some((window, width));
        // Rule 4: a re-granted identity, its entry not banned, as the join judges it
        // (`mantle_meta::root`'s `regranted_as`).
        self.regranted = ledger
            .iter()
            .filter(|(_, entry)| !entry.banned)
            .filter_map(|(_, entry)| entry.regranted.as_ref())
            .map(|r| (r.node, r.fingerprint))
            .collect();
        self.regranted.sort_unstable();
        self.ledger = match rejoin {
            Rejoin::Off => Vec::new(),
            Rejoin::Automatic => ledger
                .iter()
                .filter(|(_, entry)| !entry.banned)
                .map(|(old, entry)| (*old, entry.fingerprint))
                .collect(),
        };
        self.ledger.sort_unstable();
        // Every identity admitted before is judged again under the new rows: a member gone or
        // removed, or a pinned node refused since, is withdrawn.
        let mut withdrawn: Vec<PeerId> = was
            .iter()
            .filter(|m| m.admitted)
            .filter(|m| !self.member_admitted(m.id, &m.fingerprint))
            .map(|m| m.id)
            .collect();
        withdrawn.extend(
            pinned_refused(self)
                .into_iter()
                .filter(|id| !refused_before.contains(id)),
        );
        withdrawn.extend(
            was_regranted
                .iter()
                .filter(|(id, fingerprint)| {
                    !self.member_admitted(*id, fingerprint)
                        && !self.regrant_admits(*id, fingerprint)
                })
                .map(|(id, _)| *id),
        );
        let ledger = &self.ledger;
        self.enrolling.retain(|e| {
            let kept = ledger.binary_search(&(e.old, e.fingerprint)).is_ok();
            if !kept {
                withdrawn.push(e.old);
            }
            kept
        });
        withdrawn.sort_unstable();
        withdrawn.dedup();
        withdrawn
    }

    /// Names `id`, a node this node dials and pins nowhere, by `node-ID`, the name its certificate
    /// is checked against (`Directory::server_name`).
    pub fn name(&mut self, id: PeerId) {
        if self.get(id).is_some() {
            return;
        }
        if let Err(at) = self.named.binary_search_by_key(&id, |(n, _)| *n) {
            self.named.insert(at, (id, mantle_protocol::node_name(id)));
        }
    }

    /// Forgets the name of `id`, a node this node dials no more.
    pub fn unname(&mut self, id: PeerId) {
        if let Ok(at) = self.named.binary_search_by_key(&id, |(n, _)| *n) {
            self.named.remove(at);
        }
    }

    /// Whether the root range holds `id` serving or leaving under the certificate whose
    /// fingerprint is `fingerprint`.
    fn member_admitted(&self, id: PeerId, fingerprint: &[u8; FINGERPRINT]) -> bool {
        self.members
            .binary_search_by_key(&id, |m| m.id)
            .ok()
            .and_then(|at| self.members.get(at))
            .is_some_and(|m| m.admitted && m.fingerprint == *fingerprint)
    }

    /// Whether a committed re-grant names `id` under `fingerprint` and the root range holds no
    /// member of it yet: rule 4, the issuer's certificate before its join (membership.md §5.1).
    fn regrant_admits(&self, id: PeerId, fingerprint: &[u8; FINGERPRINT]) -> bool {
        !self.is_member(id) && self.regranted.binary_search(&(id, *fingerprint)).is_ok()
    }

    /// The written-off node `old` admitted on the enrollment protocol, as its handshake proved
    /// its old certificate: what a member forwards for it, never what its request names.
    pub fn enrolling(&self, old: PeerId) -> Option<Enrolling> {
        self.enrolling
            .binary_search_by_key(&old, |e| e.old)
            .ok()
            .and_then(|at| self.enrolling.get(at))
            .copied()
    }

    /// Whether more written-off nodes are admitted on the enrollment protocol than the node serves
    /// at once: the one whose connection came up last is refused, typed (`Refusal::Identities`), as a
    /// client past the node's bound is, and asks again after.
    pub fn enrollments_past_bound(&self) -> bool {
        self.enrolling.len() > self.most_enrolling
    }

    /// The node `old` asking on the enrollment protocol under `certificate`, its old one, if a
    /// ledger entry not banned holds that certificate's fingerprint under `old` and the cell's
    /// setting is not `off` (membership.md §5.1 step 3): one connection's admission, kept with the
    /// fingerprint and key the handshake proved.
    fn enroll(&mut self, certificate: &[u8], dialed: Option<PeerId>) -> Option<(PeerId, Role)> {
        // A member dials no node to enroll it.
        if dialed.is_some() {
            return None;
        }
        let names = mantle_cert::node_names(certificate).ok()?;
        let old = mantle_protocol::node_id(names.name)?;
        let fingerprint = mantle_cert::fingerprint(certificate);
        self.ledger.binary_search(&(old, fingerprint)).ok()?;
        let key: [u8; PUBLIC_KEY] = mantle_cert::public_key(certificate).ok()?.try_into().ok()?;
        let enrolling = Enrolling {
            old,
            fingerprint,
            key,
        };
        match self.enrolling.binary_search_by_key(&old, |e| e.old) {
            Ok(at) => *self.enrolling.get_mut(at)? = enrolling,
            Err(at) => self.enrolling.insert(at, enrolling),
        }
        Some((old, Role::Enrolling))
    }

    /// Whether the root range holds a member of `id`, in any state.
    fn is_member(&self, id: PeerId) -> bool {
        self.members.binary_search_by_key(&id, |m| m.id).is_ok()
    }

    /// Whether a node certificate of `id` under `grant` names a node the cell removed: its grant
    /// spent by the window and no member of its id held.
    fn removed(&self, id: PeerId, grant: Option<u64>) -> bool {
        match (grant, &self.window) {
            (Some(grant), Some((window, width))) => {
                !self.is_member(id) && !root::admits(window, grant, *width)
            }
            _ => false,
        }
    }

    /// Whether `pinned` is a node the root range's rows refuse (membership.md §2 rule 1), as
    /// they refuse a node pinned nowhere: a member of its id that they hold neither serving nor
    /// leaving under its certificate, nor removed and kept as a learner (gone, or removed and
    /// dropped), or none and its grant spent (delisted). A gateway is judged by no row.
    fn pinned_refused(&self, pinned: &Pinned) -> bool {
        if pinned.peer.role != Role::Node {
            return false;
        }
        if self.is_member(pinned.peer.id) {
            return !self.member_admitted(pinned.peer.id, &pinned.fingerprint);
        }
        self.removed(pinned.peer.id, pinned.grant)
    }

    /// Whether a node certificate of `id` under `grant`, with `fingerprint`, is admitted by the
    /// root range's rows: a member serving or leaving under it, or no member and a grant the
    /// window has not spent.
    fn node_admitted(
        &self,
        id: PeerId,
        grant: Option<u64>,
        fingerprint: &[u8; FINGERPRINT],
    ) -> bool {
        if self.is_member(id) {
            return self.member_admitted(id, fingerprint);
        }
        if self.seeds.binary_search(&id).is_ok() && !self.removed(id, grant) {
            return true;
        }
        match (grant, &self.window) {
            (Some(grant), Some((window, width))) => root::admits(window, grant, *width),
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// The principal `peer`'s operations act for: a client's account, a pinned peer's id; none for
    /// a client whose connections have all gone.
    pub fn principal(&self, peer: PeerId) -> Option<String> {
        match self.clients.binary_search_by_key(&peer, |(id, _)| *id) {
            Ok(at) => self.clients.get(at).map(|(_, account)| account.clone()),
            Err(_) if self.get(peer).is_some() || self.is_member(peer) => Some(peer.to_string()),
            Err(_) => None,
        }
    }

    /// The clients admitted now.
    pub fn clients(&self) -> usize {
        self.clients.len()
    }

    /// Keeps the clients, joining nodes and enrolling nodes that hold a connection, `held` the
    /// transport's identified connections (hyper-transport `Endpoint::established`), and forgets
    /// the rest: those whose connections went, and those identified and then refused admission.
    pub fn keep_clients(&mut self, held: &[(PeerId, SocketAddr)]) {
        self.clients
            .retain(|(id, _)| held.iter().any(|(peer, _)| peer == id));
        self.joining
            .retain(|id| held.iter().any(|(peer, _)| peer == id));
        self.enrolling
            .retain(|e| held.iter().any(|(peer, _)| *peer == e.old));
    }
}

impl Directory for Peers {
    type Role = Role;

    /// A pinned peer whose certificate the handshake verified, byte for byte, and for a dial only
    /// the peer dialed, unless it is a node the root range's rows refuse; else, for a connection
    /// a peer opened, the client the authority's certificate names, unless its id is a pinned
    /// peer's or names another account, or the clients held are at their bound.
    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        let certificate = authenticated.certificate?;
        // The enrollment protocol admits a written-off identity's certificate alone, and nothing
        // else is admitted on it.
        if authenticated.protocol == ENROLL {
            return self.enroll(certificate, authenticated.dialed);
        }
        if let Some(pinned) = self
            .peers
            .iter()
            .find(|p| p.peer.certificate.as_slice() == certificate)
        {
            if authenticated
                .dialed
                .is_some_and(|dialed| dialed != pinned.peer.id)
                || self.pinned_refused(pinned)
            {
                return None;
            }
            return Some((pinned.peer.id, pinned.peer.role));
        }
        let by_issuer = self.by_issuer(certificate);
        // A node's certificate, known by its name under the authority (membership.md §2).
        if let Ok(names) = mantle_cert::node_names(certificate) {
            let id = mantle_protocol::node_id(names.name)?;
            if authenticated.dialed.is_some_and(|dialed| dialed != id) || self.get(id).is_some() {
                return None;
            }
            let fingerprint = mantle_cert::fingerprint(certificate);
            // The issuer's: a member's under the fingerprint the root range committed, rule 2, or
            // before its join the fingerprint a committed re-grant names, rule 4, and nothing
            // else; its grant is the issuer's, which the authority's window does not judge.
            if by_issuer {
                if !self.member_admitted(id, &fingerprint) && !self.regrant_admits(id, &fingerprint)
                {
                    return None;
                }
                return Some((id, Role::Node));
            }
            if !self.node_admitted(id, names.grant, &fingerprint) {
                return None;
            }
            if !self.is_member(id)
                && self.seeds.binary_search(&id).is_err()
                && let Err(at) = self.joining.binary_search(&id)
            {
                let width = self.window.as_ref().map_or(0, |(_, width)| *width);
                if self.joining.len() >= usize::try_from(width).unwrap_or(usize::MAX) {
                    return None;
                }
                self.joining.insert(at, id);
            }
            return Some((id, Role::Node));
        }
        if authenticated.dialed.is_some() || by_issuer {
            return None;
        }
        let names = mantle_cert::client_names(certificate).ok()?;
        let id = mantle_protocol::client_id(names.name)?;
        if self.get(id).is_some() {
            return None;
        }
        match self.clients.binary_search_by_key(&id, |(c, _)| *c) {
            Ok(at) => {
                let (_, account) = self.clients.get(at)?;
                if account != names.account {
                    return None;
                }
            }
            Err(at) => {
                if self.clients.len() >= self.most_clients {
                    return None;
                }
                self.clients.insert(at, (id, names.account.to_string()));
            }
        }
        Some((id, Role::Client))
    }

    fn server_name(&self, peer: PeerId) -> Option<&str> {
        self.peers
            .iter()
            .find(|p| p.peer.id == peer)
            .map(|p| p.name.as_str())
            .or_else(|| {
                self.named
                    .binary_search_by_key(&peer, |(n, _)| *n)
                    .ok()
                    .and_then(|at| self.named.get(at))
                    .map(|(_, name)| name.as_str())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_cert::{KeyPair, Serial, Validity};
    use mantle_meta::record::Regranted;

    fn peer(id: u64, certificate: &[u8]) -> Peer {
        Peer {
            id,
            role: Role::Node,
            certificate: certificate.to_vec(),
        }
    }

    fn seen(certificate: &[u8], dialed: Option<PeerId>) -> Authenticated<'_> {
        Authenticated {
            certificate: Some(certificate),
            protocol: b"mantle/1",
            dialed,
        }
    }

    #[test]
    fn a_peer_is_named_by_its_certificate_and_a_dial_only_by_the_peer_dialed() {
        let mut peers = Peers::new(vec![peer(2, b"two"), peer(3, b"three")], 0);
        assert_eq!(peers.identify(&seen(b"two", None)), Some((2, Role::Node)));
        assert_eq!(
            peers.identify(&seen(b"three", Some(3))),
            Some((3, Role::Node))
        );
        assert_eq!(peers.identify(&seen(b"three", Some(2))), None);
        assert_eq!(peers.identify(&seen(b"four", None)), None);
        assert_eq!(peers.server_name(3), Some("node-3"));
        assert_eq!(peers.server_name(4), None);
        assert_eq!(peers.principal(2), Some("2".into()));
    }

    fn client(id: u64, account: &str) -> Vec<u8> {
        let authority = KeyPair::generate().unwrap();
        let key = KeyPair::generate().unwrap();
        mantle_cert::client(
            &authority,
            "cell",
            key.public_key(),
            (&mantle_protocol::client_name(id), account),
            (
                Validity {
                    not_before: 0,
                    not_after: u64::from(u32::MAX),
                },
                Serial::from_octets([7; 16]),
            ),
        )
        .unwrap()
    }

    fn granted(authority: &KeyPair, id: u64, grant: u64) -> Vec<u8> {
        let key = KeyPair::generate().unwrap();
        mantle_cert::granted_node(
            authority,
            "cell",
            key.public_key(),
            (&mantle_protocol::node_name(id), grant),
            (
                Validity {
                    not_before: 0,
                    not_after: u64::from(u32::MAX),
                },
                Serial::from_octets([7; 16]),
            ),
        )
        .unwrap()
    }

    fn member(id: u64, certificate: &[u8], admitted: bool) -> Admitted {
        Admitted {
            id,
            fingerprint: mantle_cert::fingerprint(certificate),
            admitted,
        }
    }

    fn validity() -> (Validity, Serial) {
        (
            Validity {
                not_before: 0,
                not_after: u64::from(u32::MAX),
            },
            Serial::from_octets([7; 16]),
        )
    }

    /// A node's certificate the cell's issuer signed is a node's alone (membership.md §2 rule 4,
    /// §5.1): admitted as the member the root range holds under its fingerprint, and never by the
    /// authority's grant window, as a seed, or as a client's; the authority's own are judged as
    /// before. A directory told no authority takes every chain as the authority's.
    #[test]
    fn an_issuer_signed_certificate_is_admitted_only_at_a_members_fingerprint() {
        let authority_key = KeyPair::generate().unwrap();
        let authority =
            mantle_cert::authority(&authority_key, "cell", validity().0, validity().1).unwrap();
        let issuer_key = KeyPair::generate().unwrap();
        mantle_cert::issuer(
            &authority_key,
            "cell",
            issuer_key.public_key(),
            "cell issuer",
            validity(),
        )
        .unwrap();
        let issued = |id: u64, grant: u64| {
            mantle_cert::granted_node(
                &issuer_key,
                "cell issuer",
                KeyPair::generate().unwrap().public_key(),
                (&mantle_protocol::node_name(id), grant),
                validity(),
            )
            .unwrap()
        };
        let (seven, eight) = (issued(7, 1), issued(8, 2));
        let granted_nine = granted(&authority_key, 9, 3);
        let mut peers = Peers::new(Vec::new(), 2);
        peers.authority(authority);
        peers.seed(8);
        assert!(
            peers
                .set_members(Vec::new(), (Grants::default(), 3), (&[], Rejoin::Automatic))
                .is_empty()
        );
        // New by the authority's window, and a seed: neither admits the issuer's.
        assert_eq!(peers.identify(&seen(&seven, None)), None);
        assert_eq!(peers.identify(&seen(&eight, Some(8))), None);
        assert_eq!(
            peers.identify(&seen(&granted_nine, None)),
            Some((9, Role::Node))
        );
        // Joined: the member under its fingerprint, and no other certificate of its id.
        peers.set_members(
            vec![member(7, &seven, true)],
            (Grants::default(), 3),
            (&[], Rejoin::Automatic),
        );
        assert_eq!(peers.identify(&seen(&seven, None)), Some((7, Role::Node)));
        assert_eq!(
            peers.identify(&seen(&seven, Some(7))),
            Some((7, Role::Node))
        );
        assert_eq!(peers.identify(&seen(&issued(7, 1), None)), None);
        peers.set_members(
            vec![member(7, &seven, false)],
            (Grants::default(), 3),
            (&[], Rejoin::Automatic),
        );
        assert_eq!(peers.identify(&seen(&seven, None)), None);
        // A client's the issuer signed is no client.
        let issued_client = mantle_cert::client(
            &issuer_key,
            "cell issuer",
            KeyPair::generate().unwrap().public_key(),
            (&mantle_protocol::client_name(5), "123456789012"),
            validity(),
        )
        .unwrap();
        assert_eq!(peers.identify(&seen(&issued_client, None)), None);
        let authority_client = mantle_cert::client(
            &authority_key,
            "cell",
            KeyPair::generate().unwrap().public_key(),
            (&mantle_protocol::client_name(5), "123456789012"),
            validity(),
        )
        .unwrap();
        assert_eq!(
            peers.identify(&seen(&authority_client, None)),
            Some((5, Role::Client))
        );
        // Told no authority, as before: every chain the authority's.
        let mut untold = Peers::new(Vec::new(), 0);
        untold.set_members(Vec::new(), (Grants::default(), 3), (&[], Rejoin::Automatic));
        assert_eq!(untold.identify(&seen(&seven, None)), Some((7, Role::Node)));
    }

    fn ledgered(certificate: &[u8], banned: bool, regranted: Option<(u64, &[u8])>) -> Ledgered {
        Ledgered {
            order: 1,
            grant: 1,
            fingerprint: mantle_cert::fingerprint(certificate),
            banned,
            regranted: regranted.map(|(node, certificate)| Regranted {
                node,
                grant: 1,
                fingerprint: mantle_cert::fingerprint(certificate),
                key: mantle_cert::public_key(certificate)
                    .unwrap()
                    .try_into()
                    .unwrap(),
                certificate: certificate.to_vec(),
            }),
        }
    }

    fn enrolling_seen(certificate: &[u8], dialed: Option<PeerId>) -> Authenticated<'_> {
        Authenticated {
            certificate: Some(certificate),
            protocol: ENROLL,
            dialed,
        }
    }

    /// The cell's authority, its certificate and its issuer's key, and a node certificate the
    /// issuer signs.
    fn issuer_cell() -> (KeyPair, Vec<u8>, KeyPair) {
        let authority_key = KeyPair::generate().unwrap();
        let authority =
            mantle_cert::authority(&authority_key, "cell", validity().0, validity().1).unwrap();
        (authority_key, authority, KeyPair::generate().unwrap())
    }

    fn issued_by(issuer_key: &KeyPair, id: u64, grant: u64) -> Vec<u8> {
        mantle_cert::granted_node(
            issuer_key,
            "cell issuer",
            KeyPair::generate().unwrap().public_key(),
            (&mantle_protocol::node_name(id), grant),
            validity(),
        )
        .unwrap()
    }

    /// Rule 4 (membership.md §2, §5.1): a certificate the cell's issuer signed is admitted before
    /// its join at the fingerprint a committed re-grant names, and at no other; a ban since
    /// refuses it and gives it back to close; once joined, its member's row admits it.
    #[test]
    fn rule_4_admits_an_issuer_signed_certificate_at_a_committed_regrant_alone() {
        let (authority_key, authority, issuer_key) = issuer_cell();
        let old = granted(&authority_key, 4, 1);
        let new = issued_by(&issuer_key, 9, 1);
        let mut peers = Peers::new(Vec::new(), 2);
        peers.authority(authority);
        let window = (Grants::default(), 3);
        let committed = vec![(4, ledgered(&old, false, Some((9, &new))))];
        let members = vec![member(4, &old, false)];
        assert!(
            peers
                .set_members(
                    members.clone(),
                    window.clone(),
                    (&committed, Rejoin::Automatic)
                )
                .is_empty()
        );
        assert_eq!(peers.identify(&seen(&new, None)), Some((9, Role::Node)));
        assert_eq!(peers.identify(&seen(&new, Some(9))), Some((9, Role::Node)));
        assert!(peers.is_node(9));
        // Another certificate of its id, and one no committed re-grant names: refused.
        assert_eq!(
            peers.identify(&seen(&issued_by(&issuer_key, 9, 1), None)),
            None
        );
        assert_eq!(
            peers.identify(&seen(&issued_by(&issuer_key, 10, 2), None)),
            None
        );
        // The re-grant not yet committed: refused.
        let open = vec![(4, ledgered(&old, false, None))];
        let mut before = Peers::new(Vec::new(), 2);
        before.authority(peers.authority.clone().unwrap());
        before.set_members(members.clone(), window.clone(), (&open, Rejoin::Automatic));
        assert_eq!(before.identify(&seen(&new, None)), None);
        // Banned since: refused, and given back for its connections to close.
        let banned = vec![(4, ledgered(&old, true, Some((9, &new))))];
        assert_eq!(
            peers.set_members(members, window.clone(), (&banned, Rejoin::Automatic)),
            vec![9]
        );
        assert_eq!(peers.identify(&seen(&new, None)), None);
        // Joined, its predecessor's entry dropped: its member's row admits it.
        peers.set_members(
            vec![member(9, &new, true)],
            window,
            (&[], Rejoin::Automatic),
        );
        assert_eq!(peers.identify(&seen(&new, None)), Some((9, Role::Node)));
    }

    /// The enrollment protocol (membership.md §5.1 step 3) admits a written-off node under its old
    /// certificate where that certificate's fingerprint is a ledger entry's, not banned, the
    /// setting not `off`, as an enrolling node alone, kept with the fingerprint and key its
    /// handshake proved; another certificate, a client's, a dial, a banned entry and the setting
    /// `off` are refused on it, and the old certificate stays refused as a node's.
    #[test]
    fn the_enrollment_protocol_admits_a_ledgered_old_certificate_alone() {
        let authority = KeyPair::generate().unwrap();
        let old_key = KeyPair::generate().unwrap();
        let old = mantle_cert::granted_node(
            &authority,
            "cell",
            old_key.public_key(),
            (&mantle_protocol::node_name(4), 1),
            validity(),
        )
        .unwrap();
        let mut peers = Peers::new(Vec::new(), 2);
        peers.enrollments_at_once(1);
        let window = (Grants::default(), 3);
        let gone = || vec![member(4, &old, false)];
        let ledger = vec![(4, ledgered(&old, false, None))];
        peers.set_members(gone(), window.clone(), (&ledger, Rejoin::Automatic));
        assert_eq!(
            peers.identify(&enrolling_seen(&old, None)),
            Some((4, Role::Enrolling))
        );
        // What the member forwards is what the handshake proved.
        let proved = peers.enrolling(4).unwrap();
        assert_eq!(proved.old, 4);
        assert_eq!(proved.fingerprint, mantle_cert::fingerprint(&old));
        assert_eq!(proved.key.as_slice(), old_key.public_key());
        assert!(!peers.is_node(4));
        assert!(!peers.enrollments_past_bound());
        // Its member is gone: refused on the transport's own protocol.
        assert_eq!(peers.identify(&seen(&old, None)), None);
        // A dial, a certificate the ledger does not hold, another of the old id, a client's.
        assert_eq!(peers.identify(&enrolling_seen(&old, Some(4))), None);
        assert_eq!(
            peers.identify(&enrolling_seen(&granted(&authority, 5, 2), None)),
            None
        );
        assert_eq!(
            peers.identify(&enrolling_seen(&granted(&authority, 4, 1), None)),
            None
        );
        assert_eq!(
            peers.identify(&enrolling_seen(&client(4, "123456789012"), None)),
            None
        );
        // Banned: refused, and the enrolling node given back for its connection to close.
        let banned = vec![(4, ledgered(&old, true, None))];
        assert_eq!(
            peers.set_members(gone(), window.clone(), (&banned, Rejoin::Automatic)),
            vec![4]
        );
        assert_eq!(peers.enrolling(4), None);
        assert_eq!(peers.identify(&enrolling_seen(&old, None)), None);
        // The setting `off` re-grants none; `automatic` again admits it.
        peers.set_members(gone(), window.clone(), (&ledger, Rejoin::Off));
        assert_eq!(peers.identify(&enrolling_seen(&old, None)), None);
        peers.set_members(gone(), window, (&ledger, Rejoin::Automatic));
        assert_eq!(
            peers.identify(&enrolling_seen(&old, None)),
            Some((4, Role::Enrolling))
        );
    }

    /// Past the enrollments the node serves at once the directory says so, for the node to refuse
    /// the last typed; once a connection goes, there is room again.
    #[test]
    fn enrollments_past_the_bound_are_refused() {
        let authority = KeyPair::generate().unwrap();
        let (four, five) = (granted(&authority, 4, 1), granted(&authority, 5, 2));
        let mut peers = Peers::new(Vec::new(), 0);
        peers.enrollments_at_once(1);
        let ledger = vec![
            (4, ledgered(&four, false, None)),
            (5, ledgered(&five, false, None)),
        ];
        peers.set_members(
            vec![member(4, &four, false), member(5, &five, false)],
            (Grants::default(), 3),
            (&ledger, Rejoin::Automatic),
        );
        assert_eq!(
            peers.identify(&enrolling_seen(&four, None)),
            Some((4, Role::Enrolling))
        );
        assert!(!peers.enrollments_past_bound());
        assert_eq!(
            peers.identify(&enrolling_seen(&five, None)),
            Some((5, Role::Enrolling))
        );
        assert!(peers.enrollments_past_bound());
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        peers.keep_clients(&[(4, addr)]);
        assert!(!peers.enrollments_past_bound());
        assert_eq!(peers.enrolling(5), None);
    }

    /// membership.md §2's rules: a granted node's certificate is admitted while its grant is new
    /// by the window, then as the member the root range holds under its fingerprint, and not once
    /// it is gone or removed; a dial checks the id dialed; another certificate under a member's id
    /// is refused.
    #[test]
    fn a_granted_node_is_admitted_by_the_window_then_by_its_member_row() {
        let authority = KeyPair::generate().unwrap();
        let (two, three) = (granted(&authority, 2, 1), granted(&authority, 3, 2));
        let mut peers = Peers::new(Vec::new(), 0);
        // No root range applied yet: a cell made before grants pins its nodes.
        assert_eq!(peers.identify(&seen(&two, None)), None);
        let empty = Grants::default();
        assert!(
            peers
                .set_members(Vec::new(), (empty.clone(), 3), (&[], Rejoin::Automatic))
                .is_empty()
        );
        assert_eq!(peers.identify(&seen(&two, None)), Some((2, Role::Node)));
        assert_eq!(peers.identify(&seen(&two, Some(2))), Some((2, Role::Node)));
        assert_eq!(peers.identify(&seen(&two, Some(3))), None);
        // Node 2 joined, grant 1 spent: admitted by its row.
        let spent = Grants {
            top: 1,
            floor: 1,
            seen: vec![1],
        };
        let rows = vec![member(2, &two, true)];
        assert!(
            peers
                .set_members(rows, (spent.clone(), 3), (&[], Rejoin::Automatic))
                .is_empty()
        );
        assert_eq!(peers.identify(&seen(&two, None)), Some((2, Role::Node)));
        assert_eq!(peers.identify(&seen(&three, None)), Some((3, Role::Node)));
        // Another certificate under node 2's id and grant is refused.
        let impostor = granted(&authority, 2, 1);
        assert_eq!(peers.identify(&seen(&impostor, None)), None);
        // Gone: refused, and its connections are to close.
        let rows = vec![member(2, &two, false)];
        assert_eq!(
            peers.set_members(rows, (spent.clone(), 3), (&[], Rejoin::Automatic)),
            vec![2]
        );
        assert_eq!(peers.identify(&seen(&two, None)), None);
        // Removed: its grant spent, no row: refused, and so is a pinned copy of it.
        assert!(
            peers
                .set_members(Vec::new(), (spent.clone(), 3), (&[], Rejoin::Automatic))
                .is_empty()
        );
        assert_eq!(peers.identify(&seen(&two, None)), None);
        let mut pinned = Peers::new(vec![peer(2, &two)], 0);
        assert_eq!(pinned.identify(&seen(&two, None)), Some((2, Role::Node)));
        assert_eq!(
            pinned.set_members(Vec::new(), (spent, 3), (&[], Rejoin::Automatic)),
            vec![2]
        );
        assert_eq!(pinned.identify(&seen(&two, None)), None);
        // A node dialed by its seed name is checked against it.
        peers.name(3);
        assert_eq!(peers.server_name(3), Some("node-3"));
        peers.unname(3);
        assert_eq!(peers.server_name(3), None);
    }

    /// A client is named by its certificate's names alone, acts for the account they give, and is
    /// held within the bound until its connections go; a dial, a pinned peer's id, another account
    /// under a held id, and a client past the bound are refused.
    #[test]
    fn a_client_is_admitted_by_its_names_within_the_bound_and_pruned_as_it_goes() {
        let mut peers = Peers::new(vec![peer(2, b"two")], 2);
        let seven = client(7, "111111111111");
        assert_eq!(peers.identify(&seen(&seven, None)), Some((7, Role::Client)));
        assert_eq!(peers.principal(7), Some("111111111111".into()));
        assert_eq!(peers.identify(&seen(&seven, Some(7))), None);
        assert_eq!(peers.identify(&seen(&client(2, "1"), None)), None);
        assert_eq!(
            peers.identify(&seen(&client(7, "222222222222"), None)),
            None
        );
        assert_eq!(
            peers.identify(&seen(&client(8, "222222222222"), None)),
            Some((8, Role::Client))
        );
        assert_eq!(peers.identify(&seen(&client(9, "1"), None)), None);
        assert_eq!(peers.clients(), 2);
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        peers.keep_clients(&[(8, addr), (2, addr)]);
        assert_eq!(peers.principal(7), None);
        assert_eq!(peers.principal(8), Some("222222222222".into()));
        assert_eq!(
            peers.identify(&seen(&client(9, "1"), None)),
            Some((9, Role::Client))
        );
        let none = Peers::new(Vec::new(), 0).identify(&seen(&seven, None));
        assert_eq!(none, None);
    }
}
