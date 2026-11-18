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
//! or by them.

use std::net::SocketAddr;

use hyper_transport::{Authenticated, Directory, PeerId};
use mantle_meta::record::{FINGERPRINT, Grants};
use mantle_meta::root;
use mantle_protocol::classes::Role;

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
                        Role::Gateway | Role::Client => None,
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
        }
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
    }

    /// The pinned peer named `id`.
    pub fn get(&self, id: PeerId) -> Option<&Peer> {
        self.iter().find(|p| p.id == id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.iter().map(|p| &p.peer)
    }

    /// The cell's members and grant window as this node's root range holds them now, `width` the
    /// window's, the cell's bound on nodes (docs/design/membership.md §2). The members whose
    /// identity this takes away, admitted before and not now, are given back, for the node to
    /// close their connections: a change applies to the handshakes after it, and a connection
    /// already identified keeps its peer until its owner closes it (hyper-transport
    /// `Endpoint::directory_mut`).
    pub fn set_members(
        &mut self,
        mut members: Vec<Admitted>,
        window: Grants,
        width: u32,
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
        self.members = members;
        self.window = Some((window, width));
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

    /// Keeps the clients that hold a connection, `held` the transport's identified connections
    /// (hyper-transport `Endpoint::established`), and forgets the rest: those whose connections
    /// went, and those identified and then refused admission.
    pub fn keep_clients(&mut self, held: &[(PeerId, SocketAddr)]) {
        self.clients
            .retain(|(id, _)| held.iter().any(|(peer, _)| peer == id));
        self.joining
            .retain(|id| held.iter().any(|(peer, _)| peer == id));
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
        // A node's certificate, known by its name under the authority (membership.md §2).
        if let Ok(names) = mantle_cert::node_names(certificate) {
            let id = mantle_protocol::node_id(names.name)?;
            if authenticated.dialed.is_some_and(|dialed| dialed != id) || self.get(id).is_some() {
                return None;
            }
            let fingerprint = mantle_cert::fingerprint(certificate);
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
        if authenticated.dialed.is_some() {
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
        assert!(peers.set_members(Vec::new(), empty.clone(), 3).is_empty());
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
        assert!(peers.set_members(rows, spent.clone(), 3).is_empty());
        assert_eq!(peers.identify(&seen(&two, None)), Some((2, Role::Node)));
        assert_eq!(peers.identify(&seen(&three, None)), Some((3, Role::Node)));
        // Another certificate under node 2's id and grant is refused.
        let impostor = granted(&authority, 2, 1);
        assert_eq!(peers.identify(&seen(&impostor, None)), None);
        // Gone: refused, and its connections are to close.
        let rows = vec![member(2, &two, false)];
        assert_eq!(peers.set_members(rows, spent.clone(), 3), vec![2]);
        assert_eq!(peers.identify(&seen(&two, None)), None);
        // Removed: its grant spent, no row: refused, and so is a pinned copy of it.
        assert!(peers.set_members(Vec::new(), spent.clone(), 3).is_empty());
        assert_eq!(peers.identify(&seen(&two, None)), None);
        let mut pinned = Peers::new(vec![peer(2, &two)], 0);
        assert_eq!(pinned.identify(&seen(&two, None)), Some((2, Role::Node)));
        assert_eq!(pinned.set_members(Vec::new(), spent, 3), vec![2]);
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
