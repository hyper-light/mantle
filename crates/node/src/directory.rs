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
//! The pinned table is the node's configuration for now: placement's map and the root range's rows
//! of nodes (node.md §1.5, §7) take its place once they run.

use std::net::SocketAddr;

use hyper_transport::{Authenticated, Directory, PeerId};
use mantle_protocol::classes::Role;

/// A peer as the directory holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerId,
    pub role: Role,
    /// Its certificate, DER, as its authority issued it.
    pub certificate: Vec<u8>,
}

/// The node's peers.
#[derive(Debug, Default)]
pub struct Peers {
    peers: Vec<(Peer, String)>,
    /// The clients admitted, each its id and the account it acts for, sorted by id.
    clients: Vec<(PeerId, String)>,
    /// The most clients held at once: the identities and handshakes the transport admits, or none
    /// where the node admits no client.
    most_clients: usize,
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
                    (peer, name)
                })
                .collect(),
            clients: Vec::new(),
            most_clients,
        }
    }

    /// The pinned peer named `id`.
    pub fn get(&self, id: PeerId) -> Option<&Peer> {
        self.peers.iter().map(|(p, _)| p).find(|p| p.id == id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.iter().map(|(p, _)| p)
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
            Err(_) => self.get(peer).map(|p| p.id.to_string()),
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
    }
}

impl Directory for Peers {
    type Role = Role;

    /// A pinned peer whose certificate the handshake verified, byte for byte, and for a dial only
    /// the peer dialed; else, for a connection a peer opened, the client the authority's
    /// certificate names, unless its id is a pinned peer's or names another account, or the
    /// clients held are at their bound.
    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        let certificate = authenticated.certificate?;
        if let Some(peer) = self
            .peers
            .iter()
            .map(|(p, _)| p)
            .find(|p| p.certificate.as_slice() == certificate)
        {
            if authenticated.dialed.is_some_and(|dialed| dialed != peer.id) {
                return None;
            }
            return Some((peer.id, peer.role));
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
            .find(|(p, _)| p.id == peer)
            .map(|(_, name)| name.as_str())
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
