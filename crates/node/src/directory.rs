//! Which peer a connection names (docs/design/node.md §3.6): the cell's nodes this node knows, each
//! by the certificate its authority issued it and the name a dial checks that certificate against.
//! Where a peer listens is the node's to learn (`crate::node::Node::learn`). A connection whose
//! certificate the directory does not hold is refused at its handshake, charged to no identity
//! (hyper-transport `Directory::identify`).
//!
//! The table is the node's configuration for now: placement's map and the root range's rows of
//! nodes (node.md §1.5, §7) take its place once they run.

use hyper_transport::{Authenticated, Directory, PeerId};

use crate::classes::Role;

/// A peer as the directory holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerId,
    pub role: Role,
    /// Its certificate, DER, as its authority issued it.
    pub certificate: Vec<u8>,
}

/// The name a node's certificate carries and a dial checks it against.
pub fn node_name(id: PeerId) -> String {
    format!("node-{id}")
}

/// The node's peers.
#[derive(Debug, Default)]
pub struct Peers {
    peers: Vec<(Peer, String)>,
}

impl Peers {
    /// The directory of `peers`, each known by [`node_name`].
    pub fn new(peers: Vec<Peer>) -> Self {
        Self {
            peers: peers
                .into_iter()
                .map(|peer| {
                    let name = node_name(peer.id);
                    (peer, name)
                })
                .collect(),
        }
    }

    /// The peer named `id`.
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
}

impl Directory for Peers {
    type Role = Role;

    /// The peer whose certificate the handshake verified, byte for byte; for a dial, only the peer
    /// dialed.
    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        let certificate = authenticated.certificate?;
        let peer = self
            .peers
            .iter()
            .map(|(p, _)| p)
            .find(|p| p.certificate.as_slice() == certificate)?;
        if authenticated.dialed.is_some_and(|dialed| dialed != peer.id) {
            return None;
        }
        Some((peer.id, peer.role))
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

    fn peer(id: u64, certificate: &[u8]) -> Peer {
        Peer {
            id,
            role: Role::Node,
            certificate: certificate.to_vec(),
        }
    }

    #[test]
    fn a_peer_is_named_by_its_certificate_and_a_dial_only_by_the_peer_dialed() {
        let mut peers = Peers::new(vec![peer(2, b"two"), peer(3, b"three")]);
        let seen = |certificate: &'static [u8], dialed| Authenticated {
            certificate: Some(certificate),
            protocol: b"mantle/1",
            dialed,
        };
        assert_eq!(peers.identify(&seen(b"two", None)), Some((2, Role::Node)));
        assert_eq!(
            peers.identify(&seen(b"three", Some(3))),
            Some((3, Role::Node))
        );
        assert_eq!(peers.identify(&seen(b"three", Some(2))), None);
        assert_eq!(peers.identify(&seen(b"four", None)), None);
        assert_eq!(peers.server_name(3), Some("node-3"));
        assert_eq!(peers.server_name(4), None);
    }
}
