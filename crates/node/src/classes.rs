//! What a node's QUIC connections carry (docs/design/node.md §3.1): each message's kind, the class
//! it travels in, and the roles that may send it. A class is decided by what a message is and who
//! sends it, never by the stream a peer chose (audit §13.3). Consensus control that fits a datagram
//! travels on the sealed plane instead, ahead of everything (`crate::route`).
//!
//! | Kind | Class | Sent by | Carries |
//! |---|---|---|---|
//! | `Raft` | replication, on a lane | nodes | a range's Raft message: appends with entries, and control too large for a datagram |
//! | `Command` | request, an exchange | anyone | a gateway's or client's commands to a range |
//! | `Snapshot` | bulk, an exchange | nodes | a range's snapshot to a member that lags past the log's start |
//!
//! The frame bound of each class is the largest message of that kind the node's ranges can make,
//! stated once a process ([`Bounds`]), so a receiver refuses a larger frame before it reserves
//! memory for it (node.md §3.2).

use std::sync::OnceLock;

use hyper_transport::{Classes, Embedded};

/// A message's kind on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Raft,
    Command,
    Snapshot,
    Read,
}

/// The classes a node's QUIC connections share credit by, most urgent first (node.md §3.1, §3.3):
/// their order is their urgency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Class {
    Replication,
    Request,
    Bulk,
}

/// What a peer is to this node, as its certificate names it (node.md §3.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// A node hosting range replicas.
    Node,
    /// A gateway: requests to ranges, never a Raft message.
    Gateway,
    /// A client of mantle's own protocol.
    Client,
}

/// The largest message of each class the node's ranges make, in bytes: a frame past its class's
/// bound is refused before it is read (hyper-transport `Classes::frame_bound`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    pub replication: u64,
    pub request: u64,
    pub bulk: u64,
}

/// Why the bounds could not be stated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("this process states other message bounds: {stated:?}, not {asked:?}")]
pub struct BoundsTaken {
    pub stated: Bounds,
    pub asked: Bounds,
}

/// The process's bounds, stated once: hyper-transport asks a class's bound of the classes' type,
/// not of a value, and a process runs one node in production, or nodes of one configuration in a
/// test, so one statement serves every endpoint it opens.
static BOUNDS: OnceLock<Bounds> = OnceLock::new();

/// States the process's bounds; a second statement must be the same.
pub fn state_bounds(bounds: Bounds) -> Result<(), BoundsTaken> {
    let stated = *BOUNDS.get_or_init(|| bounds);
    if stated == bounds {
        Ok(())
    } else {
        Err(BoundsTaken {
            stated,
            asked: bounds,
        })
    }
}

/// The classes of a node's connections ([`Kind`], [`Class`], [`Role`]).
pub struct NodeClasses;

const KINDS: [Kind; 4] = [Kind::Raft, Kind::Command, Kind::Snapshot, Kind::Read];

impl Classes for NodeClasses {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;

    const RANKS: u8 = 3;

    fn rank(class: Class) -> u8 {
        match class {
            Class::Replication => 0,
            Class::Request => 1,
            Class::Bulk => 2,
        }
    }

    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        match (kind, role) {
            (Kind::Raft, Role::Node) => Some(Class::Replication),
            (Kind::Snapshot, Role::Node) => Some(Class::Bulk),
            (Kind::Command | Kind::Read, _) => Some(Class::Request),
            (Kind::Raft | Kind::Snapshot, Role::Gateway | Role::Client) => None,
        }
    }

    /// Zero, which refuses every frame, until the process states its bounds ([`state_bounds`]).
    fn frame_bound(class: Class) -> u64 {
        BOUNDS.get().map_or(0, |b| match class {
            Class::Replication => b.replication,
            Class::Request => b.request,
            Class::Bulk => b.bulk,
        })
    }

    fn kind_code(kind: Kind) -> u16 {
        match kind {
            Kind::Raft => 1,
            Kind::Command => 2,
            Kind::Snapshot => 3,
            Kind::Read => 4,
        }
    }

    fn kind_of(code: u16) -> Option<Kind> {
        KINDS
            .iter()
            .copied()
            .find(|kind| Self::kind_code(*kind) == code)
    }

    /// A Raft frame carries its own CRC-32C first, of every byte after it (`crate::frame`): the
    /// transport verifies it on receipt rather than sum the frame twice.
    fn embedded_checksum(kind: Kind) -> Option<Embedded> {
        if kind == Kind::Raft {
            Some(crate::frame::EMBEDDED)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_kind_reads_back_from_its_code_and_no_other_code_names_one() {
        for kind in KINDS {
            assert_eq!(
                NodeClasses::kind_of(NodeClasses::kind_code(kind)),
                Some(kind)
            );
        }
        assert_eq!(NodeClasses::kind_of(0), None);
        assert_eq!(NodeClasses::kind_of(5), None);
    }

    #[test]
    fn only_a_node_sends_raft_messages_and_snapshots() {
        for role in [Role::Gateway, Role::Client] {
            assert_eq!(NodeClasses::class_of(Kind::Raft, role), None);
            assert_eq!(NodeClasses::class_of(Kind::Snapshot, role), None);
            assert_eq!(
                NodeClasses::class_of(Kind::Command, role),
                Some(Class::Request)
            );
        }
        assert_eq!(
            NodeClasses::class_of(Kind::Raft, Role::Node),
            Some(Class::Replication)
        );
    }

    #[test]
    fn the_bounds_are_stated_once_and_a_different_statement_is_refused() {
        let bounds = Bounds {
            replication: 4096,
            request: 8192,
            bulk: 1 << 20,
        };
        // Tests share the process: whichever states first, the same statement holds after.
        let first = *BOUNDS.get_or_init(|| bounds);
        assert!(state_bounds(first).is_ok());
        let other = Bounds {
            replication: first.replication + 1,
            ..first
        };
        assert!(state_bounds(other).is_err());
        assert_eq!(NodeClasses::frame_bound(Class::Bulk), first.bulk);
    }
}
