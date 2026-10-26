//! What a node's QUIC connections carry (docs/design/node.md §3.1): each message's kind, the class
//! it travels in, and the roles that may send it. A class is decided by what a message is and who
//! sends it, never by the stream a peer chose (audit §13.3). Consensus control that fits a datagram
//! travels on the sealed plane instead, ahead of everything.
//!
//! | Kind | Class | Sent by | Carries |
//! |---|---|---|---|
//! | `Raft` | replication, on a lane | nodes | a range's Raft message: appends with entries, and control too large for a datagram |
//! | `Command`, `Read` | request, an exchange | nodes and gateways | a gateway's commands to a range, and its reads |
//! | `ChunkPut`, `ChunkGet` | transfer, an exchange | nodes and gateways | a chunk's bytes to the volume that keeps them, a range of them back (mantle-node's `chunk`) |
//! | `Object` | transfer, an exchange | anyone | a bucket made, an object's bytes put, a range of them got ([`crate::object`]) |
//! | `Snapshot` | bulk, an exchange | nodes | a range's snapshot to a member that lags past the log's start |
//!
//! The frame bound of each class is the largest message of that kind the node's ranges can make,
//! stated once a process ([`Bounds`]), so a receiver refuses a larger frame before it reserves
//! memory for it (node.md §3.2).

use std::sync::OnceLock;

use hyper_transport::{Classes, Embedded};

/// Where a Raft frame's own checksum lies, and what it covers: its CRC-32C first, of every byte
/// after it (mantle-node's `frame`), which the transport checks on a lane.
pub const RAFT_CHECKSUM: Embedded = Embedded {
    covered_from: 4,
    at: 0,
};

/// A message's kind on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Raft,
    Command,
    Snapshot,
    Read,
    ChunkPut,
    ChunkGet,
    Object,
}

/// The classes a node's QUIC connections share credit by, most urgent first (node.md §3.1, §3.3):
/// their order is their urgency. `Transfer` is node.md's request class's lower level, the bodies
/// of chunk writes and reads, below the requests someone waits on and above bulk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Class {
    Replication,
    Request,
    Transfer,
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
    pub transfer: u64,
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

const KINDS: [Kind; 7] = [
    Kind::Raft,
    Kind::Command,
    Kind::Snapshot,
    Kind::Read,
    Kind::ChunkPut,
    Kind::ChunkGet,
    Kind::Object,
];

impl Classes for NodeClasses {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;

    const RANKS: u8 = 4;

    fn rank(class: Class) -> u8 {
        match class {
            Class::Replication => 0,
            Class::Request => 1,
            Class::Transfer => 2,
            Class::Bulk => 3,
        }
    }

    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        match (kind, role) {
            (Kind::Raft, Role::Node) => Some(Class::Replication),
            (Kind::Snapshot, Role::Node) => Some(Class::Bulk),
            // A range applies what a command says and answers what a read asks of its rows: a
            // gateway's to ask, which judges a client's object operation first (node.md §3.6).
            (Kind::Command | Kind::Read, Role::Node | Role::Gateway) => Some(Class::Request),
            // A chunk is a gateway's to place, a node's gateway's included; a client asks a
            // gateway for objects and never writes a volume (node.md §3.6).
            (Kind::ChunkPut | Kind::ChunkGet, Role::Node | Role::Gateway) => Some(Class::Transfer),
            // An object's body is a large request's: the transfer level (node.md §3.1).
            (Kind::Object, _) => Some(Class::Transfer),
            (Kind::Raft | Kind::Snapshot, Role::Gateway | Role::Client)
            | (Kind::Command | Kind::Read | Kind::ChunkPut | Kind::ChunkGet, Role::Client) => None,
        }
    }

    /// Zero, which refuses every frame, until the process states its bounds ([`state_bounds`]).
    fn frame_bound(class: Class) -> u64 {
        BOUNDS.get().map_or(0, |b| match class {
            Class::Replication => b.replication,
            Class::Request => b.request,
            Class::Transfer => b.transfer,
            Class::Bulk => b.bulk,
        })
    }

    fn kind_code(kind: Kind) -> u16 {
        match kind {
            Kind::Raft => 1,
            Kind::Command => 2,
            Kind::Snapshot => 3,
            Kind::Read => 4,
            Kind::ChunkPut => 5,
            Kind::ChunkGet => 6,
            Kind::Object => 7,
        }
    }

    fn kind_of(code: u16) -> Option<Kind> {
        KINDS
            .iter()
            .copied()
            .find(|kind| Self::kind_code(*kind) == code)
    }

    /// A Raft frame carries its own CRC-32C first, of every byte after it ([`RAFT_CHECKSUM`]):
    /// the transport verifies it on receipt rather than sum the frame twice.
    fn embedded_checksum(kind: Kind) -> Option<Embedded> {
        if kind == Kind::Raft {
            Some(RAFT_CHECKSUM)
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
        assert_eq!(NodeClasses::kind_of(8), None);
    }

    #[test]
    fn chunks_travel_on_the_transfer_class_from_nodes_and_gateways_only() {
        for kind in [Kind::ChunkPut, Kind::ChunkGet] {
            for role in [Role::Node, Role::Gateway] {
                assert_eq!(NodeClasses::class_of(kind, role), Some(Class::Transfer));
            }
            assert_eq!(NodeClasses::class_of(kind, Role::Client), None);
        }
        for role in [Role::Node, Role::Gateway, Role::Client] {
            assert_eq!(
                NodeClasses::class_of(Kind::Object, role),
                Some(Class::Transfer)
            );
        }
        assert!(NodeClasses::rank(Class::Request) < NodeClasses::rank(Class::Transfer));
        assert!(NodeClasses::rank(Class::Transfer) < NodeClasses::rank(Class::Bulk));
    }

    #[test]
    fn only_a_node_sends_raft_messages_and_snapshots() {
        for role in [Role::Gateway, Role::Client] {
            assert_eq!(NodeClasses::class_of(Kind::Raft, role), None);
            assert_eq!(NodeClasses::class_of(Kind::Snapshot, role), None);
        }
        assert_eq!(
            NodeClasses::class_of(Kind::Raft, Role::Node),
            Some(Class::Replication)
        );
    }

    /// A client asks for objects and nothing else: a range's commands and reads are a gateway's,
    /// which judges the client's operation before it asks them.
    #[test]
    fn a_client_sends_objects_alone() {
        for kind in KINDS {
            let class = NodeClasses::class_of(kind, Role::Client);
            if kind == Kind::Object {
                assert_eq!(class, Some(Class::Transfer));
            } else {
                assert_eq!(class, None, "{kind:?}");
            }
        }
        for role in [Role::Node, Role::Gateway] {
            for kind in [Kind::Command, Kind::Read] {
                assert_eq!(NodeClasses::class_of(kind, role), Some(Class::Request));
            }
        }
    }

    #[test]
    fn the_bounds_are_stated_once_and_a_different_statement_is_refused() {
        let bounds = Bounds {
            replication: 4096,
            request: 8192,
            transfer: 1 << 16,
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
