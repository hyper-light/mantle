//! The cell's upgrade fence (docs/research/91 §4; node.md §1.7): the wire profile every member of
//! the cell has been seen to speak, which the root range holds (`mantle_meta::root::fence`). Each
//! turn this node reads it again from its own member of the root range once that member applied
//! an entry since ([`Node::step_fence`]), as it reads the volume table. Where this node leads the
//! root range and the fence is below the profile it announces, it raises the fence once every node
//! the table holds speaks that profile on a connection to it: the members' own statements, made at
//! each connection's handshake. The raise is the profile's first kept entry, written only once
//! every member can read it, and is never taken back.
//!
//! Once the fence rises past a node's profile, that node can no longer read what the cell writes:
//! this node refuses a connection of a member on an older profile (`Peers::fence_at`), closes any
//! it holds, and a node whose own profile is below the fence stops (`NodeError::Fenced`).

use hyper_transport::PeerId;
use mantle_meta::engine::Engine;
use mantle_meta::root;
use mantle_meta::wire::Command;
use mantle_protocol::classes::Role;
use mantle_protocol::wire::Profile;

use super::{Node, NodeError, NodeLog};
use crate::ask::{CommandAsk, Wait};

/// The fence as this node last read it, and the raise it asked.
pub(super) struct Fence {
    /// The fence the root range held.
    profile: Profile,
    /// The root member's applied index it was read at; none before the first read.
    applied: Option<u64>,
    /// The nodes the table held then, at most the cell's stated nodes.
    members: Vec<PeerId>,
    /// The raise this node asked of the root range, until it is answered.
    raising: Option<CommandAsk>,
}

impl Fence {
    /// The fence before the root range was read: profile one, which every mantle speaks.
    pub(super) fn unread() -> Self {
        Self {
            profile: Profile::One,
            applied: None,
            members: Vec::new(),
            raising: None,
        }
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// The fence at a turn: its raise's answer taken; read again where the root member applied an
    /// entry since; raised where this node leads the root range, the fence is below its profile and
    /// every node the table holds speaks that profile.
    pub(super) fn step_fence(&mut self) -> Result<(), NodeError> {
        if let Some(ask) = self.fence.raising
            && self.take_command(ask).is_some()
        {
            // Raised, kept, or refused: what the fence is now the next read says, and a raise not
            // made is asked again at a later turn.
            self.fence.raising = None;
        }
        let Some((group, handle)) = self
            .ranges
            .iter()
            .find(|k| k.layer == mantle_meta::apply::Layer::Root)
            .map(|k| (k.group, k.handle))
        else {
            return Ok(());
        };
        let meta = |e: mantle_meta::error::MetaError| NodeError::Bounds(e.to_string());
        let Some(shell) = self.owner.get_mut(handle) else {
            return Ok(());
        };
        let applied = shell.applied().index;
        if self.fence.applied != Some(applied) {
            let engine = shell.machine_mut().engine_mut();
            let held = root::fence(engine).map_err(meta)?;
            let nodes = root::census(engine).map_err(meta)?.nodes;
            let nodes =
                usize::try_from(nodes).map_err(|_| NodeError::Bounds("the census".into()))?;
            self.fence.members = root::members(engine, nodes).map_err(meta)?;
            self.fence.applied = Some(applied);
            // A fence this binary has no profile of, or past the one it announces: what the cell
            // writes it cannot read.
            let profile = Profile::numbered(held)
                .filter(|p| *p <= self.profile)
                .ok_or(NodeError::Fenced {
                    fence: held,
                    profile: self.profile,
                })?;
            if profile > self.fence.profile {
                self.fence.profile = profile;
                self.fenced()?;
            }
        }
        let raise = self.fence.raising.is_none()
            && self.fence.profile < self.profile
            && self.leader_of(group) == self.id
            && self.fence.members.iter().all(|member| {
                *member == self.id || self.speaks(*member).is_some_and(|p| p >= self.profile)
            });
        if raise {
            let command = Command::Root(root::Command::Fence(root::Fence {
                profile: self.profile.number(),
            }));
            match self.command(group, command, u64::MAX, Wait::Answered) {
                Ok(ask) => self.fence.raising = Some(ask),
                // No room for a command of its own now: asked at a later turn.
                Err(NodeError::Asks) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// The fence risen: the directory refuses a member's connection of an older profile from now
    /// on, and each such connection held is closed, its member unable to read what the cell
    /// writes.
    fn fenced(&mut self) -> Result<(), NodeError> {
        let fence = self.fence.profile;
        self.driver.endpoint().directory_mut().fence_at(fence);
        let older: Vec<PeerId> = self
            .spoken
            .iter()
            .filter(|(_, _, party)| party.role == Role::Node && party.profile < fence)
            .map(|(peer, _, _)| *peer)
            .collect();
        if older.is_empty() {
            return Ok(());
        }
        let clock = self.driver.clock()?;
        for peer in older {
            self.driver.endpoint().disconnect(clock, peer);
        }
        Ok(())
    }

    /// The cell's upgrade fence as this node last read it.
    pub fn fence(&self) -> Profile {
        self.fence.profile
    }
}
