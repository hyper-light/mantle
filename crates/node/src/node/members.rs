//! The cell's members on a node (docs/design/membership.md): what this node's member of the root
//! range holds of them, read again once per entry it applies, and what follows from it each turn.
//!
//! - **The directory and the peers.** Each member serving or leaving is admitted by its
//!   certificate's fingerprint, and the grant window admits a node granted and not yet joined
//!   (`crate::directory`, membership.md §2); a member gone or removed is refused, its connections
//!   closed. Each member is a peer this node keeps, dialed where it says it is reached, its reach a
//!   host the process looks up where it names one ([`Node::take_reached`]).
//! - **Each range this node leads** steps its configuration toward the members (membership.md §6;
//!   `mantle_range::membership::Toward`): one change at a time, only while none is pending.
//! - **The root range's leader** records each member's absence and return from its node-pair
//!   liveness stream; judges a member gone once its recorded absence passes the time the cell's
//!   durability model takes a lost chunk to be whole again (its repair rate), past which the cell's
//!   durability no longer allows waiting for it; and removes and then delists a member leaving or
//!   gone once its volumes are retired and no range names it (membership.md §4, §5): each a
//!   command of the root range, one out at a time a member.
//!
//! A cell made before grants holds no member's row, and none of this acts on it: its members are
//! its configuration's, as before.

use std::net::SocketAddr;

use hyper_timing::Trust;
use hyper_transport::{PeerId, Refusal};
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::record::{Grants, MemberState, Membership};
use mantle_meta::root;
use mantle_meta::wire::Command;
use mantle_range::ConfState;
use mantle_range::membership::{Step, Toward, drops_absent};

use super::{Node, NodeError, NodeLog, Remote};
use crate::ask::Wait;
use crate::directory::Admitted;
use mantle_protocol::bounds;

/// The cell's members as this node's member of the root range held them after an entry it
/// applied.
pub struct Cell {
    /// The entry the root range's member had applied when they were read.
    applied: u64,
    /// Each member and its row, in the order of their ids: at most the table's nodes.
    members: Vec<(u64, Membership)>,
    /// The nodes the volume table holds a volume of, sorted: at most the table's volumes.
    holding: Vec<u64>,
    /// Each member removed and the entry this node's member of the root range had applied when it
    /// first read it so, at or past the removal's: the commit a removed member must say it holds
    /// before it is dropped from the root range, so it applies its own removal (membership.md §4
    /// step 5). At most the table's nodes.
    removed: Vec<(u64, u64)>,
}

impl Cell {
    /// Member `id`'s row, if the root range holds one.
    pub fn member(&self, id: u64) -> Option<&Membership> {
        self.members
            .binary_search_by_key(&id, |(m, _)| *m)
            .ok()
            .and_then(|at| self.members.get(at))
            .map(|(_, row)| row)
    }

    /// Every member and its row, in the order of their ids.
    pub fn members(&self) -> &[(u64, Membership)] {
        &self.members
    }
}

/// Whether a member's identity is admitted: serving or leaving.
fn admitted(state: MemberState) -> bool {
    matches!(state, MemberState::Serving | MemberState::Leaving)
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// The cell's members as this node's member of the root range last held them: none before it
    /// held a member's row, and none in a cell made before grants.
    pub fn cell(&self) -> Option<&Cell> {
        self.cell.as_ref()
    }

    /// Whether this node is a member of the cell, serving or leaving, by its own replica of the
    /// root range.
    pub fn member(&self) -> bool {
        self.cell
            .as_ref()
            .and_then(|c| c.member(self.id))
            .is_some_and(|row| admitted(row.state))
    }

    /// The configuration this node's member of `group` applied.
    pub fn configuration(&self, group: u128) -> Option<&ConfState> {
        self.kept(group)
            .and_then(|k| self.owner.get(k.handle))
            .map(|r| r.configuration())
    }

    /// The members whose reach names a host to look up, since the last call: each one's id and its
    /// QUIC and plane places, `HOST:PORT`, which the process resolves as it resolves a peer's name
    /// (`crate::names`) and gives back by [`Node::learn`].
    pub fn take_reached(&mut self) -> std::vec::Drain<'_, (u64, String, String)> {
        self.reached.drain(..)
    }

    /// A turn's step of the cell's membership: the members read again once the root range's
    /// member applied an entry, the directory and the peers kept to them, each range this node
    /// leads stepped toward them, and, as the root range's leader, its records and judgments.
    pub(super) fn step_members(&mut self) -> Result<(), NodeError> {
        if self.max_nodes == 0 {
            return Ok(());
        }
        self.read_members()?;
        if self.cell.is_none() {
            return Ok(());
        }
        self.step_configurations();
        self.step_duties()
    }

    /// The root range's member: its slot's handle and its group.
    fn root_range(&self) -> Option<(hyper_durable::Handle, u128)> {
        self.ranges
            .iter()
            .find(|k| k.layer == Layer::Root)
            .map(|k| (k.handle, k.group))
    }

    /// Reads the members again where the root range's member applied an entry since the last
    /// read, and keeps the directory and the peers to them.
    fn read_members(&mut self) -> Result<(), NodeError> {
        let Some((handle, _)) = self.root_range() else {
            return Ok(());
        };
        let Some(shell) = self.owner.get_mut(handle) else {
            return Ok(());
        };
        let applied = shell.applied().index;
        if self.cell.as_ref().is_some_and(|c| c.applied == applied) {
            return Ok(());
        }
        let meta = |e: mantle_meta::error::MetaError| NodeError::Bounds(e.to_string());
        let engine = shell.machine_mut().engine_mut();
        let census = root::census(engine).map_err(meta)?;
        let nodes = usize::try_from(census.nodes).unwrap_or(usize::MAX);
        let members = root::memberships(engine, nodes).map_err(meta)?;
        let grants: Grants = root::grants(engine).map_err(meta)?;
        if members.is_empty() && grants.top == 0 && self.cell.is_none() {
            // No member has joined: a cell made before grants, or one whose founders have yet to.
            return Ok(());
        }
        let most = usize::try_from(census.volumes).unwrap_or(usize::MAX);
        let mut holding: Vec<u64> = root::volumes(engine, None, most)
            .map_err(meta)?
            .into_iter()
            .map(|p| p.node)
            .collect();
        holding.sort_unstable();
        holding.dedup();
        let admitted_rows: Vec<Admitted> = members
            .iter()
            .map(|(id, row)| Admitted {
                id: *id,
                fingerprint: row.fingerprint,
                admitted: admitted(row.state),
            })
            .collect();
        let withdrawn = self.driver.endpoint().directory_mut().set_members(
            admitted_rows,
            grants,
            self.max_nodes,
        );
        let clock = self.driver.clock()?;
        for peer in withdrawn {
            self.driver
                .endpoint()
                .refuse(clock, peer, Refusal::Identity);
            self.unkey(peer);
        }
        let removed = members
            .iter()
            .filter(|(_, row)| row.state == MemberState::Removed)
            .map(|(id, _)| {
                let seen = self
                    .cell
                    .as_ref()
                    .and_then(|c| c.removed.iter().find(|(was, _)| was == id))
                    .map_or(applied, |(_, at)| *at);
                (*id, seen)
            })
            .collect();
        self.cell = Some(Cell {
            applied,
            members,
            holding,
            removed,
        });
        self.keep_peers()
    }

    /// Keeps a peer for each member serving or leaving but this node, where its row says it is
    /// reached, and lets go of the peers the root range no longer admits that this node learned
    /// from it (a pinned peer or a seed is the configuration's, and stays).
    fn keep_peers(&mut self) -> Result<(), NodeError> {
        let Some(cell) = self.cell.as_ref() else {
            return Ok(());
        };
        let mut reached: Vec<(u64, String, String)> = Vec::new();
        let mut gone: Vec<u64> = Vec::new();
        for (id, row) in &cell.members {
            if *id == self.id {
                continue;
            }
            if !admitted(row.state) {
                gone.push(*id);
                continue;
            }
            reached.push((*id, row.quic.clone(), row.plane.clone()));
        }
        // Learned peers whose rows went: delisted members.
        for remote in &self.remotes {
            if remote.learned && cell.member(remote.id).is_none() {
                gone.push(remote.id);
            }
        }
        for id in gone {
            if let Some(at) = self.remotes.iter().position(|r| r.id == id && r.learned) {
                self.unkey(id);
                self.remotes.swap_remove(at);
                self.driver.endpoint().directory_mut().unname(id);
            }
        }
        let mut held: Vec<(PeerId, SocketAddr)> = Vec::new();
        self.driver.endpoint().established(&mut held);
        for (id, quic, plane) in reached {
            if !self.remotes.iter().any(|r| r.id == id) {
                self.driver.endpoint().directory_mut().name(id);
                let mut remote = Remote::new(id, true);
                remote.keyed = held.iter().any(|(peer, _)| *peer == id);
                self.remotes.push(remote);
            }
            // Only what the row says anew: where the member stated it listens at its last start,
            // which a later statement of a move on standard input may have overtaken already.
            let Some(remote) = self.remotes.iter_mut().find(|r| r.id == id) else {
                continue;
            };
            if remote
                .row
                .as_ref()
                .is_some_and(|(q, p)| *q == quic && *p == plane)
            {
                continue;
            }
            remote.row = Some((quic.clone(), plane.clone()));
            match (quic.parse::<SocketAddr>(), plane.parse::<SocketAddr>()) {
                (Ok(quic), Ok(plane)) => self.learn(id, quic, plane)?,
                // A host name: the process looks it up, as it looks up a peer's name.
                _ => self.reached.push((id, quic, plane)),
            }
        }
        Ok(())
    }

    /// What `group`'s configuration should be, `conf` its member's: each member the root range
    /// holds serving a voter, but absent ones where dropping them lets the range survive more
    /// failures, which then learn; in the root range, each one leaving a learner, and each one
    /// removed until it has said it holds the commit that removed it (`told`); a member gone
    /// nowhere; and each member of `conf` the root range holds no row of as it is, a founder yet to
    /// join (membership.md §6).
    fn target(
        &self,
        conf: &ConfState,
        layer: Layer,
        told: impl Fn(u64, u64) -> bool,
    ) -> Option<Toward> {
        let cell = self.cell.as_ref()?;
        let mut voters: Vec<u64> = Vec::new();
        let mut learners: Vec<u64> = Vec::new();
        let named = conf
            .voters
            .iter()
            .chain(&conf.learners)
            .copied()
            .chain(cell.members.iter().map(|(id, _)| *id));
        for id in named {
            match cell.member(id).map(|row| row.state) {
                None if conf.voters.contains(&id) => voters.push(id),
                None if conf.learners.contains(&id) => learners.push(id),
                None => {}
                Some(MemberState::Serving) => voters.push(id),
                Some(MemberState::Leaving) if layer == Layer::Root => learners.push(id),
                Some(MemberState::Removed)
                    if layer == Layer::Root
                        && conf.learners.contains(&id)
                        && cell
                            .removed
                            .iter()
                            .find(|(was, _)| *was == id)
                            .is_none_or(|(_, at)| !told(id, *at)) =>
                {
                    learners.push(id);
                }
                Some(MemberState::Leaving | MemberState::Gone | MemberState::Removed) => {}
            }
        }
        voters.sort_unstable();
        voters.dedup();
        let absent: Vec<u64> = voters
            .iter()
            .copied()
            .filter(|id| {
                *id != self.id
                    && cell.member(*id).is_some_and(|row| {
                        row.state == MemberState::Serving && row.absent_since_ns != 0
                    })
            })
            .collect();
        if drops_absent(voters.len(), absent.len()) {
            voters.retain(|id| !absent.contains(id));
            learners.extend(absent);
        }
        learners.sort_unstable();
        learners.dedup();
        learners.retain(|id| !voters.contains(id));
        Some(Toward { voters, learners })
    }

    /// Steps each range this node leads toward its target: a change proposed only while none is
    /// pending and no replacement steps the range; a lead handed on only while none is.
    fn step_configurations(&mut self) {
        for at in 0..self.ranges.len() {
            let Some((handle, layer, replacing)) = self
                .ranges
                .get(at)
                .map(|k| (k.handle, k.layer, k.replacing.is_some()))
            else {
                continue;
            };
            if replacing {
                continue;
            }
            let Some(shell) = self.owner.get(handle) else {
                continue;
            };
            if !shell.is_leader()
                || shell.core().raft.has_pending_conf()
                || shell.core().raft.lead_transferee().is_some()
            {
                continue;
            }
            let conf = shell.configuration().clone();
            // Whether a member has said its storage holds the commit at `at`.
            let tracker = shell.core().raft.tracker();
            let told =
                |member: u64, at: u64| tracker.get(member).is_some_and(|p| p.committed_index >= at);
            let Some(target) = self.target(&conf, layer, told) else {
                continue;
            };
            let Some(shell) = self.owner.get_mut(handle) else {
                continue;
            };
            let step = target.step(&conf, self.id, |member| shell.caught_up(member));
            match step {
                // A change or a transfer the core refuses (its leader stepping down, its log full)
                // is asked again at a later turn.
                Step::Propose(change) => {
                    let _ = shell.change(Vec::new(), &change);
                    self.owner.schedule(handle);
                }
                Step::Transfer(to) => {
                    let _ = shell.transfer(to);
                    self.owner.schedule(handle);
                }
                Step::Wait | Step::Done => {}
            }
        }
    }

    /// Whether no range this node hosts names `id` as a voter, and, where `entirely`, as a member
    /// at all: what the root range's leader waits for before it removes, and delists, a member.
    fn named_nowhere(&self, id: u64, entirely: bool) -> bool {
        self.ranges.iter().all(|kept| {
            self.owner.get(kept.handle).is_none_or(|r| {
                let c = r.configuration();
                let voting = c.voters.contains(&id) || c.voters_outgoing.contains(&id);
                let learning = c.learners.contains(&id) || c.learners_next.contains(&id);
                !voting && !(entirely && learning)
            })
        })
    }

    /// The root range's leader's records and judgments (membership.md §4, §5): for each member, at
    /// most one command out, of the first that applies: its absence, where this node's stream
    /// suspects it or took its stop, no keyed connection to it is left, and the table holds it
    /// present; its return, where a keyed connection to it is, or it is this node, and the table
    /// holds it absent; gone, where its absence passes the time the cell's
    /// durability model takes a lost chunk to be whole again ([`Node::repair_ns`]); removed, where
    /// it leaves or is gone, holds no volume and no range has it vote; delisted, where it is
    /// removed and no range names it.
    fn step_duties(&mut self) -> Result<(), NodeError> {
        let Some((handle, group)) = self.root_range() else {
            return Ok(());
        };
        let duties = std::mem::take(&mut self.duties);
        self.duties = duties
            .into_iter()
            .filter(|(ask, _)| self.take_command(*ask).is_none())
            .collect();
        if !self.owner.get(handle).is_some_and(|r| r.is_leader()) {
            return Ok(());
        }
        let Some(cell) = self.cell.as_ref() else {
            return Ok(());
        };
        let now_ns = self.wall.now_ns();
        let repair_ns = self.repair_ns();
        let mut asks: Vec<(u64, root::Command)> = Vec::new();
        for (id, row) in &cell.members {
            if self.duties.iter().any(|(_, m)| m == id) {
                continue;
            }
            if *id == self.id {
                // This node is present by leading: an absence of it a former leader recorded,
                // its stream having suspected it, ends.
                if row.absent_since_ns != 0
                    && matches!(row.state, MemberState::Serving | MemberState::Leaving)
                {
                    asks.push((*id, root::Command::Returned { node: *id }));
                }
                continue;
            }
            // Away by both the cell's facts: its stream suspects it or took its stop, and the
            // transport holds no keyed connection to it, which QUIC's idle timeout ends once the
            // member falls silent (docs/design/repair.md §2.1); a connection keyed again is the
            // member seen. A suspicion alone, its connection live, records nothing.
            let trust = self.liveness.trust(*id);
            let stopped = self.liveness.stopped(*id);
            let keyed = self.remotes.iter().any(|r| r.id == *id && r.keyed);
            let away = (trust == Some(Trust::Suspected) || stopped) && !keyed;
            let back = keyed && !stopped;
            let holds = cell.holding.binary_search(id).is_ok();
            let command = match row.state {
                MemberState::Serving | MemberState::Leaving if away && row.absent_since_ns == 0 => {
                    Some(root::Command::Absent {
                        node: *id,
                        at_ns: now_ns,
                    })
                }
                MemberState::Serving | MemberState::Leaving if back && row.absent_since_ns != 0 => {
                    Some(root::Command::Returned { node: *id })
                }
                MemberState::Serving | MemberState::Leaving if gone_by(row, now_ns, repair_ns) => {
                    Some(root::Command::Gone { node: *id })
                }
                MemberState::Leaving | MemberState::Gone
                    if !holds && self.named_nowhere(*id, false) =>
                {
                    Some(root::Command::Remove { node: *id })
                }
                MemberState::Removed if self.named_nowhere(*id, true) => {
                    Some(root::Command::Delist { node: *id })
                }
                _ => None,
            };
            if let Some(command) = command {
                asks.push((*id, command));
            }
        }
        for (id, command) in asks {
            if !self.command_room() {
                break;
            }
            let ask = self.command(group, Command::Root(command), u64::MAX, Wait::Answered)?;
            self.duties.push((ask, id));
        }
        Ok(())
    }
}

/// Whether a member `row` absent since its recorded start is judged gone at `now_ns`: its absence
/// passes `repair_ns`, the time the cell's durability model takes a lost chunk to be whole again
/// (membership.md §5); none where the node states no model.
fn gone_by(row: &Membership, now_ns: u64, repair_ns: Option<u64>) -> bool {
    match repair_ns {
        Some(repair_ns) => {
            row.absent_since_ns != 0
                && now_ns
                    .checked_sub(row.absent_since_ns)
                    .is_some_and(|absent| absent > repair_ns)
        }
        None => false,
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// The time, in nanoseconds, the cell's durability model takes a lost chunk to be whole again:
    /// the mean of its repairs, the reciprocal of the repair rate a block's scheme is chosen under
    /// (`durability.repair_rate`, a rate an hour; node.md §1.4's `durability.*` row). A member
    /// absent longer is no longer one the cell's durability allows it to wait for (membership.md
    /// §5). None for a node that hosts no objects, which states no model.
    pub(super) fn repair_ns(&self) -> Option<u64> {
        let rate = self.gateway.as_ref()?.rates.repair;
        // A mean past what a duration or the clock holds is past every absence the clock measures.
        let mean = std::time::Duration::try_from_secs_f64(rate.recip() * SECONDS_AN_HOUR).ok()?;
        u64::try_from(mean.as_nanos()).ok()
    }
}

/// Seconds an hour, the repair rate's unit: a unit conversion.
const SECONDS_AN_HOUR: f64 = 3_600.0;

impl Remote {
    /// A peer of `id`, where it listens not yet known; `learned` from the root range's rows rather
    /// than the configuration.
    pub(super) fn new(id: PeerId, learned: bool) -> Self {
        Self {
            id,
            quic: None,
            plane: None,
            waiting: std::collections::VecDeque::new(),
            keyed: false,
            redial_at: None,
            backoff: bounds::initial_pto(),
            redials: 0,
            dials: 0,
            ended: 0,
            suspected: 0,
            refusal: None,
            suspect: false,
            learned,
            row: None,
            seed: false,
        }
    }
}
