//! The cell's members on a node (docs/design/membership.md): what this node's member of the root
//! range holds of them, read again once per entry it applies, and what follows from it each turn.
//!
//! - **The directory and the peers.** Each member serving or leaving is admitted by its
//!   certificate's fingerprint, and the grant window admits a node granted and not yet joined
//!   (`crate::directory`, membership.md §2); a member gone or removed is refused, pinned in the
//!   configuration or not, at its handshakes and, as its row changes, its open connections. Each member is a peer this node keeps, dialed where it says it is reached, its reach a
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

use hyper_transport::{PeerId, Refusal};
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::record::{Grants, Ledgered, MemberState, Membership, Presence, Rejoin};
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
    /// Each member's presence as the cell has seen it, in the order of `members`: whether it is
    /// away, and since when, is repair's record (docs/design/membership.md §8).
    presences: Vec<Presence>,
    /// The nodes the volume table holds a volume of, sorted: at most the table's volumes.
    holding: Vec<u64>,
    /// Each member removed and the entry this node's member of the root range had applied when it
    /// first read it so, at or past the removal's: the commit a removed member must say it holds
    /// before it is dropped from the root range, so it applies its own removal (membership.md §4
    /// step 5). At most the table's nodes.
    removed: Vec<(u64, u64)>,
    /// The ledger's entries of written-off identities that may come back, in the order of their old
    /// ids: at most the entries the ledger counts, which the cell's nodes bound (membership.md
    /// §5.1).
    ledger: Vec<(u64, Ledgered)>,
    /// The cell's setting for a written-off node coming back.
    rejoin: Rejoin,
    /// The highest grant the cell's issuer's window has spent (membership.md §5.1).
    issuer_top: u64,
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

    /// Whether the volume table holds a volume of node `id`.
    pub fn holds(&self, id: u64) -> bool {
        self.holding.binary_search(&id).is_ok()
    }

    /// Member `id`'s presence as the cell has seen it, if the root range holds its row.
    pub fn presence(&self, id: u64) -> Option<&Presence> {
        self.members
            .binary_search_by_key(&id, |(m, _)| *m)
            .ok()
            .and_then(|at| self.presences.get(at))
    }

    /// The ledger's entries, in the order of their old nodes' ids (membership.md §5.1).
    pub fn ledger(&self) -> &[(u64, Ledgered)] {
        &self.ledger
    }

    /// The cell's setting for a written-off node coming back (membership.md §5.1).
    pub fn rejoin(&self) -> Rejoin {
        self.rejoin
    }

    /// The highest grant the cell's issuer's window has spent: a re-grant takes the one after.
    pub fn issuer_top(&self) -> u64 {
        self.issuer_top
    }
}

/// The root range's rows the membership reads, as its member held them after an entry it applied:
/// read on the node's shard where its task drives the root range, and on the root range's shard,
/// and handed over (`shard::Out::Rows`), where another shard does (node.md §2.9).
pub(crate) struct Rows {
    /// The entry the root range's member had applied when they were read.
    pub(crate) applied: u64,
    /// Each member and its row, in the order of their ids: at most the table's nodes.
    pub(crate) members: Vec<(u64, Membership)>,
    /// Each member's presence, in the order of `members`.
    presences: Vec<Presence>,
    grants: Grants,
    /// The nodes the volume table holds a volume of, sorted: at most the table's volumes.
    holding: Vec<u64>,
    /// The ledger's entries, at most those it counts, and the cell's `rejoin` setting.
    ledger: Vec<(u64, Ledgered)>,
    rejoin: Rejoin,
    /// The highest grant the issuer's window has spent.
    issuer_top: u64,
}

impl Rows {
    /// The rows of `engine`, the root range's, which applied through `applied`. The volumes are
    /// read only where a member joined or a grant was made: a cell made before grants reads none.
    pub(crate) fn read<E: Engine>(engine: &mut E, applied: u64) -> Result<Self, NodeError> {
        let meta = |e: mantle_meta::error::MetaError| NodeError::Bounds(e.to_string());
        let census = root::census(engine).map_err(meta)?;
        let nodes = usize::try_from(census.nodes).unwrap_or(usize::MAX);
        let members = root::memberships(engine, nodes).map_err(meta)?;
        let presences = members
            .iter()
            .map(|(id, _)| root::presence(engine, *id))
            .collect::<Result<Vec<Presence>, _>>()
            .map_err(meta)?;
        let grants: Grants = root::grants(engine).map_err(meta)?;
        let mut holding: Vec<u64> = Vec::new();
        if !members.is_empty() || grants.top != 0 {
            let most = usize::try_from(census.volumes).unwrap_or(usize::MAX);
            holding = root::volumes(engine, None, most)
                .map_err(meta)?
                .into_iter()
                .map(|p| p.node)
                .collect();
            holding.sort_unstable();
            holding.dedup();
        }
        let held = root::ledger_count(engine).map_err(meta)?.held;
        let ledger =
            root::ledger(engine, usize::try_from(held).unwrap_or(usize::MAX)).map_err(meta)?;
        let rejoin = root::rejoin(engine).map_err(meta)?;
        let issuer_top = root::issuer_grants(engine).map_err(meta)?.top;
        Ok(Self {
            applied,
            members,
            presences,
            grants,
            holding,
            ledger,
            rejoin,
            issuer_top,
        })
    }
}

/// Whether a member's identity is admitted: serving or leaving, or removed while the root range
/// keeps it as a learner (`root_learner`), so it can be heard saying its storage holds the commit
/// that removed it, which the root range's leader waits for before it drops it (membership.md §4
/// step 5). Refused at its removal, it could say nothing more: it would never hear its removal
/// either, or, hearing it, never be heard, and the leader would keep it as a learner for good.
fn admitted(state: MemberState, root_learner: bool) -> bool {
    match state {
        MemberState::Serving | MemberState::Leaving => true,
        MemberState::Removed => root_learner,
        MemberState::Gone => false,
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// The cell's members as this node's member of the root range last held them: none before it
    /// held a member's row, and none in a cell made before grants.
    pub fn cell(&self) -> Option<&Cell> {
        self.cell.as_ref()
    }

    /// Where each member this node knows listens, its QUIC address as its lines, its replica of the
    /// root range and its input last said it: each peer the root range holds serving or leaving
    /// where this node holds the cell's members, else each peer, in the order the node keeps them.
    /// What a written-off node keeps to ask for its new identity (membership.md §5.1 step 1).
    pub fn listening(&self) -> impl Iterator<Item = (PeerId, SocketAddr)> + '_ {
        let cell = self.cell.as_ref();
        self.remotes.iter().filter_map(move |r| {
            let member = cell.is_none_or(|c| {
                c.member(r.id).is_some_and(|row| {
                    matches!(row.state, MemberState::Serving | MemberState::Leaving)
                })
            });
            r.quic.filter(|_| member).map(|quic| (r.id, quic))
        })
    }

    /// Whether this node is a member of the cell, serving or leaving, by its own replica of the
    /// root range.
    pub fn member(&self) -> bool {
        self.cell
            .as_ref()
            .and_then(|c| c.member(self.id))
            .is_some_and(|row| matches!(row.state, MemberState::Serving | MemberState::Leaving))
    }

    /// Whether the root range's configuration, as this node's member of it applied it, keeps `id`
    /// as a learner.
    fn root_learner(&self, id: u64) -> bool {
        self.root_range()
            .and_then(|(_, group)| self.configuration(group))
            .is_some_and(|c| c.learners.contains(&id) || c.learners_next.contains(&id))
    }

    /// The configuration this node's member of `group` applied: read from its replica where the
    /// node's own task drives it, and as its shard last reported it where another shard does
    /// (node.md §2.9).
    pub fn configuration(&self, group: u128) -> Option<&ConfState> {
        if let Some(far) = self.far.iter().find(|far| far.group == group) {
            return Some(&far.configured.conf);
        }
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
        self.step_configurations()?;
        self.step_duties()
    }

    /// The root range's member: its slot's handle and its group.
    pub(super) fn root_range(&self) -> Option<(hyper_durable::Handle, u128)> {
        self.ranges
            .iter()
            .find(|k| k.layer == Layer::Root)
            .map(|k| (k.handle, k.group))
    }

    /// Reads the members again where the root range's member applied an entry since the last
    /// read, and keeps the directory and the peers to them: from its engine where the node's own
    /// task drives the root range, and as its shard last handed them over where another shard
    /// does ([`Rows`]), once each.
    fn read_members(&mut self) -> Result<(), NodeError> {
        let Some((handle, group)) = self.root_range() else {
            return Ok(());
        };
        let rows = if let Some(far) = self.far.iter_mut().find(|far| far.group == group) {
            let Some(rows) = far.rows.take() else {
                return Ok(());
            };
            *rows
        } else {
            let Some(shell) = self.owner.get_mut(handle) else {
                return Ok(());
            };
            let applied = shell.applied().index;
            if self.cell.as_ref().is_some_and(|c| c.applied == applied) {
                return Ok(());
            }
            Rows::read(shell.machine_mut().engine_mut(), applied)?
        };
        let Rows {
            applied,
            members,
            presences,
            grants,
            holding,
            ledger,
            rejoin,
            issuer_top,
        } = rows;
        if members.is_empty() && grants.top == 0 && self.cell.is_none() {
            // No member has joined: a cell made before grants, or one whose founders have yet to.
            return Ok(());
        }
        let admitted_rows: Vec<Admitted> = members
            .iter()
            .map(|(id, row)| Admitted {
                id: *id,
                fingerprint: row.fingerprint,
                admitted: admitted(row.state, self.root_learner(*id)),
            })
            .collect();
        let withdrawn = self.driver.endpoint().directory_mut().set_members(
            admitted_rows,
            (grants, self.max_nodes),
            (&ledger, rejoin),
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
            presences,
            holding,
            removed,
            ledger,
            rejoin,
            issuer_top,
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
            if !admitted(row.state, self.root_learner(*id)) {
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
                    && cell
                        .member(*id)
                        .is_some_and(|row| row.state == MemberState::Serving)
                    && cell.presence(*id).is_some_and(|p| !p.present)
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
    fn step_configurations(&mut self) -> Result<(), NodeError> {
        for at in 0..self.ranges.len() {
            let Some((handle, group, layer, replacing)) = self
                .ranges
                .get(at)
                .map(|k| (k.handle, k.group, k.layer, k.replacing.is_some()))
            else {
                continue;
            };
            if replacing {
                continue;
            }
            if self.far.iter().any(|far| far.group == group) {
                self.step_far(group, layer)?;
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
            super::shard::step_toward(&mut self.owner, handle, &target);
        }
        Ok(())
    }

    /// Steps a far range this node leads toward its target, as [`Node::step_configurations`] steps
    /// its own, from what the range last showed (`shard::Configured`): the target worked out here
    /// and handed to its shard ([`super::shard::In::Toward`]), which steps it from what its replica
    /// shows then, and only where a step is due by what it showed, so a range at its target, or
    /// one waiting on a learner to catch up, costs its lane nothing. A full lane drops it: the
    /// target is worked out afresh at the next turn and handed on then. A shard whose task ended
    /// stops the node.
    fn step_far(&self, group: u128, layer: Layer) -> Result<(), NodeError> {
        let Some(far) = self.far.iter().find(|far| far.group == group) else {
            return Ok(());
        };
        let shown = &far.configured;
        if !far.seen.leading
            || shown.pending
            || far.seen.transferee.is_some()
            || far.replacing.is_some()
        {
            return Ok(());
        }
        let told = |member: u64, at: u64| {
            shown
                .learners
                .iter()
                .any(|(id, committed, _)| *id == member && *committed >= at)
        };
        let Some(target) = self.target(&shown.conf, layer, told) else {
            return Ok(());
        };
        // A voter's progress is not shown; its shard reads it where it matters (an heir chosen).
        let caught_up = |member: u64| {
            shown
                .learners
                .iter()
                .find(|(id, ..)| *id == member)
                .is_none_or(|(_, _, caught_up)| *caught_up)
        };
        if matches!(
            target.step(&shown.conf, self.id, caught_up),
            Step::Wait | Step::Done
        ) {
            return Ok(());
        }
        let lanes = self
            .shards
            .iter()
            .find(|lanes| lanes.shard() == far.shard)
            .ok_or(NodeError::ShardEnded(far.shard))?;
        match lanes.send(super::shard::In::Toward(group, target)) {
            Ok(()) | Err(NodeError::LaneFull(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Whether no range this node hosts names `id` as a voter, and, where `entirely`, as a member
    /// at all: what the root range's leader waits for before it removes, and delists, a member.
    fn named_nowhere(&self, id: u64, entirely: bool) -> bool {
        self.ranges.iter().all(|kept| {
            self.configuration(kept.group).is_none_or(|c| {
                let voting = c.voters.contains(&id) || c.voters_outgoing.contains(&id);
                let learning = c.learners.contains(&id) || c.learners_next.contains(&id);
                !voting && !(entirely && learning)
            })
        })
    }

    /// The root range's leader's judgments (membership.md §4, §5): for each member, at most one
    /// command out, of the first that applies: gone, where its presence has been away longer than
    /// the time the cell's durability model takes a lost chunk to be whole again
    /// ([`Node::repair_ns`]); removed, where it leaves or is gone, holds no volume and no range has
    /// it vote; delisted, where it is removed and no range names it. Whether it is away is repair's
    /// record, which the same leader keeps (`Node::observe_presence`; membership.md §8).
    fn step_duties(&mut self) -> Result<(), NodeError> {
        let Some((_, group)) = self.root_range() else {
            return Ok(());
        };
        let duties = std::mem::take(&mut self.duties);
        self.duties = duties
            .into_iter()
            .filter(|(ask, _)| self.take_command(*ask).is_none())
            .collect();
        if !self.is_leader(group) {
            return Ok(());
        }
        let Some(cell) = self.cell.as_ref() else {
            return Ok(());
        };
        let now_ns = self.wall.now_ns();
        let repair_ns = self.repair_ns();
        let mut asks: Vec<(u64, root::Command)> = Vec::new();
        for ((id, row), presence) in cell.members.iter().zip(&cell.presences) {
            if self.duties.iter().any(|(_, m)| m == id) || *id == self.id {
                continue;
            }
            let holds = cell.holding.binary_search(id).is_ok();
            let command = match row.state {
                MemberState::Serving | MemberState::Leaving
                    if gone_by(presence, now_ns, repair_ns) =>
                {
                    Some(root::Command::Gone {
                        node: *id,
                        max_nodes: self.max_nodes,
                        // The entry that carries it gives it its time.
                        at_ns: 0,
                    })
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

/// Whether a member whose `presence` the cell keeps is judged gone at `now_ns`: away, and for
/// longer than `repair_ns`, the time the cell's durability model takes a lost chunk to be whole
/// again (membership.md §5); none where the node states no model.
fn gone_by(presence: &Presence, now_ns: u64, repair_ns: Option<u64>) -> bool {
    match repair_ns {
        Some(repair_ns) => {
            !presence.present
                && now_ns
                    .checked_sub(presence.since_ns)
                    .is_some_and(|away| away > repair_ns)
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
            was_up: false,
            suspect: false,
            learned,
            row: None,
            seed: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A presence as the root range keeps it: `present`, or away, since `since_ns`.
    fn presence(present: bool, since_ns: u64) -> Presence {
        Presence {
            present,
            since_ns,
            present_ns: 0,
            away_ns: 0,
            absences: 0,
            longest_away_ns: 0,
        }
    }

    /// A member is judged gone once its recorded absence passes the durability model's mean repair
    /// time, and not at it (membership.md §5); a member seen is not, however long ago its presence
    /// was recorded; a cell that states no model judges none by time; and an absence recorded past
    /// the judging leader's clock, a leader whose clock is behind the entry's, is none yet.
    #[test]
    fn a_member_is_judged_gone_once_its_absence_passes_the_repair_time_and_not_before() {
        let repair = Some(1_000);
        assert!(!gone_by(&presence(false, 100), 1_100, repair));
        assert!(gone_by(&presence(false, 100), 1_101, repair));
        assert!(!gone_by(&presence(true, 100), u64::MAX, repair));
        assert!(!gone_by(&presence(false, 100), u64::MAX, None));
        assert!(!gone_by(&presence(false, 2_000), 1_500, repair));
    }

    /// membership.md §2: a founder pinned in the configuration is judged by its member's row as a
    /// node pinned nowhere is, so a row that writes it off refuses it at its handshakes at once,
    /// and gives it back for `read_members` to close its open connections `Identity`. Admitted
    /// serving, leaving, and removed while the root range keeps it as a learner (§4 step 5);
    /// refused gone, learner or not, and removed once dropped.
    #[test]
    fn a_pinned_founder_is_refused_at_once_by_a_row_that_writes_it_off() {
        use crate::directory::{Peer, Peers};
        use hyper_transport::{Authenticated, Directory};
        use mantle_protocol::classes::Role;
        let founder = |id: u64| Peer {
            id,
            role: Role::Node,
            certificate: format!("founder {id}").into_bytes(),
        };
        let (two, three) = (founder(2), founder(3));
        let hello = |peer: &Peer, dialed: Option<PeerId>| {
            let certificate = peer.certificate.clone();
            move |peers: &mut Peers| {
                peers.identify(&Authenticated {
                    certificate: Some(&certificate),
                    protocol: b"mantle/1",
                    dialed,
                })
            }
        };
        let (accepted, dialed) = (hello(&two, None), hello(&two, Some(2)));
        let row = |peer: &Peer, state: MemberState, learner: bool| Admitted {
            id: peer.id,
            fingerprint: mantle_cert::fingerprint(&peer.certificate),
            admitted: admitted(state, learner),
        };
        // The founders' rows, two's as given and three's serving: the identities withdrawn.
        let judge = |peers: &mut Peers, state: MemberState, learner: bool| {
            let rows = vec![
                row(&two, state, learner),
                row(&three, MemberState::Serving, false),
            ];
            peers.set_members(rows, (Grants::default(), 3), (&[], Rejoin::Automatic))
        };
        let mut peers = Peers::new(vec![two.clone(), three.clone()], 0);
        // Before any row, admitted by its pin.
        assert_eq!(accepted(&mut peers), Some((2, Role::Node)));
        for (state, learner) in [
            (MemberState::Serving, false),
            (MemberState::Leaving, true),
            (MemberState::Removed, true),
        ] {
            assert!(judge(&mut peers, state, learner).is_empty());
            assert_eq!(accepted(&mut peers), Some((2, Role::Node)));
            assert_eq!(dialed(&mut peers), Some((2, Role::Node)));
            assert!(peers.is_node(2));
        }
        // Removed and dropped from the root range: refused at once, its connections to close, once.
        assert_eq!(judge(&mut peers, MemberState::Removed, false), vec![2]);
        assert_eq!(accepted(&mut peers), None);
        assert_eq!(dialed(&mut peers), None);
        assert!(!peers.is_node(2));
        assert!(judge(&mut peers, MemberState::Removed, false).is_empty());
        assert_eq!(accepted(&mut peers), None);
        // The founder beside it, serving, stays admitted.
        assert_eq!(hello(&three, None)(&mut peers), Some((3, Role::Node)));
        // Gone, learner or not: refused at once. A node whose first rows say so closes the
        // connections its pin admitted before.
        for learner in [false, true] {
            let mut peers = Peers::new(vec![two.clone(), three.clone()], 0);
            assert_eq!(accepted(&mut peers), Some((2, Role::Node)));
            assert_eq!(judge(&mut peers, MemberState::Gone, learner), vec![2]);
            assert_eq!(accepted(&mut peers), None);
            assert_eq!(dialed(&mut peers), None);
        }
        let mut peers = Peers::new(vec![two.clone(), three.clone()], 0);
        assert!(judge(&mut peers, MemberState::Serving, false).is_empty());
        assert_eq!(judge(&mut peers, MemberState::Gone, false), vec![2]);
        assert_eq!(accepted(&mut peers), None);
        // A row of its id under another certificate admits not the pinned one.
        let mut other = row(&founder(4), MemberState::Serving, false);
        other.id = 2;
        let rows = vec![other, row(&three, MemberState::Serving, false)];
        assert!(
            peers
                .set_members(rows, (Grants::default(), 3), (&[], Rejoin::Automatic))
                .is_empty()
        );
        assert_eq!(accepted(&mut peers), None);
    }
}
