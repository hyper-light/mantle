//! The orderly stop (docs/design/node.md §1.7): begun at its process's first stop signal
//! ([`Node::stop`]), it begins no object operation (each new one is answered `SlowDown`) and lets
//! those running end; then hands each range this node leads to a voter caught up with it and waits
//! until that voter leads, or until the range's core gives the transfer up (Raft §3.10: after about
//! an election timeout), after which the range elects as after a crash; then takes no connection,
//! closes each it holds and keeps each in its closing state until it drains (RFC 9000 §10.2: the
//! node keeps the socket its peers know, and answers their late packets). Each turn steps it
//! ([`Node::step_stop`]); once it is done ([`Node::stopped`]) nothing the node holds is owed to the
//! cell, and its process closes its log and volumes and ends.

use std::net::SocketAddr;

use hyper_transport::PeerId;
use mantle_meta::engine::Engine;

use super::{Node, NodeError, NodeLog};

/// Where an orderly stop stands.
pub(super) enum Stopping {
    /// Object operations running end; none begins.
    Objects,
    /// Each led range's lead handed over: the ranges whose transfer is out, each by its group and
    /// the voter it goes to.
    Handing(Vec<(u128, u64)>),
    /// No connection taken, each held closed and kept in its closing state until it drains.
    Closing,
    /// Done: nothing held is owed to the cell.
    Done,
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// Begins the orderly stop; a stop begun already goes on as it was.
    pub fn stop(&mut self) {
        if self.stopping.is_none() {
            self.stopping = Some(Stopping::Objects);
        }
    }

    /// Whether an orderly stop has begun.
    pub fn stopping(&self) -> bool {
        self.stopping.is_some()
    }

    /// Whether the orderly stop is done.
    pub fn stopped(&self) -> bool {
        matches!(self.stopping, Some(Stopping::Done))
    }

    /// The ranges whose lead this node handed over since the last call, each by its group and the
    /// voter that then led it, as this node's member learned.
    pub fn take_handed(&mut self) -> impl Iterator<Item = (u128, u64)> + '_ {
        self.handed.drain(..)
    }

    /// Whether the stop no longer takes connections or dials.
    pub(super) fn closing(&self) -> bool {
        matches!(self.stopping, Some(Stopping::Closing | Stopping::Done))
    }

    /// A turn's step of the stop.
    pub(super) fn step_stop(&mut self) -> Result<(), NodeError> {
        let Some(stopping) = self.stopping.take() else {
            return Ok(());
        };
        let next = match stopping {
            Stopping::Objects if self.jobs.is_empty() => Stopping::Handing(self.hand_over()),
            Stopping::Handing(out) => {
                let out = self.handing(out);
                if out.is_empty() {
                    self.refuse_connections()?;
                    Stopping::Closing
                } else {
                    Stopping::Handing(out)
                }
            }
            Stopping::Closing => {
                self.close_held()?;
                if self.driver.endpoint().stats().connections == 0 {
                    Stopping::Done
                } else {
                    Stopping::Closing
                }
            }
            held => held,
        };
        self.stopping = Some(next);
        Ok(())
    }

    /// Hands each range this node leads to a voter whose log holds everything committed, or, with
    /// none such, the voter whose log holds the most, which the core brings up to its own before
    /// it hands over (Raft §3.10): the ranges whose transfer is out.
    fn hand_over(&mut self) -> Vec<(u128, u64)> {
        let mut out = Vec::with_capacity(self.ranges.len());
        for at in 0..self.ranges.len() {
            let Some((group, handle)) = self.ranges.get(at).map(|k| (k.group, k.handle)) else {
                continue;
            };
            let Some(replica) = self.owner.get(handle) else {
                continue;
            };
            if !replica.is_leader() {
                continue;
            }
            let tracker = replica.core().raft.tracker();
            let target = replica
                .configuration()
                .voters
                .iter()
                .copied()
                .filter(|voter| *voter != self.id)
                .max_by_key(|voter| {
                    (
                        replica.caught_up(*voter),
                        tracker.get(*voter).map_or(0, |p| p.matched),
                    )
                });
            let Some(to) = target else {
                continue;
            };
            // A transfer the core refuses leaves the range to elect as after a crash.
            if let Some(replica) = self.owner.get_mut(handle)
                && replica.transfer(to).is_ok()
            {
                self.owner.schedule(handle);
                out.push((group, to));
            }
        }
        out
    }

    /// The transfers still out of `out`. A range whose lead another member now holds, as this
    /// node's member learned from it, is handed over (to the voter it went to, or to one the range
    /// elected instead); one whose member steps down to an election waits for its winner; one whose
    /// core gave the transfer up waits no more, and elects as after a crash once the node is gone.
    fn handing(&mut self, out: Vec<(u128, u64)>) -> Vec<(u128, u64)> {
        let mut still = Vec::with_capacity(out.len());
        for (group, to) in out {
            let Some(replica) = self.kept(group).and_then(|k| self.owner.get(k.handle)) else {
                continue;
            };
            let leader = replica.leader();
            if leader != 0 && leader != self.id {
                self.handed.push((group, leader));
            } else if leader == 0 || replica.core().raft.lead_transferee() == Some(to) {
                still.push((group, to));
            }
        }
        still
    }

    /// Takes no connection from now on, dials no peer, and closes each connection held.
    fn refuse_connections(&mut self) -> Result<(), NodeError> {
        if let Some(tls) = self.dial_only.take() {
            self.driver
                .endpoint()
                .set_tls(tls)
                .map_err(NodeError::Transport)?;
        }
        for remote in &mut self.remotes {
            remote.redial_at = None;
        }
        self.close_held()
    }

    /// Closes each connection held: a peer's identified since the stop took no more, as its
    /// handshake was under way, closes too.
    fn close_held(&mut self) -> Result<(), NodeError> {
        let clock = self.driver.clock()?;
        let mut held: Vec<(PeerId, SocketAddr)> = Vec::new();
        self.driver.endpoint().established(&mut held);
        for (peer, _) in held {
            self.driver.endpoint().disconnect(clock, peer);
        }
        Ok(())
    }
}
