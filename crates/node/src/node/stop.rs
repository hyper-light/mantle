//! The orderly stop (docs/design/node.md §1.7): begun at its process's first stop signal
//! ([`Node::stop`]), it begins no object operation (each new one is answered `SlowDown`) and lets
//! those running end; then hands each range this node leads to a voter caught up with it and waits
//! until that voter leads. A transfer the range's core gives up (Raft §3.10: one not done within
//! its rounds), the range still this node's, is begun again, to another voter where there is one:
//! on a loaded host a target can take longer than the core's rounds to campaign, and a range left
//! as it was would elect only after this node is gone, with no leader meanwhile, the gap the
//! stop is for. The stop's bound ends the tries. Then it tells its peers' liveness streams the run
//! ends in order (hyper-liveness `Liveness::stop`: its restart no failure, the run sent nothing
//! more), takes no connection, closes each it holds
//! and keeps each in its closing state until it drains (RFC 9000 §10.2: the node keeps the socket
//! its peers know, and answers their late packets). Each turn steps it ([`Node::step_stop`]); once
//! it is done ([`Node::stopped`]) nothing the node holds is owed to the cell, and its process closes
//! its log and volumes and ends. A range another shard drives is handed over by that shard
//! (`shard::In::Transfer`), which tells the voter it goes to (`shard::Out::Handing`), and the stop
//! follows it as that shard last showed it (node.md §2.9).

use std::net::SocketAddr;

use hyper_durable::Handle;
use hyper_transport::PeerId;
use mantle_meta::engine::Engine;

use super::{Node, NodeError, NodeLog, shard};

/// Where an orderly stop stands.
pub(super) enum Stopping {
    /// Object operations running end; none begins.
    Objects,
    /// Each led range's lead handed over: the ranges whose transfer is out, each by its group and
    /// the voter it goes to, 0 while a far range's shard has yet to tell it.
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

    /// Ends the orderly stop at its bound where all it has left is its connections' closing
    /// periods, and says whether it did: its ranges handed over and each connection closed, nothing
    /// it holds is owed to the cell, and a closing period, three probe timeouts that one slow round
    /// trip on a loaded host can stretch past the bound (windows-aarch64, a connection's 9.5 s),
    /// answers only late packets, which RFC 9000 §10.2 leaves to a stateless reset once its state
    /// is gone. A stop with more to do ends as a crash would.
    pub fn stop_at_bound(&mut self) -> bool {
        if matches!(self.stopping, Some(Stopping::Closing)) {
            self.stopping = Some(Stopping::Done);
        }
        self.stopped()
    }

    /// The ranges whose lead this node began to hand over since the last call, each by its group
    /// and the voter it goes to: a range handed again, once its core gave a transfer up, is told
    /// again.
    pub fn take_handing(&mut self) -> impl Iterator<Item = (u128, u64)> + '_ {
        self.handing.drain(..)
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
            Stopping::Objects if self.jobs.is_empty() => Stopping::Handing(self.hand_over()?),
            Stopping::Handing(out) => {
                let out = self.handing(out)?;
                if out.is_empty() {
                    // Every range handed over: the stop's last act on the plane tells each peer
                    // this run ends in order, so its next run is no failure in their MTBF's
                    // evidence and they send this one nothing more (hyper-liveness
                    // `Liveness::stop`), queued now so this turn's plane sends it.
                    self.liveness
                        .stop(hyper_rt::futures::now_ns(), &mut self.asked);
                    self.act_on_liveness()?;
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

    /// Hands each range this node leads to a voter ([`Node::transfer_lead`]; a far range's, as its
    /// shard last showed it, by that shard, [`shard::ask_transfer`]): the ranges whose transfer is
    /// out, each by its group and the voter it goes to, 0 for a far range's until its shard tells.
    fn hand_over(&mut self) -> Result<Vec<(u128, u64)>, NodeError> {
        let mut out = Vec::with_capacity(self.ranges.len());
        for at in 0..self.ranges.len() {
            let Some((group, handle)) = self.ranges.get(at).map(|k| (k.group, k.handle)) else {
                continue;
            };
            if let Some(far) = self.far.iter().find(|far| far.group == group) {
                if far.seen.leading && shard::ask_transfer(&self.shards, far, None)? {
                    out.push((group, 0));
                }
                continue;
            }
            if !self.owner.get(handle).is_some_and(|r| r.is_leader()) {
                continue;
            }
            if let Some(to) = self.transfer_lead(handle, None) {
                self.handing.push((group, to));
                out.push((group, to));
            }
        }
        Ok(out)
    }

    /// The voter a far range's hand-over goes to, as its shard told it ([`shard::Out::Handing`]):
    /// the stop's transfer of it out to that voter, or, where its shard began none, done, the
    /// range electing once this node is gone.
    pub(super) fn handing_far(&mut self, group: u128, to: Option<u64>) {
        let Some(Stopping::Handing(out)) = self.stopping.as_mut() else {
            return;
        };
        let Some(at) = out.iter().position(|&(g, voter)| g == group && voter == 0) else {
            return;
        };
        match to {
            Some(to) => {
                if let Some(entry) = out.get_mut(at) {
                    entry.1 = to;
                }
                // Its core hands to `to` from now on, which the range's next shown status, a drive
                // later, says too: until then the stop follows the shard's word.
                if let Some(far) = self.far.iter_mut().find(|far| far.group == group) {
                    far.seen.transferee = Some(to);
                }
                self.handing.push((group, to));
            }
            None => {
                out.swap_remove(at);
            }
        }
    }

    /// Begins handing the lead of the range at `handle`, which this node leads, to a voter whose
    /// log holds everything committed, or, with none such, the voter whose log holds the most,
    /// which the core brings up to its own before it hands over (Raft §3.10); once a transfer to
    /// `failed` was given up, to another voter where there is one. The voter it goes to, or `None`
    /// where the core refuses, after which the range elects as after a crash.
    fn transfer_lead(&mut self, handle: Handle, failed: Option<u64>) -> Option<u64> {
        let to = shard::transfer(self.owner.get_mut(handle)?, failed)?;
        self.owner.schedule(handle);
        Some(to)
    }

    /// The transfers still out of `out`. A range whose lead another member now holds, as this
    /// node's member learned from it, is handed over (to the voter it went to, or to one the range
    /// elected instead); one whose member steps down to an election waits for its winner; one whose
    /// core gave the transfer up, this node still leading, is handed again. A far range's is
    /// followed as its shard last showed it, and one whose shard has yet to tell its voter waits.
    fn handing(&mut self, out: Vec<(u128, u64)>) -> Result<Vec<(u128, u64)>, NodeError> {
        let mut still = Vec::with_capacity(out.len());
        for (group, to) in out {
            if let Some(far) = self.far.iter().find(|far| far.group == group) {
                let (leader, transferee) = (far.seen.leader, far.seen.transferee);
                if to == 0 {
                    still.push((group, to));
                } else if leader != 0 && leader != self.id {
                    self.handed.push((group, leader));
                } else if leader == 0 || transferee == Some(to) {
                    still.push((group, to));
                } else if shard::ask_transfer(&self.shards, far, Some(to))? {
                    still.push((group, 0));
                }
                continue;
            }
            let Some(handle) = self.kept(group).map(|k| k.handle) else {
                continue;
            };
            let Some(replica) = self.owner.get(handle) else {
                continue;
            };
            let leader = replica.leader();
            if leader != 0 && leader != self.id {
                self.handed.push((group, leader));
            } else if leader == 0 || replica.core().raft.lead_transferee() == Some(to) {
                still.push((group, to));
            } else if let Some(again) = self.transfer_lead(handle, Some(to)) {
                self.handing.push((group, again));
                still.push((group, again));
            }
        }
        Ok(still)
    }

    /// Takes no connection from now on, dials no peer, and closes each connection held.
    fn refuse_connections(&mut self) -> Result<(), NodeError> {
        if let Some(tls) = self.dial_only.take() {
            self.driver
                .endpoint()
                .set_tls(tls)
                .map_err(NodeError::Transport)?;
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
