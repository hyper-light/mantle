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
//! its log and volumes and ends.

use std::net::SocketAddr;

use hyper_durable::Handle;
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

/// Scratch (diag/roll-stop-bound): what the stop's timed lines remember.
#[derive(Default)]
pub(super) struct StopDiag {
    /// The runtime's clock as the stop began.
    began_ns: u64,
    /// Each range's transfers begun, by its group.
    attempts: Vec<(u128, u32)>,
    /// Each range's state as last said, by its group.
    said: Vec<(u128, String)>,
    /// The connections held and established as last said in the closing.
    connections: Option<(usize, usize)>,
    /// The jobs running as last said.
    jobs: Option<usize>,
    /// The stop's steps taken.
    steps: u64,
}

impl StopDiag {
    fn attempt(&mut self, group: u128) -> u32 {
        match self.attempts.iter_mut().find(|(g, _)| *g == group) {
            Some((_, n)) => {
                *n = n.saturating_add(1);
                *n
            }
            None => {
                self.attempts.push((group, 1));
                1
            }
        }
    }

    fn attempts(&self, group: u128) -> u32 {
        self.attempts
            .iter()
            .find(|(g, _)| *g == group)
            .map_or(0, |(_, n)| *n)
    }

    /// Whether `state` differs from what was last said of `group`, remembering it.
    fn moved(&mut self, group: u128, state: &str) -> bool {
        match self.said.iter_mut().find(|(g, _)| *g == group) {
            Some((_, was)) if was == state => false,
            Some((_, was)) => {
                *was = state.to_owned();
                true
            }
            None => {
                self.said.push((group, state.to_owned()));
                true
            }
        }
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// Begins the orderly stop; a stop begun already goes on as it was.
    pub fn stop(&mut self) {
        if self.stopping.is_none() {
            self.stopping = Some(Stopping::Objects);
            self.stop_diag.began_ns = hyper_rt::futures::now_ns();
            let led: Vec<String> = self
                .ranges
                .iter()
                .filter(|k| self.owner.get(k.handle).is_some_and(|r| r.is_leader()))
                .map(|k| format!("{:x}", k.group))
                .collect();
            let jobs: Vec<String> = self.jobs.iter().map(|j| j.diag()).collect();
            crate::diag::say(
                self.id,
                &format!(
                    "began ranges={} led=[{}] jobs={} [{}] connections={}",
                    self.ranges.len(),
                    led.join(","),
                    self.jobs.len(),
                    jobs.join(" "),
                    self.driver.endpoint().stats().connections
                ),
            );
        }
    }

    /// Scratch (diag/roll-stop-bound): milliseconds since the stop began.
    fn since_ms(&self) -> u64 {
        hyper_rt::futures::now_ns().saturating_sub(self.stop_diag.began_ns) / 1_000_000
    }

    /// Scratch (diag/roll-stop-bound): range `group`'s member as this node holds it, with the
    /// voter `to` its lead goes to.
    fn range_diag(&self, group: u128, to: u64) -> String {
        let Some(handle) = self.kept(group).map(|k| k.handle) else {
            return format!("{group:x}: not kept");
        };
        let Some(replica) = self.owner.get(handle) else {
            return format!("{group:x}: no replica");
        };
        let raft = &replica.core().raft;
        let last = raft.log().last_index().unwrap_or(0);
        let matched = raft.tracker().get(to).map(|p| p.matched);
        format!(
            "{group:x} to={to} leader={} term={} role={:?} transferee={:?} last={last} \
             commit={} matched={matched:?} caught_up={} elapsed={} deadline={:?} timing={:?}",
            replica.leader(),
            raft.term(),
            raft.state(),
            raft.lead_transferee(),
            raft.log().committed(),
            replica.caught_up(to),
            raft.election_elapsed(),
            raft.deadline(),
            raft.timing().map(|t| (t.round, t.span, t.election)),
        )
    }

    /// Scratch (diag/roll-stop-bound): where the stop stands and what it waits on.
    pub fn stop_diag(&mut self) -> String {
        let since = self.since_ms();
        let steps = self.stop_diag.steps;
        let phase = match &self.stopping {
            None => "not stopping".to_owned(),
            Some(Stopping::Objects) => {
                let jobs: Vec<String> = self.jobs.iter().map(|j| j.diag()).collect();
                format!("objects jobs={} [{}]", self.jobs.len(), jobs.join(" "))
            }
            Some(Stopping::Handing(out)) => {
                let out = out.clone();
                let ranges: Vec<String> = out
                    .iter()
                    .map(|(group, to)| {
                        format!(
                            "[{} attempts={}]",
                            self.range_diag(*group, *to),
                            self.stop_diag.attempts(*group)
                        )
                    })
                    .collect();
                format!("handing out={} {}", out.len(), ranges.join(" "))
            }
            Some(Stopping::Closing) => self.closing_diag(),
            Some(Stopping::Done) => "done".to_owned(),
        };
        format!("since_ms={since} steps={steps} phase: {phase}")
    }

    /// Scratch (diag/roll-stop-bound): the connections the closing waits on.
    fn closing_diag(&mut self) -> String {
        let mut held: Vec<(PeerId, SocketAddr)> = Vec::new();
        self.driver.endpoint().established(&mut held);
        let connections = self.driver.endpoint().stats().connections;
        let next = match (self.driver.endpoint().poll_timeout(), self.driver.clock()) {
            (Some(due), Ok(now)) => format!(
                "{}ms",
                due.saturating_duration_since(now).as_millis()
            ),
            (None, _) => "none".to_owned(),
            (Some(_), Err(e)) => format!("clock {e:?}"),
        };
        let peers: Vec<String> = held.iter().map(|(p, a)| format!("{p:?}@{a}")).collect();
        format!(
            "closing connections={connections} established={} [{}] next_timer={next}",
            held.len(),
            peers.join(" ")
        )
    }

    /// Whether an orderly stop has begun.
    pub fn stopping(&self) -> bool {
        self.stopping.is_some()
    }

    /// Whether the orderly stop is done.
    pub fn stopped(&self) -> bool {
        matches!(self.stopping, Some(Stopping::Done))
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
        self.stop_diag.steps = self.stop_diag.steps.saturating_add(1);
        if matches!(stopping, Stopping::Objects) && self.stop_diag.jobs != Some(self.jobs.len()) {
            self.stop_diag.jobs = Some(self.jobs.len());
            let jobs: Vec<String> = self.jobs.iter().map(|j| j.diag()).collect();
            crate::diag::say(
                self.id,
                &format!(
                    "objects since_ms={} jobs={} [{}]",
                    self.since_ms(),
                    self.jobs.len(),
                    jobs.join(" ")
                ),
            );
        }
        let next = match stopping {
            Stopping::Objects if self.jobs.is_empty() => {
                let out = self.hand_over();
                let ranges: Vec<String> = out
                    .iter()
                    .map(|(group, to)| format!("[{}]", self.range_diag(*group, *to)))
                    .collect();
                crate::diag::say(
                    self.id,
                    &format!(
                        "hand-over since_ms={} out={} {}",
                        self.since_ms(),
                        out.len(),
                        ranges.join(" ")
                    ),
                );
                Stopping::Handing(out)
            }
            Stopping::Handing(out) => {
                let out = self.handing(out);
                if out.is_empty() {
                    crate::diag::say(
                        self.id,
                        &format!(
                            "handed-all since_ms={} connections={}",
                            self.since_ms(),
                            self.driver.endpoint().stats().connections
                        ),
                    );
                    // Every range handed over: the stop's last act on the plane tells each peer
                    // this run ends in order, so its next run is no failure in their MTBF's
                    // evidence and they send this one nothing more (hyper-liveness
                    // `Liveness::stop`), queued now so this turn's plane sends it.
                    self.liveness
                        .stop(hyper_rt::futures::now_ns(), &mut self.asked);
                    self.act_on_liveness()?;
                    crate::diag::say(
                        self.id,
                        &format!("liveness-stopped since_ms={}", self.since_ms()),
                    );
                    self.refuse_connections()?;
                    let closing = self.closing_diag();
                    crate::diag::say(
                        self.id,
                        &format!("refused since_ms={} {closing}", self.since_ms()),
                    );
                    Stopping::Closing
                } else {
                    Stopping::Handing(out)
                }
            }
            Stopping::Closing => {
                self.close_held()?;
                let mut held: Vec<(PeerId, SocketAddr)> = Vec::new();
                self.driver.endpoint().established(&mut held);
                let now = (self.driver.endpoint().stats().connections, held.len());
                if self.stop_diag.connections != Some(now) {
                    self.stop_diag.connections = Some(now);
                    let closing = self.closing_diag();
                    crate::diag::say(
                        self.id,
                        &format!("closing-step since_ms={} {closing}", self.since_ms()),
                    );
                }
                if self.driver.endpoint().stats().connections == 0 {
                    crate::diag::say(
                        self.id,
                        &format!(
                            "done since_ms={} steps={}",
                            self.since_ms(),
                            self.stop_diag.steps
                        ),
                    );
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

    /// Hands each range this node leads to a voter ([`Node::transfer_lead`]): the ranges whose
    /// transfer is out, each by its group and the voter it goes to.
    fn hand_over(&mut self) -> Vec<(u128, u64)> {
        let mut out = Vec::with_capacity(self.ranges.len());
        for at in 0..self.ranges.len() {
            let Some((group, handle)) = self.ranges.get(at).map(|k| (k.group, k.handle)) else {
                continue;
            };
            if !self.owner.get(handle).is_some_and(|r| r.is_leader()) {
                continue;
            }
            if let Some(to) = self.transfer_lead(handle, None) {
                self.stop_diag.attempt(group);
                self.handing.push((group, to));
                out.push((group, to));
            } else {
                crate::diag::say(
                    self.id,
                    &format!(
                        "transfer-refused {group:x} since_ms={} (elects as after a crash)",
                        self.since_ms()
                    ),
                );
            }
        }
        out
    }

    /// Begins handing the lead of the range at `handle`, which this node leads, to a voter whose
    /// log holds everything committed, or, with none such, the voter whose log holds the most,
    /// which the core brings up to its own before it hands over (Raft §3.10); once a transfer to
    /// `failed` was given up, to another voter where there is one. The voter it goes to, or `None`
    /// where the core refuses, after which the range elects as after a crash.
    fn transfer_lead(&mut self, handle: Handle, failed: Option<u64>) -> Option<u64> {
        let replica = self.owner.get(handle)?;
        let tracker = replica.core().raft.tracker();
        let to = replica
            .configuration()
            .voters
            .iter()
            .copied()
            .filter(|voter| *voter != self.id)
            .max_by_key(|voter| {
                (
                    Some(*voter) != failed,
                    replica.caught_up(*voter),
                    tracker.get(*voter).map_or(0, |p| p.matched),
                )
            })?;
        self.owner.get_mut(handle)?.transfer(to).ok()?;
        self.owner.schedule(handle);
        Some(to)
    }

    /// The transfers still out of `out`. A range whose lead another member now holds, as this
    /// node's member learned from it, is handed over (to the voter it went to, or to one the range
    /// elected instead); one whose member steps down to an election waits for its winner; one whose
    /// core gave the transfer up, this node still leading, is handed again.
    fn handing(&mut self, out: Vec<(u128, u64)>) -> Vec<(u128, u64)> {
        let mut still = Vec::with_capacity(out.len());
        for (group, to) in out {
            let Some(handle) = self.kept(group).map(|k| k.handle) else {
                continue;
            };
            let Some(replica) = self.owner.get(handle) else {
                continue;
            };
            let leader = replica.leader();
            let state = self.range_diag(group, to);
            // The state said without its clocks, which move every step.
            let key = state.split(" elapsed=").next().unwrap_or_default().to_owned();
            if self.stop_diag.moved(group, &key) {
                crate::diag::say(
                    self.id,
                    &format!("range since_ms={} {state}", self.since_ms()),
                );
            }
            let Some(replica) = self.owner.get(handle) else {
                continue;
            };
            if leader != 0 && leader != self.id {
                crate::diag::say(
                    self.id,
                    &format!(
                        "handed {group:x} leader={leader} to={to} attempts={} since_ms={}",
                        self.stop_diag.attempts(group),
                        self.since_ms()
                    ),
                );
                self.handed.push((group, leader));
            } else if leader == 0 || replica.core().raft.lead_transferee() == Some(to) {
                still.push((group, to));
            } else if let Some(again) = self.transfer_lead(handle, Some(to)) {
                let n = self.stop_diag.attempt(group);
                crate::diag::say(
                    self.id,
                    &format!(
                        "again {group:x} given-up-to={to} now-to={again} attempt={n} since_ms={}",
                        self.since_ms()
                    ),
                );
                self.handing.push((group, again));
                still.push((group, again));
            } else {
                crate::diag::say(
                    self.id,
                    &format!(
                        "again-refused {group:x} given-up-to={to} since_ms={} (dropped from out)",
                        self.since_ms()
                    ),
                );
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
