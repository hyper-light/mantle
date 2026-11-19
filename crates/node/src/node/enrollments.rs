//! A written-off node's request for a new identity on a member (docs/design/membership.md §5.1
//! step 3): read once from the body of the one exchange its enrolling connection opens, judged
//! against the old identity its handshake proved (`Peers::enrolling`), never what the request
//! names, forwarded to the root range's leader, and answered once.
//!
//! - **The member** holds at most as many requests as the commands it asks at once (the
//!   enrollment bound, `command_asks`), each until the stock patience has passed
//!   (`bounds::STOCK_PATIENCE`), after which it is answered `Unanswered`. A node that does not lead
//!   the root range forwards the request to the one that does, naming the proved identity
//!   (`enroll::Request::Forward`), and relays its answer; one forwarded to a node that no longer
//!   leads is answered `Unled` there and forwarded again here to the leader known then.
//! - **The root range's leader** draws the new id from the operating system, takes the issuer
//!   window's next grant, one re-grant out at a time, signs (`crate::enroll::sign`) and proposes
//!   the `Regrant`, and answers the root range's outcome: the certificate the ledger holds for the
//!   old identity, the issuer's and the members' places, or the refusal.

use hyper_transport::{ExchangeId, PeerId, Progress, Refusal};
use mantle_meta::engine::Engine;
use mantle_meta::record::MemberState;
use mantle_meta::root;
use mantle_meta::wire::{Answer, Command};
use mantle_protocol::bounds;
use mantle_protocol::classes::Kind;
use mantle_protocol::enroll::{self as wire, Granted, Member, Request};

use super::{Arriving, Body, Node, NodeError, NodeLog, Receiving, Then, Writing};
use crate::ask::{CommandAsk, CommandOutcome, Wait};
use crate::directory::Enrolling;
use crate::enroll::{self, EnrollError};

/// A request held: the exchange its answer goes on, the old identity and the key asked for, when
/// its patience ends on the shard's clock, and where it stands.
pub(super) struct Enrollment {
    exchange: ExchangeId,
    enrolling: Enrolling,
    key: [u8; wire::KEY],
    /// A member's forward: answered `Unled` where this node does not lead the root range, never
    /// forwarded on.
    forwarded: bool,
    until_ns: u64,
    stands: Stands,
}

enum Stands {
    /// Waiting for the root range's leader: this node once it leads and no re-grant of its own is
    /// out, else the leader it knows.
    Waiting,
    /// Forwarded to the leader on this exchange.
    Forwarded(ExchangeId),
    /// Proposed by this node as the root range's leader: its command, and the certificate signed.
    Proposed {
        ask: CommandAsk,
        node: u64,
        grant: u64,
        certificate: Vec<u8>,
    },
    /// Refused `NotRemoved` by the root range: waiting, until `until_ns` ([`removal_waited`]), for
    /// the rows to hold the old member removed or no longer hold it, then proposed again.
    Removing { until_ns: u64 },
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// An `Enroll` exchange arrived: its request, its body, read whole before it is taken. One
    /// longer than any request is refused before it is read.
    pub(super) fn take_enrollment(
        &mut self,
        exchange: ExchangeId,
        peer: PeerId,
        body: Option<u64>,
    ) -> Result<(), NodeError> {
        let body = body.unwrap_or(0);
        let longest = u64::try_from(wire::MAX_REQUEST).unwrap_or(u64::MAX);
        if body == 0 || body > longest {
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::Kind);
            return Ok(());
        }
        self.receiving.push(Receiving {
            exchange,
            peer,
            body,
            read: 0,
            bytes: Vec::with_capacity(wire::MAX_REQUEST),
            what: Arriving::Enroll,
        });
        self.read_exchange(exchange)
    }

    /// The request on `exchange`, read whole, taken once its asker is known: a written-off node on
    /// its enrolling connection, judged against the identity its handshake proved, or a member
    /// forwarding one's. One past the enrollment bound, or a second of one node while its first is
    /// held, is refused `Budget`, as a chunk with no room is.
    pub(super) fn enrollment_read(
        &mut self,
        exchange: ExchangeId,
        peer: PeerId,
        bytes: &[u8],
    ) -> Result<(), NodeError> {
        let now = hyper_rt::futures::now_ns();
        let endpoint = self.driver.endpoint();
        let directory = endpoint.directory();
        let asked = Request::decode(bytes)
            .ok()
            .and_then(|r| asker(r, directory.enrolling(peer), directory.is_node(peer)));
        let Some((enrolling, key, forwarded)) = asked else {
            endpoint.refuse_exchange(exchange, Refusal::Kind);
            return Ok(());
        };
        if self.enrollments.len() >= self.command_asks
            || self
                .enrollments
                .iter()
                .any(|e| !e.forwarded && !forwarded && e.enrolling.old == enrolling.old)
        {
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::Budget);
            return Ok(());
        }
        // Judged against the identity the handshake proved, before the root range is asked.
        if let Err(refused) = enroll::judge(self.issuer.as_ref(), &enrolling, &key) {
            return self.answer_enrollment(exchange, &wire::Answer::Refused(refusal_of(refused)));
        }
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap_or(u64::MAX);
        self.enrollments.push(Enrollment {
            exchange,
            enrolling,
            key,
            forwarded,
            until_ns: now.saturating_add(patience),
            stands: Stands::Waiting,
        });
        self.step_enrollments(now)
    }

    /// A turn's step of the requests held: those past their patience answered `Unanswered`, each
    /// proposal's outcome answered, one refused `NotRemoved` waiting for the old member's removal
    /// (membership.md §5.1 step 3) and proposed again once the rows hold it, or answered so once
    /// its wait's bound passes ([`removal_waited`]), and each waiting one proposed as the leader,
    /// one re-grant out at a time, or forwarded to the leader.
    pub(super) fn step_enrollments(&mut self, now: u64) -> Result<(), NodeError> {
        let mut at = 0;
        while at < self.enrollments.len() {
            let Some(held) = self.enrollments.get(at) else {
                break;
            };
            let (exchange, until_ns, old) = (held.exchange, held.until_ns, held.enrolling.old);
            let removing = match held.stands {
                Stands::Removing { until_ns } => Some(until_ns),
                _ => None,
            };
            let outcome = match &held.stands {
                Stands::Proposed { ask, .. } => self.take_command(*ask),
                _ => None,
            };
            let taken = outcome.is_some();
            let waits = removal_waited(until_ns, bounds::idle_timeout());
            let answer = match (outcome, removing) {
                (Some(outcome), _) => match self.regrant_answer(at, outcome) {
                    wire::Answer::Refused(wire::Refusal::NotRemoved) if now < waits => {
                        self.stand(at, Stands::Removing { until_ns: waits });
                        None
                    }
                    answer => Some(answer),
                },
                (None, Some(_)) if self.removed(old) => {
                    self.stand(at, Stands::Waiting);
                    None
                }
                (None, Some(waits)) if waits <= now => {
                    Some(wire::Answer::Refused(wire::Refusal::NotRemoved))
                }
                (None, _) if until_ns <= now => {
                    Some(wire::Answer::Refused(wire::Refusal::Unanswered))
                }
                (None, _) => None,
            };
            match answer {
                Some(answer) => {
                    let mut held = self.enrollments.swap_remove(at);
                    // A proposal whose outcome was taken leaves nothing to drop.
                    if taken {
                        held.stands = Stands::Waiting;
                    }
                    self.let_go(held);
                    self.answer_enrollment(exchange, &answer)?;
                }
                None => at = at.saturating_add(1),
            }
        }
        let Some((_, group)) = self.root_range() else {
            return Ok(());
        };
        let leads = self.is_leader(group);
        let proposing = self
            .enrollments
            .iter()
            .any(|e| matches!(e.stands, Stands::Proposed { .. }));
        let waiting: Vec<usize> = self
            .enrollments
            .iter()
            .enumerate()
            .filter(|(_, e)| matches!(e.stands, Stands::Waiting))
            .map(|(at, _)| at)
            .collect();
        for at in waiting {
            if leads {
                if proposing || !self.command_room() {
                    break;
                }
                self.propose_regrant(at, group)?;
                break;
            }
            let forwarded = self.enrollments.get(at).is_some_and(|e| e.forwarded);
            if forwarded {
                let exchange = self.enrollments.swap_remove(at).exchange;
                self.answer_enrollment(exchange, &wire::Answer::Refused(wire::Refusal::Unled))?;
                // The indices after it moved: the rest wait for the next turn.
                break;
            }
            self.forward_enrollment(at, group, now)?;
        }
        Ok(())
    }

    /// Request `at` stands at `stands` from here.
    fn stand(&mut self, at: usize, stands: Stands) {
        if let Some(held) = self.enrollments.get_mut(at) {
            held.stands = stands;
        }
    }

    /// Whether the root range's rows, as this node last read them, hold node `old` removed or no
    /// longer hold it: the fact a request refused `NotRemoved` waits for, after which no volume of
    /// it is left in the table and nothing on its disks is needed (membership.md §5.1).
    fn removed(&self, old: u64) -> bool {
        self.cell.as_ref().is_some_and(|c| {
            c.members()
                .iter()
                .find(|(id, _)| *id == old)
                .is_none_or(|(_, row)| row.state == MemberState::Removed)
        })
    }

    /// As the root range's leader, re-grants request `at`: a new id drawn from the operating
    /// system, the issuer window's next grant, the certificate the issuer signs for them, and the
    /// `Regrant` proposed. A request the issuer refuses is answered so.
    fn propose_regrant(&mut self, at: usize, group: u128) -> Result<(), NodeError> {
        let Some(held) = self.enrollments.get(at) else {
            return Ok(());
        };
        let (enrolling, key, exchange) = (held.enrolling, held.key, held.exchange);
        let mut drawn = [0u8; 8];
        let regrant = match getrandom::fill(&mut drawn) {
            Ok(()) => {
                // The window's top as the rows last read it, or the grant this node's last
                // re-grant took, which the rows may not yet hold.
                let top = self.cell.as_ref().map_or(0, |c| c.issuer_top());
                match next_grant(top, self.issuer_spent) {
                    Some(grant) => enroll::sign(
                        self.issuer.as_ref(),
                        (&enrolling, &key),
                        (u64::from_le_bytes(drawn), grant),
                        (self.max_nodes, self.wall.now_ns() / NANOS_A_SECOND),
                    )
                    .map_err(|e| match e {
                        EnrollError::Refused(refused) => refusal_of(refused),
                        EnrollError::Certificate(_) => wire::Refusal::Unissued,
                    }),
                    None => Err(wire::Refusal::Full),
                }
            }
            Err(_) => Err(wire::Refusal::Unanswered),
        };
        let regrant = match regrant {
            Ok(regrant) => regrant,
            Err(refusal) => {
                self.enrollments.swap_remove(at);
                return self.answer_enrollment(exchange, &wire::Answer::Refused(refusal));
            }
        };
        let (node, grant, certificate) = (regrant.node, regrant.grant, regrant.certificate.clone());
        let ask = self.command(
            group,
            Command::Root(root::Command::Regrant(regrant)),
            u64::MAX,
            Wait::Request,
        )?;
        if let Some(held) = self.enrollments.get_mut(at) {
            held.stands = Stands::Proposed {
                ask,
                node,
                grant,
                certificate,
            };
        }
        Ok(())
    }

    /// Forwards request `at` to the root range's leader this node knows, naming the identity the
    /// written-off node's handshake proved; none known, it waits for the next turn.
    fn forward_enrollment(&mut self, at: usize, group: u128, now: u64) -> Result<(), NodeError> {
        let leader = self.leader_of(group);
        let Some(held) = self.enrollments.get(at) else {
            return Ok(());
        };
        if leader == 0 || leader == self.id {
            return Ok(());
        }
        let body = Request::Forward {
            old: held.enrolling.old,
            fingerprint: held.enrolling.fingerprint,
            old_key: held.enrolling.key,
            key: held.key,
        }
        .encode();
        let left = held.until_ns.saturating_sub(now).max(1);
        let len = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let clock = self.driver.clock()?;
        let period = Progress::new(std::time::Duration::from_nanos(left))?;
        // Not reached: forwarded again at the next turn, within its patience.
        let Ok(out) =
            self.driver
                .endpoint()
                .open(clock, leader, Kind::Enroll, &[], Some(len), period)
        else {
            return Ok(());
        };
        if let Some(held) = self.enrollments.get_mut(at) {
            held.stands = Stands::Forwarded(out);
        }
        self.writing.push(Writing {
            exchange: out,
            bytes: Body::Made(body),
            written: 0,
            then: Then::Forwarded,
        });
        self.write_exchange(out);
        Ok(())
    }

    /// Whether `exchange` is a request this node forwarded to the leader.
    pub(super) fn forwarded_on(&self, exchange: ExchangeId) -> bool {
        self.enrollments
            .iter()
            .any(|e| matches!(e.stands, Stands::Forwarded(out) if out == exchange))
    }

    /// The leader replied to a forward on `exchange`: its answer, the reply's body, read whole
    /// before it is relayed.
    pub(super) fn forward_replied(
        &mut self,
        exchange: ExchangeId,
        body: Option<u64>,
    ) -> Result<(), NodeError> {
        let body = body.unwrap_or(0);
        let Some(peer) = self
            .enrollments
            .iter()
            .find(|e| matches!(e.stands, Stands::Forwarded(out) if out == exchange))
            .map(|e| e.enrolling.old)
        else {
            return Ok(());
        };
        self.receiving.push(Receiving {
            exchange,
            peer,
            body,
            read: 0,
            bytes: Vec::with_capacity(usize::try_from(body).unwrap_or(0)),
            what: Arriving::Enrolled,
        });
        self.read_exchange(exchange)
    }

    /// The leader's answer to a forward on `exchange`, whole: relayed to the written-off node,
    /// unless the node asked no longer leads, when the request waits to be forwarded again.
    pub(super) fn forward_answered(
        &mut self,
        exchange: ExchangeId,
        bytes: &[u8],
    ) -> Result<(), NodeError> {
        self.driver.endpoint().end(exchange);
        let Some(at) = self
            .enrollments
            .iter()
            .position(|e| matches!(e.stands, Stands::Forwarded(out) if out == exchange))
        else {
            return Ok(());
        };
        let answer =
            wire::Answer::decode(bytes).unwrap_or(wire::Answer::Refused(wire::Refusal::Unanswered));
        if answer == wire::Answer::Refused(wire::Refusal::Unled) {
            if let Some(held) = self.enrollments.get_mut(at) {
                held.stands = Stands::Waiting;
            }
            return Ok(());
        }
        let origin = self.enrollments.swap_remove(at).exchange;
        self.answer_enrollment(origin, &answer)
    }

    /// Lets go of request `held`, answered or lost: its forward ended, and its proposal's outcome,
    /// which may still come, taken and dropped with the root range's duties (`step_duties`), so it
    /// holds none of the node's commands for good.
    fn let_go(&mut self, held: Enrollment) {
        match held.stands {
            Stands::Forwarded(out) => {
                self.driver.endpoint().end(out);
                self.receiving.remove(out);
            }
            Stands::Proposed { ask, .. } => self.duties.push((ask, held.enrolling.old)),
            Stands::Waiting | Stands::Removing { .. } => {}
        }
    }

    /// `exchange` ended without its answer: a request whose own exchange it was is let go, and
    /// one forwarded on it waits to be forwarded again.
    pub(super) fn enrollment_lost(&mut self, exchange: ExchangeId) {
        while let Some(at) = self.enrollments.iter().position(|e| e.exchange == exchange) {
            let held = self.enrollments.swap_remove(at);
            self.let_go(held);
        }
        for held in &mut self.enrollments {
            if matches!(held.stands, Stands::Forwarded(out) if out == exchange) {
                held.stands = Stands::Waiting;
            }
        }
    }

    /// Answers the request on `exchange`, once: the answer's bytes are the reply's body, and the
    /// exchange ends once they are written whole.
    fn answer_enrollment(
        &mut self,
        exchange: ExchangeId,
        answer: &wire::Answer,
    ) -> Result<(), NodeError> {
        let bytes = answer
            .encode()
            .map_err(|e| NodeError::Bounds(e.to_string()))?;
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        match self.driver.endpoint().reply(exchange, &[], Some(len)) {
            Ok(()) => {
                self.writing.push(Writing {
                    exchange,
                    bytes: Body::Made(bytes),
                    written: 0,
                    then: Then::Reply,
                });
                self.write_exchange(exchange);
            }
            // Its asker is gone: nothing is left to answer.
            Err(_) => self.driver.endpoint().end(exchange),
        }
        Ok(())
    }

    /// The answer to request `at`, whose proposal came to `outcome`: the certificate the ledger
    /// holds for its old identity, the issuer's and the members' places, or the refusal.
    fn regrant_answer(&mut self, at: usize, outcome: CommandOutcome) -> wire::Answer {
        let refused = wire::Answer::Refused;
        let Some(held) = self.enrollments.get(at) else {
            return refused(wire::Refusal::Unanswered);
        };
        let outcome = match outcome {
            CommandOutcome::Answered(Answer::Root(outcome)) => outcome,
            CommandOutcome::Answered(_) | CommandOutcome::Deadline | CommandOutcome::Refused(_) => {
                return refused(wire::Refusal::Unanswered);
            }
        };
        let root::Outcome::Granted { node, grant } = outcome else {
            return refused(refusal_of_outcome(&outcome));
        };
        let certificate = match &held.stands {
            Stands::Proposed {
                node: proposed,
                grant: taken,
                certificate,
                ..
            } if *proposed == node && *taken == grant => Some(certificate.clone()),
            // Re-granted before, to the same key: the certificate the ledger holds.
            _ => self
                .cell
                .as_ref()
                .and_then(|c| {
                    c.ledger()
                        .iter()
                        .find(|(old, _)| *old == held.enrolling.old)
                })
                .and_then(|(_, entry)| entry.regranted.as_ref())
                .filter(|r| r.node == node && r.grant == grant)
                .map(|r| r.certificate.clone()),
        };
        let (Some(certificate), Some(issuer)) = (certificate, self.issuer.as_ref()) else {
            return refused(wire::Refusal::Unanswered);
        };
        let members = self.cell.as_ref().map_or_else(Vec::new, |c| {
            c.members()
                .iter()
                .filter(|(_, row)| matches!(row.state, MemberState::Serving | MemberState::Leaving))
                .map(|(id, row)| Member {
                    id: *id,
                    quic: row.quic.clone(),
                    plane: row.plane.clone(),
                })
                .collect()
        });
        let answer = wire::Answer::Granted(Granted {
            certificate,
            issuer: issuer.certificate().to_vec(),
            members,
        });
        self.issuer_spent = self.issuer_spent.max(grant);
        answer
    }
}

/// The wire's refusal for the root range's outcome of a re-grant other than `Granted`.
fn refusal_of_outcome(outcome: &root::Outcome) -> wire::Refusal {
    match outcome {
        root::Outcome::Off => wire::Refusal::Off,
        root::Outcome::Unknown => wire::Refusal::Unknown,
        root::Outcome::Banned => wire::Refusal::Banned,
        root::Outcome::Regranted => wire::Refusal::Regranted,
        root::Outcome::NotRemoved => wire::Refusal::NotRemoved,
        root::Outcome::Conflict => wire::Refusal::Conflict,
        root::Outcome::Stale { .. } => wire::Refusal::Stale,
        root::Outcome::Full => wire::Refusal::Full,
        _ => wire::Refusal::Unanswered,
    }
}

/// Who asks `request`, and for what key: a written-off node's `Regrant` as its enrolling
/// connection's handshake proved it (`enrolling`, the directory's), never anything the request
/// names; a member's `Forward` (`member`, a node the directory admits that is not enrolling) as
/// it names the identity its own handshake proved. Whether it is a forward. None for any other.
fn asker(
    request: Request,
    enrolling: Option<Enrolling>,
    member: bool,
) -> Option<(Enrolling, [u8; wire::KEY], bool)> {
    match request {
        Request::Regrant { key } => enrolling.map(|e| (e, key, false)),
        Request::Forward {
            old,
            fingerprint,
            old_key,
            key,
        } if enrolling.is_none() && member => Some((
            Enrolling {
                old,
                fingerprint,
                key: old_key,
            },
            key,
            true,
        )),
        Request::Forward { .. } => None,
    }
}

/// The issuer's grant a re-grant takes: the one after the highest the window has spent as the
/// root range's rows last held it, `top`, or the highest this node's own re-grants spent, `spent`,
/// which the rows may not yet hold; none past the grants a u64 numbers.
fn next_grant(top: u64, spent: u64) -> Option<u64> {
    top.max(spent).checked_add(1)
}

/// When a request the root range refused `NotRemoved` stops waiting for the old member's removal
/// and is answered so: its patience's end, `until_ns` (the stock patience from its read), less
/// [`ANSWER_LEGS`] idle timeouts (`idle`, RFC 9000 §10.1). The written-off node gives each member
/// the stock patience from its dial (`rejoin::ask`), and the dial connects within one idle timeout
/// before its request is read here; the answer reaches it within another, or its connection
/// ends; so the answer arrives while it still listens.
fn removal_waited(until_ns: u64, idle: std::time::Duration) -> u64 {
    let idle = u64::try_from(idle.as_nanos()).unwrap_or(u64::MAX);
    until_ns.saturating_sub(idle.saturating_mul(ANSWER_LEGS))
}

/// The legs a request's wait leaves room for within its asker's patience, an idle timeout each:
/// the asker's dial before the request is read, and the answer's way back.
const ANSWER_LEGS: u64 = 2;

/// Nanoseconds a second: a unit conversion.
const NANOS_A_SECOND: u64 = 1_000_000_000;

/// The wire's refusal for one a member makes itself.
fn refusal_of(refused: enroll::Refused) -> wire::Refusal {
    match refused {
        enroll::Refused::Unissued => wire::Refusal::Unissued,
        enroll::Refused::SameKey => wire::Refusal::SameKey,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// membership.md §5.1 step 3: a request refused `NotRemoved` waits for the old member's
    /// removal no longer than leaves its answer time to reach the written-off node within the
    /// patience it gave this member from its dial, which connected within an idle timeout before
    /// the request was read; a patience shorter than the legs waits none.
    #[test]
    fn a_wait_for_removal_ends_while_its_asker_still_listens() {
        let idle = bounds::idle_timeout();
        let idle_ns = u64::try_from(idle.as_nanos()).unwrap();
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap();
        let dialed = 1_000_000_000_000u64;
        // Read as late as the dial's connection allows.
        let read = dialed + idle_ns;
        let waits = removal_waited(read + patience, idle);
        assert_eq!(waits, read + patience - 2 * idle_ns);
        assert!(waits > read, "the stock patience leaves the wait no time");
        // The answer, sent at the wait's end, arrives within an idle timeout: before the asker's
        // patience, from its dial, passes.
        assert!(waits + idle_ns <= dialed + patience);
        assert_eq!(removal_waited(idle_ns, idle), 0);
    }

    fn proved() -> Enrolling {
        Enrolling {
            old: 4,
            fingerprint: [7; 32],
            key: [3; 32],
        }
    }

    /// membership.md §5.1 step 3: a written-off node's request is taken as its handshake proved
    /// it, never as anything it names; a forward only from a member that is not enrolling, as it
    /// names the identity its own handshake proved; and the judge applies to that identity.
    #[test]
    fn a_request_is_judged_as_its_handshake_proved_it() {
        let fresh = Request::Regrant { key: [2; 32] };
        assert_eq!(
            asker(fresh, Some(proved()), false),
            Some((proved(), [2; 32], false))
        );
        // No enrolling connection proved it: refused, whatever the peer is.
        assert_eq!(asker(fresh, None, true), None);
        let forward = Request::Forward {
            old: 4,
            fingerprint: [7; 32],
            old_key: [3; 32],
            key: [2; 32],
        };
        assert_eq!(asker(forward, None, true), Some((proved(), [2; 32], true)));
        // An enrolling node forwards nothing, and a peer no member forwards nothing.
        assert_eq!(asker(forward, Some(proved()), true), None);
        assert_eq!(asker(forward, None, false), None);
        // The old key asked again is the proved identity's own: refused before the root range.
        let (enrolling, key, _) =
            asker(Request::Regrant { key: [3; 32] }, Some(proved()), false).unwrap();
        assert_eq!(
            enroll::judge(None, &enrolling, &key),
            Err(enroll::Refused::Unissued)
        );
    }

    /// The wire bounds what the root range holds as the root range bounds it.
    #[test]
    fn the_wire_bounds_what_the_root_range_holds_alike() {
        assert_eq!(wire::MAX_REACH, root::MAX_REACH);
        assert_eq!(wire::KEY, mantle_meta::record::PUBLIC_KEY);
        assert_eq!(wire::FINGERPRINT, mantle_meta::record::FINGERPRINT);
    }

    /// The issuer's next grant is past both the window's top and this node's own last spent.
    #[test]
    fn the_next_grant_is_past_the_window_and_this_nodes_last() {
        assert_eq!(next_grant(0, 0), Some(1));
        assert_eq!(next_grant(5, 3), Some(6));
        assert_eq!(next_grant(5, 9), Some(10));
        assert_eq!(next_grant(u64::MAX, 0), None);
    }

    /// Each refusal of the root range's re-grant is answered as itself on the wire.
    #[test]
    fn each_root_refusal_is_answered_as_itself() {
        for (outcome, refusal) in [
            (root::Outcome::Off, wire::Refusal::Off),
            (root::Outcome::Unknown, wire::Refusal::Unknown),
            (root::Outcome::Banned, wire::Refusal::Banned),
            (root::Outcome::Regranted, wire::Refusal::Regranted),
            (root::Outcome::NotRemoved, wire::Refusal::NotRemoved),
            (root::Outcome::Conflict, wire::Refusal::Conflict),
            (root::Outcome::Stale { top: 3 }, wire::Refusal::Stale),
            (root::Outcome::Full, wire::Refusal::Full),
            (root::Outcome::Kept, wire::Refusal::Unanswered),
        ] {
            assert_eq!(refusal_of_outcome(&outcome), refusal, "{outcome:?}");
        }
        assert_eq!(refusal_of(enroll::Refused::SameKey), wire::Refusal::SameKey);
        assert_eq!(
            refusal_of(enroll::Refused::Unissued),
            wire::Refusal::Unissued
        );
    }
}
