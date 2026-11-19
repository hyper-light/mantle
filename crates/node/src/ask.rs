//! The commands a node asks of the cell's ranges itself (docs/design/node.md §6.6): its enlistment
//! in the root range, and the gateway host's requests after it (§5.1). A range's commands are asked
//! within a session the node registers with the range at first use, as many entries out at once as
//! the range's log has frames in flight ([`ENTRIES_OUT`]), each asked on its own, the commands
//! waiting taken into the next entry in the order they were asked, each a serial of the session
//! (replica.md §1). An entry asked again after its exchange was lost carries the same serials,
//! which the range answers from what the session keeps without applying them twice.
//!
//! What is here decides what an entry carries and what its answers mean; the node asks it, of its
//! own member when that leads the range, or of a peer over the request class
//! (`crate::node::Node::command`). A read the node asks itself (`crate::node::Node::read`) needs no
//! session: it is asked of the range's leader as an entry is, confirmed by a quorum there
//! (`crate::read`), and asked again of the next leader when its asking ends unanswered, which a
//! read may be, since it changes nothing.

use std::collections::VecDeque;
use std::time::Duration;

use hyper_transport::{ExchangeId, PeerId};
use mantle_meta::wire::{Answer, Command, ENTRY_HEAD_BYTES, Entry, MAX_COMMANDS, Sessioned};

use crate::command;
use crate::read::{Query, Read};

/// The number a command this node asked is answered under (`crate::node::Node::take_command`).
pub type CommandAsk = u64;

/// What became of a command this node asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome {
    /// The range applied it, and answered.
    Answered(Answer),
    /// Its deadline passed before its answer came: it may still apply, once.
    Deadline,
    /// The range refused its entry and none of it applied: malformed, larger than the range takes,
    /// or asked of a range of another layer (`command::Status`); or it applied and its answers are
    /// larger than a reply carries (`TooLarge`).
    Refused(command::Status),
}

/// What a read or a command this node asks of a range waits for (node.md §5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    /// A request waits on it: past its range's budget without a leader that can serve it, it ends
    /// as one past its deadline does.
    Request,
    /// The node's own statement to the range, its enlistment or its history: asked until the
    /// range answers, the range's laws, not a budget of the node's, deciding when.
    Answered,
}

/// The number a read this node asked is answered under (`crate::node::Node::take_read`).
pub type ReadAsk = u64;

/// What became of a read this node asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
    /// The range's leader read it, confirmed by a quorum: it sees every write committed before it
    /// was asked.
    Read(Box<Read>),
    /// Its deadline passed before its answer came.
    Deadline,
    /// The range refused it for what it is: malformed, of another layer's rows (`WrongLayer`), its
    /// answer past what a reply carries (`TooLarge`), or its engine failed it (`Refused`).
    Refused(command::Status),
}

/// A read this node asked, and how it is being asked: as an entry is, with the leader the nodes
/// last named and the backoff between rounds that found none.
#[derive(Debug)]
pub(crate) struct ReadOut {
    pub ask: ReadAsk,
    pub group: u128,
    pub query: Query,
    pub deadline_ns: u64,
    pub wait: Wait,
    pub via: Via,
    pub leader: Option<PeerId>,
    pub tried: usize,
    pub backoff: Duration,
    pub pauses: u32,
    /// Its clock without a leader that can serve it (`Node::unled`): where this node's member
    /// judges, running while that member and the members its detectors trust are no quorum;
    /// elsewhere from the first round of asking that found no node to answer, or this node's
    /// member refusing or taking it. Past its range's budget, judged while it is not out on an
    /// exchange, it ends (node.md §5.4).
    pub leaderless_since: Option<u64>,
    /// The patience the members it asked last stated in a redirect, 0 for none.
    pub stated_patience_ns: u64,
}

/// A command waiting for its range's next entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Wanted {
    pub ask: CommandAsk,
    pub command: Command,
    pub deadline_ns: u64,
    pub wait: Wait,
    /// The serial the session gave it already: refused for room (`Answer::AnswersFull`), it is
    /// asked again under the same one, since a copy of its entry still in flight may apply it
    /// first, and under a new serial it would apply twice.
    pub serial: Option<u64>,
}

/// What an entry out carries: the registration of the node's session with the range, under a
/// nonce the node draws, or commands, each its ask, its serial and its deadline, in the entry's
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Carried {
    Registration { nonce: u64 },
    Commands(Vec<(CommandAsk, u64, u64)>),
}

/// How an entry out is being asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Via {
    /// Not asked of anyone yet, or asked again after its last asking ended.
    Unasked,
    /// Proposed to this node's own member, which led the range.
    Local,
    /// On an exchange with a peer.
    Exchange { exchange: ExchangeId, peer: PeerId },
    /// Asked of no one until `at_ns`: no node named a leader (`leaderless`), which ends too as soon
    /// as this node's own member of the range names one, or the leader refused it for room, or this
    /// node's member took it not, which only the pause ends.
    Paused { at_ns: u64, leaderless: bool },
}

/// The entries a node keeps out to one range at once: as many as the range's log has frames in
/// flight, the one on the device, flushing, and the one queued behind it, which group commit fills
/// while the first flushes, so a command asked while an entry is out goes into the next frame
/// rather than waiting out the first's flush, quorum and answer (research/71 step 9; research/87
/// §1, LogCabin's double buffering). A third would wait for the second's frame as well.
pub(crate) const ENTRIES_OUT: usize = 2;

/// An entry out: its place among the range's entries, what it carries, the entry, how it is being
/// asked, since when it waits without a leader that can serve it, as a read does
/// ([`ReadOut::leaderless_since`]), and what its commands wait for, every one the same: a
/// registration's, the first command's that waited for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Out {
    /// Its place in the order the range's entries went out, which tells the entries out apart
    /// and an answer the one it is for: distinct among the at most [`ENTRIES_OUT`] out, which a
    /// count wrapping past `u64::MAX` keeps.
    pub id: u64,
    pub carried: Carried,
    pub entry: Entry,
    pub via: Via,
    pub leaderless_since: Option<u64>,
    pub wait: Wait,
}

/// One range's commands: the node's session with it, the commands waiting, the entries out, and
/// the leader the nodes last named, with the backoff between rounds that found none.
#[derive(Debug)]
pub(crate) struct Asking {
    pub group: u128,
    /// The session the range registered for this node; 0 until it has.
    pub session: u64,
    pub next_serial: u64,
    pub waiting: VecDeque<Wanted>,
    /// The entries out, oldest first: at most [`ENTRIES_OUT`], each asked on its own.
    pub outs: VecDeque<Out>,
    /// The place the next entry out takes ([`Out::id`]).
    next_out: u64,
    pub leader: Option<PeerId>,
    /// Nodes asked since the last pause, and the pause's backoff and draws.
    pub tried: usize,
    pub backoff: Duration,
    pub pauses: u32,
    /// The patience the members it asked last stated in a redirect, 0 for none.
    pub stated_patience_ns: u64,
    /// The commands the range answered corrupt once (`Answer::Corrupt`) and that wait or are out
    /// again: one answered so a second time is answered to its caller as such, so a command is
    /// sent at most twice for its checksum, as a failed check is redone once and fails if the
    /// second fails too (node.md §5.6; research/31 §5.7). At most the commands the range holds.
    corrupted: Vec<CommandAsk>,
    /// The commands of this node's the range answered corrupt since the node opened: each a
    /// mismatch between a command's bytes and the CRC-32C this node computed as it built them,
    /// attributed to this node, their gateway (replica.md §2; node.md §5.6).
    pub corrupt: u64,
}

impl Asking {
    pub fn new(group: u128, backoff: Duration) -> Self {
        Self {
            group,
            session: 0,
            next_serial: 1,
            waiting: VecDeque::new(),
            outs: VecDeque::with_capacity(ENTRIES_OUT),
            next_out: 0,
            leader: None,
            tried: 0,
            backoff,
            pauses: 0,
            stated_patience_ns: 0,
            corrupted: Vec::new(),
            corrupt: 0,
        }
    }

    /// Whether the command asked as `ask` waits or is in an entry out.
    fn holds(&self, ask: CommandAsk) -> bool {
        let out = self.outs.iter().any(|o| match &o.carried {
            Carried::Commands(commands) => commands.iter().any(|(a, _, _)| *a == ask),
            Carried::Registration { .. } => false,
        });
        out || self.waiting.iter().any(|w| w.ask == ask)
    }

    /// The commands this range holds for the node: waiting and out.
    pub fn held(&self) -> usize {
        self.outs
            .iter()
            .map(|o| match &o.carried {
                Carried::Commands(commands) => commands.len(),
                Carried::Registration { .. } => 0,
            })
            .fold(self.waiting.len(), usize::saturating_add)
    }

    /// Whether a request waits on what the range holds: an entry out's commands, or one waiting.
    pub fn holds_request(&self) -> bool {
        let out = self
            .outs
            .iter()
            .any(|o| matches!(o.carried, Carried::Commands(_)) && o.wait == Wait::Request);
        out || self.waiting.iter().any(|w| w.wait == Wait::Request)
    }

    /// What the waiting commands the next entry may carry wait for: the leading run's, so that an
    /// entry's commands wait for one thing (`Out::wait`).
    fn run(&self) -> Wait {
        self.waiting.front().map_or(Wait::Request, |w| w.wait)
    }

    /// The lowest serial of the session whose answer the node has not received: a command's
    /// waiting to be asked again under its serial, one's in an entry out under the session, or
    /// the next. The range forgets only the answers below the `unanswered` an entry names
    /// (`mantle_meta::session::Touched::acknowledge`), so an entry that applies before one out
    /// ahead of it acknowledges none of that one's answers, which the range keeps for its copies.
    fn unanswered(&self) -> u64 {
        let out = self
            .outs
            .iter()
            .flat_map(|o| &o.entry.commands)
            .filter(|c| c.session == self.session)
            .map(|c| c.serial);
        self.waiting
            .iter()
            .filter_map(|w| w.serial)
            .chain(out)
            .fold(self.next_serial, u64::min)
    }

    /// Whether another entry goes out now ([`Self::another_entry`]): fewer than [`ENTRIES_OUT`]
    /// are, a command waits, and no registration is out while the range holds no session for the
    /// node, since the session its answer opens is every entry's after: one registration out at a
    /// time.
    pub fn room(&self) -> bool {
        self.outs.len() < ENTRIES_OUT
            && !self.waiting.is_empty()
            && (self.session != 0
                || !self
                    .outs
                    .iter()
                    .any(|o| matches!(o.carried, Carried::Registration { .. })))
    }

    /// The next entry, beside those out, while there is room for it ([`Self::room`]): the
    /// session's registration first, as `owner` (the node, as it
    /// authenticates) in its `run`, placed at `nth` among the run's registrations, under `nonce`;
    /// then the leading run of waiting commands that wait for the same, as many as fit
    /// `max_entry_bytes` once encoded, each under the serial it was given or the next, no count
    /// besides: the batch is what waited (research/87 §1, LogCabin's double buffering). Each
    /// names the session's lowest serial unanswered ([`Self::unanswered`]). A waiting command that
    /// fits no entry alone is answered refused, `Malformed`, in `refused`.
    pub fn another_entry(
        &mut self,
        max_entry_bytes: usize,
        (owner, run, nth): (u64, u64, u64),
        nonce: u64,
        refused: &mut impl Extend<(CommandAsk, CommandOutcome)>,
    ) {
        if !self.room() {
            return;
        }
        if self.session == 0 {
            let entry = Entry {
                at_ns: 0,
                commands: vec![Sessioned {
                    session: 0,
                    serial: nonce,
                    unanswered: 0,
                    command: Command::Register { owner, run, nth },
                }],
            };
            let wait = self.run();
            self.put_out(Carried::Registration { nonce }, entry, wait);
            return;
        }
        let unanswered = self.unanswered();
        loop {
            let wait = self.run();
            let mut commands = Vec::new();
            let mut bytes = ENTRY_HEAD_BYTES;
            let mut next = self.next_serial;
            for wanted in self
                .waiting
                .iter()
                .take_while(|w| w.wait == wait)
                .take(MAX_COMMANDS)
            {
                let serial = wanted.serial.unwrap_or(next);
                let sessioned = Sessioned {
                    session: self.session,
                    serial,
                    unanswered,
                    command: wanted.command.clone(),
                };
                let Some(total) = sessioned
                    .encoded_len()
                    .ok()
                    .and_then(|len| bytes.checked_add(len))
                    .filter(|&total| total <= max_entry_bytes)
                else {
                    break;
                };
                bytes = total;
                if wanted.serial.is_none() {
                    next = next.saturating_add(1);
                }
                commands.push(sessioned);
            }
            if commands.is_empty() {
                // No entry carries this command alone: it is answered and the next tried.
                if let Some(wanted) = self.waiting.pop_front() {
                    refused.extend([(
                        wanted.ask,
                        CommandOutcome::Refused(command::Status::Malformed),
                    )]);
                }
                if self.waiting.is_empty() {
                    return;
                }
                continue;
            }
            let carried = self
                .waiting
                .drain(..commands.len())
                .zip(&commands)
                .map(|(wanted, sessioned)| (wanted.ask, sessioned.serial, wanted.deadline_ns))
                .collect();
            self.next_serial = next;
            self.put_out(
                Carried::Commands(carried),
                Entry { at_ns: 0, commands },
                wait,
            );
            return;
        }
    }

    /// Puts an entry out after those out, in the next place.
    fn put_out(&mut self, carried: Carried, entry: Entry, wait: Wait) {
        let out = Out {
            id: self.next_out,
            carried,
            entry,
            via: Via::Unasked,
            leaderless_since: None,
            wait,
        };
        self.next_out = self.next_out.wrapping_add(1);
        self.outs.push_back(out);
    }

    /// Entry out `id`.
    pub fn out_mut(&mut self, id: u64) -> Option<&mut Out> {
        self.outs.iter_mut().find(|o| o.id == id)
    }

    /// Takes entry out `id`, those after it keeping their order.
    pub fn take_out(&mut self, id: u64) -> Option<Out> {
        let at = self.outs.iter().position(|o| o.id == id)?;
        self.outs.remove(at)
    }

    /// Entry out `id` was answered with `answers`, one for each of its commands, in its order:
    /// each command's outcome into `done`. A registration answered `Registered` opens the
    /// session; one answered `Superseded` says the owner registered in a later run, a process
    /// other than this one, and each waiting command is answered so; one answered otherwise is
    /// asked again. A command whose session the range no longer holds was not applied
    /// (`Answer::SessionExpired`): the session is registered anew and the command waits again for
    /// a serial of the new one. A command its session's row had no room for
    /// (`Answer::AnswersFull`) did not apply then: it waits again under its serial, and the next
    /// entry acknowledges the answers before it, which makes the room. It is never told refused,
    /// since a copy of its entry still in flight may yet apply it; past its deadline it is
    /// `Deadline`, which may still apply. A command of a session that has ended since its entry
    /// went out, which another entry's answer found expired, waits for a serial of the next: the
    /// range refuses every copy of its old one. Commands asked again wait before every command
    /// waiting, in their entry's order; entries out together keep no order between them, as the
    /// range may apply either first.
    pub fn answered_entry(
        &mut self,
        id: u64,
        answers: Vec<Answer>,
        done: &mut impl Extend<(CommandAsk, CommandOutcome)>,
    ) {
        let Some(out) = self.take_out(id) else {
            return;
        };
        self.leader_found();
        match out.carried {
            Carried::Registration { .. } => match answers.as_slice() {
                [Answer::Registered { session }] => {
                    self.session = *session;
                    self.next_serial = 1;
                }
                [Answer::Superseded] => {
                    for wanted in self.waiting.drain(..) {
                        done.extend([(wanted.ask, CommandOutcome::Answered(Answer::Superseded))]);
                    }
                }
                _ => {}
            },
            Carried::Commands(commands) => {
                if answers.len() != commands.len() || out.entry.commands.len() != commands.len() {
                    for (ask, _, _) in commands {
                        done.extend([(ask, CommandOutcome::Refused(command::Status::Malformed))]);
                    }
                    return;
                }
                let mut again = Vec::new();
                let mut expired = false;
                for (((ask, serial, deadline_ns), sessioned), answer) in
                    commands.into_iter().zip(out.entry.commands).zip(answers)
                {
                    let current = sessioned.session == self.session;
                    let serial = match answer {
                        Answer::SessionExpired => {
                            expired |= current;
                            None
                        }
                        Answer::AnswersFull if current => Some(serial),
                        Answer::AnswersFull => None,
                        // Its bytes did not match the CRC this node built them with, and nothing
                        // of it was written (replica.md §2): it is sent again from what the node
                        // holds, under its serial, since a copy of its entry still in flight may
                        // apply it first; a second mismatch is its caller's answer.
                        Answer::Corrupt => {
                            self.corrupt = self.corrupt.saturating_add(1);
                            if let Some(at) = self.corrupted.iter().position(|a| *a == ask) {
                                self.corrupted.swap_remove(at);
                                done.extend([(ask, CommandOutcome::Answered(Answer::Corrupt))]);
                                continue;
                            }
                            self.corrupted.push(ask);
                            if current { Some(serial) } else { None }
                        }
                        answer => {
                            done.extend([(ask, CommandOutcome::Answered(answer))]);
                            continue;
                        }
                    };
                    again.push(Wanted {
                        ask,
                        command: sessioned.command,
                        deadline_ns,
                        wait: out.wait,
                        serial,
                    });
                }
                for wanted in again.into_iter().rev() {
                    self.waiting.push_front(wanted);
                }
                // A command answered or ended since its mismatch is no longer sent again.
                let corrupted = std::mem::take(&mut self.corrupted);
                self.corrupted = corrupted.into_iter().filter(|a| self.holds(*a)).collect();
                // A session the range no longer holds ends every serial of it: the commands
                // wait for the new session's.
                if expired {
                    self.session = 0;
                    for wanted in &mut self.waiting {
                        wanted.serial = None;
                    }
                }
            }
        }
    }

    /// Entry out `id` was refused for what it is, and none of it applied: each of its commands
    /// answered so, a registration asked again.
    pub fn refused_entry(
        &mut self,
        id: u64,
        status: command::Status,
        done: &mut impl Extend<(CommandAsk, CommandOutcome)>,
    ) {
        if let Some(Out {
            carried: Carried::Commands(commands),
            ..
        }) = self.take_out(id)
        {
            for (ask, _, _) in commands {
                done.extend([(ask, CommandOutcome::Refused(status))]);
            }
        }
    }

    /// The commands past their deadline at `now_ns`: those waiting are answered so and leave;
    /// each entry out whose every command is past it is given up, since one that may still apply
    /// is told to its caller as such, and a registration nothing waits for any more. The entries
    /// given up, each its place and how it was being asked, whose asking then ends.
    pub fn expire(
        &mut self,
        now_ns: u64,
        done: &mut impl Extend<(CommandAsk, CommandOutcome)>,
    ) -> Vec<(u64, Via)> {
        let mut kept = VecDeque::with_capacity(self.waiting.len());
        for wanted in self.waiting.drain(..) {
            if wanted.deadline_ns <= now_ns {
                done.extend([(wanted.ask, CommandOutcome::Deadline)]);
            } else {
                kept.push_back(wanted);
            }
        }
        self.waiting = kept;
        let nothing_waits = self.waiting.is_empty();
        self.give_up(done, |out| match &out.carried {
            Carried::Commands(commands) => {
                commands.iter().all(|(_, _, deadline)| *deadline <= now_ns)
            }
            Carried::Registration { .. } => nothing_waits,
        })
    }

    /// Its range's budget without a leader that can serve it spent on the entries out `spent`
    /// names (node.md §5.4): each command a request waits on answered as past its deadline, which
    /// may still apply, once, and each of those entries given up that carries them, or is a
    /// registration no command waits for now. The entries given up, as [`Self::expire`]'s. One of
    /// them the node states of its own is asked on, its clock started again: the range's laws
    /// decide when it is answered. An entry out whose budget is not spent is judged on its own.
    pub fn unled(
        &mut self,
        spent: &[u64],
        done: &mut impl Extend<(CommandAsk, CommandOutcome)>,
    ) -> Vec<(u64, Via)> {
        let mut kept = VecDeque::with_capacity(self.waiting.len());
        for wanted in self.waiting.drain(..) {
            match wanted.wait {
                Wait::Request => done.extend([(wanted.ask, CommandOutcome::Deadline)]),
                Wait::Answered => kept.push_back(wanted),
            }
        }
        self.waiting = kept;
        let nothing_waits = self.waiting.is_empty();
        let given_up = self.give_up(done, |out| {
            spent.contains(&out.id)
                && match out.carried {
                    Carried::Registration { .. } => nothing_waits,
                    Carried::Commands(_) => out.wait == Wait::Request,
                }
        });
        for out in self.outs.iter_mut().filter(|o| spent.contains(&o.id)) {
            out.leaderless_since = None;
        }
        given_up
    }

    /// Gives up each entry out `past` names, oldest first, its commands answered as past their
    /// deadline, which may still apply, once; those kept keep their order. The entries given up,
    /// each its place and how it was being asked: at most [`ENTRIES_OUT`].
    fn give_up(
        &mut self,
        done: &mut impl Extend<(CommandAsk, CommandOutcome)>,
        past: impl Fn(&Out) -> bool,
    ) -> Vec<(u64, Via)> {
        let mut given_up = Vec::new();
        let mut at = 0;
        while let Some(out) = self.outs.get(at) {
            if !past(out) {
                at = at.saturating_add(1);
                continue;
            }
            if let Some(out) = self.outs.remove(at) {
                given_up.push((out.id, out.via));
                given_up_with(out, done);
            }
        }
        given_up
    }

    /// The earliest deadline of a command this range holds.
    pub fn next_deadline(&self) -> Option<u64> {
        let out = self
            .outs
            .iter()
            .flat_map(|o| match &o.carried {
                Carried::Commands(commands) => commands.as_slice(),
                Carried::Registration { .. } => &[],
            })
            .map(|(_, _, d)| *d);
        self.waiting.iter().map(|w| w.deadline_ns).chain(out).min()
    }

    /// A node answered for the range: the next round's backoff starts over.
    fn leader_found(&mut self) {
        self.tried = 0;
        self.pauses = 0;
    }
}

/// An entry out given up: each of its commands answered as past its deadline.
fn given_up_with(out: Out, done: &mut impl Extend<(CommandAsk, CommandOutcome)>) {
    if let Carried::Commands(commands) = out.carried {
        for (ask, _, _) in commands {
            done.extend([(ask, CommandOutcome::Deadline)]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wanted(ask: CommandAsk, deadline_ns: u64) -> Wanted {
        Wanted {
            ask,
            command: Command::Register {
                owner: 1,
                run: 1,
                nth: 1,
            },
            deadline_ns,
            wait: Wait::Request,
            serial: None,
        }
    }

    /// The bytes of an entry of `n` of [`wanted`]'s commands: a bound that takes `n` to an entry.
    fn holding(n: usize) -> usize {
        let one = Sessioned {
            session: 5,
            serial: 1,
            unanswered: 1,
            command: wanted(0, 0).command,
        };
        ENTRY_HEAD_BYTES + n * one.encoded_len().unwrap()
    }

    /// The next entry when none is out, as a node that keeps one out at a time makes it.
    fn next_entry(
        a: &mut Asking,
        max_entry_bytes: usize,
        owner: (u64, u64, u64),
        nonce: u64,
        refused: &mut Vec<(CommandAsk, CommandOutcome)>,
    ) {
        if a.outs.is_empty() {
            a.another_entry(max_entry_bytes, owner, nonce, refused);
        }
    }

    /// The oldest entry out answered.
    fn answered(
        a: &mut Asking,
        answers: Vec<Answer>,
        done: &mut impl Extend<(CommandAsk, CommandOutcome)>,
    ) {
        if let Some(id) = a.outs.front().map(|o| o.id) {
            a.answered_entry(id, answers, done);
        }
    }

    /// The range's budget spent on every entry out: whether one was given up.
    fn unled(a: &mut Asking, done: &mut Vec<(CommandAsk, CommandOutcome)>) -> bool {
        let spent: Vec<u64> = a.outs.iter().map(|o| o.id).collect();
        !a.unled(&spent, done).is_empty()
    }

    fn asking(waiting: &[Wanted]) -> Asking {
        let mut a = Asking::new(7, Duration::from_millis(1));
        a.waiting.extend(waiting.iter().cloned());
        a
    }

    #[test]
    fn a_session_is_registered_first_then_commands_go_in_order_as_serials() {
        let mut a = asking(&[wanted(1, 100), wanted(2, 100), wanted(3, 100)]);
        let mut done = Vec::new();
        next_entry(&mut a, 1 << 20, (1, 1, 42), 42, &mut done);
        let out = a.outs.front().cloned().unwrap();
        assert_eq!(out.carried, Carried::Registration { nonce: 42 });
        assert_eq!(out.entry.commands[0].serial, 42);
        answered(&mut a, vec![Answer::Registered { session: 9 }], &mut done);
        assert_eq!((a.session, a.next_serial), (9, 1));
        // Two to an entry's bytes: serials 1 and 2, the oldest unanswered the first.
        next_entry(&mut a, holding(2), (1, 1, 43), 43, &mut done);
        let out = a.outs.front().cloned().unwrap();
        assert_eq!(
            out.carried,
            Carried::Commands(vec![(1, 1, 100), (2, 2, 100)])
        );
        assert!(
            out.entry
                .commands
                .iter()
                .all(|c| c.session == 9 && c.unanswered == 1)
        );
        answered(
            &mut a,
            vec![Answer::Repeated, Answer::WrongLayer],
            &mut done,
        );
        next_entry(&mut a, holding(2), (1, 1, 44), 44, &mut done);
        assert_eq!(
            a.outs.front().unwrap().carried,
            Carried::Commands(vec![(3, 3, 100)])
        );
        assert_eq!(
            done,
            vec![
                (1, CommandOutcome::Answered(Answer::Repeated)),
                (2, CommandOutcome::Answered(Answer::WrongLayer)),
            ]
        );
    }

    fn answered_by(ask: CommandAsk) -> Wanted {
        Wanted {
            wait: Wait::Answered,
            ..wanted(ask, u64::MAX)
        }
    }

    #[test]
    fn an_entrys_commands_wait_for_one_thing() {
        let mut a = asking(&[
            wanted(1, 100),
            wanted(2, 100),
            answered_by(3),
            wanted(4, 100),
        ]);
        a.session = 5;
        let mut done = Vec::new();
        for (carried, wait) in [
            (vec![(1, 1, 100), (2, 2, 100)], Wait::Request),
            (vec![(3, 3, u64::MAX)], Wait::Answered),
            (vec![(4, 4, 100)], Wait::Request),
        ] {
            next_entry(&mut a, 1 << 20, (1, 1, 0), 0, &mut done);
            let out = a.outs.front().cloned().unwrap();
            assert_eq!((out.carried, out.wait), (Carried::Commands(carried), wait));
            let answers = vec![Answer::Repeated; out.entry.commands.len()];
            answered(&mut a, answers, &mut done);
        }
        assert!(a.waiting.is_empty() && a.outs.is_empty());
    }

    #[test]
    fn a_spent_budget_ends_what_requests_wait_on_and_the_nodes_own_statements_are_asked_on() {
        let mut done = Vec::new();
        // An entry requests wait on: given up, and the request waiting behind it ended.
        let mut a = asking(&[wanted(1, 100), answered_by(2), wanted(3, 100)]);
        a.session = 5;
        next_entry(&mut a, 1 << 20, (1, 1, 0), 0, &mut done);
        assert!(unled(&mut a, &mut done));
        assert_eq!(
            done,
            vec![(3, CommandOutcome::Deadline), (1, CommandOutcome::Deadline)]
        );
        assert_eq!(a.waiting, VecDeque::from([answered_by(2)]));
        // The node's own statement: asked on, its clock started again, the request behind it ended.
        done.clear();
        a.waiting.push_back(wanted(4, 100));
        next_entry(&mut a, 1 << 20, (1, 1, 0), 0, &mut done);
        if let Some(out) = a.outs.front_mut() {
            out.leaderless_since = Some(7);
        }
        assert!(!unled(&mut a, &mut done));
        assert_eq!(done, vec![(4, CommandOutcome::Deadline)]);
        let out = a.outs.front().cloned().unwrap();
        assert_eq!(
            (out.carried, out.leaderless_since),
            (Carried::Commands(vec![(2, 2, u64::MAX)]), None)
        );
        // A registration: given up once no command waits for it.
        done.clear();
        let mut b = asking(&[answered_by(5)]);
        next_entry(&mut b, 1 << 20, (1, 1, 0), 0, &mut done);
        assert!(!unled(&mut b, &mut done) && !b.outs.is_empty());
        let mut c = asking(&[wanted(6, 100)]);
        next_entry(&mut c, 1 << 20, (1, 1, 0), 0, &mut done);
        assert!(unled(&mut c, &mut done) && c.outs.is_empty());
        assert_eq!(done, vec![(6, CommandOutcome::Deadline)]);
    }

    #[test]
    fn a_superseded_registration_answers_each_waiting_command_so() {
        // The owner registered in a later run: this process's commands go to no session.
        let mut a = asking(&[wanted(1, 100), wanted(2, 100)]);
        let mut done = Vec::new();
        next_entry(&mut a, 1 << 20, (1, 1, 5), 5, &mut done);
        answered(&mut a, vec![Answer::Superseded], &mut done);
        assert_eq!(a.session, 0);
        assert!(a.waiting.is_empty() && a.outs.is_empty());
        assert_eq!(
            done,
            vec![
                (1, CommandOutcome::Answered(Answer::Superseded)),
                (2, CommandOutcome::Answered(Answer::Superseded)),
            ]
        );
    }

    #[test]
    fn an_entry_takes_what_fits_its_bytes_and_one_that_fits_none_is_refused() {
        let mut a = asking(&[wanted(1, 100), wanted(2, 100)]);
        a.session = 5;
        let one = Entry {
            at_ns: 0,
            commands: vec![Sessioned {
                session: 5,
                serial: 1,
                unanswered: 1,
                command: Command::Register {
                    owner: 1,
                    run: 1,
                    nth: 1,
                },
            }],
        }
        .encode()
        .unwrap()
        .len();
        let mut done = Vec::new();
        // Room for one command an entry: the first goes alone.
        next_entry(&mut a, one, (1, 1, 0), 0, &mut done);
        assert_eq!(
            a.outs.front().unwrap().carried,
            Carried::Commands(vec![(1, 1, 100)])
        );
        // Room for none: each is refused, nothing goes.
        let mut b = asking(&[wanted(1, 100), wanted(2, 100)]);
        b.session = 5;
        next_entry(&mut b, one - 1, (1, 1, 0), 0, &mut done);
        assert!(b.outs.is_empty() && b.waiting.is_empty());
        assert_eq!(
            done,
            vec![
                (1, CommandOutcome::Refused(command::Status::Malformed)),
                (2, CommandOutcome::Refused(command::Status::Malformed)),
            ]
        );
    }

    #[test]
    fn a_command_whose_session_expired_waits_again_in_its_place_under_a_new_session() {
        let mut a = asking(&[wanted(1, 100), wanted(2, 100), wanted(3, 100)]);
        a.session = 5;
        let mut done = Vec::new();
        next_entry(&mut a, holding(2), (1, 1, 0), 0, &mut done);
        answered(
            &mut a,
            vec![Answer::SessionExpired, Answer::SessionExpired],
            &mut done,
        );
        assert!(done.is_empty());
        assert_eq!(a.session, 0);
        let order: Vec<_> = a.waiting.iter().map(|w| w.ask).collect();
        assert_eq!(order, vec![1, 2, 3]);
        // The new session first.
        next_entry(&mut a, holding(2), (1, 1, 8), 8, &mut done);
        assert_eq!(
            a.outs.front().unwrap().carried,
            Carried::Registration { nonce: 8 }
        );
    }

    /// Commands the session's row had no room for wait again in their place under their serials,
    /// the next entry naming the lowest as unanswered, so a copy still in flight that applies one
    /// meets the same serial asked again, answered from what was kept; a command after them takes
    /// the next serial, and none is told refused. Once the session expires, they wait for the new
    /// session's serials.
    #[test]
    fn a_command_refused_for_room_is_asked_again_under_its_serial() {
        let mut a = asking(&[wanted(1, 100), wanted(2, 100), wanted(3, 100)]);
        a.session = 5;
        let mut done = Vec::new();
        next_entry(&mut a, holding(3), (1, 1, 0), 0, &mut done);
        let applied = Answer::Name(mantle_meta::name::Outcome::Put {
            version: "v".into(),
        });
        answered(
            &mut a,
            vec![applied.clone(), Answer::AnswersFull, Answer::AnswersFull],
            &mut done,
        );
        assert_eq!(done, vec![(1, CommandOutcome::Answered(applied.clone()))]);
        let serials: Vec<_> = a.waiting.iter().map(|w| (w.ask, w.serial)).collect();
        assert_eq!(serials, vec![(2, Some(2)), (3, Some(3))]);
        a.waiting.push_back(wanted(4, 100));
        next_entry(&mut a, 1 << 20, (1, 1, 0), 0, &mut done);
        let out = a.outs.front().cloned().unwrap();
        assert_eq!(
            out.carried,
            Carried::Commands(vec![(2, 2, 100), (3, 3, 100), (4, 4, 100)])
        );
        assert!(out.entry.commands.iter().all(|c| c.unanswered == 2));
        assert_eq!(a.next_serial, 5);
        // The range no longer holds the session: every command waits for the new one's serials.
        answered(
            &mut a,
            vec![
                Answer::SessionExpired,
                Answer::AnswersFull,
                Answer::SessionExpired,
            ],
            &mut done,
        );
        assert_eq!(a.session, 0);
        assert!(a.waiting.iter().all(|w| w.serial.is_none()));
        assert_eq!(done.len(), 1);
    }

    /// A command a range answered corrupt (its bytes did not match the CRC this node built them
    /// with, and nothing of it was written) is sent again from what the node holds, under its
    /// serial; answered corrupt a second time, its caller is answered so: at most two sendings for
    /// its checksum, each mismatch counted against this node (replica.md §2; node.md §5.6).
    #[test]
    fn a_command_answered_corrupt_is_sent_again_once_under_its_serial() {
        let mut a = asking(&[wanted(1, 100), wanted(2, 100)]);
        a.session = 5;
        let mut done = Vec::new();
        next_entry(&mut a, holding(2), (1, 1, 0), 0, &mut done);
        let applied = Answer::Name(mantle_meta::name::Outcome::Put {
            version: "v".into(),
        });
        answered(&mut a, vec![Answer::Corrupt, applied.clone()], &mut done);
        assert_eq!(done, vec![(2, CommandOutcome::Answered(applied.clone()))]);
        let serials: Vec<_> = a.waiting.iter().map(|w| (w.ask, w.serial)).collect();
        assert_eq!(serials, vec![(1, Some(1))]);
        assert_eq!(a.corrupt, 1);
        // Sent again, the same command under the same serial, and corrupt again: answered so.
        next_entry(&mut a, 1 << 20, (1, 1, 0), 0, &mut done);
        let out = a.outs.front().cloned().unwrap();
        assert_eq!(out.carried, Carried::Commands(vec![(1, 1, 100)]));
        answered(&mut a, vec![Answer::Corrupt], &mut done);
        assert_eq!(done[1], (1, CommandOutcome::Answered(Answer::Corrupt)));
        assert!(a.waiting.is_empty() && a.outs.is_empty());
        assert_eq!(a.corrupt, 2);
        assert!(a.corrupted.is_empty());
        // One answered corrupt once and applied when sent again leaves nothing behind.
        a.waiting.push_back(wanted(3, 100));
        next_entry(&mut a, 1 << 20, (1, 1, 0), 0, &mut done);
        answered(&mut a, vec![Answer::Corrupt], &mut done);
        next_entry(&mut a, 1 << 20, (1, 1, 0), 0, &mut done);
        answered(&mut a, vec![applied.clone()], &mut done);
        assert_eq!(done[2], (3, CommandOutcome::Answered(applied)));
        assert!(a.corrupted.is_empty());
    }

    #[test]
    fn deadlines_answer_waiting_commands_and_give_up_an_entry_whose_every_command_passed() {
        let mut a = asking(&[wanted(1, 50), wanted(2, 200), wanted(3, 60)]);
        a.session = 5;
        let mut done = Vec::new();
        next_entry(&mut a, holding(2), (1, 1, 0), 0, &mut done);
        assert_eq!(a.next_deadline(), Some(50));
        // Command 3, waiting, passes; the entry out holds 2, not yet past.
        assert!(a.expire(100, &mut done).is_empty());
        assert_eq!(done, vec![(3, CommandOutcome::Deadline)]);
        // Both of the entry's past: it is given up.
        assert!(!a.expire(200, &mut done).is_empty());
        assert!(a.outs.is_empty());
        assert_eq!(
            done,
            vec![
                (3, CommandOutcome::Deadline),
                (1, CommandOutcome::Deadline),
                (2, CommandOutcome::Deadline),
            ]
        );
        assert_eq!(a.held(), 0);
    }

    /// Two entries out from `asks`, two commands each, under session 5.
    fn two_out(asks: &[Wanted]) -> (Asking, u64, u64) {
        let mut a = asking(asks);
        a.session = 5;
        let mut done = Vec::new();
        a.another_entry(holding(2), (1, 1, 0), 0, &mut done);
        a.another_entry(holding(2), (1, 1, 0), 0, &mut done);
        assert!(done.is_empty());
        let first = a.outs.front().unwrap().id;
        let second = a.outs[1].id;
        (a, first, second)
    }

    fn unanswered(out: &Out) -> Vec<u64> {
        out.entry.commands.iter().map(|c| c.unanswered).collect()
    }

    /// An entry that goes out beside another names the other's serials unanswered, so the range,
    /// should the later apply first, keeps the earlier's answers for its copies; no third goes
    /// out past [`ENTRIES_OUT`].
    #[test]
    fn two_entries_go_out_each_naming_the_lowest_serial_unanswered_of_both() {
        let five: Vec<_> = (1..=5).map(|ask| wanted(ask, 100)).collect();
        let (mut a, first, second) = two_out(&five);
        let mut done = Vec::new();
        a.another_entry(holding(2), (1, 1, 0), 0, &mut done);
        let outs: Vec<_> = a
            .outs
            .iter()
            .map(|o| (o.carried.clone(), unanswered(o)))
            .collect();
        assert_eq!(
            outs,
            vec![
                (
                    Carried::Commands(vec![(1, 1, 100), (2, 2, 100)]),
                    vec![1, 1]
                ),
                (
                    Carried::Commands(vec![(3, 3, 100), (4, 4, 100)]),
                    vec![1, 1]
                ),
            ]
        );
        assert_eq!((a.waiting.len(), a.held()), (1, 5));
        // The second answered first: the third still names the first's serials unanswered.
        a.answered_entry(second, vec![Answer::Repeated; 2], &mut done);
        a.another_entry(holding(2), (1, 1, 0), 0, &mut done);
        assert_eq!(a.outs[1].carried, Carried::Commands(vec![(5, 5, 100)]));
        assert_eq!(unanswered(&a.outs[1]), vec![1]);
        // The first answered, the third the oldest out: the next names the third's serial.
        a.answered_entry(first, vec![Answer::Repeated; 2], &mut done);
        a.waiting.push_back(wanted(6, 100));
        a.another_entry(holding(2), (1, 1, 0), 0, &mut done);
        assert_eq!(a.outs[1].carried, Carried::Commands(vec![(6, 6, 100)]));
        assert_eq!(unanswered(&a.outs[1]), vec![5]);
        assert_eq!(done.len(), 4);
    }

    /// Each answer goes to the commands of the entry it is for: two entries of two commands
    /// each, the second answered first, an answer given the other's commands would show.
    #[test]
    fn an_answer_is_matched_to_its_own_entry_whichever_is_answered_first() {
        let four: Vec<_> = (1..=4).map(|ask| wanted(ask, 100)).collect();
        let (mut a, first, second) = two_out(&four);
        assert_ne!(first, second);
        let put = |v: &str| Answer::Name(mantle_meta::name::Outcome::Put { version: v.into() });
        let mut done = Vec::new();
        a.answered_entry(second, vec![put("v3"), put("v4")], &mut done);
        // The first is still out, the oldest.
        assert_eq!(a.outs.front().map(|o| o.id), Some(first));
        assert_eq!(a.outs.len(), 1);
        a.answered_entry(first, vec![put("v1"), put("v2")], &mut done);
        // An answer for an entry no longer out is no command's.
        a.answered_entry(second, vec![put("x"), put("y")], &mut done);
        assert_eq!(
            done,
            vec![
                (3, CommandOutcome::Answered(put("v3"))),
                (4, CommandOutcome::Answered(put("v4"))),
                (1, CommandOutcome::Answered(put("v1"))),
                (2, CommandOutcome::Answered(put("v2"))),
            ]
        );
        assert!(a.outs.is_empty() && a.held() == 0);
    }

    /// A command refused for room while another entry is out waits again under its serial,
    /// before the commands asked after it, and the entry that asks it again names it unanswered;
    /// the other entry's refused commands wait under theirs, in their entry's order.
    #[test]
    fn a_command_refused_for_room_beside_another_entry_is_asked_again_under_its_serial() {
        let four: Vec<_> = (1..=4).map(|ask| wanted(ask, 100)).collect();
        let (mut a, first, second) = two_out(&four);
        let applied = Answer::Name(mantle_meta::name::Outcome::Put {
            version: "v".into(),
        });
        let mut done = Vec::new();
        a.waiting.push_back(wanted(5, 100));
        a.answered_entry(first, vec![applied.clone(), Answer::AnswersFull], &mut done);
        let serials: Vec<_> = a.waiting.iter().map(|w| (w.ask, w.serial)).collect();
        assert_eq!(serials, vec![(2, Some(2)), (5, None)]);
        a.another_entry(1 << 20, (1, 1, 0), 0, &mut done);
        assert_eq!(
            a.outs[1].carried,
            Carried::Commands(vec![(2, 2, 100), (5, 5, 100)])
        );
        assert_eq!(unanswered(&a.outs[1]), vec![2, 2]);
        a.answered_entry(second, vec![Answer::AnswersFull; 2], &mut done);
        let serials: Vec<_> = a.waiting.iter().map(|w| (w.ask, w.serial)).collect();
        assert_eq!(serials, vec![(3, Some(3)), (4, Some(4))]);
        a.another_entry(1 << 20, (1, 1, 0), 0, &mut done);
        assert_eq!(
            a.outs[1].carried,
            Carried::Commands(vec![(3, 3, 100), (4, 4, 100)])
        );
        assert_eq!(unanswered(&a.outs[1]), vec![2, 2]);
        assert_eq!(a.next_serial, 6);
        assert_eq!(done, vec![(1, CommandOutcome::Answered(applied))]);
    }

    /// One entry's answer finding the session expired ends its serials in every entry: the new
    /// session's registration goes out beside the other entry, the new session's entries name
    /// none of the old one's serials unanswered, and the other entry's answers, of the old
    /// session, neither end the new one nor keep an old serial.
    #[test]
    fn a_session_found_expired_ends_its_serials_in_every_entry_and_spares_the_next_session() {
        let four: Vec<_> = (1..=4).map(|ask| wanted(ask, 100)).collect();
        let (mut a, first, second) = two_out(&four);
        let mut done = Vec::new();
        a.answered_entry(first, vec![Answer::SessionExpired; 2], &mut done);
        assert_eq!(a.session, 0);
        a.another_entry(holding(2), (1, 1, 8), 8, &mut done);
        a.another_entry(holding(2), (1, 1, 9), 9, &mut done);
        assert_eq!(a.outs.len(), 2);
        assert_eq!(a.outs[1].carried, Carried::Registration { nonce: 8 });
        let registration = a.outs[1].id;
        a.answered_entry(
            registration,
            vec![Answer::Registered { session: 9 }],
            &mut done,
        );
        assert_eq!((a.session, a.next_serial), (9, 1));
        a.another_entry(holding(2), (1, 1, 0), 0, &mut done);
        assert_eq!(
            a.outs[1].carried,
            Carried::Commands(vec![(1, 1, 100), (2, 2, 100)])
        );
        assert!(
            a.outs[1]
                .entry
                .commands
                .iter()
                .all(|c| c.session == 9 && c.unanswered == 1)
        );
        a.answered_entry(
            second,
            vec![Answer::AnswersFull, Answer::SessionExpired],
            &mut done,
        );
        assert_eq!(a.session, 9);
        let serials: Vec<_> = a.waiting.iter().map(|w| (w.ask, w.serial)).collect();
        assert_eq!(serials, vec![(3, None), (4, None)]);
        a.another_entry(holding(2), (1, 1, 0), 0, &mut done);
        assert_eq!(
            a.outs[1].carried,
            Carried::Commands(vec![(3, 3, 100), (4, 4, 100)])
        );
        assert!(done.is_empty());
    }

    /// Deadlines, refusals and a spent budget each end only the entry they are for, the oldest
    /// or the later, the entries kept in their order.
    #[test]
    fn deadlines_refusals_and_a_spent_budget_end_only_their_own_entry() {
        let mut done = Vec::new();
        // The oldest past: given up, the later the oldest.
        let early = [wanted(1, 50), wanted(2, 60), wanted(3, 200), wanted(4, 300)];
        let (mut a, _, second) = two_out(&early);
        assert_eq!(a.next_deadline(), Some(50));
        assert!(!a.expire(100, &mut done).is_empty());
        assert_eq!(a.outs.front().map(|o| o.id), Some(second));
        assert_eq!(a.outs.len(), 1);
        assert_eq!(a.next_deadline(), Some(200));
        assert!(a.expire(150, &mut done).is_empty());
        // The later past: given up, the oldest kept.
        let late = [wanted(5, 300), wanted(6, 300), wanted(7, 50), wanted(8, 60)];
        let (mut b, first, _) = two_out(&late);
        assert!(!b.expire(100, &mut done).is_empty());
        assert_eq!(b.outs.front().map(|o| o.id), Some(first));
        assert_eq!(b.outs.len(), 1);
        // A refusal: the oldest refused, the later kept.
        let (mut c, first, second) = two_out(&late);
        c.refused_entry(first, command::Status::Forbidden, &mut done);
        assert_eq!(c.outs.front().map(|o| o.id), Some(second));
        assert_eq!(
            done,
            vec![
                (1, CommandOutcome::Deadline),
                (2, CommandOutcome::Deadline),
                (7, CommandOutcome::Deadline),
                (8, CommandOutcome::Deadline),
                (5, CommandOutcome::Refused(command::Status::Forbidden)),
                (6, CommandOutcome::Refused(command::Status::Forbidden)),
            ]
        );
        // A spent budget: the entry a request waits on given up, the node's own statement asked on.
        done.clear();
        let mut d = asking(&[answered_by(9), wanted(10, 100)]);
        d.session = 5;
        d.another_entry(1 << 20, (1, 1, 0), 0, &mut done);
        d.another_entry(1 << 20, (1, 1, 0), 0, &mut done);
        for out in &mut d.outs {
            out.leaderless_since = Some(7);
        }
        let spent: Vec<u64> = d.outs.iter().map(|o| o.id).collect();
        assert_eq!(d.unled(&spent, &mut done).len(), 1);
        assert_eq!(done, vec![(10, CommandOutcome::Deadline)]);
        let out = d.outs.front().cloned().unwrap();
        assert_eq!(
            (out.carried, out.leaderless_since),
            (Carried::Commands(vec![(9, 1, u64::MAX)]), None)
        );
        assert_eq!(d.outs.len(), 1);
        // A budget spent on one entry alone: that entry given up, the other, judged on its own
        // clock, kept with it.
        done.clear();
        let (mut e, first, second) = two_out(&early);
        for out in &mut e.outs {
            out.leaderless_since = Some(7);
        }
        assert_eq!(e.unled(&[second], &mut done), vec![(second, Via::Unasked)]);
        assert_eq!(
            done,
            vec![(3, CommandOutcome::Deadline), (4, CommandOutcome::Deadline)]
        );
        let kept = e.outs.front().unwrap();
        assert_eq!((kept.id, kept.leaderless_since), (first, Some(7)));
        assert_eq!(e.outs.len(), 1);
    }

    /// A session's commands take effect once, under deterministic simulation with injected faults
    /// (CLAUDE.md §8), with one entry out and with [`ENTRIES_OUT`], each answer given to the
    /// entry it is for: the node's own asking drives a Name range's entries through a log that
    /// loses an entry before it is appended, loses a reply after it applied, and appends copies
    /// of an entry asked again, each late by a delay the seed draws, past entries after it; the
    /// commands' deadlines give entries up; and the session's row holds few answers, so commands
    /// are refused for room and asked again. Each command is a versioned delete, which makes a
    /// marker each time it applies, so a second application would show. Every command answered
    /// took effect exactly once, its answer naming the marker it made; none took effect twice;
    /// and every session row stayed within its bound, as the range refuses a row past it.
    #[test]
    fn a_sessions_commands_take_effect_once_through_lost_replies_late_copies_and_refusals() {
        use mantle_meta::apply::{Layer, apply_entry};
        use mantle_meta::engine::{Engine, Model, Rows};
        use mantle_meta::name::{self, Scan, VersionFrom};
        use mantle_meta::record::{Session, Versioning};
        use mantle_meta::session::Rules;
        use std::collections::BTreeMap;

        let delete = |ask: CommandAsk| {
            Command::Name(Box::new(name::Command::Delete(name::Delete {
                bucket: "b".into(),
                incarnation: 1,
                key: format!("k{ask}"),
                versioning: Versioning::Enabled,
                named: None,
                if_match: None,
                at_ns: 0,
                bypass: false,
                owner: "o".into(),
            })))
        };
        let one = Answer::Name(name::Outcome::Deleted {
            marker: true,
            version: Some(mantle_meta::key::version_id(u64::MAX)),
        })
        .encode()
        .unwrap()
        .len();
        let rules = Rules {
            lifetime_ns: u64::MAX,
            max_sessions: 4,
            expiries_per_entry: 1,
            max_key_bytes: usize::MAX,
            // Four answers at a time: an entry of more is refused past them.
            max_value_bytes: Session::row_bytes(4, 4 * one).unwrap(),
        };
        for entries_out in [1, ENTRIES_OUT] {
            let (mut refusals, mut doubles, mut lost) = (0u64, 0u64, 0u64);
            for seed in 1..=48u64 {
                let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let mut next = move |n: u64| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng % n
                };
                let mut m = Model::default();
                m.install(0, name::first(1).unwrap()).unwrap();
                let mut index = 0u64;
                let mut now = 0u64;
                let gate = Entry {
                    at_ns: 0,
                    commands: vec![Sessioned {
                        session: 0,
                        serial: 0,
                        unanswered: 0,
                        command: Command::Register {
                            owner: 9,
                            run: 1,
                            nth: 1,
                        },
                    }],
                };
                // The bucket's gate, opened by a session of another owner.
                index += 1;
                let [Answer::Registered { session }] =
                    apply_entry(&mut m, index, &gate, Layer::Name, &rules).unwrap()[..]
                else {
                    panic!()
                };
                let open = Entry {
                    at_ns: 0,
                    commands: vec![Sessioned {
                        session,
                        serial: 1,
                        unanswered: 1,
                        command: Command::Name(Box::new(name::Command::Gate(name::GateChange {
                            bucket: "b".into(),
                            incarnation: 1,
                            attempt: 1,
                            from: None,
                            to: Some(mantle_meta::record::GateState::Open),
                            generation: 1,
                        }))),
                    }],
                };
                index += 1;
                apply_entry(&mut m, index, &open, Layer::Name, &rules).unwrap();

                let mut a = Asking::new(7, Duration::from_millis(1));
                let mut outcomes: BTreeMap<CommandAsk, CommandOutcome> = BTreeMap::new();
                // Copies of entries on their way to the log: when each arrives, whether it is
                // appended, and whether its reply comes back.
                let mut flight: Vec<(u64, Entry, bool, bool)> = Vec::new();
                let mut applied: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
                let (mut asks, mut registrations) = (0u64, 0u64);
                for step in 0..400u64 {
                    now += 1 + next(4);
                    if step < 300 && next(3) == 0 {
                        asks += 1;
                        a.waiting.push_back(Wanted {
                            ask: asks,
                            command: delete(asks),
                            deadline_ns: now + 20 + next(200),
                            wait: Wait::Request,
                            serial: None,
                        });
                    }
                    let mut done = Vec::new();
                    a.expire(now, &mut done);
                    let before = a.outs.len();
                    if before < entries_out {
                        let registering = a
                            .outs
                            .iter()
                            .any(|o| matches!(o.carried, Carried::Registration { .. }));
                        if a.session == 0 && !registering {
                            registrations += 1;
                        }
                        a.another_entry(1 << 20, (1, 1, registrations), registrations, &mut done);
                    }
                    if a.outs.len() > before {
                        let out = a.outs.back().unwrap();
                        flight.push((
                            now + next(30),
                            out.entry.clone(),
                            next(8) != 0,
                            next(4) != 0,
                        ));
                    } else if next(6) == 0 && before > 0 {
                        // Asked again, as after a reply lost or a leader changed.
                        let at = usize::try_from(next(before as u64)).unwrap();
                        let out = a.outs.get(at).unwrap();
                        flight.push((
                            now + next(30),
                            out.entry.clone(),
                            next(8) != 0,
                            next(4) != 0,
                        ));
                    }
                    let drain = step >= 399;
                    flight.sort_by_key(|f| f.0);
                    while let Some(at) = flight.iter().position(|f| drain || f.0 <= now) {
                        let (_, mut entry, appended, replied) = flight.remove(at);
                        if !appended {
                            continue;
                        }
                        let key = entry.encode().unwrap();
                        let copies = applied.entry(key).or_insert(0);
                        *copies += 1;
                        doubles += u64::from(*copies > 1);
                        entry.at_ns = now;
                        index += 1;
                        let answers =
                            apply_entry(&mut m, index, &entry, Layer::Name, &rules).unwrap();
                        refusals += answers
                            .iter()
                            .filter(|x| **x == Answer::AnswersFull)
                            .count() as u64;
                        entry.at_ns = 0;
                        if !replied {
                            lost += 1;
                            continue;
                        }
                        let id = a.outs.iter().find(|o| o.entry == entry).map(|o| o.id);
                        if let Some(id) = id {
                            a.answered_entry(id, answers, &mut done);
                        }
                    }
                    for (ask, outcome) in done {
                        assert!(
                            outcomes.insert(ask, outcome).is_none(),
                            "seed {seed}: {ask} twice"
                        );
                    }
                }
                // What each command's key holds: its markers, each named as S3 shows it.
                let mut markers: BTreeMap<String, Vec<String>> = BTreeMap::new();
                let mut from = Vec::new();
                loop {
                    let mut budget = usize::MAX;
                    let found = name::next_version(
                        &mut m,
                        "b",
                        VersionFrom::Key(&from),
                        &[0xFF],
                        &mut budget,
                    )
                    .unwrap();
                    let Scan::Found(v) = found else { break };
                    assert!(v.version.marker);
                    from = format!("{}\0", v.key).into_bytes();
                    let mut all = vec![name::shown_id(v.version.null, v.order)];
                    let mut after = v;
                    while let Scan::Found(w) = name::next_version(
                        &mut m,
                        "b",
                        VersionFrom::After {
                            key: &after.key,
                            version_id: &name::shown_id(after.version.null, after.order),
                        },
                        &[0xFF],
                        &mut usize::MAX.clone(),
                    )
                    .unwrap()
                    {
                        if w.key != after.key {
                            break;
                        }
                        all.push(name::shown_id(w.version.null, w.order));
                        after = w;
                    }
                    markers.insert(after.key.clone(), all);
                }
                for (key, made) in &markers {
                    assert_eq!(
                        made.len(),
                        1,
                        "seed {seed}: {key} took effect {}",
                        made.len()
                    );
                }
                for ask in 1..=asks {
                    let made = markers.get(&format!("k{ask}"));
                    match outcomes.get(&ask) {
                        Some(CommandOutcome::Answered(Answer::Name(name::Outcome::Deleted {
                            marker: true,
                            version: Some(version),
                        }))) => {
                            assert_eq!(made, Some(&vec![version.clone()]), "seed {seed}: {ask}")
                        }
                        Some(CommandOutcome::Deadline) | None => {}
                        other => panic!("seed {seed}: {ask} answered {other:?}"),
                    }
                }
                let _ = m.get(b"").unwrap();
            }
            assert!(
                refusals > 0 && doubles > 0 && lost > 0,
                "{entries_out}: {refusals} {doubles} {lost}"
            );
        }
    }
}
