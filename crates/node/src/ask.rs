//! The commands a node asks of the cell's ranges itself (docs/design/node.md §6.6): its enlistment
//! in the root range, and the gateway host's requests after it (§5.1). A range's commands are asked
//! within a session the node registers with the range at first use, an entry out at a time, the
//! commands waiting for it taken into the next entry in the order they were asked, each a serial of
//! the session (replica.md §1). An entry asked again after its exchange was lost carries the same
//! serials, which the range answers from what the session keeps without applying them twice.
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
use mantle_meta::wire::{Answer, Command, Entry, Sessioned};

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
    pub via: Via,
    pub leader: Option<PeerId>,
    pub tried: usize,
    pub backoff: Duration,
    pub pauses: u32,
}

/// A command waiting for its range's next entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Wanted {
    pub ask: CommandAsk,
    pub command: Command,
    pub deadline_ns: u64,
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
    /// Asked of no one until `at_ns`: no node named a leader, or the leader refused it for room.
    Paused { at_ns: u64 },
}

/// An entry out: what it carries, the entry, and how it is being asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Out {
    pub carried: Carried,
    pub entry: Entry,
    pub via: Via,
}

/// One range's commands: the node's session with it, the commands waiting, the entry out, and the
/// leader the nodes last named, with the backoff between rounds that found none.
#[derive(Debug)]
pub(crate) struct Asking {
    pub group: u128,
    /// The session the range registered for this node; 0 until it has.
    pub session: u64,
    pub next_serial: u64,
    pub waiting: VecDeque<Wanted>,
    pub out: Option<Out>,
    pub leader: Option<PeerId>,
    /// Nodes asked since the last pause, and the pause's backoff and draws.
    pub tried: usize,
    pub backoff: Duration,
    pub pauses: u32,
}

impl Asking {
    pub fn new(group: u128, backoff: Duration) -> Self {
        Self {
            group,
            session: 0,
            next_serial: 1,
            waiting: VecDeque::new(),
            out: None,
            leader: None,
            tried: 0,
            backoff,
            pauses: 0,
        }
    }

    /// The commands this range holds for the node: waiting and out.
    pub fn held(&self) -> usize {
        let out = match self.out.as_ref().map(|o| &o.carried) {
            Some(Carried::Commands(commands)) => commands.len(),
            Some(Carried::Registration { .. }) | None => 0,
        };
        self.waiting.len().saturating_add(out)
    }

    /// The next entry, when none is out and a command waits: the session's registration first,
    /// as `owner` (the node, as it authenticates) in its `run`, placed at `nth` among the run's
    /// registrations, under `nonce`; then as many waiting commands as `max_commands` lets one
    /// entry carry and fit `max_entry_bytes` once encoded, each the next serial. A waiting command
    /// that fits no entry alone is answered refused, `Malformed`, in `refused`.
    pub fn next_entry(
        &mut self,
        (max_commands, max_entry_bytes): (usize, usize),
        (owner, run, nth): (u64, u64, u64),
        nonce: u64,
        refused: &mut Vec<(CommandAsk, CommandOutcome)>,
    ) {
        if self.out.is_some() || self.waiting.is_empty() {
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
            self.out = Some(Out {
                carried: Carried::Registration { nonce },
                entry,
                via: Via::Unasked,
            });
            return;
        }
        let first = self.next_serial;
        let mut take = self.waiting.len().min(max_commands.max(1));
        loop {
            let entry = Entry {
                at_ns: 0,
                commands: (0u64..)
                    .zip(self.waiting.iter().take(take))
                    .map(|(at, wanted)| Sessioned {
                        session: self.session,
                        serial: first.saturating_add(at),
                        unanswered: first,
                        command: wanted.command.clone(),
                    })
                    .collect(),
            };
            let fits = entry
                .encode()
                .is_ok_and(|bytes| bytes.len() <= max_entry_bytes);
            if fits {
                let mut carried = Vec::with_capacity(take);
                for (at, wanted) in (0u64..).zip(self.waiting.drain(..take)) {
                    carried.push((wanted.ask, first.saturating_add(at), wanted.deadline_ns));
                }
                self.next_serial = first.saturating_add(u64::try_from(take).unwrap_or(u64::MAX));
                self.out = Some(Out {
                    carried: Carried::Commands(carried),
                    entry,
                    via: Via::Unasked,
                });
                return;
            }
            if take <= 1 {
                // No entry carries this command alone: it is answered and the next tried.
                if let Some(wanted) = self.waiting.pop_front() {
                    refused.push((
                        wanted.ask,
                        CommandOutcome::Refused(command::Status::Malformed),
                    ));
                }
                if self.waiting.is_empty() {
                    return;
                }
                take = self.waiting.len().min(max_commands.max(1));
                continue;
            }
            take = take.saturating_sub(1);
        }
    }

    /// The entry out was answered with `answers`, one for each of its commands: each command's
    /// outcome into `done`. A registration answered `Registered` opens the session; one answered
    /// `Superseded` says the owner registered in a later run, a process other than this one, and
    /// each waiting command is answered so; one answered otherwise is asked again. A command whose
    /// session the range no longer holds was not applied (`Answer::SessionExpired`): the session is
    /// registered anew and the command waits again, in its place.
    pub fn answered(&mut self, answers: Vec<Answer>, done: &mut Vec<(CommandAsk, CommandOutcome)>) {
        let Some(out) = self.out.take() else {
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
                        done.push((wanted.ask, CommandOutcome::Answered(Answer::Superseded)));
                    }
                }
                _ => {}
            },
            Carried::Commands(commands) => {
                if answers.len() != commands.len() || out.entry.commands.len() != commands.len() {
                    for (ask, _, _) in commands {
                        done.push((ask, CommandOutcome::Refused(command::Status::Malformed)));
                    }
                    return;
                }
                let mut again = Vec::new();
                for (((ask, _, deadline_ns), sessioned), answer) in
                    commands.into_iter().zip(out.entry.commands).zip(answers)
                {
                    if answer == Answer::SessionExpired {
                        again.push(Wanted {
                            ask,
                            command: sessioned.command,
                            deadline_ns,
                        });
                    } else {
                        done.push((ask, CommandOutcome::Answered(answer)));
                    }
                }
                if again.is_empty() {
                    return;
                }
                self.session = 0;
                // Each command expired waits again in its place, before those that came after.
                for wanted in again.into_iter().rev() {
                    self.waiting.push_front(wanted);
                }
            }
        }
    }

    /// The entry out was refused for what it is, and none of it applied: each of its commands
    /// answered so, a registration asked again.
    pub fn refused(
        &mut self,
        status: command::Status,
        done: &mut Vec<(CommandAsk, CommandOutcome)>,
    ) {
        let Some(out) = self.out.take() else {
            return;
        };
        if let Carried::Commands(commands) = out.carried {
            for (ask, _, _) in commands {
                done.push((ask, CommandOutcome::Refused(status)));
            }
        }
    }

    /// The commands past their deadline at `now_ns`: those waiting are answered so and leave;
    /// an entry out whose every command is past it is given up, since one that may still apply is
    /// told to its caller as such. Whether the entry out was given up, which its asking then ends.
    pub fn expire(&mut self, now_ns: u64, done: &mut Vec<(CommandAsk, CommandOutcome)>) -> bool {
        let mut kept = VecDeque::with_capacity(self.waiting.len());
        for wanted in self.waiting.drain(..) {
            if wanted.deadline_ns <= now_ns {
                done.push((wanted.ask, CommandOutcome::Deadline));
            } else {
                kept.push_back(wanted);
            }
        }
        self.waiting = kept;
        let past = match self.out.as_ref().map(|o| &o.carried) {
            Some(Carried::Commands(commands)) => {
                commands.iter().all(|(_, _, deadline)| *deadline <= now_ns)
            }
            // A registration nothing waits for any more.
            Some(Carried::Registration { .. }) => self.waiting.is_empty(),
            None => false,
        };
        if !past {
            return false;
        }
        if let Some(Out {
            carried: Carried::Commands(commands),
            ..
        }) = self.out.take()
        {
            for (ask, _, _) in commands {
                done.push((ask, CommandOutcome::Deadline));
            }
        }
        true
    }

    /// The earliest deadline of a command this range holds.
    pub fn next_deadline(&self) -> Option<u64> {
        let out = match self.out.as_ref().map(|o| &o.carried) {
            Some(Carried::Commands(commands)) => commands.iter().map(|(_, _, d)| *d).min(),
            Some(Carried::Registration { .. }) | None => None,
        };
        self.waiting.iter().map(|w| w.deadline_ns).chain(out).min()
    }

    /// A node answered for the range: the next round's backoff starts over.
    fn leader_found(&mut self) {
        self.tried = 0;
        self.pauses = 0;
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
        }
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
        a.next_entry((2, 1 << 20), (1, 1, 42), 42, &mut done);
        let out = a.out.clone().unwrap();
        assert_eq!(out.carried, Carried::Registration { nonce: 42 });
        assert_eq!(out.entry.commands[0].serial, 42);
        a.answered(vec![Answer::Registered { session: 9 }], &mut done);
        assert_eq!((a.session, a.next_serial), (9, 1));
        // Two to an entry: serials 1 and 2, the oldest unanswered the first.
        a.next_entry((2, 1 << 20), (1, 1, 43), 43, &mut done);
        let out = a.out.clone().unwrap();
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
        a.answered(vec![Answer::Repeated, Answer::WrongLayer], &mut done);
        a.next_entry((2, 1 << 20), (1, 1, 44), 44, &mut done);
        assert_eq!(
            a.out.as_ref().unwrap().carried,
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

    #[test]
    fn a_superseded_registration_answers_each_waiting_command_so() {
        // The owner registered in a later run: this process's commands go to no session.
        let mut a = asking(&[wanted(1, 100), wanted(2, 100)]);
        let mut done = Vec::new();
        a.next_entry((2, 1 << 20), (1, 1, 5), 5, &mut done);
        a.answered(vec![Answer::Superseded], &mut done);
        assert_eq!(a.session, 0);
        assert!(a.waiting.is_empty() && a.out.is_none());
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
        a.next_entry((16, one), (1, 1, 0), 0, &mut done);
        assert_eq!(
            a.out.as_ref().unwrap().carried,
            Carried::Commands(vec![(1, 1, 100)])
        );
        // Room for none: each is refused, nothing goes.
        let mut b = asking(&[wanted(1, 100), wanted(2, 100)]);
        b.session = 5;
        b.next_entry((16, one - 1), (1, 1, 0), 0, &mut done);
        assert!(b.out.is_none() && b.waiting.is_empty());
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
        a.next_entry((2, 1 << 20), (1, 1, 0), 0, &mut done);
        a.answered(
            vec![Answer::SessionExpired, Answer::SessionExpired],
            &mut done,
        );
        assert!(done.is_empty());
        assert_eq!(a.session, 0);
        let order: Vec<_> = a.waiting.iter().map(|w| w.ask).collect();
        assert_eq!(order, vec![1, 2, 3]);
        // The new session first.
        a.next_entry((2, 1 << 20), (1, 1, 8), 8, &mut done);
        assert_eq!(
            a.out.as_ref().unwrap().carried,
            Carried::Registration { nonce: 8 }
        );
    }

    #[test]
    fn deadlines_answer_waiting_commands_and_give_up_an_entry_whose_every_command_passed() {
        let mut a = asking(&[wanted(1, 50), wanted(2, 200), wanted(3, 60)]);
        a.session = 5;
        let mut done = Vec::new();
        a.next_entry((2, 1 << 20), (1, 1, 0), 0, &mut done);
        assert_eq!(a.next_deadline(), Some(50));
        // Command 3, waiting, passes; the entry out holds 2, not yet past.
        assert!(!a.expire(100, &mut done));
        assert_eq!(done, vec![(3, CommandOutcome::Deadline)]);
        // Both of the entry's past: it is given up.
        assert!(a.expire(200, &mut done));
        assert!(a.out.is_none());
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
}
