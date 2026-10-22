//! Gateways' sessions with a range (docs/design/replica.md §1): the Raft dissertation's
//! client sessions, which make each command take effect once however often it is retried
//! (06 §A1.8). A session is registered by a command and named by the entry that registered
//! it. It keeps the answers the gateway has yet to acknowledge, and expires when entries'
//! times pass its lifetime without it, or when those answers pass its bounds, so every replica
//! expires the same sessions at the same entry.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::engine::{Rows, Write};
use crate::error::MetaError;
use crate::key::LOCAL;
use crate::key::marker::{EXPIRY, OWNER, SESSION, SESSIONS};
use crate::record::{self, Session};
use crate::wire::{Answer, MAX_COMMANDS, Sessioned};

// A session's row is `[LOCAL, SESSION, session]`, its place in the order of last use
// `[LOCAL, EXPIRY, last use, session]`, and its owner's `[LOCAL, OWNER, owner]` holds the session
// that owner's last registration opened and that registration's run and place in the run, the
// session named still where the range has since removed it.

/// How many sessions the range holds.
const COUNT: &[u8] = &[LOCAL, SESSIONS];

/// The bounds a range keeps its sessions within, from how long gateways go between commands
/// and how many commands each keeps in flight (docs/design/replica.md §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rules {
    /// Entry time after a session's last use at which it expires.
    pub lifetime_ns: u64,
    pub max_sessions: u64,
    /// Answers one session keeps unacknowledged: past it, the session expires.
    pub max_answers: usize,
    /// Encoded bytes of the answers one session keeps unacknowledged: past it, the session
    /// expires too, since a count bounds no answer's size (audit P04).
    pub max_answer_bytes: usize,
    /// Sessions one entry expires at most, which bounds the entry's work.
    pub expiries_per_entry: usize,
}

/// What a command's session says of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// No such session: it never was, or it expired.
    Unknown,
    /// Answered, and forgotten once the gateway acknowledged the answer.
    Repeated,
    /// Answered, and the answer is kept.
    Answered(Answer),
    New,
    /// The session's serials are spent.
    Spent,
}

fn session_key(session: u64) -> Vec<u8> {
    let mut k = vec![LOCAL, SESSION];
    k.extend_from_slice(&session.to_be_bytes());
    k
}

fn owner_key(owner: u64) -> Vec<u8> {
    let mut k = vec![LOCAL, OWNER];
    k.extend_from_slice(&owner.to_be_bytes());
    k
}

fn expiry_key(last_ns: u64, session: u64) -> Vec<u8> {
    let mut k = vec![LOCAL, EXPIRY];
    k.extend_from_slice(&last_ns.to_be_bytes());
    k.extend_from_slice(&session.to_be_bytes());
    k
}

fn read<R: Rows>(rows: &mut R, session: u64) -> Result<Option<Session>, MetaError> {
    Ok(rows
        .get(&session_key(session))?
        .map(|b| Session::decode(&b))
        .transpose()?)
}

fn count<R: Rows>(rows: &mut R) -> Result<u64, MetaError> {
    Ok(match rows.get(COUNT)? {
        None => 0,
        Some(bytes) => record::decode_number(&bytes, "sessions")?,
    })
}

/// A session as an entry holds it: read and decoded at the first command that names it, changed
/// in memory by each command after, and written once (`Touched::write`).
#[derive(Debug)]
struct Held {
    /// The last use its row records, whose place in the order of last use the entry moves.
    found_ns: u64,
    last_ns: u64,
    low: u64,
    /// The answers kept, in the order they were given, each encoded, and their bytes.
    answers: Vec<(u64, Vec<u8>)>,
    bytes: usize,
    changed: bool,
    /// Its answers passed the session's bounds: it is removed when the entry writes it.
    expired: bool,
}

/// The sessions one entry's commands name. Each is read and decoded at its first command,
/// checked and changed in memory by every command after, and written once, so a command
/// costs what it changes: before, every command read, searched and wrote back every answer its
/// session kept, and an entry of many commands from one session did so for each (audit P04).
/// Commands see each other's effects on their session as they did, since they run in order
/// against the same held state.
#[derive(Debug, Default)]
pub struct Touched {
    held: BTreeMap<u64, Held>,
}

impl Touched {
    /// The held state of `session`, read from `rows` at its first use; `None` if it has none.
    fn held<R: Rows>(
        &mut self,
        rows: &mut R,
        session: u64,
    ) -> Result<Option<&mut Held>, MetaError> {
        Ok(match self.held.entry(session) {
            std::collections::btree_map::Entry::Occupied(held) => Some(held.into_mut()),
            std::collections::btree_map::Entry::Vacant(place) => match read(rows, session)? {
                None => None,
                Some(s) => Some(
                    place.insert(Held {
                        found_ns: s.last_ns,
                        last_ns: s.last_ns,
                        low: s.low,
                        bytes: s
                            .answers
                            .iter()
                            .fold(0usize, |sum, (_, a)| sum.saturating_add(a.len())),
                        answers: s.answers,
                        changed: false,
                        expired: false,
                    }),
                ),
            },
        })
    }

    /// What `session` says of the command with `serial`. The last serial is spent: no serial
    /// after it could carry the acknowledgement of its answer, so a command bearing it is
    /// refused before it takes effect, and the gateway registers a session anew (audit B03).
    pub fn check<R: Rows>(
        &mut self,
        rows: &mut R,
        session: u64,
        serial: u64,
    ) -> Result<Check, MetaError> {
        let Some(s) = self.held(rows, session)?.filter(|s| !s.expired) else {
            return Ok(Check::Unknown);
        };
        if serial == u64::MAX {
            return Ok(Check::Spent);
        }
        if serial < s.low {
            return Ok(Check::Repeated);
        }
        match s.answers.iter().find(|(kept, _)| *kept == serial) {
            Some((_, answer)) => Ok(Check::Answered(Answer::decode(answer)?)),
            None => Ok(Check::New),
        }
    }

    /// Records that `command`'s session, which `check` found, was used at `at_ns`: forgets the
    /// answers the gateway has received, those before `command.unanswered`, keeps `answer` for
    /// `command.serial` if there is one, and moves the session in the order of last use.
    ///
    /// Only the gateway's acknowledgement forgets an answer. Serials reach the log in any
    /// order, so an answer forgotten for room could lie above a serial still in flight, and a
    /// watermark raised past it would take that serial for a repeat it never was. A session
    /// whose unacknowledged answers pass its bounds expires instead, as the Raft
    /// dissertation bounds sessions (06 §A1.8): every command after it is refused, never
    /// applied, and the gateway registers anew.
    pub fn record(
        &mut self,
        command: &Sessioned,
        answer: Option<&Answer>,
        at_ns: u64,
        rules: &Rules,
    ) -> Result<(), MetaError> {
        let Some(s) = self.held.get_mut(&command.session) else {
            return Ok(());
        };
        s.low = s.low.max(command.unanswered);
        let low = s.low;
        let mut bytes = s.bytes;
        s.answers.retain(|(kept, a)| {
            let keep = *kept >= low;
            if !keep {
                bytes = bytes.saturating_sub(a.len());
            }
            keep
        });
        s.bytes = bytes;
        if let Some(answer) = answer {
            let encoded = answer.encode()?;
            s.bytes = s.bytes.saturating_add(encoded.len());
            s.answers.push((command.serial, encoded));
        }
        if s.answers.len() > rules.max_answers || s.bytes > rules.max_answer_bytes {
            s.expired = true;
            s.answers.clear();
            s.bytes = 0;
        }
        s.last_ns = at_ns;
        s.changed = true;
        Ok(())
    }

    /// Writes every session the entry changed, and moves each in the order of last use, or
    /// removes it if it expired, then lets them go: a command after reads them anew.
    pub fn write<R: Rows>(&mut self, rows: &mut R, index: u64) -> Result<(), MetaError> {
        let mut writes = Vec::new();
        let mut expired = 0u64;
        for (session, s) in std::mem::take(&mut self.held) {
            if s.expired {
                writes.extend(removal(&expiry_key(s.found_ns, session))?);
                expired = expired.checked_add(1).ok_or(MetaError::Corrupt)?;
                continue;
            }
            if !s.changed {
                continue;
            }
            if s.found_ns != s.last_ns {
                writes.push(Write::Delete(expiry_key(s.found_ns, session)));
            }
            let row = Session {
                last_ns: s.last_ns,
                low: s.low,
                answers: s.answers,
            };
            writes.push(Write::Put(session_key(session), row.encode()?));
            writes.push(Write::Put(expiry_key(s.last_ns, session), Vec::new()));
        }
        if expired > 0 {
            let held = count(rows)?
                .checked_sub(expired)
                .ok_or(MetaError::Corrupt)?;
            writes.push(Write::Put(COUNT.to_vec(), record::encode_number(held)));
        }
        if !writes.is_empty() {
            rows.apply(index, &writes)?;
        }
        Ok(())
    }
}

/// What a registration gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registration {
    Session(u64),
    /// The range holds `max_sessions` sessions, each within its lifetime: none is taken
    /// from its gateway, and one ends its lifetime at `until_ns`, unless used again.
    Full {
        until_ns: u64,
    },
    /// The owner registered later than this registration, in a later run or later in this one:
    /// no session is opened, and the later registration's is kept.
    Superseded,
}

/// Registers a session at the command in place `position` of entry `index`, and returns its
/// ID. A range holding its bound takes the place of the least recently used session only once
/// that session's lifetime has passed. A session within its lifetime may have commands in
/// flight, whose outcome its gateway learns only through it; expired to make room, their
/// retries were refused `SessionExpired`, their outcome unknown to the gateway, and a
/// registration past the bound took a live session from each gateway in turn (audit S17).
/// The Raft dissertation expires sessions by a rule all replicas apply alike, the lifetime,
/// and has a client whose session expired treat its commands' outcome as unknown (06 §A1.8),
/// so a bound on sessions is kept by refusing registration, not by expiring the living.
///
/// A registration names its owner, the identity its sender authenticated as, the owner's run,
/// and its place among the run's registrations (docs/design/node.md §1.4), and the range keeps
/// that order of each owner's last. A later registration ends the session the owner's last one
/// opened, if the range still holds it: the process that registered it has ended, or registers
/// again only once it holds nothing under it. The same registration delivered again is
/// answered the session it opened, and an earlier one is refused, `Superseded`, so none
/// delivered late ends a session opened after it, whose commands may be out. An owner then holds
/// at most one session, and a restart leaves none behind to wait out a lifetime.
pub fn register<R: Rows>(
    rows: &mut R,
    index: u64,
    (position, at_ns): (usize, u64),
    rules: &Rules,
    (owner, run, nth): (u64, u64, u64),
) -> Result<Registration, MetaError> {
    let places = u64::try_from(MAX_COMMANDS).map_err(|_| MetaError::Corrupt)?;
    let session = u64::try_from(position)
        .ok()
        .filter(|&p| p < places)
        .and_then(|p| index.checked_mul(places)?.checked_add(p))
        .ok_or(MetaError::Corrupt)?;
    let last = owned(rows, owner)?;
    if let Some(last) = &last {
        match last.order.cmp(&(run, nth)) {
            Ordering::Greater => return Ok(Registration::Superseded),
            // The session it opened, which the range may have expired since: its owner finds
            // that at its first command.
            Ordering::Equal => return Ok(Registration::Session(last.session)),
            Ordering::Less => {}
        }
    }
    let mut held = count(rows)?;
    let mut writes = Vec::new();
    match last.and_then(|l| Some((l.session, l.last_ns?))) {
        // Its own earlier session's place, which the bound already counts.
        Some((earlier, last_ns)) => {
            writes.extend(removal(&expiry_key(last_ns, earlier))?);
            held = held.checked_sub(1).ok_or(MetaError::Corrupt)?;
        }
        None if held >= rules.max_sessions => {
            let (from, to) = (vec![LOCAL, EXPIRY], vec![LOCAL, EXPIRY.saturating_add(1)]);
            let (k, _) = rows.next(&from, &to)?.ok_or(MetaError::Corrupt)?;
            let ends_ns = last_used(&k)?.saturating_add(rules.lifetime_ns);
            // Within its lifetime at `ends_ns` itself, as `expire` counts it.
            if ends_ns >= at_ns {
                return Ok(Registration::Full {
                    until_ns: ends_ns.saturating_add(1),
                });
            }
            writes.extend(removal(&k)?);
            held = held.checked_sub(1).ok_or(MetaError::Corrupt)?;
        }
        None => {}
    }
    let row = Session {
        last_ns: at_ns,
        low: 0,
        answers: Vec::new(),
    };
    let held = held.checked_add(1).ok_or(MetaError::Corrupt)?;
    writes.push(Write::Put(session_key(session), row.encode()?));
    writes.push(Write::Put(expiry_key(at_ns, session), Vec::new()));
    writes.push(Write::Put(COUNT.to_vec(), record::encode_number(held)));
    let last = [session, run, nth].map(u64::to_be_bytes).concat();
    writes.push(Write::Put(owner_key(owner), last));
    rows.apply(index, &writes)?;
    Ok(Registration::Session(session))
}

/// Bytes of an owner's row: the session, the run and the place in it, each a big-endian word.
const OWNED_BYTES: usize = 24;

/// What a range keeps of an owner's last registration.
struct Owned {
    /// The session it opened.
    session: u64,
    /// Its run, then its place in the run.
    order: (u64, u64),
    /// That session's last use, while the range holds it.
    last_ns: Option<u64>,
}

/// What the range keeps of `owner`'s last registration, if it took one.
fn owned<R: Rows>(rows: &mut R, owner: u64) -> Result<Option<Owned>, MetaError> {
    let Some(bytes) = rows.get(&owner_key(owner))? else {
        return Ok(None);
    };
    let word = |at: std::ops::Range<usize>| {
        bytes
            .get(at)
            .and_then(|w| <[u8; 8]>::try_from(w).ok())
            .map(u64::from_be_bytes)
            .ok_or(MetaError::Corrupt)
    };
    if bytes.len() != OWNED_BYTES {
        return Err(MetaError::Corrupt);
    }
    let session = word(0..8)?;
    let order = (word(8..16)?, word(16..24)?);
    let last_ns = read(rows, session)?.map(|s| s.last_ns);
    Ok(Some(Owned {
        session,
        order,
        last_ns,
    }))
}

/// Expires the sessions whose lifetime entry time `at_ns` has passed, at most
/// `expiries_per_entry` of them; the rest expire at the entries after.
pub fn expire<R: Rows>(
    rows: &mut R,
    index: u64,
    at_ns: u64,
    rules: &Rules,
) -> Result<(), MetaError> {
    let (mut from, to) = (vec![LOCAL, EXPIRY], vec![LOCAL, EXPIRY.saturating_add(1)]);
    let mut writes = Vec::new();
    let mut expired = 0u64;
    for _ in 0..rules.expiries_per_entry {
        let Some((k, _)) = rows.next(&from, &to)? else {
            break;
        };
        let last_ns = last_used(&k)?;
        // A lifetime past the end of time never expires.
        if last_ns.saturating_add(rules.lifetime_ns) >= at_ns {
            break;
        }
        writes.extend(removal(&k)?);
        expired = expired.checked_add(1).ok_or(MetaError::Corrupt)?;
        from = k;
        from.push(0);
    }
    if expired == 0 {
        return Ok(());
    }
    let held = count(rows)?
        .checked_sub(expired)
        .ok_or(MetaError::Corrupt)?;
    writes.push(Write::Put(COUNT.to_vec(), record::encode_number(held)));
    rows.apply(index, &writes)?;
    Ok(())
}

/// When the session an expiry row names was last used.
fn last_used(k: &[u8]) -> Result<u64, MetaError> {
    k.get(2..10)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or(MetaError::Corrupt)
}

/// The session an expiry row names.
fn expired_session(k: &[u8]) -> Result<u64, MetaError> {
    k.get(10..18)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or(MetaError::Corrupt)
}

/// The writes that remove the session an expiry row names; the count is the caller's.
fn removal(expiry: &[u8]) -> Result<[Write; 2], MetaError> {
    let session = expired_session(expiry)?;
    Ok([
        Write::Delete(session_key(session)),
        Write::Delete(expiry.to_vec()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;
    use crate::name::Outcome;

    const RULES: Rules = Rules {
        lifetime_ns: 100,
        max_sessions: 2,
        max_answers: 2,
        max_answer_bytes: usize::MAX,
        expiries_per_entry: 8,
    };

    fn used(session: u64, serial: u64, unanswered: u64) -> Sessioned {
        Sessioned {
            session,
            serial,
            unanswered,
            command: crate::wire::Command::Register {
                owner: 1,
                run: 1,
                nth: 1,
            },
        }
    }

    fn answer(n: u8) -> Answer {
        Answer::Name(Outcome::Created {
            upload: format!("u{n}"),
        })
    }

    /// A registration at `position` of entry `index`, its owner's first: each test's
    /// registrations are distinct owners' unless a test names the owner, its run and the
    /// registration's place in it (`register_as`).
    fn register(
        m: &mut Model,
        index: u64,
        position: usize,
        at_ns: u64,
        rules: &Rules,
    ) -> Result<u64, MetaError> {
        register_as(
            m,
            (index, position),
            at_ns,
            rules,
            (index * 100 + position as u64, 1, 1),
        )
    }

    fn register_as(
        m: &mut Model,
        (index, position): (u64, usize),
        at_ns: u64,
        rules: &Rules,
        owner: (u64, u64, u64),
    ) -> Result<u64, MetaError> {
        match super::register(m, index, (position, at_ns), rules, owner)? {
            Registration::Session(s) => Ok(s),
            Registration::Full { .. } | Registration::Superseded => Err(MetaError::Corrupt),
        }
    }

    fn check(m: &mut Model, session: u64, serial: u64) -> Result<Check, MetaError> {
        Touched::default().check(m, session, serial)
    }

    /// One command's use of its session, in an entry of its own.
    fn record(
        m: &mut Model,
        index: u64,
        command: &Sessioned,
        answer: Option<&Answer>,
        at_ns: u64,
        rules: &Rules,
    ) -> Result<(), MetaError> {
        let mut touched = Touched::default();
        touched.check(&mut *m, command.session, command.serial)?;
        touched.record(command, answer, at_ns, rules)?;
        touched.write(m, index)
    }

    #[test]
    fn a_session_answers_repeats_from_what_it_keeps() {
        let mut m = Model::default();
        let s = register(&mut m, 1, 3, 10, &RULES).unwrap();
        assert_eq!(s, (1 << 16) + 3);
        assert_eq!(check(&mut m, s, 1).unwrap(), Check::New);
        record(&mut m, 2, &used(s, 1, 1), Some(&answer(1)), 20, &RULES).unwrap();
        assert_eq!(check(&mut m, s, 1).unwrap(), Check::Answered(answer(1)));
        record(&mut m, 3, &used(s, 2, 1), Some(&answer(2)), 30, &RULES).unwrap();
        // The gateway received 1's answer: it is forgotten, and 1 is a repeat now.
        record(&mut m, 4, &used(s, 3, 2), Some(&answer(3)), 40, &RULES).unwrap();
        assert_eq!(check(&mut m, s, 1).unwrap(), Check::Repeated);
        assert_eq!(check(&mut m, s, 2).unwrap(), Check::Answered(answer(2)));
        // Past the bound of answers kept unacknowledged, the session expires, and a retry of
        // any serial is refused rather than taken for a repeat.
        record(&mut m, 5, &used(s, 4, 2), Some(&answer(4)), 50, &RULES).unwrap();
        for serial in 1..=4 {
            assert_eq!(check(&mut m, s, serial).unwrap(), Check::Unknown);
        }
        assert_eq!(count(&mut m).unwrap(), 0);
        assert_eq!(check(&mut m, s + 1, 1).unwrap(), Check::Unknown);
    }

    /// The answers a gateway has received are forgotten wherever they lie among those kept: a
    /// command reordered on its way to the log is answered after one with a later serial.
    #[test]
    fn received_answers_are_forgotten_in_any_order() {
        let mut m = Model::default();
        let rules = Rules {
            max_answers: 8,
            ..RULES
        };
        let s = register(&mut m, 1, 0, 10, &rules).unwrap();
        record(&mut m, 2, &used(s, 5, 1), Some(&answer(5)), 20, &rules).unwrap();
        record(&mut m, 3, &used(s, 3, 1), Some(&answer(3)), 30, &rules).unwrap();
        // The gateway has every answer before 4: 3's is forgotten, 5's kept.
        record(&mut m, 4, &used(s, 6, 4), Some(&answer(6)), 40, &rules).unwrap();
        let kept: Vec<u64> = read(&mut m, s)
            .unwrap()
            .unwrap()
            .answers
            .iter()
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(kept, [5, 6]);
        assert_eq!(check(&mut m, s, 3).unwrap(), Check::Repeated);
        assert_eq!(check(&mut m, s, 5).unwrap(), Check::Answered(answer(5)));
    }

    /// Past the bytes of answers a session may keep unacknowledged, it expires as past the
    /// count, whatever order its serials came in: no answer is forgotten unacknowledged.
    #[test]
    fn a_session_past_its_byte_budget_expires() {
        let mut m = Model::default();
        let size = answer(1).encode().unwrap().len();
        let rules = Rules {
            max_answers: 8,
            max_answer_bytes: 2 * size,
            ..RULES
        };
        let s = register(&mut m, 1, 0, 10, &rules).unwrap();
        for (serial, index) in [3u8, 1].into_iter().zip(2u64..) {
            let command = used(s, u64::from(serial), 1);
            record(
                &mut m,
                index,
                &command,
                Some(&answer(serial)),
                10 * index,
                &rules,
            )
            .unwrap();
        }
        assert_eq!(check(&mut m, s, 1).unwrap(), Check::Answered(answer(1)));
        assert_eq!(check(&mut m, s, 3).unwrap(), Check::Answered(answer(3)));
        record(&mut m, 4, &used(s, 2, 1), Some(&answer(2)), 40, &rules).unwrap();
        for serial in 1..=3 {
            assert_eq!(check(&mut m, s, serial).unwrap(), Check::Unknown);
        }
        assert_eq!(count(&mut m).unwrap(), 0);
    }

    #[test]
    fn sessions_expire_by_entry_time_and_the_least_recent_makes_room() {
        let mut m = Model::default();
        let a = register(&mut m, 1, 0, 10, &RULES).unwrap();
        let b = register(&mut m, 2, 0, 20, &RULES).unwrap();
        record(&mut m, 3, &used(a, 1, 0), Some(&answer(1)), 30, &RULES).unwrap();
        // At the bound, with both sessions live, a third is refused until b's lifetime ends,
        // b being the least recently used, and neither is taken from its gateway.
        assert_eq!(
            super::register(&mut m, 4, (0, 40), &RULES, (400, 1, 1)).unwrap(),
            Registration::Full { until_ns: 121 }
        );
        assert_eq!(
            super::register(&mut m, 4, (0, 120), &RULES, (400, 1, 1)).unwrap(),
            Registration::Full { until_ns: 121 }
        );
        assert_eq!(check(&mut m, b, 1).unwrap(), Check::New);
        // At 121 b's lifetime has passed, and its place goes to a new session.
        let c = register(&mut m, 4, 0, 121, &RULES).unwrap();
        assert_eq!(check(&mut m, b, 1).unwrap(), Check::Unknown);
        assert_eq!(check(&mut m, a, 1).unwrap(), Check::Answered(answer(1)));
        // An entry at 131 passes a's lifetime (30 + 100) but not c's (121 + 100).
        expire(&mut m, 5, 131, &RULES).unwrap();
        assert_eq!(check(&mut m, a, 1).unwrap(), Check::Unknown);
        assert_eq!(check(&mut m, c, 1).unwrap(), Check::New);
        assert_eq!(count(&mut m).unwrap(), 1);
        expire(&mut m, 6, 222, &RULES).unwrap();
        assert_eq!(count(&mut m).unwrap(), 0);
    }

    #[test]
    fn a_later_registration_ends_the_session_its_owner_registered_before() {
        let mut m = Model::default();
        let first = register_as(&mut m, (1, 0), 10, &RULES, (9, 1, 1)).unwrap();
        record(&mut m, 2, &used(first, 1, 0), Some(&answer(1)), 20, &RULES).unwrap();
        let other = register_as(&mut m, (3, 0), 30, &RULES, (8, 1, 1)).unwrap();
        // The owner's process ended and its next run registers: the first session goes with
        // its answers, the other owner's stays, and the range holds two.
        let second = register_as(&mut m, (4, 0), 40, &RULES, (9, 2, 1)).unwrap();
        assert_eq!(check(&mut m, first, 1).unwrap(), Check::Unknown);
        assert_eq!(check(&mut m, second, 1).unwrap(), Check::New);
        assert_eq!(check(&mut m, other, 1).unwrap(), Check::New);
        assert_eq!(count(&mut m).unwrap(), 2);
        // Later in the same run: a process that let its session go, holding nothing under it,
        // registers again.
        let third = register_as(&mut m, (5, 0), 50, &RULES, (9, 2, 2)).unwrap();
        assert_eq!(check(&mut m, second, 1).unwrap(), Check::Unknown);
        assert_eq!(check(&mut m, third, 1).unwrap(), Check::New);
        assert_eq!(count(&mut m).unwrap(), 2);
    }

    #[test]
    fn a_registration_delivered_again_is_answered_its_session_and_ends_none() {
        let mut m = Model::default();
        let first = register_as(&mut m, (1, 0), 10, &RULES, (9, 1, 1)).unwrap();
        record(&mut m, 2, &used(first, 1, 0), Some(&answer(1)), 20, &RULES).unwrap();
        // The same registration at a later entry, as a leader's retry puts it in the log twice:
        // the session it opened, its answer kept.
        assert_eq!(
            super::register(&mut m, 3, (0, 30), &RULES, (9, 1, 1)).unwrap(),
            Registration::Session(first)
        );
        assert_eq!(check(&mut m, first, 1).unwrap(), Check::Answered(answer(1)));
        assert_eq!(count(&mut m).unwrap(), 1);
    }

    #[test]
    fn a_registration_older_than_its_owners_last_is_superseded() {
        // Delivered late, after a later registration opened a session whose commands may be out,
        // an earlier one of the same run, or one of an earlier run, ends nothing.
        let mut m = Model::default();
        let session = register_as(&mut m, (1, 0), 10, &RULES, (9, 2, 2)).unwrap();
        record(
            &mut m,
            2,
            &used(session, 1, 0),
            Some(&answer(1)),
            20,
            &RULES,
        )
        .unwrap();
        for (index, (run, nth)) in [(3, (2, 1)), (4, (1, 7))] {
            assert_eq!(
                super::register(&mut m, index, (0, 10 * index), &RULES, (9, run, nth)).unwrap(),
                Registration::Superseded
            );
        }
        assert_eq!(
            check(&mut m, session, 1).unwrap(),
            Check::Answered(answer(1))
        );
        assert_eq!(count(&mut m).unwrap(), 1);
    }

    #[test]
    fn an_owner_at_the_bound_takes_its_own_place_back() {
        // Two owners fill the bound, both within their lifetimes: a third owner is refused,
        // and either of the two registering again takes its own session's place.
        let mut m = Model::default();
        let a = register_as(&mut m, (1, 0), 10, &RULES, (1, 1, 1)).unwrap();
        register_as(&mut m, (2, 0), 20, &RULES, (2, 1, 1)).unwrap();
        assert_eq!(
            super::register(&mut m, 3, (0, 30), &RULES, (3, 1, 1)).unwrap(),
            Registration::Full { until_ns: 111 }
        );
        let again = register_as(&mut m, (4, 0), 40, &RULES, (1, 2, 1)).unwrap();
        assert_eq!(check(&mut m, a, 1).unwrap(), Check::Unknown);
        assert_eq!(check(&mut m, again, 1).unwrap(), Check::New);
        assert_eq!(count(&mut m).unwrap(), 2);
    }

    #[test]
    fn an_owner_whose_session_expired_registers_without_counting_it_twice() {
        let mut m = Model::default();
        register_as(&mut m, (1, 0), 10, &RULES, (5, 1, 1)).unwrap();
        // Unused past its lifetime, the session expires; its owner's row still names it.
        expire(&mut m, 2, 111, &RULES).unwrap();
        assert_eq!(count(&mut m).unwrap(), 0);
        let next = register_as(&mut m, (3, 0), 120, &RULES, (5, 1, 2)).unwrap();
        assert_eq!(check(&mut m, next, 1).unwrap(), Check::New);
        assert_eq!(count(&mut m).unwrap(), 1);
    }
}
