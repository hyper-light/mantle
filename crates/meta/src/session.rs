//! Gateways' sessions with a range (docs/design/replica.md §1): the Raft dissertation's
//! client sessions, which make each command take effect once however often it is retried
//! (06 §A1.8). A session is registered by a command and named by the entry that registered
//! it. It keeps the answers the gateway has yet to acknowledge, and expires when entries'
//! times pass its lifetime without it, so every replica expires the same sessions at the
//! same entry.

use crate::engine::{Rows, Write};
use crate::error::MetaError;
use crate::key::LOCAL;
use crate::key::marker::{EXPIRY, SESSION, SESSIONS};
use crate::record::{self, Session};
use crate::wire::{Answer, MAX_COMMANDS, Sessioned};

// A session's row is `[LOCAL, SESSION, session]`, and its place in the order of last use
// `[LOCAL, EXPIRY, last use, session]`.

/// How many sessions the range holds.
const COUNT: &[u8] = &[LOCAL, SESSIONS];

/// The bounds a range keeps its sessions within, from how long gateways go between commands
/// and how many commands each keeps in flight (docs/design/replica.md §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rules {
    /// Entry time after a session's last use at which it expires.
    pub lifetime_ns: u64,
    pub max_sessions: u64,
    /// Answers one session keeps: past it, the oldest is forgotten.
    pub max_answers: usize,
    /// Sessions one entry expires at most, which bounds the entry's work.
    pub expiries_per_entry: usize,
}

/// What a command's session says of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// No such session: it never was, or it expired.
    Unknown,
    /// Answered and forgotten.
    Repeated,
    /// Answered, and the answer is kept.
    Answered(Answer),
    New,
}

fn session_key(session: u64) -> Vec<u8> {
    let mut k = vec![LOCAL, SESSION];
    k.extend_from_slice(&session.to_be_bytes());
    k
}

fn expiry_key(last_ns: u64, session: u64) -> Vec<u8> {
    let mut k = vec![LOCAL, EXPIRY];
    k.extend_from_slice(&last_ns.to_be_bytes());
    k.extend_from_slice(&session.to_be_bytes());
    k
}

fn read<R: Rows>(rows: &R, session: u64) -> Result<Option<Session>, MetaError> {
    Ok(rows
        .get(&session_key(session))?
        .map(|b| Session::decode(&b))
        .transpose()?)
}

fn count<R: Rows>(rows: &R) -> Result<u64, MetaError> {
    Ok(match rows.get(COUNT)? {
        None => 0,
        Some(bytes) => record::decode_number(&bytes, "sessions")?,
    })
}

/// What `session` says of the command with `serial`.
pub fn check<R: Rows>(rows: &R, session: u64, serial: u64) -> Result<Check, MetaError> {
    let Some(s) = read(rows, session)? else {
        return Ok(Check::Unknown);
    };
    if serial < s.low {
        return Ok(Check::Repeated);
    }
    match s.answers.iter().find(|(kept, _)| *kept == serial) {
        Some((_, answer)) => Ok(Check::Answered(Answer::decode(answer)?)),
        None => Ok(Check::New),
    }
}

/// Registers a session at the command in place `position` of entry `index`, expiring the
/// least recently used when the range holds its bound, and returns its ID.
pub fn register<R: Rows>(
    rows: &mut R,
    index: u64,
    position: usize,
    at_ns: u64,
    rules: &Rules,
) -> Result<u64, MetaError> {
    let places = u64::try_from(MAX_COMMANDS).map_err(|_| MetaError::Corrupt)?;
    let session = u64::try_from(position)
        .ok()
        .filter(|&p| p < places)
        .and_then(|p| index.checked_mul(places)?.checked_add(p))
        .ok_or(MetaError::Corrupt)?;
    let mut held = count(rows)?;
    let mut writes = Vec::with_capacity(5);
    if held >= rules.max_sessions {
        let (from, to) = (vec![LOCAL, EXPIRY], vec![LOCAL, EXPIRY.saturating_add(1)]);
        if let Some((k, _)) = rows.next(&from, &to)? {
            writes.extend(removal(&k)?);
            held = held.checked_sub(1).ok_or(MetaError::Corrupt)?;
        }
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
    rows.apply(index, &writes)?;
    Ok(session)
}

/// Records that `command`'s session was used at `at_ns`: forgets the answers the gateway has
/// received, those before `command.unanswered`, keeps `answer` for `command.serial` if there
/// is one, and moves the session in the order of last use.
pub fn record<R: Rows>(
    rows: &mut R,
    index: u64,
    command: &Sessioned,
    answer: Option<&Answer>,
    at_ns: u64,
    rules: &Rules,
) -> Result<(), MetaError> {
    let (session, serial, unanswered) = (command.session, command.serial, command.unanswered);
    let Some(mut s) = read(rows, session)? else {
        return Ok(());
    };
    let mut writes = vec![Write::Delete(expiry_key(s.last_ns, session))];
    s.low = s.low.max(unanswered);
    s.answers.retain(|(kept, _)| *kept >= s.low);
    if let Some(answer) = answer {
        s.answers.push((serial, answer.encode()?));
    }
    while s.answers.len() > rules.max_answers {
        let (forgotten, _) = s.answers.remove(0);
        s.low = s.low.max(forgotten.saturating_add(1));
    }
    s.last_ns = at_ns;
    writes.push(Write::Put(session_key(session), s.encode()?));
    writes.push(Write::Put(expiry_key(at_ns, session), Vec::new()));
    rows.apply(index, &writes)?;
    Ok(())
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
        let last_ns = k
            .get(2..10)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or(MetaError::Corrupt)?;
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
        expiries_per_entry: 8,
    };

    fn used(session: u64, serial: u64, unanswered: u64) -> Sessioned {
        Sessioned {
            session,
            serial,
            unanswered,
            command: crate::wire::Command::Register,
        }
    }

    fn answer(n: u8) -> Answer {
        Answer::Name(Outcome::Created {
            upload: format!("u{n}"),
        })
    }

    #[test]
    fn a_session_answers_repeats_from_what_it_keeps() {
        let mut m = Model::default();
        let s = register(&mut m, 1, 3, 10, &RULES).unwrap();
        assert_eq!(s, (1 << 16) + 3);
        assert_eq!(check(&m, s, 1).unwrap(), Check::New);
        record(&mut m, 2, &used(s, 1, 1), Some(&answer(1)), 20, &RULES).unwrap();
        assert_eq!(check(&m, s, 1).unwrap(), Check::Answered(answer(1)));
        record(&mut m, 3, &used(s, 2, 1), Some(&answer(2)), 30, &RULES).unwrap();
        // The gateway received 1's answer: it is forgotten, and 1 is a repeat now.
        record(&mut m, 4, &used(s, 3, 2), Some(&answer(3)), 40, &RULES).unwrap();
        assert_eq!(check(&m, s, 1).unwrap(), Check::Repeated);
        assert_eq!(check(&m, s, 2).unwrap(), Check::Answered(answer(2)));
        // Past the bound of answers kept, the oldest is forgotten too.
        record(&mut m, 5, &used(s, 4, 2), Some(&answer(4)), 50, &RULES).unwrap();
        assert_eq!(check(&m, s, 2).unwrap(), Check::Repeated);
        assert_eq!(check(&m, s, 4).unwrap(), Check::Answered(answer(4)));
        assert_eq!(check(&m, s + 1, 1).unwrap(), Check::Unknown);
    }

    #[test]
    fn sessions_expire_by_entry_time_and_the_least_recent_makes_room() {
        let mut m = Model::default();
        let a = register(&mut m, 1, 0, 10, &RULES).unwrap();
        let b = register(&mut m, 2, 0, 20, &RULES).unwrap();
        record(&mut m, 3, &used(a, 1, 0), Some(&answer(1)), 30, &RULES).unwrap();
        // A third session expires b, now the least recently used.
        let c = register(&mut m, 4, 0, 40, &RULES).unwrap();
        assert_eq!(check(&m, b, 1).unwrap(), Check::Unknown);
        assert_eq!(check(&m, a, 1).unwrap(), Check::Answered(answer(1)));
        // An entry at 131 passes a's lifetime (30 + 100) but not c's (40 + 100).
        expire(&mut m, 5, 131, &RULES).unwrap();
        assert_eq!(check(&m, a, 1).unwrap(), Check::Unknown);
        assert_eq!(check(&m, c, 1).unwrap(), Check::New);
        assert_eq!(count(&m).unwrap(), 1);
        expire(&mut m, 6, 141, &RULES).unwrap();
        assert_eq!(count(&m).unwrap(), 0);
    }
}
