//! Applying a range's entry (docs/design/replica.md §2): every command of the entry in order,
//! each checked against its session and run by the range's layer, through an overlay, so
//! that the engine takes the whole entry as one batch at its index.

use crate::engine::{EngineError, Rows};
use crate::error::{MetaError, reserved};
use crate::overlay::Overlay;
use crate::session::{self, Check, Rules, Touched};
use crate::wire::{Answer, Command, Entry};
use crate::{block, bucket, file, name, root};

/// Which layer's rows a range holds (docs/design/metadata.md §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Bucket,
    Name,
    File,
    Block,
    /// The cell's root range: its volume table (docs/design/node.md §5.6).
    Root,
}

/// Applies entry `index` to the range's rows and answers its commands in order. An error
/// means the entry breaks the state machine, and the replica stops.
pub fn apply_entry<R: Rows>(
    rows: &mut R,
    index: u64,
    entry: &Entry,
    layer: Layer,
    rules: &Rules,
) -> Result<Vec<Answer>, MetaError> {
    let (answers, writes) = {
        let mut overlay = Overlay::new(&mut *rows, (rules.max_key_bytes, rules.max_value_bytes));
        session::expire(&mut overlay, index, entry.at_ns, rules)?;
        let mut touched = Touched::default();
        let mut answers = reserved(entry.commands.len())?;
        for (position, c) in entry.commands.iter().enumerate() {
            let answer = match &c.command {
                // Registering may expire the session least recently used, which it finds by
                // the order of last use: the sessions used so far are written first, and read
                // anew after.
                Command::Register { owner, run, nth } => {
                    touched.write(&mut overlay, index)?;
                    let at = (position, entry.at_ns);
                    match session::register(&mut overlay, index, at, rules, (*owner, *run, *nth))? {
                        session::Registration::Session(session) => Answer::Registered { session },
                        session::Registration::Full { until_ns } => {
                            Answer::SessionsFull { until_ns }
                        }
                        session::Registration::Superseded => Answer::Superseded,
                    }
                }
                command => match touched.check(&mut overlay, c.session, c.serial)? {
                    // A session whose serials are spent is done: the gateway registers anew.
                    Check::Unknown | Check::Spent => Answer::SessionExpired,
                    Check::Repeated => Answer::Repeated,
                    Check::Answered(answer) => {
                        touched.acknowledge(c, entry.at_ns);
                        answer
                    }
                    Check::New if touched.refused(c) => {
                        touched.acknowledge(c, entry.at_ns);
                        Answer::AnswersFull
                    }
                    Check::New => {
                        touched.acknowledge(c, entry.at_ns);
                        // The command runs over the entry's writes so far, and its own take effect
                        // only if its session keeps its answer: refused for room, it applied
                        // nothing, and a copy of it later is its first application.
                        let bound = (rules.max_key_bytes, rules.max_value_bytes);
                        let mut own = Overlay::new(&mut overlay, bound);
                        let answer = match run(&mut own, index, command, layer) {
                            // A row past what every member's engine holds whole: the command is
                            // refused whole, each layer writing its command's batch at once,
                            // and every member refuses it alike.
                            Err(MetaError::Engine(EngineError::RowTooLarge { .. })) => {
                                Answer::RowTooLarge
                            }
                            ran => ran?,
                        };
                        let writes = own.take();
                        let encoded = answer.encode()?;
                        if touched.fits(c, encoded.len(), rules) {
                            overlay.absorb(writes);
                            touched.keep(c, encoded);
                            answer
                        } else {
                            touched.refuse(c);
                            Answer::AnswersFull
                        }
                    }
                },
            };
            answers.push(answer);
        }
        touched.write(&mut overlay, index)?;
        (answers, overlay.into_writes())
    };
    rows.apply(index, writes)?;
    Ok(answers)
}

fn run<R: Rows>(
    rows: &mut R,
    index: u64,
    command: &Command,
    layer: Layer,
) -> Result<Answer, MetaError> {
    Ok(match (layer, command) {
        (Layer::Bucket, Command::Bucket(c)) => Answer::Bucket(bucket::apply(rows, index, c)?),
        (Layer::Name, Command::Name(c)) => Answer::Name(name::apply(rows, index, c)?),
        (Layer::File, Command::File(c)) => Answer::File(file::apply(rows, index, c)?),
        (Layer::Block, Command::Block(c)) => Answer::Block(block::apply(rows, index, c)?),
        (Layer::Root, Command::Root(c)) => Answer::Root(root::apply(rows, index, c)?),
        _ => Answer::WrongLayer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, Model, Write};
    use crate::name::{GateChange, Match, Outcome, Preconditions, Put};
    use crate::record::{GateState, Session, Version, Versioning};
    use crate::wire::Sessioned;

    const RULES: Rules = Rules {
        lifetime_ns: 1_000,
        max_sessions: 8,
        expiries_per_entry: 8,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
    };

    /// A PUT carrying a file made for it alone, which the same key, ETag and precondition
    /// name: a command sent again carries the same file, and another carries another.
    fn put(key: &str, etag: &str, none_match: bool) -> Command {
        let file = mantle_crc::crc32c(format!("{key}/{etag}/{none_match}").as_bytes());
        Command::Name(Box::new(crate::name::Command::Put(Put {
            bucket: "b".into(),
            incarnation: 1,
            key: key.into(),
            versioning: Versioning::Enabled,
            preconditions: Preconditions {
                if_match: None,
                if_none_match: if none_match { Some(Match::Any) } else { None },
            },
            at_ns: 0,
            ordered_ns: None,
            version: Version {
                marker: false,
                null: false,
                modified_ns: 0,
                etag: etag.into(),
                size: 1,
                checksum: None,
                file: Some(u128::from(file)),
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default: None,
            id: 0,
            deadline_ns: u64::MAX,
        })))
    }

    fn from(session: u64, serial: u64, command: Command) -> Sessioned {
        Sessioned {
            session,
            serial,
            unanswered: 1,
            command,
        }
    }

    /// Owner 1's first registration.
    fn registration() -> Sessioned {
        let command = Command::Register {
            owner: 1,
            run: 1,
            nth: 1,
        };
        from(0, 0, command)
    }

    fn entry(at_ns: u64, commands: Vec<Sessioned>) -> Entry {
        Entry { at_ns, commands }
    }

    fn open_gate() -> Command {
        Command::Name(Box::new(crate::name::Command::Gate(GateChange {
            bucket: "b".into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(GateState::Open),
            generation: 1,
        })))
    }

    /// A cell's first Name range, holding every key.
    fn name_range() -> Model {
        let mut m = Model::default();
        m.install(0, crate::name::first(1).unwrap()).unwrap();
        m
    }

    #[test]
    fn commands_apply_once_in_order_and_see_each_other() {
        let mut m = name_range();
        let run = |m: &mut Model, index, e: Entry| {
            apply_entry(m, index, &e, Layer::Name, &RULES).unwrap()
        };
        let answers = run(&mut m, 1, entry(10, vec![registration()]));
        let [Answer::Registered { session }] = answers[..] else {
            panic!("{answers:?}")
        };
        // The second put sees the first in the same entry and fails its precondition; the
        // third repeats the first and is answered, not applied.
        let answers = run(
            &mut m,
            2,
            entry(
                20,
                vec![
                    from(session, 1, open_gate()),
                    from(session, 2, put("k", "a", true)),
                    from(session, 3, put("k", "b", true)),
                    from(session, 2, put("k", "a", true)),
                ],
            ),
        );
        let Answer::Name(Outcome::Put { version }) = &answers[1] else {
            panic!("{answers:?}")
        };
        assert_eq!(answers[2], Answer::Name(Outcome::PreconditionFailed));
        assert_eq!(answers[3], answers[1]);
        assert_eq!(m.applied(), 2);
        // A retry in a later entry is answered the same, and creates no second version.
        let answers = run(
            &mut m,
            3,
            entry(30, vec![from(session, 2, put("k", "a", true))]),
        );
        assert_eq!(
            answers,
            [Answer::Name(Outcome::Put {
                version: version.clone()
            })]
        );
        let current = crate::name::current(&mut m, "b", "k").unwrap().unwrap();
        assert_eq!(current.1.etag, "a");
        // Wrong layer, unknown session, and a serial the gateway already acknowledged.
        let block = Command::Block(crate::block::Command::Delete { block: 1 });
        let answers = run(
            &mut m,
            4,
            entry(
                40,
                vec![
                    from(session, 4, block),
                    from(session + 1, 1, put("k", "c", false)),
                    Sessioned {
                        session,
                        serial: 5,
                        unanswered: 5,
                        command: put("j", "x", false),
                    },
                    from(session, 1, put("k", "d", false)),
                ],
            ),
        );
        assert_eq!(answers[0], Answer::WrongLayer);
        assert_eq!(answers[1], Answer::SessionExpired);
        assert!(matches!(answers[2], Answer::Name(Outcome::Put { .. })));
        assert_eq!(answers[3], Answer::Repeated);
    }

    /// A command whose writes hold a row past the range's bounds is refused whole, as every
    /// member refuses it, and the entry goes on: the command after it applies, the refused one
    /// changed nothing, and asked again it is answered so from its session.
    #[test]
    fn a_command_whose_row_passes_the_bounds_is_refused_whole_and_the_entry_goes_on() {
        let mut m = name_range();
        let rules = Rules {
            max_key_bytes: 32,
            ..RULES
        };
        let run = |m: &mut Model, index, e: Entry| {
            apply_entry(m, index, &e, Layer::Name, &rules).unwrap()
        };
        let answers = run(&mut m, 1, entry(10, vec![registration()]));
        let [Answer::Registered { session }] = answers[..] else {
            panic!("{answers:?}")
        };
        run(&mut m, 2, entry(20, vec![from(session, 1, open_gate())]));
        let rows = all(&mut m);
        let long = from(session, 2, put(&"k".repeat(40), "e", false));
        let answers = run(
            &mut m,
            3,
            entry(
                30,
                vec![long.clone(), from(session, 3, put("k", "e", false))],
            ),
        );
        assert_eq!(answers[0], Answer::RowTooLarge);
        assert!(matches!(answers[1], Answer::Name(Outcome::Put { .. })));
        let mut after = all(&mut m);
        after.retain(|row| !rows.contains(row));
        assert!(
            after.iter().all(|(k, _)| k.len() <= rules.max_key_bytes),
            "{after:?}"
        );
        assert_eq!(run(&mut m, 4, entry(40, vec![long])), [Answer::RowTooLarge]);
    }

    /// The last serial is refused before it takes effect, so forgetting its answer cannot
    /// let a retry of it take effect twice, as it could when eviction's watermark saturated
    /// there (audit B03).
    #[test]
    fn the_spent_last_serial_never_takes_effect() {
        let mut m = name_range();
        let rules = RULES;
        let run = |m: &mut Model, index, e: Entry| {
            apply_entry(m, index, &e, Layer::Name, &rules).unwrap()
        };
        let answers = run(&mut m, 1, entry(10, vec![registration()]));
        let [Answer::Registered { session }] = answers[..] else {
            panic!("{answers:?}")
        };
        run(&mut m, 2, entry(20, vec![from(session, 1, open_gate())]));
        let last = |etag: &str| Sessioned {
            session,
            serial: u64::MAX,
            unanswered: 1,
            command: put("k", etag, false),
        };
        // The audit's order: the last serial, a reordered one before it, and the last again.
        assert_eq!(
            run(&mut m, 3, entry(30, vec![last("a")])),
            [Answer::SessionExpired]
        );
        let before = Sessioned {
            session,
            serial: u64::MAX - 1,
            unanswered: 1,
            command: put("j", "x", false),
        };
        assert!(matches!(
            run(&mut m, 4, entry(40, vec![before]))[..],
            [Answer::Name(Outcome::Put { .. })]
        ));
        assert_eq!(
            run(&mut m, 5, entry(50, vec![last("a")])),
            [Answer::SessionExpired]
        );
        assert!(crate::name::current(&mut m, "b", "k").unwrap().is_none());
    }

    /// Answers are forgotten only once the gateway has acknowledged them, and a session whose row
    /// would not hold a command's answer refuses the command, applying nothing. A reordered
    /// command still in flight is applied or refused for room, never taken for a repeat; a retry
    /// is answered from what was kept, and the refused one, asked again once the answers before
    /// it are acknowledged, is applied, once. Before, the oldest answer kept was forgotten and
    /// the watermark raised past it, past serials not yet applied; then the session expired, and
    /// a retry whose reply was lost met it expired.
    #[test]
    fn a_reordered_command_is_applied_or_refused_never_taken_for_a_repeat() {
        let one = Answer::Name(Outcome::Put {
            version: crate::key::version_id(!12_345u64),
        })
        .encode()
        .unwrap()
        .len();
        let put = |serial: u64, unanswered: u64| Sessioned {
            session: 0,
            serial,
            unanswered,
            command: put(&format!("k{serial}"), "e", false),
        };
        let applied = |m: &mut Model, serial: u64| {
            crate::name::current(m, "b", &format!("k{serial}"))
                .unwrap()
                .is_some()
        };
        let register = |m: &mut Model, rules: &Rules| {
            let e = entry(10, vec![registration()]);
            let [Answer::Registered { session }] =
                apply_entry(m, 1, &e, Layer::Name, rules).unwrap()[..]
            else {
                panic!()
            };
            let gate = entry(20, vec![from(session, 1, open_gate())]);
            apply_entry(m, 2, &gate, Layer::Name, rules).unwrap();
            session
        };
        // The largest row the range writes of these commands besides a session's, measured with
        // no bound; then the least row of `n` answers holding it, so only answers pass it.
        let data = {
            let mut m = name_range();
            let session = register(&mut m, &RULES);
            let puts = (2..=9)
                .map(|serial| Sessioned {
                    session,
                    ..put(serial, 2)
                })
                .collect();
            apply_entry(&mut m, 3, &entry(30, puts), Layer::Name, &RULES).unwrap();
            let session_row = [crate::key::LOCAL, crate::key::marker::SESSION];
            all(&mut m)
                .iter()
                .filter(|(k, _)| !k.starts_with(&session_row))
                .map(|(_, v)| v.len())
                .max()
                .unwrap()
        };
        // At least two, so the refused serial has room once the one below it is acknowledged:
        // with one, the answer above it would hold the row until it is answered, which it never
        // is, and only a gateway sending its serials out of order meets that.
        let n = (2u64..)
            .find(|&n| Session::row_bytes(n as usize, n as usize * one).unwrap() >= data)
            .unwrap();
        let rules = Rules {
            max_value_bytes: Session::row_bytes(n as usize, n as usize * one).unwrap(),
            ..RULES
        };
        let mut m = name_range();
        let run = |m: &mut Model, index, commands: Vec<Sessioned>| {
            apply_entry(m, index, &entry(10 * index, commands), Layer::Name, &rules).unwrap()
        };
        let session = register(&mut m, &rules);
        let sessioned = |c: Sessioned| Sessioned { session, ..c };
        // Serials 2 to n + 2 in flight, the last first: the session's row holds n of their
        // answers, so the one to reach the log last is refused.
        let order: Vec<u64> = std::iter::once(n + 2).chain(2..=n + 1).collect();
        let mut answers = Vec::new();
        for (index, &serial) in (3u64..).zip(&order) {
            answers.extend(run(&mut m, index, vec![sessioned(put(serial, 2))]));
        }
        for (answer, &serial) in answers.iter().zip(&order) {
            match answer {
                Answer::Name(Outcome::Put { .. }) => assert!(applied(&mut m, serial)),
                Answer::AnswersFull => assert!(!applied(&mut m, serial)),
                other => panic!("serial {serial} answered {other:?}"),
            }
        }
        assert_eq!(answers.last(), Some(&Answer::AnswersFull), "{answers:?}");
        // A retry of them all, as after a lost reply, is answered as they were, nothing twice.
        let all_again = |unanswered| {
            order
                .iter()
                .map(|&s| sessioned(put(s, unanswered)))
                .collect()
        };
        assert_eq!(run(&mut m, 100, all_again(2)), answers);
        // The gateway has every answer before n + 1: the refused one is applied, once, and the
        // others are repeats or answered as they were.
        let again = run(&mut m, 101, all_again(n + 1));
        assert!(applied(&mut m, n + 1));
        for (answer, (&serial, first)) in again.iter().zip(order.iter().zip(&answers)) {
            match serial {
                s if s < n + 1 => assert_eq!(*answer, Answer::Repeated),
                s if s == n + 1 => assert!(matches!(answer, Answer::Name(Outcome::Put { .. }))),
                _ => assert_eq!(answer, first),
            }
        }
        assert_eq!(run(&mut m, 102, all_again(n + 1)), again);
        // Acknowledged, answers are forgotten wherever they lie, and a retry is a repeat.
        let rules = RULES;
        let mut m = name_range();
        let run = |m: &mut Model, index, commands: Vec<Sessioned>| {
            apply_entry(m, index, &entry(10 * index, commands), Layer::Name, &rules).unwrap()
        };
        let [Answer::Registered { session }] = run(&mut m, 1, vec![registration()])[..] else {
            panic!()
        };
        let sessioned = |c: Sessioned| Sessioned { session, ..c };
        run(&mut m, 2, vec![from(session, 1, open_gate())]);
        let four = run(&mut m, 3, vec![sessioned(put(4, 2))]);
        let two = run(&mut m, 4, vec![sessioned(put(2, 2))]);
        assert_eq!(run(&mut m, 5, vec![sessioned(put(2, 2))]), two);
        let three = run(&mut m, 6, vec![sessioned(put(3, 3))]);
        assert_eq!(
            run(&mut m, 7, vec![sessioned(put(2, 4)), sessioned(put(4, 4))]),
            [Answer::Repeated, four[0].clone()]
        );
        assert!(matches!(three[..], [Answer::Name(Outcome::Put { .. })]));
    }

    #[test]
    fn a_session_unused_past_its_lifetime_expires_at_the_entry_that_passes_it() {
        let mut m = name_range();
        let e = entry(10, vec![registration()]);
        let [Answer::Registered { session }] =
            apply_entry(&mut m, 1, &e, Layer::Name, &RULES).unwrap()[..]
        else {
            panic!()
        };
        let late = entry(1_011, vec![from(session, 1, open_gate())]);
        assert_eq!(
            apply_entry(&mut m, 2, &late, Layer::Name, &RULES).unwrap(),
            [Answer::SessionExpired]
        );
    }

    /// Every row of a model engine, in key order.
    fn all(m: &mut Model) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows = Vec::new();
        let mut at = Vec::new();
        while let Some((k, v)) = m.next(&at, &[0xFF]).unwrap() {
            at = k.clone();
            at.push(0);
            rows.push((k, v));
        }
        rows
    }

    /// How entries applied before sessions were held for the entry: each command read,
    /// decoded, searched, changed, encoded and wrote its session back.
    fn apply_per_command(
        rows: &mut Model,
        index: u64,
        entry: &Entry,
        layer: Layer,
        rules: &Rules,
    ) -> Vec<Answer> {
        let key = |session: u64| {
            let mut k = vec![crate::key::LOCAL, crate::key::marker::SESSION];
            k.extend_from_slice(&session.to_be_bytes());
            k
        };
        let expiry = |last_ns: u64, session: u64| {
            let mut k = vec![crate::key::LOCAL, crate::key::marker::EXPIRY];
            k.extend_from_slice(&last_ns.to_be_bytes());
            k.extend_from_slice(&session.to_be_bytes());
            k
        };
        let read = |rows: &mut Overlay<'_, Model>, session: u64| {
            rows.get(&key(session))
                .unwrap()
                .map(|b| Session::decode(&b).unwrap())
        };
        let (answers, writes) = {
            let bound = (rules.max_key_bytes, rules.max_value_bytes);
            let mut overlay = Overlay::new(&mut *rows, bound);
            session::expire(&mut overlay, index, entry.at_ns, rules).unwrap();
            let mut answers = Vec::new();
            let mut refused = std::collections::BTreeSet::new();
            for (position, c) in entry.commands.iter().enumerate() {
                let answer = match &c.command {
                    Command::Register { owner, run, nth } => {
                        match session::register(
                            &mut overlay,
                            index,
                            (position, entry.at_ns),
                            rules,
                            (*owner, *run, *nth),
                        )
                        .unwrap()
                        {
                            session::Registration::Session(session) => {
                                Answer::Registered { session }
                            }
                            session::Registration::Full { until_ns } => {
                                Answer::SessionsFull { until_ns }
                            }
                            session::Registration::Superseded => Answer::Superseded,
                        }
                    }
                    command => {
                        let check = match read(&mut overlay, c.session) {
                            None => Check::Unknown,
                            Some(_) if c.serial == u64::MAX => Check::Spent,
                            Some(s) if c.serial < s.low => Check::Repeated,
                            Some(s) => match s.answers.iter().find(|(k, _)| *k == c.serial) {
                                Some((_, a)) => Check::Answered(Answer::decode(a).unwrap()),
                                None => Check::New,
                            },
                        };
                        // The answer, and what the session keeps of it.
                        let (answer, keep) = match check {
                            Check::Unknown | Check::Spent => {
                                answers.push(Answer::SessionExpired);
                                continue;
                            }
                            Check::Repeated => {
                                answers.push(Answer::Repeated);
                                continue;
                            }
                            Check::Answered(answer) => (answer, None),
                            Check::New if refused.contains(&c.session) => {
                                (Answer::AnswersFull, None)
                            }
                            Check::New => {
                                let mut own = Overlay::new(&mut overlay, bound);
                                let answer = match run(&mut own, index, command, layer) {
                                    Err(MetaError::Engine(EngineError::RowTooLarge { .. })) => {
                                        Answer::RowTooLarge
                                    }
                                    ran => ran.unwrap(),
                                };
                                let writes = own.into_writes();
                                let mut s = read(&mut overlay, c.session).unwrap();
                                s.low = s.low.max(c.unanswered);
                                s.answers.retain(|(kept, _)| *kept >= s.low);
                                s.answers.push((c.serial, answer.encode().unwrap()));
                                if s.encode().unwrap().len() <= rules.max_value_bytes {
                                    overlay.apply(index, writes).unwrap();
                                    let encoded = answer.encode().unwrap();
                                    (answer, Some(encoded))
                                } else {
                                    refused.insert(c.session);
                                    (Answer::AnswersFull, None)
                                }
                            }
                        };
                        let mut s = read(&mut overlay, c.session).unwrap();
                        let mut writes = vec![Write::Delete(expiry(s.last_ns, c.session))];
                        s.low = s.low.max(c.unanswered);
                        s.answers.retain(|(kept, _)| *kept >= s.low);
                        if let Some(encoded) = keep {
                            s.answers.push((c.serial, encoded));
                        }
                        s.last_ns = entry.at_ns;
                        writes.push(Write::Put(key(c.session), s.encode().unwrap()));
                        writes.push(Write::Put(expiry(entry.at_ns, c.session), Vec::new()));
                        overlay.apply(index, writes).unwrap();
                        answer
                    }
                };
                answers.push(answer);
            }
            (answers, overlay.into_writes())
        };
        rows.apply(index, writes).unwrap();
        answers
    }

    /// Holding an entry's sessions changes no answer and no row: random entries of up to 64
    /// commands, from sessions registered, forgotten and expired along the way, with serials
    /// repeated, reordered and spent and answers refused for room, answer the same and leave the
    /// same rows as applying each command's session change on its own did (audit P04).
    #[test]
    fn holding_an_entrys_sessions_answers_and_writes_as_each_command_did() {
        // A row of three puts' answers, which three and a gate's pass, so commands are refused
        // for room; and a key a put of a long key passes, which is refused whole.
        let one = Answer::Name(Outcome::Put {
            version: crate::key::version_id(!12_345u64),
        })
        .encode()
        .unwrap()
        .len();
        let rules = Rules {
            lifetime_ns: 400,
            max_sessions: 4,
            expiries_per_entry: 2,
            max_key_bytes: 32,
            max_value_bytes: Session::row_bytes(3, 3 * one).unwrap(),
        };
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let (mut held, mut each) = (name_range(), name_range());
        // Each session registered, with the highest serial its gateway has sent.
        let mut sessions: Vec<(u64, u64)> = Vec::new();
        // Answers seen: repeated, refused for an unknown session, run, refused for a row, and
        // refused for room.
        let mut seen = [0u64; 5];
        let mut registered = 0u64;
        let mut at_ns = 0u64;
        // Each of a few owners' last registration: so some registrations end an owner's last
        // session, and now and then one arrives again or late.
        let mut last = [0u64; 8];
        for index in 1..=400u64 {
            at_ns += next(20);
            let commands = (0..1 + next(64))
                .map(|_| {
                    if sessions.is_empty() || next(64) == 0 {
                        let owner = next(8);
                        let nth = &mut last[owner as usize];
                        let nth = match next(8) {
                            0 => nth.saturating_sub(1),
                            1 => *nth,
                            _ => {
                                *nth += 1;
                                *nth
                            }
                        };
                        return from(0, 0, Command::Register { owner, run: 1, nth });
                    }
                    let pick = next(sessions.len() as u64) as usize;
                    let (session, sent) = &mut sessions[pick];
                    // Mostly the next serial, some sent before it again or out of order, and
                    // now and then the spent last one.
                    let serial = match next(16) {
                        0 => u64::MAX,
                        1..=4 => sent.saturating_sub(next(4)),
                        5 | 6 => *sent + 1 + next(2),
                        _ => *sent + 1,
                    };
                    if serial != u64::MAX {
                        *sent = (*sent).max(serial);
                    }
                    let command = match next(4) {
                        0 => open_gate(),
                        // Now and then a key whose rows pass the bound on a key.
                        _ if next(16) == 0 => put(&"k".repeat(40), "e", next(2) == 0),
                        _ => put(
                            &format!("k{}", next(3)),
                            &format!("e{}", next(4)),
                            next(2) == 0,
                        ),
                    };
                    Sessioned {
                        session: *session,
                        serial,
                        unanswered: sent.saturating_sub(next(5)),
                        command,
                    }
                })
                .collect();
            let e = entry(at_ns, commands);
            let a = apply_entry(&mut held, index, &e, Layer::Name, &rules).unwrap();
            let b = apply_per_command(&mut each, index, &e, Layer::Name, &rules);
            assert_eq!(a, b, "entry {index}");
            assert_eq!(all(&mut held), all(&mut each), "entry {index}");
            for (answer, c) in a.iter().zip(&e.commands) {
                match answer {
                    Answer::Registered { session } => {
                        sessions.push((*session, 0));
                        registered += 1;
                    }
                    Answer::Repeated => seen[0] += 1,
                    // The gateway registers anew, as it would.
                    Answer::SessionExpired => {
                        seen[1] += 1;
                        if c.serial != u64::MAX {
                            sessions.retain(|(s, _)| *s != c.session);
                        }
                    }
                    Answer::RowTooLarge => seen[3] += 1,
                    Answer::AnswersFull => seen[4] += 1,
                    _ => seen[2] += 1,
                }
            }
        }
        // Sessions came and went, and commands were repeated, refused, run, refused whole and
        // refused for room.
        assert!(registered > 10, "{registered} sessions");
        assert!(
            [seen[0], seen[2], seen[4]].iter().all(|&n| n > 500),
            "{seen:?}"
        );
        assert!(seen[1] > 0 && seen[3] > 0, "{seen:?}");
    }

    /// Two replicas applying the same entries hold the same rows, however the entries were
    /// batched into commands.
    #[test]
    fn replicas_applying_the_same_log_agree() {
        let log: Vec<Entry> = (1..=6u64)
            .map(|i| {
                let mut commands = vec![registration()];
                if i > 1 {
                    let session = 1u64 << 16;
                    commands = vec![
                        from(session, 2 * i, open_gate()),
                        from(session, 2 * i + 1, put(&format!("k{}", i % 3), "e", false)),
                    ];
                }
                entry(10 * i, commands)
            })
            .collect();
        let mut a = name_range();
        let mut b = name_range();
        for (i, e) in (1u64..).zip(&log) {
            assert_eq!(
                apply_entry(&mut a, i, e, Layer::Name, &RULES).unwrap(),
                apply_entry(&mut b, i, e, Layer::Name, &RULES).unwrap()
            );
        }
        assert_eq!(all(&mut a), all(&mut b));
    }
}
