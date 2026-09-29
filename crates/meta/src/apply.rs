//! Applying a range's entry (docs/design/replica.md §2): every command of the entry in order,
//! each checked against its session and run by the range's layer, through an overlay, so
//! that the engine takes the whole entry as one batch at its index.

use crate::engine::Rows;
use crate::error::MetaError;
use crate::overlay::Overlay;
use crate::session::{self, Check, Rules};
use crate::wire::{Answer, Command, Entry};
use crate::{block, bucket, file, name};

/// Which layer's rows a range holds (docs/design/metadata.md §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Bucket,
    Name,
    File,
    Block,
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
        let mut overlay = Overlay::new(&*rows);
        session::expire(&mut overlay, index, entry.at_ns, rules)?;
        let mut answers = Vec::with_capacity(entry.commands.len());
        for (position, c) in entry.commands.iter().enumerate() {
            let answer = match &c.command {
                Command::Register => Answer::Registered {
                    session: session::register(&mut overlay, index, position, entry.at_ns, rules)?,
                },
                command => match session::check(&overlay, c.session, c.serial)? {
                    Check::Unknown => Answer::SessionExpired,
                    Check::Repeated => Answer::Repeated,
                    Check::Answered(answer) => {
                        session::record(&mut overlay, index, c, None, entry.at_ns, rules)?;
                        answer
                    }
                    Check::New => {
                        let answer = run(&mut overlay, index, command, layer)?;
                        session::record(&mut overlay, index, c, Some(&answer), entry.at_ns, rules)?;
                        answer
                    }
                },
            };
            answers.push(answer);
        }
        (answers, overlay.into_writes())
    };
    rows.apply(index, &writes)?;
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
        _ => Answer::WrongLayer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, Model};
    use crate::name::{GateChange, Match, Outcome, Preconditions, Put};
    use crate::record::{GateState, Version, Versioning};
    use crate::wire::Sessioned;

    const RULES: Rules = Rules {
        lifetime_ns: 1_000,
        max_sessions: 8,
        max_answers: 4,
        expiries_per_entry: 8,
    };

    fn put(key: &str, etag: &str, none_match: bool) -> Command {
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
                file: Some(1),
                owner: "o".into(),
                headers: Vec::new(),
            },
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
        })))
    }

    #[test]
    fn commands_apply_once_in_order_and_see_each_other() {
        let mut m = Model::default();
        let run = |m: &mut Model, index, e: Entry| {
            apply_entry(m, index, &e, Layer::Name, &RULES).unwrap()
        };
        let answers = run(&mut m, 1, entry(10, vec![from(0, 0, Command::Register)]));
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
        let current = crate::name::current(&m, "b", "k").unwrap().unwrap();
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

    #[test]
    fn a_session_unused_past_its_lifetime_expires_at_the_entry_that_passes_it() {
        let mut m = Model::default();
        let e = entry(10, vec![from(0, 0, Command::Register)]);
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

    /// Two replicas applying the same entries hold the same rows, however the entries were
    /// batched into commands.
    #[test]
    fn replicas_applying_the_same_log_agree() {
        let log: Vec<Entry> = (1..=6u64)
            .map(|i| {
                let mut commands = vec![from(0, 0, Command::Register)];
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
        let mut a = Model::default();
        let mut b = Model::default();
        for (i, e) in (1u64..).zip(&log) {
            assert_eq!(
                apply_entry(&mut a, i, e, Layer::Name, &RULES).unwrap(),
                apply_entry(&mut b, i, e, Layer::Name, &RULES).unwrap()
            );
        }
        let all = |m: &Model| {
            let mut rows = Vec::new();
            let mut at = Vec::new();
            while let Some((k, v)) = m.next(&at, &[0xFF]).unwrap() {
                at = k.clone();
                at.push(0);
                rows.push((k, v));
            }
            rows
        };
        assert_eq!(all(&a), all(&b));
    }
}
