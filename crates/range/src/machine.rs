//! A range's engine and layer as the state machine hyper-raft's durable shell applies to
//! (hyper-raft docs/durable.md §9, §11 "mantle (D-1)"): `apply` is the layer's `apply_entry`,
//! `durable` the engine's durable index with its term, and no entry of a range is one a member
//! acts on at its next start.
//!
//! The term of the engine's durable index is kept in the engine, beside its rows, so a restart
//! knows the point its engine stands at without reading the log (docs/design/replica.md §4): a
//! row (`TERM`) holds the term of the last entry applied, written in the batch of the first entry
//! of each term, which is the new leader's empty entry (Ongaro, thesis §3.6.2, a leader's no-op;
//! hyper-raft's `Raft::become_leader` appends it), so an ordinary entry's batch carries nothing
//! more. An install writes the snapshot's term with its rows.

use std::collections::TryReserveError;

use hyper_durable::{EntryRef, Fatal, Point, StateMachine};
use mantle_codec::{Reader, Writer};
use mantle_meta::apply::{Layer, apply_entry_with};
use mantle_meta::engine::{Engine, EngineError, Row, Rows, Write};
use mantle_meta::error::MetaError;
use mantle_meta::key::{LOCAL, marker};
use mantle_meta::record::RecordError;
use mantle_meta::session::{Rules, Watermark};
use mantle_meta::wire::{self, Answer};

use hyper_raft::proto::{ConfChangeV2, ConfState, EntryType};

use crate::{conf, image};

/// The row that holds the term of the last entry applied.
pub const TERM: &[u8] = &[LOCAL, marker::TERM];

/// The bytes of a term's row: the term and its CRC-32C.
const TERM_BYTES: usize = 8 + 4;

/// A term's bytes: the term and its CRC-32C, in a buffer made at their length.
pub(crate) fn encode_term(term: u64) -> Vec<u8> {
    let mut w = Writer::with_capacity(TERM_BYTES);
    w.u64(term);
    let crc = mantle_crc::crc32c(w.as_slice());
    w.u32(crc);
    w.into_vec()
}

fn decode_term(bytes: &[u8]) -> Option<u64> {
    let body_len = bytes.len().checked_sub(4)?;
    let (body, crc) = bytes.split_at(body_len);
    if mantle_crc::crc32c(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let mut r = Reader::new(body);
    let term = r.u64()?;
    if r.remaining() != 0 {
        return None;
    }
    Some(term)
}

/// A command's answer as a normal entry applied it: the entry's index, the command's session and
/// serial, so a gateway finds the answer to its command, and the answer. An entry's answers come
/// in the order it holds its commands, each a record of its own in the owner's buffer, so applying
/// an entry allocates nothing beyond what the layer makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    pub index: u64,
    pub session: u64,
    pub serial: u64,
    pub answer: Answer,
}

/// The error a step of the machine met where it failed, which the [`Fatal`] it answers the shell
/// names only by the step. The shell fences the replica on the first and asks the machine nothing
/// more until the member opens again, so the machine keeps that one alone
/// ([`RangeMachine::failure`]); an engine that was fenced keeps its own error ([`Engine::fault`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Failure {
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Record(#[from] RecordError),
    #[error(transparent)]
    Memory(#[from] TryReserveError),
}

/// The [`Fatal`] of the step `what`, which failed on `e`, kept in `failure` unless one is
/// already, the first being the cause.
fn failed(failure: &mut Option<Failure>, what: &'static str, e: impl Into<Failure>) -> Fatal {
    failure.get_or_insert_with(|| e.into());
    Fatal(what)
}

/// The engine, with the term row added to the batch it applies next: what the first entry of a
/// term carries besides its own writes.
struct Termed<'a, E> {
    engine: &'a mut E,
    row: Option<Write>,
    /// The batch is applied only with room for all of it now ([`Engine::apply_now`]), refused
    /// [`EngineError::NoRoom`] otherwise: an entry's, which waits committed rather than the shard.
    /// A change's is applied whatever it waits on: the core has applied it already.
    now: bool,
}

impl<'a, E> Termed<'a, E> {
    /// `engine`, the term row for an entry of `term` added to its next batch if the term differs
    /// from `last`, the last entry's.
    fn new(engine: &'a mut E, last: u64, term: u64) -> Self {
        let row = (term != last).then(|| Write::Put(TERM.to_vec(), encode_term(term)));
        Self {
            engine,
            row,
            now: false,
        }
    }
}

impl<E: Engine> Termed<'_, E> {
    fn apply_batch(&mut self, index: u64, writes: Vec<Write>) -> Result<(), EngineError> {
        if !self.now {
            return self.engine.apply(index, writes);
        }
        if self.engine.apply_now(index, writes)? {
            Ok(())
        } else {
            Err(EngineError::NoRoom)
        }
    }
}

impl<E: Engine> Rows for Termed<'_, E> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.engine.get(key)
    }

    fn next(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        self.engine.next(from, to)
    }

    fn apply(&mut self, index: u64, mut writes: Vec<Write>) -> Result<(), EngineError> {
        let Some(row) = self.row.take() else {
            return self.apply_batch(index, writes);
        };
        writes
            .try_reserve_exact(1)
            .map_err(|_| EngineError::Memory)?;
        writes.push(row);
        self.apply_batch(index, writes)
    }
}

/// A range's engine, its layer and the bounds of its sessions, and what the shell asks of them.
pub struct RangeMachine<E> {
    engine: E,
    layer: Layer,
    rules: Rules,
    /// What the range knows of its sessions' last use, which spares an entry no session's
    /// lifetime has passed the scan of the order of last use (`mantle_meta::session::Watermark`).
    /// Held in memory only, as it derives from the rows: nothing is known at open or after an
    /// install, whose rows may be of any time, until the next entry's scan.
    watermark: Watermark,
    /// The configuration as of the last entry applied.
    configuration: ConfState,
    /// The last entry applied.
    applied: Point,
    /// The last entry applied when the engine's checkpoint out began ([`Engine::persist_now`]):
    /// durable once it completes.
    persisting: Option<Point>,
    /// The last entry the engine holds durably, as far as this machine knows: where it opened,
    /// installed or last persisted. An engine that persists more on its own schedule is behind
    /// this only until the next `persist`; the shell is then told less than the engine holds,
    /// which keeps every rule that reads it (the fence, compaction) on the safe side.
    durable: Point,
    /// The error of the step that failed the machine, the first ([`Failure`]).
    failure: Option<Failure>,
}

impl<E: Engine> RangeMachine<E> {
    /// The machine of `engine`, which a restart hands over as it stands: what it applied is made
    /// durable first, so the point it opens at is one a crash keeps. Its configuration is the one
    /// it keeps, or `boot` for an engine that has applied no change.
    pub fn open(
        mut engine: E,
        layer: Layer,
        rules: Rules,
        boot: &ConfState,
    ) -> Result<Self, crate::ReplicaError> {
        if engine.durable() < engine.applied() {
            engine.persist()?;
        }
        let configuration = match engine.get(conf::ROW)? {
            Some(bytes) => conf::decode(&bytes).ok_or_else(|| {
                crate::ReplicaError::Stopped("the configuration does not decode".into())
            })?,
            None => boot.clone(),
        };
        let term = match engine.get(TERM)? {
            Some(bytes) => decode_term(&bytes)
                .ok_or_else(|| crate::ReplicaError::Stopped("the term does not decode".into()))?,
            None => 0,
        };
        let point = Point {
            index: engine.applied(),
            term,
        };
        Ok(Self {
            engine,
            layer,
            rules,
            watermark: Watermark::default(),
            configuration,
            applied: point,
            persisting: None,
            durable: point,
            failure: None,
        })
    }

    pub fn engine(&self) -> &E {
        &self.engine
    }

    /// The error of the step that failed the machine, the first, if one has: what its replica
    /// was fenced on.
    pub fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }

    /// The layer whose rows the range holds.
    pub fn layer(&self) -> Layer {
        self.layer
    }

    /// The engine for a read, which takes it whole: an engine's read moves its cache.
    pub fn engine_mut(&mut self) -> &mut E {
        &mut self.engine
    }

    pub fn into_engine(self) -> E {
        self.engine
    }
}

impl<E: Engine> StateMachine for RangeMachine<E> {
    type Answer = Applied;

    /// The entry's batch is applied only with room in the engine for all of it now
    /// ([`Engine::apply_now`]): with none, nothing of it is applied or answered and the shell
    /// gives it again at the drive the engine's worker wakes the owner for, the layer's work then
    /// done again over the same rows, the same batch.
    fn apply(&mut self, entry: &EntryRef<'_>, answers: &mut Vec<Applied>) -> Result<bool, Fatal> {
        let (layer, rules) = (self.layer, self.rules);
        // Lent to the entry, as the engine is, and kept only once the entry applies.
        let mut watermark = self.watermark;
        let failure = &mut self.failure;
        let mut engine = Termed::new(&mut self.engine, self.applied.term, entry.term);
        engine.now = true;
        if entry.kind != EntryType::EntryNormal {
            return Err(Fatal(
                "a change of configuration given to apply as an entry",
            ));
        }
        // A new leader's empty entry changes no row of the range.
        if entry.data.is_empty() {
            match engine.apply(entry.index, Vec::new()) {
                Ok(()) => {}
                Err(EngineError::NoRoom) => return Ok(false),
                Err(e) => return Err(failed(failure, "the engine refused an entry", e)),
            }
        } else {
            let batch = wire::Entry::decode(entry.data)
                .map_err(|e| failed(failure, "a committed entry does not decode", e))?;
            let applied = apply_entry_with(
                &mut engine,
                entry.index,
                &batch,
                layer,
                &rules,
                &mut watermark,
            );
            let made = match applied {
                Ok(made) => made,
                Err(MetaError::Engine(EngineError::NoRoom)) => return Ok(false),
                Err(e) => {
                    return Err(failed(
                        failure,
                        "a committed entry breaks the range's state machine",
                        e,
                    ));
                }
            };
            answers
                .try_reserve(made.len())
                .map_err(|e| failed(failure, "no memory for an entry's answers", e))?;
            answers.extend(batch.commands.iter().zip(made).map(|(c, answer)| Applied {
                index: entry.index,
                session: c.session,
                serial: c.serial,
                answer,
            }));
        }
        self.watermark = watermark;
        self.applied = Point {
            index: entry.index,
            term: entry.term,
        };
        Ok(true)
    }

    /// The configuration is kept in the engine, in the batch of the entry that made it, so a
    /// restart tells the core the configuration of the state it opens at (docs/design/replica.md
    /// §4). A change the core refused leaves the configuration as it was, written again.
    fn apply_change(
        &mut self,
        at: Point,
        _change: &ConfChangeV2,
        configuration: &ConfState,
    ) -> Result<(), Fatal> {
        let bytes = conf::encode(configuration).ok_or(Fatal("a configuration past u32 members"))?;
        Termed::new(&mut self.engine, self.applied.term, at.term)
            .apply(at.index, vec![Write::Put(conf::ROW.to_vec(), bytes)])
            .map_err(|e| failed(&mut self.failure, "the engine refused a change", e))?;
        self.configuration = configuration.clone();
        self.applied = at;
        Ok(())
    }

    fn durable(&self) -> Point {
        self.durable
    }

    fn configuration(&self) -> &ConfState {
        &self.configuration
    }

    /// Mantle's layers have no entry a member acts on at its next start before its group tells
    /// it anything: a restart takes its engine as it stood and learns the rest from the group.
    fn acts_at_start(&self, _entry: &EntryRef<'_>) -> bool {
        false
    }

    /// Every row as of the last entry applied, under the configuration held there: the image is
    /// made when the core asks for a snapshot for a member behind the log's start, never for a
    /// compaction, so it is of everything applied then (docs/design/replica.md §4). It is made at
    /// its length and moved into `into`, not copied.
    fn image(&mut self, into: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal> {
        let rows = self
            .engine
            .image()
            .map_err(|e| failed(&mut self.failure, "the engine gave no image", e))?;
        *into = image::encode(&rows).ok_or(Fatal("a row past u32 bytes"))?;
        Ok((self.applied, self.configuration.clone()))
    }

    /// The bytes `image` would write now. Asked when a member opens and at each compaction
    /// (`hyper_durable::Compaction`), so only from what the engine counts without reading its
    /// rows; an engine that cannot, or rows past what the image counts, gives none, and the log is
    /// not weighed against an image until the member installs one. Mantle compacts on its log's
    /// room, not by that rule (node.md §6.2).
    fn image_bytes(&self) -> Option<u64> {
        let (rows, bytes) = self.engine.image_size()?;
        image::encoded_len_of(rows, bytes)
    }

    /// The engine takes the image's rows, with the configuration and the term of its point, and
    /// makes them durable before the log's start moves to it (I8).
    fn install(&mut self, image: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal> {
        // The image's sessions may have been used at any time, and an install that fails part
        // way leaves rows of both.
        self.watermark = Watermark::default();
        let mut rows = image::decode(image).ok_or(Fatal("a snapshot's rows do not decode"))?;
        let conf = conf::encode(configuration).ok_or(Fatal("a configuration past u32 members"))?;
        rows.retain(|(k, _)| k.as_slice() != conf::ROW && k.as_slice() != TERM);
        rows.push((conf::ROW.to_vec(), conf));
        rows.push((TERM.to_vec(), encode_term(at.term)));
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.engine
            .install(at.index, rows)
            .map_err(|e| failed(&mut self.failure, "the engine refused a snapshot", e))?;
        self.engine.persist().map_err(|e| {
            failed(
                &mut self.failure,
                "the engine failed to persist a snapshot",
                e,
            )
        })?;
        self.configuration = configuration.clone();
        self.applied = at;
        self.durable = at;
        // The engine's persist finished any checkpoint out, which the install passed.
        self.persisting = None;
        Ok(())
    }

    /// The engine's checkpoint made without waiting ([`Engine::persist_now`]) on a node's shard,
    /// which serves its consensus and transport too: begun at the entry applied now, and asked
    /// again at each of the replica's drives until it completes; that entry is durable then, and
    /// the log's start may move to it, never past (docs/design/replica.md §4, I8).
    fn persist(&mut self) -> Result<bool, Fatal> {
        let at = *self.persisting.get_or_insert(self.applied);
        let done = self
            .engine
            .persist_now()
            .map_err(|e| failed(&mut self.failure, "the engine failed to persist", e))?;
        if done {
            self.persisting = None;
            self.durable = at;
        }
        Ok(done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_term_round_trips_and_damage_is_refused() {
        let bytes = encode_term(7);
        assert_eq!(decode_term(&bytes), Some(7));
        let mut bad = bytes.clone();
        bad[0] ^= 1;
        assert_eq!(decode_term(&bad), None);
        assert_eq!(decode_term(&bytes[..bytes.len() - 1]), None);
    }

    /// `encode_term` as it was before Lean doors step 7, in a writer that grows.
    fn reference_term(term: u64) -> Vec<u8> {
        let mut w = Writer::default();
        w.u64(term);
        let crc = mantle_crc::crc32c(w.as_slice());
        w.u32(crc);
        w.into_vec()
    }

    /// A term's row is one buffer made at its length, the bytes the encoder before step 7 wrote,
    /// for the least term, one and the most.
    #[test]
    fn a_term_is_written_at_its_length() {
        for term in [0, 1, u64::MAX] {
            let (expected, before) = crate::counted(|| reference_term(term));
            let (bytes, made) = crate::counted(|| encode_term(term));
            println!(
                "term {term}: before {before:?}, now {made:?}, {} bytes",
                bytes.len()
            );
            assert_eq!((made, bytes.capacity()), ((1, 0), bytes.len()), "{term}");
            assert_eq!(bytes, expected, "{term}");
            assert_eq!(decode_term(&bytes), Some(term), "{term}");
        }
    }

    /// A model engine that counts the reads of the order of last use it is asked, and, while
    /// `roomless`, has no room for a batch now, as an engine whose rotation waits on its worker.
    struct Counted {
        rows: mantle_meta::engine::Model,
        scans: u64,
        roomless: bool,
    }

    impl Rows for Counted {
        fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
            self.rows.get(key)
        }

        fn next(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
            if from.starts_with(&[LOCAL, marker::EXPIRY]) {
                self.scans += 1;
            }
            self.rows.next(from, to)
        }

        fn apply(&mut self, index: u64, writes: Vec<Write>) -> Result<(), EngineError> {
            self.rows.apply(index, writes)
        }
    }

    impl Engine for Counted {
        fn applied(&self) -> u64 {
            self.rows.applied()
        }

        fn durable(&self) -> u64 {
            self.rows.durable()
        }

        fn persist(&mut self) -> Result<(), EngineError> {
            self.rows.persist()
        }

        fn image(&mut self) -> Result<Vec<Row>, EngineError> {
            self.rows.image()
        }

        fn image_size(&self) -> Option<(u64, u64)> {
            self.rows.image_size()
        }

        fn install(&mut self, index: u64, rows: Vec<Row>) -> Result<(), EngineError> {
            self.rows.install(index, rows)
        }

        fn apply_now(&mut self, index: u64, writes: Vec<Write>) -> Result<bool, EngineError> {
            if self.roomless {
                return Ok(false);
            }
            self.rows.apply(index, writes).map(|()| true)
        }
    }

    const RULES: Rules = Rules {
        lifetime_ns: 1_000,
        max_sessions: 8,
        expiries_per_entry: 1,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
    };

    /// A cell's first Name range, as a replica opens it.
    fn opened() -> RangeMachine<Counted> {
        let mut rows = mantle_meta::engine::Model::default();
        rows.install(0, mantle_meta::name::first(1).unwrap())
            .unwrap();
        let engine = Counted {
            rows,
            scans: 0,
            roomless: false,
        };
        RangeMachine::open(engine, Layer::Name, RULES, &ConfState::default()).unwrap()
    }

    /// A command of `session` under `serial`: opening a bucket's gate, which reads no order.
    fn command(session: u64, serial: u64) -> wire::Sessioned {
        let gate = mantle_meta::name::GateChange {
            bucket: "b".into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(mantle_meta::record::GateState::Open),
            generation: 1,
        };
        wire::Sessioned {
            session,
            serial,
            unanswered: 1,
            command: wire::Command::Name(Box::new(mantle_meta::name::Command::Gate(gate))),
        }
    }

    /// Applies the entry of `commands` at `at_ns` as entry `index`: its answers, and the reads of
    /// the order of last use it made.
    fn apply_at(
        machine: &mut RangeMachine<Counted>,
        index: u64,
        at_ns: u64,
        commands: Vec<wire::Sessioned>,
    ) -> (Vec<Answer>, u64) {
        let data = wire::Entry { at_ns, commands }.encode().unwrap();
        let entry = EntryRef {
            index,
            term: 1,
            kind: EntryType::EntryNormal,
            context: &[],
            data: &data,
        };
        let scans = machine.engine().scans;
        let mut answers = Vec::new();
        machine.apply(&entry, &mut answers).unwrap();
        let answers = answers.into_iter().map(|a| a.answer).collect();
        (answers, machine.engine().scans - scans)
    }

    /// An entry the engine has no room for now is answered unapplied: no row, no answer, the
    /// machine's applied point and its knowledge of its sessions' last use as they were. Given
    /// again once the engine has room, it applies as it would have, answered once.
    #[test]
    fn an_entry_with_no_room_in_the_engine_applies_nothing_until_there_is_room() {
        let mut machine = opened();
        let registration = |nth| wire::Sessioned {
            session: 0,
            serial: 0,
            unanswered: 0,
            command: wire::Command::Register {
                owner: 1,
                run: 1,
                nth,
            },
        };
        let data = wire::Entry {
            at_ns: 10,
            commands: vec![registration(1)],
        }
        .encode()
        .unwrap();
        let entry = EntryRef {
            index: 1,
            term: 1,
            kind: EntryType::EntryNormal,
            context: &[],
            data: &data,
        };
        let rows = machine.engine_mut().image().unwrap();
        let watermark = machine.watermark;
        machine.engine_mut().roomless = true;
        let mut answers = Vec::new();
        assert!(!machine.apply(&entry, &mut answers).unwrap());
        assert!(answers.is_empty());
        assert_eq!(machine.applied, Point { index: 0, term: 0 });
        assert_eq!(machine.engine().applied(), 0);
        assert_eq!(machine.engine_mut().image().unwrap(), rows);
        assert_eq!(machine.watermark, watermark);
        assert!(machine.failure().is_none());
        machine.engine_mut().roomless = false;
        assert!(machine.apply(&entry, &mut answers).unwrap());
        assert_eq!(machine.applied, Point { index: 1, term: 1 });
        assert_eq!(answers.len(), 1);
        assert!(
            matches!(answers[0].answer, Answer::Registered { .. }),
            "{answers:?}"
        );
        assert_ne!(machine.engine_mut().image().unwrap(), rows);
    }

    /// A range reopened, or given a snapshot, knows nothing of its sessions' last use: its first
    /// entry reads the order of last use, and the entries after it read nothing until a session's
    /// lifetime can have passed. A snapshot's sessions were used at its own entries' times, which
    /// a range that kept its watermark over its own rows could not know: given one of a session
    /// that a lifetime later is due, it expires the session at the entry its source does.
    #[test]
    fn a_range_reopened_or_given_a_snapshot_scans_once_then_skips() {
        let registration = wire::Sessioned {
            session: 0,
            serial: 0,
            unanswered: 0,
            command: wire::Command::Register {
                owner: 1,
                run: 1,
                nth: 1,
            },
        };
        let mut source = opened();
        let (answers, scans) = apply_at(&mut source, 1, 10, vec![registration]);
        let [Answer::Registered { session }] = answers[..] else {
            panic!("{answers:?}")
        };
        assert_eq!(scans, 1);
        assert_eq!(apply_at(&mut source, 2, 20, vec![command(session, 1)]).1, 0);
        // Reopened, as a restart opens its engine as it stands.
        let mut source = RangeMachine::open(
            source.into_engine(),
            Layer::Name,
            RULES,
            &ConfState::default(),
        )
        .unwrap();
        // A command of a session the range does not hold changes no session.
        let stranger = || vec![command(session + 1, 1)];
        assert_eq!(apply_at(&mut source, 3, 30, stranger()).1, 1);
        assert_eq!(apply_at(&mut source, 4, 40, stranger()).1, 0);
        // A range holding no session knows none can be due, and reads nothing, until the
        // snapshot of the source's rows gives it the session, last used at 20.
        let mut given = opened();
        assert_eq!(apply_at(&mut given, 1, 400, stranger()).1, 1);
        assert_eq!(apply_at(&mut given, 2, 450, stranger()).1, 0);
        let mut image = Vec::new();
        let (at, configuration) = source.image(&mut image).unwrap();
        given.install(&image, at, &configuration).unwrap();
        let later = [
            (500, stranger()),
            (600, stranger()),
            (1_021, vec![command(session, 2)]),
        ];
        let mut read = Vec::new();
        for ((at_ns, commands), index) in later.into_iter().zip(5u64..) {
            let (answers, scans) = apply_at(&mut given, index, at_ns, commands.clone());
            assert_eq!(answers, apply_at(&mut source, index, at_ns, commands).0);
            read.push((answers, scans));
        }
        let expired = vec![Answer::SessionExpired];
        assert_eq!(
            read,
            [(expired.clone(), 1), (expired.clone(), 0), (expired, 1)]
        );
        // The same rows but the configuration's, which only an install writes here.
        let rows = |m: &mut RangeMachine<Counted>| {
            let mut rows = m.engine_mut().image().unwrap();
            rows.retain(|(k, _)| k.as_slice() != conf::ROW);
            rows
        };
        assert_eq!(rows(&mut given), rows(&mut source));
    }
}
