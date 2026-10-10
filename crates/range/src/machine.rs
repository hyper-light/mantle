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

use hyper_durable::{EntryRef, Fatal, Point, StateMachine};
use mantle_codec::{Reader, Writer};
use mantle_meta::apply::{Layer, apply_entry};
use mantle_meta::engine::{Engine, EngineError, Row, Rows, Write};
use mantle_meta::key::{LOCAL, marker};
use mantle_meta::session::Rules;
use mantle_meta::wire::{self, Answer};

use hyper_raft::proto::{ConfChangeV2, ConfState, EntryType};

use crate::{conf, image};

/// The row that holds the term of the last entry applied.
pub const TERM: &[u8] = &[LOCAL, marker::TERM];

/// A term's bytes: the term and its CRC-32C.
fn encode_term(term: u64) -> Vec<u8> {
    let mut w = Writer::default();
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

/// The engine, with the term row added to the batch it applies next: what the first entry of a
/// term carries besides its own writes.
struct Termed<'a, E> {
    engine: &'a mut E,
    row: Option<Write>,
}

impl<E: Rows> Rows for Termed<'_, E> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.engine.get(key)
    }

    fn next(&self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        self.engine.next(from, to)
    }

    fn apply(&mut self, index: u64, writes: &[Write]) -> Result<(), EngineError> {
        let Some(row) = self.row.take() else {
            return self.engine.apply(index, writes);
        };
        let mut all = Vec::new();
        all.try_reserve_exact(writes.len().saturating_add(1))
            .map_err(|_| EngineError::Memory)?;
        all.extend_from_slice(writes);
        all.push(row);
        self.engine.apply(index, &all)
    }
}

/// A range's engine, its layer and the bounds of its sessions, and what the shell asks of them.
pub struct RangeMachine<E> {
    engine: E,
    layer: Layer,
    rules: Rules,
    /// The configuration as of the last entry applied.
    configuration: ConfState,
    /// The last entry applied.
    applied: Point,
    /// The last entry the engine holds durably, as far as this machine knows: where it opened,
    /// installed or last persisted. An engine that persists more on its own schedule is behind
    /// this only until the next `persist`; the shell is then told less than the engine holds,
    /// which keeps every rule that reads it (the fence, compaction) on the safe side.
    durable: Point,
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
            configuration,
            applied: point,
            durable: point,
        })
    }

    pub fn engine(&self) -> &E {
        &self.engine
    }

    pub fn into_engine(self) -> E {
        self.engine
    }

    /// The engine with the term row for an entry of `term` added to its next batch, if the term
    /// differs from the last entry's.
    fn termed(&mut self, term: u64) -> Termed<'_, E> {
        let row = (term != self.applied.term).then(|| Write::Put(TERM.to_vec(), encode_term(term)));
        Termed {
            engine: &mut self.engine,
            row,
        }
    }
}

impl<E: Engine> StateMachine for RangeMachine<E> {
    type Answer = Applied;

    fn apply(&mut self, entry: &EntryRef<'_>, answers: &mut Vec<Applied>) -> Result<(), Fatal> {
        let (layer, rules) = (self.layer, self.rules);
        let mut engine = self.termed(entry.term);
        if entry.kind != EntryType::EntryNormal {
            return Err(Fatal(
                "a change of configuration given to apply as an entry",
            ));
        }
        // A new leader's empty entry changes no row of the range.
        if entry.data.is_empty() {
            engine
                .apply(entry.index, &[])
                .map_err(|_| Fatal("the engine refused an entry"))?;
        } else {
            let batch = wire::Entry::decode(entry.data)
                .map_err(|_| Fatal("a committed entry does not decode"))?;
            let made = apply_entry(&mut engine, entry.index, &batch, layer, &rules)
                .map_err(|_| Fatal("a committed entry breaks the range's state machine"))?;
            answers
                .try_reserve(made.len())
                .map_err(|_| Fatal("no memory for an entry's answers"))?;
            answers.extend(batch.commands.iter().zip(made).map(|(c, answer)| Applied {
                index: entry.index,
                session: c.session,
                serial: c.serial,
                answer,
            }));
        }
        self.applied = Point {
            index: entry.index,
            term: entry.term,
        };
        Ok(())
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
        let mut engine = self.termed(at.term);
        engine
            .apply(at.index, &[Write::Put(conf::ROW.to_vec(), bytes)])
            .map_err(|_| Fatal("the engine refused a change"))?;
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
    /// made on demand, so it is of everything applied (docs/design/replica.md §4).
    fn image(&mut self, into: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal> {
        let rows = self
            .engine
            .image()
            .map_err(|_| Fatal("the engine gave no image"))?;
        let bytes = image::encode(&rows).ok_or(Fatal("a row past u32 bytes"))?;
        into.clear();
        into.extend_from_slice(&bytes);
        Ok((self.applied, self.configuration.clone()))
    }

    /// The bytes `image` would write now. Asked once, when a member opens on a log never compacted
    /// (`hyper_durable::Compaction`); an engine that cannot give its rows then, or rows past what
    /// the image counts, gives none, so the log is not weighed against an image until its first
    /// compaction makes one.
    fn image_bytes(&self) -> Option<u64> {
        let rows = self.engine.image().ok()?;
        image::encoded_len(&rows)
    }

    /// The engine takes the image's rows, with the configuration and the term of its point, and
    /// makes them durable before the log's start moves to it (I8).
    fn install(&mut self, image: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal> {
        let mut rows = image::decode(image).ok_or(Fatal("a snapshot's rows do not decode"))?;
        let conf = conf::encode(configuration).ok_or(Fatal("a configuration past u32 members"))?;
        rows.retain(|(k, _)| k.as_slice() != conf::ROW && k.as_slice() != TERM);
        rows.push((conf::ROW.to_vec(), conf));
        rows.push((TERM.to_vec(), encode_term(at.term)));
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.engine
            .install(at.index, rows)
            .map_err(|_| Fatal("the engine refused a snapshot"))?;
        self.engine
            .persist()
            .map_err(|_| Fatal("the engine failed to persist a snapshot"))?;
        self.configuration = configuration.clone();
        self.applied = at;
        self.durable = at;
        Ok(())
    }

    fn persist(&mut self) -> Result<(), Fatal> {
        self.engine
            .persist()
            .map_err(|_| Fatal("the engine failed to persist"))?;
        self.durable = self.applied;
        Ok(())
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
}
