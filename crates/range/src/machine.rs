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

use hyper_durable::{EntryRef, Fatal, Imaged, Point, StateMachine};
use mantle_codec::{Reader, Writer};
use mantle_meta::apply::{Kept, Layer, apply_entry_with};
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

/// What a member re-applied as it opened of the entries its log holds past its engine's durable
/// index, through the commit its durable state stated (docs/design/node.md §2.6, the replay rate
/// research/11 §9.3 calls `ρ`; research/12 §6.2): the entries, their bytes, and the clock's
/// readings ([`clock_ns`]) at the first one's apply asked and the last one's applied, so a node
/// takes its ranges' replays together over the span from the first to begin to the last to end.
/// The span holds whatever re-applying them waited on: pages loaded, a memtable's room, the
/// drives between.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Replayed {
    pub entries: u64,
    pub bytes: u64,
    pub began_ns: u64,
    pub ended_ns: u64,
}

impl Replayed {
    /// The nanoseconds from the first entry's apply asked to the last one's applied.
    pub fn nanos(&self) -> u64 {
        self.ended_ns.saturating_sub(self.began_ns)
    }
}

/// Where a member's replay at open stands ([`RangeMachine::replay`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replay {
    /// No entry past the engine's durable index was due at open, or an image replaced what
    /// was being replayed.
    None,
    /// Entries through `to` are still to apply; what has been is `so_far`.
    Due { to: u64, so_far: Replayed },
    /// Every entry due was re-applied.
    Done(Replayed),
}

/// The process's monotonic clock in nanoseconds from its first reading: one clock for every
/// range's replay, so their spans compare. Saturates past `u64`'s 584 years.
pub fn clock_ns() -> u64 {
    static ANCHOR: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let anchor = ANCHOR.get_or_init(std::time::Instant::now);
    u64::try_from(anchor.elapsed().as_nanos()).unwrap_or(u64::MAX)
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
    #[error(transparent)]
    Image(#[from] image::ImageError),
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
    /// [`EngineError::NoRoom`] otherwise, and every row is read from memory or refused
    /// [`EngineError::Unloaded`] ([`Engine::get_now`]): an entry's, which waits committed rather
    /// than the shard. A change's is applied whatever it waits on: the core has applied it
    /// already.
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
        if self.now {
            return self.engine.get_now(key);
        }
        self.engine.get(key)
    }

    fn next(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        if self.now {
            return self.engine.next_now(from, to);
        }
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
    /// The storage the range's entries are applied in, kept from one entry to the next so an
    /// entry's overlays allocate nothing once one as large has been applied, its capacity within
    /// the bound `mantle_meta::overlay::Writes` states.
    kept: Kept,
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
    /// The replay of the entries past the engine's durable index at open ([`Replay`]).
    replay: Replay,
    /// The image under way for a member behind the log's start, made a slice a drive
    /// ([`StateMachine::image`]).
    imaging: Option<Imaging>,
}

/// An image made a slice a drive on a node's shard (docs/design/replica.md §4): of the point the
/// machine applied when it began, under the configuration held there. Its replica applies nothing
/// until the last slice is made, so each slice reads the state of that point.
struct Imaging {
    at: Point,
    configuration: ConfState,
    /// The key the next slice reads from: the least past the last row written.
    from: Vec<u8>,
    /// The last row's key written, kept as each row is.
    last: Vec<u8>,
    /// The rows written.
    rows: u64,
    /// The bytes the largest row written takes in the image.
    largest: usize,
    /// The CRC-32C of the rows' bytes written (`image::finish`).
    crc: u32,
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
            kept: Kept::default(),
            configuration,
            applied: point,
            persisting: None,
            durable: point,
            failure: None,
            replay: Replay::None,
            imaging: None,
        })
    }

    /// Times the replay of the entries through `to`, the commit the member's durable state
    /// states as it opens, past what the engine applied: none where it applied them all.
    pub fn replay_to(&mut self, to: u64) {
        self.replay = if to > self.applied.index {
            Replay::Due {
                to,
                so_far: Replayed::default(),
            }
        } else {
            Replay::None
        };
    }

    /// Where the replay at open stands.
    pub fn replay(&self) -> Replay {
        self.replay
    }

    /// The entry at `index` asked to apply: the replay's clock starts at the first it re-applies.
    fn replay_asked(&mut self, index: u64) {
        if let Replay::Due { to, so_far } = &mut self.replay
            && index <= *to
            && so_far.began_ns == 0
        {
            so_far.began_ns = clock_ns().max(1);
        }
    }

    /// The entry at `index`, of `bytes`, applied: counted while the replay is due, which ends
    /// at its last.
    fn replay_applied(&mut self, index: u64, bytes: usize) {
        let Replay::Due { to, mut so_far } = self.replay else {
            return;
        };
        if index > to {
            return;
        }
        so_far.entries = so_far.entries.saturating_add(1);
        so_far.bytes = so_far
            .bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        self.replay = if index == to {
            Replay::Done(Replayed {
                ended_ns: clock_ns(),
                ..so_far
            })
        } else {
            Replay::Due { to, so_far }
        };
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
    /// ([`Engine::apply_now`]), and only once every row its commands read is in memory
    /// ([`Engine::get_now`]): with no room, or a row's page loading, nothing of it is applied or
    /// answered and the shell gives it again at the drive the engine's worker wakes the owner
    /// for, the layer's work then done again over the same rows, the same batch. The layer
    /// buffers the entry's writes in an overlay and hands the engine its batch only after its
    /// last read, so a read refused part way leaves nothing of the entry behind.
    fn apply(&mut self, entry: &EntryRef<'_>, answers: &mut Vec<Applied>) -> Result<bool, Fatal> {
        self.replay_asked(entry.index);
        let (layer, rules) = (self.layer, self.rules);
        // Lent to the entry, as the engine is, and kept only once the entry applies.
        let mut watermark = self.watermark;
        let failure = &mut self.failure;
        let kept = &mut self.kept;
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
                Err(EngineError::NoRoom | EngineError::Unloaded) => return Ok(false),
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
                kept,
            );
            let made = match applied {
                Ok(made) => made,
                Err(MetaError::Engine(EngineError::NoRoom | EngineError::Unloaded)) => {
                    return Ok(false);
                }
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
        self.replay_applied(entry.index, entry.data.len());
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
        self.replay_asked(at.index);
        let bytes = conf::encode(configuration).ok_or(Fatal("a configuration past u32 members"))?;
        Termed::new(&mut self.engine, self.applied.term, at.term)
            .apply(at.index, vec![Write::Put(conf::ROW.to_vec(), bytes)])
            .map_err(|e| failed(&mut self.failure, "the engine refused a change", e))?;
        self.configuration = configuration.clone();
        self.applied = at;
        // A change's entry is counted, its bytes not: the shell gives it decoded.
        self.replay_applied(at.index, 0);
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
    /// compaction, so it is of everything applied then (docs/design/replica.md §4).
    ///
    /// It is made a slice a drive, each slice the engine's pages of rows from where the last
    /// stopped until `bytes` are written ([`Engine::image_page`]), at most `bytes` / 8 rows and,
    /// once the image met its largest row, `bytes` and a row, read from memory: a page not in
    /// memory loads on the engine's workers, whose message wakes the shard, and the slice stops
    /// there ([`Imaged::Waits`]). Made whole on the shard, an image of a million rows held every
    /// range the node serves for seconds. The replica applies nothing while it is under way, so
    /// every slice reads the state of the point it began at: one applied meanwhile is a broken
    /// promise the machine refuses ([`Fatal`]).
    fn image(&mut self, into: &mut Vec<u8>, bytes: u64) -> Result<Imaged, Fatal> {
        if into.is_empty() {
            image::begin(into);
            self.imaging = Some(Imaging {
                at: self.applied,
                configuration: self.configuration.clone(),
                from: Vec::new(),
                last: Vec::new(),
                rows: 0,
                largest: 0,
                crc: 0,
            });
        }
        let Some(imaging) = self.imaging.as_mut() else {
            return Err(Fatal("an image went on that was never begun"));
        };
        if imaging.at != self.applied {
            return Err(Fatal("the machine applied while its image was made"));
        }
        let budget = usize::try_from(bytes).unwrap_or(usize::MAX).max(1);
        let start = into.len();
        let mut more = true;
        let mut waits = false;
        while more {
            let written = into.len().saturating_sub(start);
            let Some(left) = budget.checked_sub(written).filter(|&left| left > 0) else {
                break;
            };
            // As many rows as the bytes left hold at the largest row written so far, one to learn
            // it from at first: a slice passes its bytes by at most a row once the image met its
            // largest, and holds no more rows than its bytes do at the least a row takes, its
            // two lengths.
            let rows = match imaging.largest {
                0 => 1,
                largest => left.checked_div(largest.max(8)).unwrap_or(1).max(1),
            };
            let Imaging {
                from,
                last,
                largest,
                ..
            } = &mut *imaging;
            let mut given = 0u64;
            let page = self.engine.image_page(from.as_slice(), rows, &mut |k, v| {
                let at = into.len();
                image::push_row(into, k, v).ok_or(EngineError::RowTooLarge {
                    key: k.len(),
                    value: v.len(),
                })?;
                *largest = (*largest).max(into.len().saturating_sub(at));
                last.clear();
                last.extend_from_slice(k);
                given = given.saturating_add(1);
                Ok(())
            });
            imaging.rows = imaging.rows.saturating_add(given);
            if given > 0 {
                // The least key past the last row: that key and a zero byte.
                std::mem::swap(&mut imaging.from, &mut imaging.last);
                imaging.from.push(0);
            }
            match page {
                Ok(filled) => more = filled,
                Err(EngineError::Unloaded) => {
                    waits = true;
                    break;
                }
                Err(e) => return Err(failed(&mut self.failure, "the engine gave no image", e)),
            }
        }
        imaging.crc = mantle_crc::crc32c_extend(imaging.crc, into.get(start..).unwrap_or_default());
        if !waits {
            // The pages a slice read go to the engine's cache, as an entry's do once applied.
            self.engine.reads_done();
        }
        if more {
            return Ok(if waits { Imaged::Waits } else { Imaged::Slice });
        }
        let Some(imaging) = self.imaging.take() else {
            return Err(Fatal("an image went on that was never begun"));
        };
        image::finish(into, imaging.rows, imaging.crc).ok_or(Fatal("an image with no head"))?;
        // Checked whole before it leaves, one pass over its bytes and no row taken, so an image
        // made wrong fails here, naming its point, rows, bytes and checksums, rather than on the
        // member that installs it: a member that installed a malformed image of a range under
        // sustained writes and stalls fenced with no word of where it was made (2026-11-20).
        image::check(into).map_err(|e| {
            failed(
                &mut self.failure,
                "an image made does not pass its checks",
                e.at(imaging.at.index, imaging.at.term),
            )
        })?;
        Ok(Imaged::Made(imaging.at, imaging.configuration))
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
        // What was being replayed is replaced, so no replay is timed.
        if matches!(self.replay, Replay::Due { .. }) {
            self.replay = Replay::None;
        }
        let mut rows = image::decode(image).map_err(|e| {
            failed(
                &mut self.failure,
                "a snapshot's rows do not decode",
                e.at(at.index, at.term),
            )
        })?;
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
    /// Its first `cold` reads without waiting each find their page not in memory, as an engine
    /// whose cache is cold does, a page landing at each: `refused` counts them.
    struct Counted {
        rows: mantle_meta::engine::Model,
        scans: u64,
        roomless: bool,
        cold: u64,
        refused: u64,
    }

    impl Counted {
        /// A read without waiting: refused while pages are cold, the next one landing.
        fn now(&mut self) -> Result<(), EngineError> {
            if self.cold == 0 {
                return Ok(());
            }
            self.cold -= 1;
            self.refused += 1;
            Err(EngineError::Unloaded)
        }
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

        fn image_page(
            &mut self,
            from: &[u8],
            rows: usize,
            each: &mut mantle_meta::engine::EachRow<'_>,
        ) -> Result<bool, EngineError> {
            self.now()?;
            self.rows.image_page(from, rows, each)
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

        fn get_now(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
            self.now()?;
            self.get(key)
        }

        fn next_now(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
            self.now()?;
            self.next(from, to)
        }
    }

    const RULES: Rules = Rules {
        lifetime_ns: 1_000,
        max_sessions: 8,
        expiries_per_entry: 1,
        max_key_bytes: usize::MAX,
        max_value_bytes: usize::MAX,
    };

    /// The image `machine` makes, a slice of about `bytes` a call, asked again while it waits on
    /// a page; and the bytes and rows each slice wrote.
    fn image_whole(
        machine: &mut RangeMachine<Counted>,
        bytes: u64,
    ) -> (Vec<u8>, Point, ConfState, Vec<(usize, u64)>) {
        let mut image = Vec::new();
        let mut slices = Vec::new();
        loop {
            let (before, rows) = (image.len(), machine.imaging.as_ref().map_or(0, |i| i.rows));
            let made = machine.image(&mut image, bytes).unwrap();
            let after = machine.imaging.as_ref().map_or(0, |i| i.rows);
            match made {
                Imaged::Made(at, configuration) => {
                    return (image, at, configuration, slices);
                }
                Imaged::Slice => slices.push((image.len() - before, after - rows)),
                Imaged::Waits => {}
            }
        }
    }

    /// A cell's first Name range, as a replica opens it.
    fn opened() -> RangeMachine<Counted> {
        let mut rows = mantle_meta::engine::Model::default();
        rows.install(0, mantle_meta::name::first(1).unwrap())
            .unwrap();
        let engine = Counted {
            rows,
            scans: 0,
            roomless: false,
            cold: 0,
            refused: 0,
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

    /// An entry whose reads find their pages not in memory is answered unapplied at each read
    /// refused, as an entry with no room is: no row, no answer, the machine's applied point and
    /// its knowledge of its sessions' last use as they were. Each drive reads again, past the
    /// pages landed, and the one that finds every page in memory applies it whole, answered once.
    #[test]
    fn an_entry_whose_rows_are_not_in_memory_applies_nothing_until_they_are() {
        let mut machine = opened();
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
        let data = wire::Entry {
            at_ns: 10,
            commands: vec![registration],
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
        machine.engine_mut().cold = 3;
        let mut answers = Vec::new();
        for refused in 1..=3 {
            assert!(!machine.apply(&entry, &mut answers).unwrap(), "{refused}");
            assert_eq!(machine.engine().refused, refused);
            assert!(answers.is_empty());
            assert_eq!(machine.applied, Point { index: 0, term: 0 });
            assert_eq!(machine.engine().applied(), 0);
            assert_eq!(machine.engine_mut().image().unwrap(), rows);
            assert_eq!(machine.watermark, watermark);
            assert!(machine.failure().is_none());
        }
        assert!(machine.apply(&entry, &mut answers).unwrap());
        assert_eq!(machine.engine().refused, 3);
        assert_eq!(machine.applied, Point { index: 1, term: 1 });
        assert_eq!(answers.len(), 1);
        assert!(
            matches!(answers[0].answer, Answer::Registered { .. }),
            "{answers:?}"
        );
        assert_ne!(machine.engine_mut().image().unwrap(), rows);
    }

    /// An image whose bytes changed after it was made is refused by the member that installs it,
    /// and the fence names the check it failed and what the image states of itself: its point,
    /// its bytes, the rows its head states, the checksum it carries and the one its bytes give.
    /// The node that died of one on the release ladder (2026-11-20) said only that "a snapshot's
    /// rows do not decode".
    #[test]
    fn an_image_that_fails_its_checks_fences_its_installer_with_its_facts() {
        let mut source = opened();
        apply_at(&mut source, 1, 10, Vec::new());
        let (mut image, at, configuration, _) = image_whole(&mut source, 64);
        let len = image.len();
        image[len / 2] ^= 1;
        let mut given = opened();
        let refused = given.install(&image, at, &configuration).unwrap_err();
        assert_eq!(refused, Fatal("a snapshot's rows do not decode"));
        let failure = given.failure().unwrap().to_string();
        assert!(
            failure.starts_with(&format!(
                "an image does not match its checksum: of index {} and term {}, {len} bytes, ",
                at.index, at.term
            )),
            "{failure}"
        );
        assert!(failure.contains("carried and"), "{failure}");
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
        let (image, at, configuration, _) = image_whole(&mut source, 64);
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

    /// An image is made a slice a call, each about the bytes asked: a slice holds no more rows
    /// than the bytes hold at a row's least, and writes no more than the bytes and a row past
    /// them. A page not in memory stops a slice, which goes on once it lands. The image decodes
    /// to the engine's rows, of the point applied when it began, under the configuration held
    /// there. One the machine applied an entry under, against the promise its replica keeps, it
    /// refuses.
    #[test]
    fn an_image_is_made_a_bounded_slice_at_a_time() {
        let mut machine = opened();
        // The first row is the largest: every slice is bounded from the first.
        let rows: Vec<Row> = (0..2_000u32)
            .map(|i| {
                let len = 96 - (i % 97) as usize;
                (format!("image/{i:05}").into_bytes(), vec![7u8; len])
            })
            .collect();
        let writes = rows.iter().map(|(k, v)| Write::Put(k.clone(), v.clone()));
        machine.engine.rows = mantle_meta::engine::Model::default();
        machine.engine.rows.apply(1, writes.collect()).unwrap();
        machine.applied = Point { index: 1, term: 1 };
        let expected = machine.engine.rows.image().unwrap();
        let largest = expected
            .iter()
            .map(|(k, v)| 8 + k.len() + v.len())
            .max()
            .unwrap();
        machine.engine.cold = 3;
        let (image, at, configuration, slices) = image_whole(&mut machine, 1_024);
        assert_eq!(at, Point { index: 1, term: 1 });
        assert_eq!(configuration, ConfState::default());
        assert_eq!(image::decode(&image), Ok(expected.clone()));
        assert_eq!(machine.engine.refused, 3, "each cold page stopped a slice");
        assert!(slices.len() > 20, "{} slices", slices.len());
        for &(bytes, rows) in &slices {
            assert!(bytes <= 1_024 + largest, "a slice of {bytes} bytes");
            assert!(rows <= 1_024 / 8 + 1, "a slice of {rows} rows");
        }
        // An entry applied while an image is under way breaks the promise: refused.
        let mut image = Vec::new();
        assert_eq!(machine.image(&mut image, 64).unwrap(), Imaged::Slice);
        apply_at(&mut machine, 2, 10, Vec::new());
        assert!(machine.image(&mut image, 64).is_err());
        // Begun again, of the point applied now.
        let (image, at, ..) = image_whole(&mut machine, 1 << 20);
        assert_eq!(at.index, 2);
        assert_eq!(
            image::decode(&image),
            Ok(machine.engine.rows.image().unwrap())
        );
    }
}
