//! The state engine a range keeps its rows in (docs/design/metadata.md §4): a batch applied
//! atomically with the index of the log entry it applies, point reads, ordered scans, key
//! ranges removed whole, and the index a crash would return to. The Raft log is the only write-ahead log, so the log
//! is truncated only below `durable()` (06 §C.b.2).

use std::collections::BTreeMap;
use std::ops::Bound;

/// A row: its key and value.
pub type Row = (Vec<u8>, Vec<u8>);

/// One change in a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Write {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    /// Removes every row in `[from, to)`, as one change whatever it holds: what a split
    /// leaves behind of the rows its child took, as RocksDB's `DeleteRange` writes one range
    /// tombstone (docs/design/metadata.md §3).
    Clear(Vec<u8>, Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// An entry at or below the last one applied: applying runs forward from `durable()`.
    #[error("entry {index} applied after entry {applied}")]
    OutOfOrder { index: u64, applied: u64 },
    /// A write or flush failed; nothing more is applied until the range recovers.
    #[error("the engine is fenced after a failed write")]
    Fenced,
    /// No memory for a batch: nothing was applied.
    #[error("no memory for a batch")]
    Memory,
    /// A row past the range's bounds on a key or a value (`session::Rules::max_key_bytes`,
    /// `max_value_bytes`), which every member's engine holds whole: nothing of the batch was
    /// applied.
    #[error("a row of a {key}-byte key and a {value}-byte value past the range's bounds")]
    RowTooLarge { key: usize, value: usize },
    /// No room for a batch without waiting on the engine's own maintenance
    /// ([`Engine::apply_now`]): nothing of it was applied, and the engine's owner is woken when
    /// the room may have come.
    #[error("no room for a batch without waiting on the engine's maintenance")]
    NoRoom,
    /// A row read without waiting ([`Engine::get_now`], [`Engine::next_now`]) is on a page not
    /// in memory now: its load is begun on the engine's own workers, and the engine's owner is
    /// woken when it lands. Nothing was read.
    #[error("a row's page is not in memory now")]
    Unloaded,
}

/// Rows as a state machine reads and writes them.
pub trait Rows {
    /// The value of `key`.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError>;

    /// The first row in `[from, to)`.
    fn next(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError>;

    /// Applies `writes` atomically as log entry `index`. The writes are given, not lent: a layer
    /// that keeps them, as an entry's overlay does, moves their rows rather than copying them.
    fn apply(&mut self, index: u64, writes: Vec<Write>) -> Result<(), EngineError>;
}

/// A range's rows, and what of them a crash keeps.
pub trait Engine: Rows {
    /// The last log entry applied.
    fn applied(&self) -> u64;

    /// The last log entry applied that a crash keeps.
    fn durable(&self) -> u64;

    /// Makes everything applied durable.
    fn persist(&mut self) -> Result<(), EngineError>;

    /// [`Engine::persist`] made without waiting on the engine's own workers or its device: true
    /// once everything applied when its checkpoint began is durable ([`Engine::durable`] reaches
    /// it); false while that checkpoint is under way, the runtime task polling now woken once it
    /// may have completed, by the work it waits on and not by a timer. Asked again while one is
    /// out, it goes on with that one and begins no other; what is applied meanwhile is not
    /// covered by it. A range compacts its log so, on a node's shard that serves its consensus
    /// and transport as well (docs/design/replica.md §4). An engine with nothing to wait on
    /// persists here.
    fn persist_now(&mut self) -> Result<bool, EngineError> {
        self.persist().map(|()| true)
    }

    /// Every row as of `applied()`: what a snapshot carries to a member that lags
    /// (docs/design/replica.md §4).
    fn image(&mut self) -> Result<Vec<Row>, EngineError>;
    /// The rows `image` would give and their keys' and values' bytes, when the engine knows them
    /// without reading its rows; `None` when it does not.
    fn image_size(&self) -> Option<(u64, u64)>;

    /// Replaces every row with `rows`, as of log entry `index`: a snapshot installed. It is
    /// durable once `persist` returns.
    fn install(&mut self, index: u64, rows: Vec<Row>) -> Result<(), EngineError>;

    /// Holds at most `bytes` of its pages in its page cache from now: the pages past them dropped
    /// at once, those within them kept and served, and a cache below them raised keeping its
    /// pages (docs/design/node.md §1.4: the caches borrow the bytes no running operation holds).
    /// The bytes its cache may hold then, a whole number of its pages, at most `bytes`; an engine
    /// with no page cache holds none.
    fn lend_cache(&mut self, bytes: u64) -> u64 {
        let _ = bytes;
        0
    }

    /// The checkpoints that made something durable since the engine opened, and the flushes of
    /// its device it made in all: what its durability cost the device, for its owner's counts. An
    /// engine held in memory flushes nothing.
    fn checkpoints(&self) -> (u64, u64) {
        (0, 0)
    }

    /// The engine's own error that fenced it, the first, where it keeps one: what each
    /// [`EngineError::Fenced`] it answered since stands for, which its owner reports beside the
    /// fence. Asked only once a call failed; an engine that keeps none answers none.
    fn fault(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    /// [`Rows::apply`] made only if the engine has room for the whole batch with no wait on its
    /// own maintenance (a full memtable's rotation waiting on a job out on a worker): true,
    /// applied; false, nothing of it applied, with the runtime task polling now woken once the
    /// room may have come, by the work it waits on (docs/design/replica.md §2). A range applies
    /// its entries so, on a node's shard that serves its consensus and transport as well: an
    /// entry waits, committed and unapplied, rather than the shard. An engine with nothing to
    /// wait on applies the batch.
    fn apply_now(&mut self, index: u64, writes: Vec<Write>) -> Result<bool, EngineError> {
        self.apply(index, writes).map(|()| true)
    }

    /// [`Rows::get`] made without waiting on the engine's device: the value from memory, or
    /// [`EngineError::Unloaded`] with the page it needs loading on the engine's own workers and
    /// the runtime task polling now woken once it lands, by that work and not by a timer
    /// (docs/design/replica.md §2). A range reads an entry's rows so, on a node's shard that
    /// serves its consensus and transport as well: the entry waits, committed and unapplied,
    /// rather than the shard. An engine that never waits on a device reads.
    fn get_now(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.get(key)
    }

    /// [`Rows::next`] made without waiting on the engine's device, as [`Engine::get_now`].
    fn next_now(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        self.next(from, to)
    }

    /// Pays up to about `budget` units of the maintenance the engine owes (its packings,
    /// compactions and consolidations, its cache's upkeep), in time its owner has no request
    /// for, waiting for nothing: what its workers finished is taken back and what is due handed
    /// out. The units done, and whether work is still owed. Nothing changes what is durable: only
    /// [`Engine::persist`] does. An engine that keeps no maintenance owes none.
    fn upkeep(&mut self, budget: u64) -> Result<(u64, bool), EngineError> {
        let _ = budget;
        Ok((0, false))
    }
}

/// An engine held in memory for deterministic simulation (06 §C.b.3): what is applied, and
/// what is durable, which a simulated crash returns it to.
#[derive(Debug, Default, Clone)]
pub struct Model {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    applied: u64,
    durable_rows: BTreeMap<Vec<u8>, Vec<u8>>,
    durable: u64,
}

impl Model {
    /// Loses everything applied since the last `persist`, as a power cut would.
    pub fn crash(&mut self) {
        self.rows.clone_from(&self.durable_rows);
        self.applied = self.durable;
    }
}

impl Rows for Model {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        Ok(self.rows.get(key).cloned())
    }

    fn next(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        if from >= to {
            return Ok(None);
        }
        Ok(self
            .rows
            .range::<[u8], _>((Bound::Included(from), Bound::Excluded(to)))
            .next()
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn apply(&mut self, index: u64, writes: Vec<Write>) -> Result<(), EngineError> {
        if index <= self.applied {
            return Err(EngineError::OutOfOrder {
                index,
                applied: self.applied,
            });
        }
        for write in writes {
            match write {
                Write::Put(k, v) => {
                    self.rows.insert(k, v);
                }
                Write::Delete(k) => {
                    self.rows.remove(&k);
                }
                Write::Clear(from, to) => {
                    if from < to {
                        let mut tail = self.rows.split_off(from.as_slice());
                        let mut kept = tail.split_off(to.as_slice());
                        self.rows.append(&mut kept);
                    }
                }
            }
        }
        self.applied = index;
        Ok(())
    }
}

impl Engine for Model {
    fn applied(&self) -> u64 {
        self.applied
    }

    fn durable(&self) -> u64 {
        self.durable
    }

    fn persist(&mut self) -> Result<(), EngineError> {
        self.durable_rows.clone_from(&self.rows);
        self.durable = self.applied;
        Ok(())
    }

    fn image(&mut self) -> Result<Vec<Row>, EngineError> {
        Ok(self
            .rows
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn image_size(&self) -> Option<(u64, u64)> {
        let bytes = self.rows.iter().try_fold(0u64, |bytes, (k, v)| {
            bytes
                .checked_add(u64::try_from(k.len()).ok()?)?
                .checked_add(u64::try_from(v.len()).ok()?)
        })?;
        Some((u64::try_from(self.rows.len()).ok()?, bytes))
    }

    fn install(&mut self, index: u64, rows: Vec<Row>) -> Result<(), EngineError> {
        self.rows = rows.into_iter().collect();
        self.applied = index;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_crash_returns_to_what_was_persisted() {
        let mut m = Model::default();
        m.apply(1, vec![Write::Put(b"a".to_vec(), b"1".to_vec())])
            .unwrap();
        m.persist().unwrap();
        m.apply(
            2,
            vec![
                Write::Delete(b"a".to_vec()),
                Write::Put(b"b".to_vec(), b"2".to_vec()),
            ],
        )
        .unwrap();
        assert_eq!(
            m.next(b"", b"z").unwrap(),
            Some((b"b".to_vec(), b"2".to_vec()))
        );
        m.crash();
        assert_eq!((m.applied(), m.durable()), (1, 1));
        assert_eq!(m.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(m.get(b"b").unwrap(), None);
        assert_eq!(
            m.apply(1, vec![]),
            Err(EngineError::OutOfOrder {
                index: 1,
                applied: 1
            })
        );
        m.apply(5, vec![]).unwrap();
        assert_eq!(m.applied(), 5);
        assert_eq!(m.next(b"z", b"a").unwrap(), None);
    }
}
