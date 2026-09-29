//! The state engine a range keeps its rows in (docs/design/metadata.md §4): a batch applied
//! atomically with the index of the log entry it applies, point reads, ordered scans, and
//! the index a crash would return to. The Raft log is the only write-ahead log, so the log
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// An entry at or below the last one applied: applying runs forward from `durable()`.
    #[error("entry {index} applied after entry {applied}")]
    OutOfOrder { index: u64, applied: u64 },
    /// A write or flush failed; nothing more is applied until the range recovers.
    #[error("the engine is fenced after a failed write")]
    Fenced,
}

/// Rows as a state machine reads and writes them.
pub trait Rows {
    /// The value of `key`.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError>;

    /// The first row in `[from, to)`.
    fn next(&self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError>;

    /// Applies `writes` atomically as log entry `index`.
    fn apply(&mut self, index: u64, writes: &[Write]) -> Result<(), EngineError>;
}

/// A range's rows, and what of them a crash keeps.
pub trait Engine: Rows {
    /// The last log entry applied.
    fn applied(&self) -> u64;

    /// The last log entry applied that a crash keeps.
    fn durable(&self) -> u64;

    /// Makes everything applied durable.
    fn persist(&mut self) -> Result<(), EngineError>;

    /// Every row as of `applied()`: what a snapshot carries to a member that lags
    /// (docs/design/replica.md §4).
    fn image(&self) -> Result<Vec<Row>, EngineError>;

    /// Replaces every row with `rows`, as of log entry `index`: a snapshot installed. It is
    /// durable once `persist` returns.
    fn install(&mut self, index: u64, rows: Vec<Row>) -> Result<(), EngineError>;
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
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        Ok(self.rows.get(key).cloned())
    }

    fn next(&self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        if from >= to {
            return Ok(None);
        }
        Ok(self
            .rows
            .range::<[u8], _>((Bound::Included(from), Bound::Excluded(to)))
            .next()
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn apply(&mut self, index: u64, writes: &[Write]) -> Result<(), EngineError> {
        if index <= self.applied {
            return Err(EngineError::OutOfOrder {
                index,
                applied: self.applied,
            });
        }
        for write in writes {
            match write {
                Write::Put(k, v) => {
                    self.rows.insert(k.clone(), v.clone());
                }
                Write::Delete(k) => {
                    self.rows.remove(k);
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

    fn image(&self) -> Result<Vec<Row>, EngineError> {
        Ok(self
            .rows
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
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
        m.apply(1, &[Write::Put(b"a".to_vec(), b"1".to_vec())])
            .unwrap();
        m.persist().unwrap();
        m.apply(
            2,
            &[
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
            m.apply(1, &[]),
            Err(EngineError::OutOfOrder {
                index: 1,
                applied: 1
            })
        );
        m.apply(5, &[]).unwrap();
        assert_eq!(m.applied(), 5);
        assert_eq!(m.next(b"z", b"a").unwrap(), None);
    }
}
