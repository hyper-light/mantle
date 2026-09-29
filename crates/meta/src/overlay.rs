//! One entry's writes buffered over the engine (docs/design/replica.md §2): the commands of an
//! entry read what the commands before them wrote, and the engine takes the entry's writes as
//! one batch at its index.

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::engine::{EngineError, Row, Rows, Write};

pub struct Overlay<'a, R> {
    rows: &'a R,
    /// Each key the entry wrote since it last cleared the key's range: its value, or `None`
    /// where it deleted the key.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// The ranges the entry cleared, in order: no row beneath shows through them.
    cleared: Vec<(Vec<u8>, Vec<u8>)>,
}

impl<'a, R: Rows> Overlay<'a, R> {
    pub fn new(rows: &'a R) -> Self {
        Self {
            rows,
            writes: BTreeMap::new(),
            cleared: Vec::new(),
        }
    }

    /// The entry's writes: its cleared ranges, then one write per key written after, in key
    /// order.
    pub fn into_writes(self) -> Vec<Write> {
        let mut out: Vec<Write> = self
            .cleared
            .into_iter()
            .map(|(from, to)| Write::Clear(from, to))
            .collect();
        out.extend(self.writes.into_iter().map(|(k, v)| match v {
            Some(v) => Write::Put(k, v),
            None => Write::Delete(k),
        }));
        out
    }

    /// The end of the cleared range holding `key`, if one does.
    fn cleared_past(&self, key: &[u8]) -> Option<&[u8]> {
        self.cleared
            .iter()
            .filter(|(from, to)| from.as_slice() <= key && key < to.as_slice())
            .map(|(_, to)| to.as_slice())
            .max()
    }
}

impl<R: Rows> Rows for Overlay<'_, R> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        match self.writes.get(key) {
            Some(value) => Ok(value.clone()),
            None if self.cleared_past(key).is_some() => Ok(None),
            None => self.rows.get(key),
        }
    }

    fn next(&self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        if from >= to {
            return Ok(None);
        }
        let range = (Bound::Included(from), Bound::Excluded(to));
        let written = self
            .writes
            .range::<[u8], _>(range)
            .find(|(_, v)| v.is_some())
            .and_then(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())));
        // The first row beneath that the entry has not deleted, replaced or cleared. Each step
        // past a row passes one of the entry's writes or leaves a cleared range, so the walk
        // ends.
        let mut at = from.to_vec();
        let beneath = loop {
            let Some((k, v)) = self.rows.next(&at, to)? else {
                break None;
            };
            if let Some(past) = self.cleared_past(&k) {
                at = past.to_vec();
                continue;
            }
            if !self.writes.contains_key(&k) {
                break Some((k, v));
            }
            at = k;
            at.push(0);
        };
        Ok(match (written, beneath) {
            (Some(w), Some(b)) => Some(if w.0 <= b.0 { w } else { b }),
            (w, b) => w.or(b),
        })
    }

    fn apply(&mut self, _index: u64, writes: &[Write]) -> Result<(), EngineError> {
        for write in writes {
            match write {
                Write::Put(k, v) => {
                    self.writes.insert(k.clone(), Some(v.clone()));
                }
                Write::Delete(k) => {
                    self.writes.insert(k.clone(), None);
                }
                Write::Clear(from, to) => {
                    if from < to {
                        let mut tail = self.writes.split_off(from.as_slice());
                        let mut kept = tail.split_off(to.as_slice());
                        self.writes.append(&mut kept);
                        self.cleared.push((from.clone(), to.clone()));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;
    use proptest::prelude::*;

    fn key() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(0u8..4, 0..3)
    }

    fn write() -> impl Strategy<Value = Write> {
        prop_oneof![
            3 => (key(), key()).prop_map(|(k, v)| Write::Put(k, v)),
            2 => key().prop_map(Write::Delete),
            1 => (key(), key()).prop_map(|(from, to)| Write::Clear(from, to)),
        ]
    }

    proptest! {
        /// Reading through the overlay is reading the engine with the entry's writes
        /// applied, and the entry's writes applied as one batch give the same rows.
        #[test]
        fn an_overlay_reads_as_the_engine_would_after_the_entry(
            before in prop::collection::vec(write(), 0..12),
            entry in prop::collection::vec(prop::collection::vec(write(), 0..4), 0..4),
            from in key(),
            to in key(),
        ) {
            let mut engine = Model::default();
            engine.apply(1, &before).unwrap();
            let mut expected = engine.clone();
            let mut overlay = Overlay::new(&engine);
            for (i, writes) in entry.iter().enumerate() {
                overlay.apply(2, writes).unwrap();
                expected.apply(2 + i as u64, writes).unwrap();
                for k in [from.clone(), to.clone()] {
                    prop_assert_eq!(overlay.get(&k).unwrap(), expected.get(&k).unwrap());
                }
                prop_assert_eq!(overlay.next(&from, &to).unwrap(), expected.next(&from, &to).unwrap());
            }
            let writes = overlay.into_writes();
            engine.apply(2, &writes).unwrap();
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
            prop_assert_eq!(all(&engine), all(&expected));
        }
    }
}
