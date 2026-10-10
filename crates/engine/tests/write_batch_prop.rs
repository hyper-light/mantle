//! Properties of `WriteBatch` over random operation sequences: what is written iterates back as
//! the same operations with the right count and content flags; decoding every record and
//! re-encoding it gives the same bytes; save points roll back to exactly the earlier bytes;
//! and no bytes, whether arbitrary or a mutation of a valid batch, make iteration panic, read
//! past the input, or accept a batch whose count disagrees with its records.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use mantle_engine::Error;
use mantle_engine::db::write_batch::{HEADER, Handler, Record, WriteBatch, read_record};
use proptest::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Put(u32, Vec<u8>, Vec<u8>),
    TimedPut(u32, Vec<u8>, Vec<u8>, u64),
    Delete(u32, Vec<u8>),
    SingleDelete(u32, Vec<u8>),
    DeleteRange(u32, Vec<u8>, Vec<u8>),
    Merge(u32, Vec<u8>, Vec<u8>),
    BlobIndex(u32, Vec<u8>, Vec<u8>),
    Entity(u32, Vec<u8>, Vec<u8>),
    LogData(Vec<u8>),
}

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..40)
}

fn cf() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0u32), 1..300u32, Just(u32::MAX)]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (cf(), bytes(), bytes()).prop_map(|(c, k, v)| Op::Put(c, k, v)),
        (cf(), bytes(), bytes(), 0..u64::MAX).prop_map(|(c, k, v, t)| Op::TimedPut(c, k, v, t)),
        (cf(), bytes()).prop_map(|(c, k)| Op::Delete(c, k)),
        (cf(), bytes()).prop_map(|(c, k)| Op::SingleDelete(c, k)),
        (cf(), bytes(), bytes()).prop_map(|(c, b, e)| Op::DeleteRange(c, b, e)),
        (cf(), bytes(), bytes()).prop_map(|(c, k, v)| Op::Merge(c, k, v)),
        (cf(), bytes(), bytes()).prop_map(|(c, k, v)| Op::BlobIndex(c, k, v)),
        (cf(), bytes(), bytes()).prop_map(|(c, k, v)| Op::Entity(c, k, v)),
        bytes().prop_map(Op::LogData),
    ]
}

fn apply(b: &mut WriteBatch, op: &Op) {
    match op {
        Op::Put(c, k, v) => b.put_cf(*c, k, v),
        Op::TimedPut(c, k, v, t) => b.timed_put(*c, k, v, *t),
        Op::Delete(c, k) => b.delete_cf(*c, k),
        Op::SingleDelete(c, k) => b.single_delete_cf(*c, k),
        Op::DeleteRange(c, bk, e) => b.delete_range_cf(*c, bk, e),
        Op::Merge(c, k, v) => b.merge_cf(*c, k, v),
        Op::BlobIndex(c, k, v) => b.put_blob_index(*c, k, v),
        // An entity's bytes are opaque to the batch.
        Op::Entity(c, k, e) => b.put_entity_serialized(*c, k, e),
        Op::LogData(d) => b.put_log_data(d),
    }
    .unwrap();
}

/// Records every operation it is given, as `Op`s.
#[derive(Default)]
struct Collect(Vec<Op>);

impl Handler for Collect {
    fn put_cf(&mut self, c: u32, k: &[u8], v: &[u8]) -> Result<(), Error> {
        self.0.push(Op::Put(c, k.to_vec(), v.to_vec()));
        Ok(())
    }
    fn timed_put_cf(&mut self, c: u32, k: &[u8], v: &[u8], t: u64) -> Result<(), Error> {
        self.0.push(Op::TimedPut(c, k.to_vec(), v.to_vec(), t));
        Ok(())
    }
    fn put_entity_cf(&mut self, c: u32, k: &[u8], e: &[u8]) -> Result<(), Error> {
        self.0.push(Op::Entity(c, k.to_vec(), e.to_vec()));
        Ok(())
    }
    fn delete_cf(&mut self, c: u32, k: &[u8]) -> Result<(), Error> {
        self.0.push(Op::Delete(c, k.to_vec()));
        Ok(())
    }
    fn single_delete_cf(&mut self, c: u32, k: &[u8]) -> Result<(), Error> {
        self.0.push(Op::SingleDelete(c, k.to_vec()));
        Ok(())
    }
    fn delete_range_cf(&mut self, c: u32, b: &[u8], e: &[u8]) -> Result<(), Error> {
        self.0.push(Op::DeleteRange(c, b.to_vec(), e.to_vec()));
        Ok(())
    }
    fn merge_cf(&mut self, c: u32, k: &[u8], v: &[u8]) -> Result<(), Error> {
        self.0.push(Op::Merge(c, k.to_vec(), v.to_vec()));
        Ok(())
    }
    fn put_blob_index_cf(&mut self, c: u32, k: &[u8], v: &[u8]) -> Result<(), Error> {
        self.0.push(Op::BlobIndex(c, k.to_vec(), v.to_vec()));
        Ok(())
    }
    fn log_data(&mut self, d: &[u8]) {
        self.0.push(Op::LogData(d.to_vec()));
    }
    fn mark_begin_prepare(&mut self, _: bool) -> Result<(), Error> {
        Ok(())
    }
    fn mark_end_prepare(&mut self, _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn mark_noop(&mut self, _: bool) -> Result<(), Error> {
        Ok(())
    }
    fn mark_rollback(&mut self, _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn mark_commit(&mut self, _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn mark_commit_with_timestamp(&mut self, _: &[u8], _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
}

/// Re-encodes one decoded record into `b`.
fn reencode(b: &mut WriteBatch, r: &Record<'_>) {
    match *r {
        Record::Put { cf, key, value } => b.put_cf(cf, key, value).unwrap(),
        Record::TimedPut {
            cf,
            key,
            value,
            write_unix_time,
        } => b.timed_put(cf, key, value, write_unix_time).unwrap(),
        Record::Delete { cf, key } => b.delete_cf(cf, key).unwrap(),
        Record::SingleDelete { cf, key } => b.single_delete_cf(cf, key).unwrap(),
        Record::DeleteRange { cf, begin, end } => b.delete_range_cf(cf, begin, end).unwrap(),
        Record::Merge { cf, key, value } => b.merge_cf(cf, key, value).unwrap(),
        Record::BlobIndex { cf, key, value } => b.put_blob_index(cf, key, value).unwrap(),
        Record::PutEntity { cf, key, entity } => b.put_entity_serialized(cf, key, entity).unwrap(),
        Record::LogData { blob } => b.put_log_data(blob).unwrap(),
        other => panic!("not written by these tests: {other:?}"),
    }
}

proptest! {
    #[test]
    fn written_ops_iterate_back(ops in prop::collection::vec(op(), 0..24), seq in 0..u64::MAX) {
        let mut b = WriteBatch::new();
        b.set_sequence(seq);
        for op in &ops {
            apply(&mut b, op);
        }
        let data_ops = ops.iter().filter(|o| !matches!(o, Op::LogData(_))).count();
        prop_assert_eq!(b.count() as usize, data_ops);
        prop_assert_eq!(b.sequence(), seq);
        let mut seen = Collect::default();
        b.iterate(&mut seen).unwrap();
        prop_assert_eq!(&seen.0, &ops);

        // The flags of a batch built from bytes are computed from its records.
        let from_bytes = WriteBatch::from_rep(b.data().to_vec()).unwrap();
        prop_assert_eq!(from_bytes.has_put(), b.has_put());
        prop_assert_eq!(from_bytes.has_merge(), b.has_merge());
        prop_assert_eq!(from_bytes.has_delete_range(), b.has_delete_range());
        prop_assert_eq!(from_bytes.has_put_entity(), b.has_put_entity());

        // Record by record, decoding and re-encoding gives the same bytes.
        let mut rebuilt = WriteBatch::new();
        rebuilt.set_sequence(seq);
        let mut input = &b.data()[HEADER..];
        while !input.is_empty() {
            let r = read_record(&mut input).unwrap();
            reencode(&mut rebuilt, &r);
        }
        prop_assert_eq!(rebuilt.data(), b.data());
    }

    #[test]
    fn save_point_restores_bytes(
        before in prop::collection::vec(op(), 0..8),
        after in prop::collection::vec(op(), 0..8),
    ) {
        let mut b = WriteBatch::new();
        for op in &before {
            apply(&mut b, op);
        }
        let snapshot = b.data().to_vec();
        b.set_save_point().unwrap();
        for op in &after {
            apply(&mut b, op);
        }
        prop_assert!(b.rollback_to_save_point().unwrap());
        prop_assert_eq!(b.data(), &snapshot[..]);
        prop_assert!(!b.rollback_to_save_point().unwrap());
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        if let Ok(b) = WriteBatch::from_rep(bytes.clone()) {
            let _ = b.iterate(&mut Collect::default());
            let _ = b.has_put();
        }
        let mut input = &bytes[..];
        while !input.is_empty() {
            let before = input.len();
            match read_record(&mut input) {
                Ok(_) => prop_assert!(input.len() < before),
                Err(_) => break,
            }
        }
    }

    #[test]
    fn mutated_batches_fail_cleanly(
        ops in prop::collection::vec(op(), 1..8),
        at in any::<prop::sample::Index>(),
        byte in any::<u8>(),
        cut in any::<prop::sample::Index>(),
    ) {
        let mut b = WriteBatch::new();
        for op in &ops {
            apply(&mut b, op);
        }
        let mut data = b.data().to_vec();
        let i = at.index(data.len());
        data[i] = byte;
        let mutated = WriteBatch::from_rep(data.clone()).unwrap();
        let mut seen = Collect::default();
        if mutated.iterate(&mut seen).is_ok() {
            // Accepted only if it is still a well-formed batch: its count is right.
            let counted = seen.0.iter().filter(|o| !matches!(o, Op::LogData(_))).count();
            prop_assert_eq!(counted, mutated.count() as usize);
        }
        // A truncated batch whose cut falls inside the records is never accepted as the
        // original.
        let n = HEADER + cut.index(data.len() - HEADER);
        let truncated = WriteBatch::from_rep(b.data()[..n].to_vec()).unwrap();
        if n < b.data_size() {
            let mut seen = Collect::default();
            let r = truncated.iterate(&mut seen);
            prop_assert!(r.is_err() || seen.0.len() < ops.len());
        }
    }
}

#[test]
fn unknown_tag_is_corruption() {
    let mut b = WriteBatch::new();
    b.put(b"k", b"v").unwrap();
    let mut data = b.data().to_vec();
    data.push(0x1A);
    let err = WriteBatch::from_rep(data)
        .unwrap()
        .iterate(&mut Collect::default())
        .unwrap_err();
    assert!(matches!(
        err,
        Error::Corruption {
            why: mantle_engine::Malformed::UnknownTag(0x1A),
            ..
        }
    ));
    // The header is no record: a batch shorter than it is refused.
    assert!(WriteBatch::from_rep(vec![0; HEADER - 1]).is_err());
}
