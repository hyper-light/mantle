//! The port's `WriteBatch` against RocksDB 11.8.1's (docs/research/24 §3.1 P2 differential):
//! 512 batches that RocksDB's own `WriteBatch` built from a SplitMix64-driven script
//! (`tests/golden/p2_batch_gen.cc`, output `tests/golden/p2_batch.txt`) are rebuilt by the port
//! from the same script and compared byte for byte, with each operation's outcome; the port
//! iterates every oracle batch, and three truncations of each, with the oracle's outcome; and
//! every oracle batch is decoded record by record and re-encoded to the same bytes.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

// The helpers other test binaries share; this one uses the generator only.
#[allow(dead_code)]
mod common;

use common::SplitMix64;
use mantle_engine::Error;
use mantle_engine::db::blob::blob_index::encode_blob;
use mantle_engine::db::dbformat::{MAX_SEQUENCE_NUMBER, ValueType};
use mantle_engine::db::wide::wide_columns::WideColumn;
use mantle_engine::db::write_batch::{
    Handler, Record, WriteBatch, pack_value_and_write_time, read_record,
};
use mantle_engine::util::coding::{put_length_prefixed_slice, put_varint32};

const GOLDEN: &str = include_str!("golden/p2_batch.txt");
const HEADER: usize = 12;

struct Gen(SplitMix64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0.next()
    }
    fn bytes(&mut self, max: u64) -> Vec<u8> {
        let mut n = self.next() % (max + 1);
        if self.next().is_multiple_of(16) {
            n += 200;
        }
        (0..n).map(|_| (self.next() & 0xff) as u8).collect()
    }
    fn cf(&mut self) -> u32 {
        if self.next().is_multiple_of(2) {
            0
        } else {
            1 + (self.next() % 300) as u32
        }
    }
}

struct AcceptAll;

impl Handler for AcceptAll {
    fn put_cf(&mut self, _: u32, _: &[u8], _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn timed_put_cf(&mut self, _: u32, _: &[u8], _: &[u8], _: u64) -> Result<(), Error> {
        Ok(())
    }
    fn put_entity_cf(&mut self, _: u32, _: &[u8], _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn delete_cf(&mut self, _: u32, _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn single_delete_cf(&mut self, _: u32, _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn delete_range_cf(&mut self, _: u32, _: &[u8], _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn merge_cf(&mut self, _: u32, _: &[u8], _: &[u8]) -> Result<(), Error> {
        Ok(())
    }
    fn put_blob_index_cf(&mut self, _: u32, _: &[u8], _: &[u8]) -> Result<(), Error> {
        Ok(())
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

fn outcome(r: &Result<(), Error>) -> char {
    match r {
        Ok(()) => 'O',
        Err(Error::Corruption { .. }) => 'C',
        Err(_) => 'X',
    }
}

fn ok(r: Result<(), Error>) -> char {
    if r.is_ok() { '1' } else { '0' }
}

/// One batch as p2_batch_gen.cc builds it: the batch, the outcomes, the truncation points.
fn build(g: &mut Gen) -> (WriteBatch, String) {
    let max_bytes = if g.next().is_multiple_of(8) {
        12 + (g.next() % 200) as usize
    } else {
        0
    };
    let mut b = WriteBatch::with_max_bytes(max_bytes);
    let two_pc = g.next().is_multiple_of(8);
    if two_pc {
        b.insert_noop();
    }
    let mut outcomes = String::new();
    let nops = g.next() % 10;
    for _ in 0..nops {
        let kind = g.next() % 14;
        let s = match kind {
            0 => {
                let cf = g.cf();
                let k = g.bytes(20);
                let v = g.bytes(20);
                Some(b.put_cf(cf, &k, &v))
            }
            1 => {
                let cf = g.cf();
                let k = g.bytes(20);
                Some(b.delete_cf(cf, &k))
            }
            2 => {
                let cf = g.cf();
                let k = g.bytes(20);
                Some(b.single_delete_cf(cf, &k))
            }
            3 => {
                let cf = g.cf();
                let k1 = g.bytes(20);
                let k2 = g.bytes(20);
                Some(b.delete_range_cf(cf, &k1, &k2))
            }
            4 => {
                let cf = g.cf();
                let k = g.bytes(20);
                let v = g.bytes(20);
                Some(b.merge_cf(cf, &k, &v))
            }
            5 => {
                let cf = g.cf();
                let k = g.bytes(20);
                let ncols = g.next() % 4;
                let mut names = Vec::new();
                let mut values = Vec::new();
                for _ in 0..ncols {
                    names.push(g.bytes(2));
                    values.push(g.bytes(10));
                }
                let cols: Vec<WideColumn<'_>> = names
                    .iter()
                    .zip(&values)
                    .map(|(n, v)| WideColumn::new(n, v))
                    .collect();
                Some(b.put_entity(cf, &k, &cols))
            }
            6 => {
                let blob = g.bytes(10);
                Some(b.put_log_data(&blob))
            }
            7 => {
                let cf = g.cf();
                let k = g.bytes(20);
                let v = g.bytes(20);
                let t = if !g.next().is_multiple_of(4) {
                    g.next()
                } else {
                    u64::MAX
                };
                Some(b.timed_put(cf, &k, &v, t))
            }
            8 => {
                let cf = g.cf();
                let k = g.bytes(20);
                let file = g.next() % 1000;
                let off = g.next() % 100_000;
                let size = g.next() % 5000;
                let mut bi = Vec::new();
                encode_blob(&mut bi, file, off, size, 0);
                Some(b.put_blob_index(cf, &k, &bi))
            }
            9 => Some(b.set_save_point()),
            10 => Some(match b.rollback_to_save_point() {
                Ok(true) => Ok(()),
                Ok(false) => Err(Error::InvalidArgument { what: "not found" }),
                Err(e) => Err(e),
            }),
            11 => Some(if b.pop_save_point() {
                Ok(())
            } else {
                Err(Error::InvalidArgument { what: "not found" })
            }),
            12 => {
                let cf = g.cf();
                let k1 = g.bytes(8);
                let k2 = g.bytes(8);
                let v1 = g.bytes(8);
                let v2 = g.bytes(8);
                let v3 = g.bytes(8);
                Some(b.put_parts(cf, &[&k1, &k2], &[&v1, &v2, &v3]))
            }
            _ => {
                if two_pc {
                    None
                } else {
                    b.clear();
                    Some(Ok(()))
                }
            }
        };
        if let Some(s) = s {
            outcomes.push(ok(s));
        }
    }
    if two_pc {
        let wac = g.next().is_multiple_of(2);
        let unprepared = g.next().is_multiple_of(2);
        let xid = g.bytes(6);
        let mut s = b.mark_end_prepare(&xid, wac, unprepared);
        outcomes.push(ok(s.clone()));
        match g.next() % 4 {
            1 => s = b.mark_commit(&xid),
            2 => s = b.mark_rollback(&xid),
            3 => {
                let mut ts = g.bytes(8);
                ts.push(b't');
                s = b.mark_commit_with_timestamp(&xid, &ts);
            }
            _ => {}
        }
        outcomes.push(ok(s));
    }
    b.set_sequence(g.next() & MAX_SEQUENCE_NUMBER);
    if g.next().is_multiple_of(8) {
        let mut c = WriteBatch::new();
        let k1 = g.bytes(8);
        let v1 = g.bytes(8);
        let s = c.put(&k1, &v1);
        c.mark_wal_termination_point();
        let k2 = g.bytes(8);
        let s2 = c.delete(&k2);
        let wal_only = g.next().is_multiple_of(2);
        let s3 = WriteBatch::append(&mut b, &c, wal_only);
        outcomes.push(if s.is_ok() && s2.is_ok() && s3.is_ok() {
            '1'
        } else {
            '0'
        });
    }
    if outcomes.is_empty() {
        outcomes.push('-');
    }
    (b, outcomes)
}

/// Re-encodes a decoded record, choosing the other-CF tag exactly when the id is not 0.
fn encode(rec: &Record<'_>, out: &mut Vec<u8>) {
    let tagged = |out: &mut Vec<u8>, cf: u32, t: ValueType, cf_t: ValueType| {
        if cf == 0 {
            out.push(t.as_u8());
        } else {
            out.push(cf_t.as_u8());
            put_varint32(out, cf);
        }
    };
    let lp = |out: &mut Vec<u8>, s: &[u8]| put_length_prefixed_slice(out, s).unwrap();
    use ValueType as V;
    match *rec {
        Record::Put { cf, key, value } => {
            tagged(out, cf, V::Value, V::ColumnFamilyValue);
            lp(out, key);
            lp(out, value);
        }
        Record::TimedPut {
            cf,
            key,
            value,
            write_unix_time,
        } => {
            tagged(
                out,
                cf,
                V::ValuePreferredSeqno,
                V::ColumnFamilyValuePreferredSeqno,
            );
            lp(out, key);
            lp(out, &pack_value_and_write_time(value, write_unix_time));
        }
        Record::Delete { cf, key } => {
            tagged(out, cf, V::Deletion, V::ColumnFamilyDeletion);
            lp(out, key);
        }
        Record::SingleDelete { cf, key } => {
            tagged(out, cf, V::SingleDeletion, V::ColumnFamilySingleDeletion);
            lp(out, key);
        }
        Record::DeleteRange { cf, begin, end } => {
            tagged(out, cf, V::RangeDeletion, V::ColumnFamilyRangeDeletion);
            lp(out, begin);
            lp(out, end);
        }
        Record::Merge { cf, key, value } => {
            tagged(out, cf, V::Merge, V::ColumnFamilyMerge);
            lp(out, key);
            lp(out, value);
        }
        Record::BlobIndex { cf, key, value } => {
            tagged(out, cf, V::BlobIndex, V::ColumnFamilyBlobIndex);
            lp(out, key);
            lp(out, value);
        }
        Record::PutEntity { cf, key, entity } => {
            tagged(
                out,
                cf,
                V::WideColumnEntity,
                V::ColumnFamilyWideColumnEntity,
            );
            lp(out, key);
            lp(out, entity);
        }
        Record::LogData { blob } => {
            out.push(V::LogData.as_u8());
            lp(out, blob);
        }
        Record::Noop => out.push(V::Noop.as_u8()),
        Record::BeginPrepare { tag } => out.push(tag.as_u8()),
        Record::EndPrepare { xid } => {
            out.push(V::EndPrepareXid.as_u8());
            lp(out, xid);
        }
        Record::Commit { xid } => {
            out.push(V::CommitXid.as_u8());
            lp(out, xid);
        }
        Record::CommitWithTimestamp { xid, commit_ts } => {
            out.push(V::CommitXidAndTimestamp.as_u8());
            lp(out, commit_ts);
            lp(out, xid);
        }
        Record::Rollback { xid } => {
            out.push(V::RollbackXid.as_u8());
            lp(out, xid);
        }
    }
}

fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn batches_match_rocksdb() {
    let mut g = Gen(SplitMix64(0x5032_4241_5443_4831));
    let mut kinds = std::collections::HashSet::new();
    let lines: Vec<&str> = GOLDEN.lines().collect();
    assert_eq!(lines.len(), 512);
    for (i, line) in lines.iter().enumerate() {
        let fields: Vec<&str> = line.split(' ').collect();
        let oracle = hex(fields[0]);
        let (b, outcomes) = build(&mut g);
        assert_eq!(outcomes, fields[1], "batch {i}: outcomes");
        assert_eq!(b.data(), &oracle[..], "batch {i}: bytes");

        // Truncations, with the oracle's outcome.
        for t in &fields[2..5] {
            let k = if oracle.len() > HEADER {
                HEADER + (g.next() % (oracle.len() - HEADER) as u64) as usize
            } else {
                oracle.len()
            };
            let (want_k, want) = t.split_once(':').unwrap();
            assert_eq!(k.to_string(), want_k, "batch {i}: truncation point");
            let tb = WriteBatch::from_rep(oracle[..k].to_vec()).unwrap();
            let got = outcome(&tb.iterate(&mut AcceptAll));
            assert_eq!(got.to_string(), want, "batch {i}: truncated to {k}");
        }

        // The whole oracle batch iterates, and decodes to records that re-encode to it.
        let ob = WriteBatch::from_rep(oracle.clone()).unwrap();
        assert_eq!(outcome(&ob.iterate(&mut AcceptAll)).to_string(), fields[5]);
        let mut input = &oracle[HEADER..];
        let mut re = oracle[..HEADER].to_vec();
        while !input.is_empty() {
            let rec = read_record(&mut input).unwrap();
            kinds.insert(
                format!("{rec:?}")
                    .split([' ', '{'])
                    .next()
                    .unwrap()
                    .to_string(),
            );
            encode(&rec, &mut re);
        }
        assert_eq!(re, oracle, "batch {i}: re-encoded");
    }
    // Every record kind appeared but Noop, which MarkEndPrepare always turns into BeginPrepare
    // here: 14 of `Record`'s 15 variants.
    assert_eq!(kinds.len(), 14, "record kinds seen: {kinds:?}");
}
