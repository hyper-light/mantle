//! RocksDB's db/write_batch_test.cc, test for test: 28 of its 31 test definitions, with the same
//! literals. `PrintContents` applies the batch to a memtable through the port's
//! `MemTableInserter` and prints the memtable's point and range-tombstone iterators, as the C++
//! helper does.
//!
//! Left out, each with the phase that ports its feature:
//! - `ColumnFamiliesBatchWithIndexTest`: `WriteBatchWithIndex`, part of transactions (P17).
//! - `SanityChecks`, `UpdateTimestamps`: the timestamped `Put`/`Delete` forms and
//!   `UpdateTimestamps`, user-defined timestamps (P16).
//!
//! `DISABLED_ManyUpdates` and `DISABLED_LargeKeyValue` are ported and `#[ignore]`d for the
//! reason RocksDB disables them (more than 18 GB of memory). `LargeKeyValueSizeLimit` runs, as
//! in RocksDB, only on a host with at least 128 GiB of memory (RocksDB's `ROCKSDB_BIGMEM_TESTS`
//! override is not read: the port's tests read no environment).
//!
//! Where RocksDB's test compares `Status` strings, these compare typed errors; where it
//! compares a `NotFound` status of `RollbackToSavePoint`/`PopSavePoint`, these compare the
//! `false` the port returns.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[allow(dead_code)]
#[path = "support/random.rs"]
mod random;

use std::collections::HashMap;

use mantle_engine::Error;
use mantle_engine::Malformed;
use mantle_engine::db::blob::blob_index::{BlobIndex, encode_blob};
use mantle_engine::db::dbformat::{
    InternalKeyComparator, MAX_SEQUENCE_NUMBER, ValueType, parse_internal_key,
};
use mantle_engine::db::memtable::{MemTable, MemTableOptions};
use mantle_engine::db::wide::wide_column_serialization::serialize_v2;
use mantle_engine::db::wide::wide_columns::{AttributeGroup, WideColumn};
use mantle_engine::db::wide::wide_columns_helper::dump_slice_as_wide_columns;
use mantle_engine::db::write_batch::{Handler, WriteBatch, parse_packed_value_with_write_time};
use mantle_engine::util::coding::put_fixed64;
use mantle_engine::util::comparator::Comparator;
use random::Random;

/// `Random::RandomString`: `len` printable characters ' '..'~' [R util/random.cc:45-52].
fn random_string(rnd: &mut Random, len: usize) -> Vec<u8> {
    (0..len).map(|_| b' ' + rnd.uniform(95) as u8).collect()
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// `Slice::ToString(true)`: upper-case hex.
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

/// `PrintContents`: applies the batch to a new memtable (with a merge operator unless
/// `merge_operator_supported` is false) and prints its point entries, then its range
/// tombstones, each `Type(key[, value])@seq`; an insert failure is appended as its error, a
/// count that disagrees with the entries as `CountMismatch()`.
fn print_contents(b: &WriteBatch, merge_operator_supported: bool) -> String {
    let cmp = InternalKeyComparator::new(Comparator::Bytewise);
    let mut mem = MemTable::new(cmp, MemTableOptions::default(), MAX_SEQUENCE_NUMBER).unwrap();
    let mut state = String::new();
    let result = b.insert_into(&mut mem, merge_operator_supported);
    let mut count = 0u32;
    let (mut put_count, mut timed_put_count, mut delete_count) = (0, 0, 0);
    let (mut single_delete_count, mut delete_range_count, mut merge_count) = (0, 0, 0);
    let mut dump = |iter: &mut mantle_engine::db::memtable::MemTableIter<'_>| {
        iter.status().unwrap();
        iter.seek_to_first();
        while iter.valid() {
            let ikey = parse_internal_key(iter.key()).unwrap();
            let (k, v) = (s(ikey.user_key), s(iter.value()));
            match ikey.value_type {
                ValueType::Value => {
                    state += &format!("Put({k}, {v})");
                    put_count += 1;
                }
                ValueType::Deletion => {
                    state += &format!("Delete({k})");
                    delete_count += 1;
                }
                ValueType::SingleDeletion => {
                    state += &format!("SingleDelete({k})");
                    single_delete_count += 1;
                }
                ValueType::RangeDeletion => {
                    state += &format!("DeleteRange({k}, {v})");
                    delete_range_count += 1;
                }
                ValueType::Merge => {
                    state += &format!("Merge({k}, {v})");
                    merge_count += 1;
                }
                ValueType::ValuePreferredSeqno => {
                    let (unpacked_value, unix_write_time) =
                        parse_packed_value_with_write_time(iter.value()).unwrap();
                    state += &format!("TimedPut({k}, {}, {unix_write_time})", s(unpacked_value));
                    timed_put_count += 1;
                }
                other => panic!("unexpected type {other:?}"),
            }
            count += 1;
            state += &format!("@{}", ikey.sequence);
            iter.next();
        }
        iter.status().unwrap();
    };
    dump(&mut mem.iter());
    if let Some(mut range_del) = mem.range_del_iter() {
        dump(&mut range_del);
    }
    match result {
        Ok(_) => {
            assert_eq!(b.has_put(), put_count > 0);
            assert_eq!(b.has_timed_put(), timed_put_count > 0);
            assert_eq!(b.has_delete(), delete_count > 0);
            assert_eq!(b.has_single_delete(), single_delete_count > 0);
            assert_eq!(b.has_delete_range(), delete_range_count > 0);
            assert_eq!(b.has_merge(), merge_count > 0);
            if count != b.count() {
                state += "CountMismatch()";
            }
        }
        Err(e) => state += &e.to_string(),
    }
    state
}

fn pc(b: &WriteBatch) -> String {
    print_contents(b, true)
}

/// `TestHandler`.
#[derive(Default)]
struct TestHandler {
    seen: String,
}

impl Handler for TestHandler {
    fn put_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if cf == 0 {
            self.seen += &format!("Put({}, {})", s(key), s(value));
        } else {
            self.seen += &format!("PutCF({cf}, {}, {})", s(key), s(value));
        }
        Ok(())
    }
    fn timed_put_cf(
        &mut self,
        cf: u32,
        key: &[u8],
        value: &[u8],
        unix_write_time: u64,
    ) -> Result<(), Error> {
        if cf == 0 {
            self.seen += &format!("TimedPut({}, {}, {unix_write_time})", s(key), s(value));
        } else {
            self.seen += &format!(
                "TimedPutCF({cf}, {}, {}, {unix_write_time})",
                s(key),
                s(value)
            );
        }
        Ok(())
    }
    fn put_entity_cf(&mut self, cf: u32, key: &[u8], entity: &[u8]) -> Result<(), Error> {
        let mut oss = String::new();
        dump_slice_as_wide_columns(entity, &mut oss, false)?;
        if cf == 0 {
            self.seen += &format!("PutEntity({}, {oss})", s(key));
        } else {
            self.seen += &format!("PutEntityCF({cf}, {}, {oss})", s(key));
        }
        Ok(())
    }
    fn delete_cf(&mut self, cf: u32, key: &[u8]) -> Result<(), Error> {
        if cf == 0 {
            self.seen += &format!("Delete({})", s(key));
        } else {
            self.seen += &format!("DeleteCF({cf}, {})", s(key));
        }
        Ok(())
    }
    fn single_delete_cf(&mut self, cf: u32, key: &[u8]) -> Result<(), Error> {
        if cf == 0 {
            self.seen += &format!("SingleDelete({})", s(key));
        } else {
            self.seen += &format!("SingleDeleteCF({cf}, {})", s(key));
        }
        Ok(())
    }
    fn delete_range_cf(&mut self, cf: u32, begin: &[u8], end: &[u8]) -> Result<(), Error> {
        if cf == 0 {
            self.seen += &format!("DeleteRange({}, {})", s(begin), s(end));
        } else {
            self.seen += &format!("DeleteRangeCF({cf}, {}, {})", s(begin), s(end));
        }
        Ok(())
    }
    fn merge_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if cf == 0 {
            self.seen += &format!("Merge({}, {})", s(key), s(value));
        } else {
            self.seen += &format!("MergeCF({cf}, {}, {})", s(key), s(value));
        }
        Ok(())
    }
    fn log_data(&mut self, blob: &[u8]) {
        self.seen += &format!("LogData({})", s(blob));
    }
    fn mark_begin_prepare(&mut self, unprepare: bool) -> Result<(), Error> {
        self.seen += &format!("MarkBeginPrepare({unprepare})");
        Ok(())
    }
    fn mark_end_prepare(&mut self, xid: &[u8]) -> Result<(), Error> {
        self.seen += &format!("MarkEndPrepare({})", s(xid));
        Ok(())
    }
    fn mark_noop(&mut self, empty_batch: bool) -> Result<(), Error> {
        self.seen += &format!("MarkNoop({empty_batch})");
        Ok(())
    }
    fn mark_commit(&mut self, xid: &[u8]) -> Result<(), Error> {
        self.seen += &format!("MarkCommit({})", s(xid));
        Ok(())
    }
    fn mark_commit_with_timestamp(&mut self, xid: &[u8], ts: &[u8]) -> Result<(), Error> {
        self.seen += &format!("MarkCommitWithTimestamp({}, {})", s(xid), hex(ts));
        Ok(())
    }
    fn mark_rollback(&mut self, xid: &[u8]) -> Result<(), Error> {
        self.seen += &format!("MarkRollback({})", s(xid));
        Ok(())
    }
}

/// `ReplayUntilCountHandler`: buffers prepared sections and replays them on commit, stopping
/// after `max_write_ops` writes.
struct ReplayUntilCountHandler {
    seen: String,
    max_write_ops: u32,
    num_write_ops: u32,
    prepared_writes: HashMap<Vec<u8>, WriteBatch>,
    buffered_writes: Option<WriteBatch>,
}

impl ReplayUntilCountHandler {
    fn new(max_write_ops: u32) -> Self {
        Self {
            seen: String::new(),
            max_write_ops,
            num_write_ops: 0,
            prepared_writes: HashMap::new(),
            buffered_writes: None,
        }
    }
}

impl Handler for ReplayUntilCountHandler {
    fn should_continue(&mut self) -> bool {
        self.num_write_ops < self.max_write_ops
    }
    fn put_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if let Some(b) = self.buffered_writes.as_mut() {
            return b.put_cf(cf, key, value);
        }
        self.seen += &format!("Put({}, {})", s(key), s(value));
        self.num_write_ops += 1;
        Ok(())
    }
    fn delete_cf(&mut self, cf: u32, key: &[u8]) -> Result<(), Error> {
        if let Some(b) = self.buffered_writes.as_mut() {
            return b.delete_cf(cf, key);
        }
        self.seen += &format!("Delete({})", s(key));
        self.num_write_ops += 1;
        Ok(())
    }
    fn mark_begin_prepare(&mut self, _unprepare: bool) -> Result<(), Error> {
        assert!(self.buffered_writes.is_none());
        self.buffered_writes = Some(WriteBatch::new());
        Ok(())
    }
    fn mark_end_prepare(&mut self, xid: &[u8]) -> Result<(), Error> {
        let b = self.buffered_writes.take().unwrap();
        self.prepared_writes.insert(xid.to_vec(), b);
        Ok(())
    }
    fn mark_noop(&mut self, _empty_batch: bool) -> Result<(), Error> {
        Ok(())
    }
    fn mark_commit(&mut self, xid: &[u8]) -> Result<(), Error> {
        let Some(b) = self.prepared_writes.remove(xid) else {
            return Err(Error::InvalidArgument {
                what: "Missing prepared batch for commit",
            });
        };
        b.iterate(self)
    }
}

/// `WriteBatch::Handler` with every default.
struct DefaultHandler;
impl Handler for DefaultHandler {}

#[test]
fn ownership_transfer() {
    let mut rnd = Random::new(301);
    let mut put_batch = WriteBatch::new();
    put_batch
        .put(&random_string(&mut rnd, 16), &random_string(&mut rnd, 1024))
        .unwrap();

    // (1) Verify `Release()` transfers string data ownership
    let expected_data = put_batch.data().as_ptr();
    let batch_str = put_batch.release();
    assert_eq!(expected_data, batch_str.as_ptr());

    // (2) Verify constructor transfers string data ownership
    let move_batch = WriteBatch::from_rep(batch_str).unwrap();
    assert_eq!(expected_data, move_batch.data().as_ptr());
}

#[test]
fn prepare_commit() {
    let mut batch = WriteBatch::new();
    batch.insert_noop();
    batch.put(b"k1", b"v1").unwrap();
    batch.put(b"k2", b"v2").unwrap();
    batch.set_save_point().unwrap();
    batch.mark_end_prepare(b"xid1", true, false).unwrap();
    // MarkEndPrepare drops the save points: RocksDB's NotFound.
    assert!(!batch.rollback_to_save_point().unwrap());
    batch.mark_commit(b"xid1").unwrap();
    batch.mark_rollback(b"xid1").unwrap();
    assert_eq!(2, batch.count());

    let mut handler = TestHandler::default();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(
        "MarkBeginPrepare(false)\
         Put(k1, v1)\
         Put(k2, v2)\
         MarkEndPrepare(xid1)\
         MarkCommit(xid1)\
         MarkRollback(xid1)",
        handler.seen
    );
}

// Expected-state restore can target a sequence number in the middle of a traced
// multi-op write batch. Verify `Continue()` stops iteration cleanly there.
#[test]
fn continue_stops_mid_batch() {
    let mut batch = WriteBatch::new();
    batch.put(b"k1", b"v1").unwrap();
    batch.delete(b"k2").unwrap();
    batch.put(b"k3", b"v3").unwrap();

    let mut handler = ReplayUntilCountHandler::new(2);
    batch.iterate(&mut handler).unwrap();
    assert_eq!(2, handler.num_write_ops);
    assert_eq!("Put(k1, v1)Delete(k2)", handler.seen);
}

// Regression test for restore replay stopping inside a committed prepared
// batch. The handler buffers prepare contents and replays them on commit,
// matching the expected-state restore logic.
#[test]
fn continue_stops_mid_prepared_commit_replay() {
    let mut batch = WriteBatch::new();
    batch.insert_noop();
    batch.put(b"k1", b"v1").unwrap();
    batch.put(b"k2", b"v2").unwrap();
    batch.set_save_point().unwrap();
    batch.mark_end_prepare(b"xid1", true, false).unwrap();
    assert!(!batch.rollback_to_save_point().unwrap());
    batch.mark_commit(b"xid1").unwrap();

    let mut handler = ReplayUntilCountHandler::new(1);
    batch.iterate(&mut handler).unwrap();
    assert_eq!(1, handler.num_write_ops);
    assert_eq!("Put(k1, v1)", handler.seen);
}

/// The handler of `DISABLED_ManyUpdates` and `DISABLED_LargeKeyValue`: checks each put and
/// fails on anything else.
struct NoopHandler<F: FnMut(&[u8], &[u8])> {
    num_seen: u32,
    limit: u32,
    check: F,
}

impl<F: FnMut(&[u8], &[u8])> Handler for NoopHandler<F> {
    fn put_cf(&mut self, _cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (self.check)(key, value);
        self.num_seen += 1;
        Ok(())
    }
    fn delete_cf(&mut self, _: u32, _: &[u8]) -> Result<(), Error> {
        panic!("unexpected Delete");
    }
    fn single_delete_cf(&mut self, _: u32, _: &[u8]) -> Result<(), Error> {
        panic!("unexpected SingleDelete");
    }
    fn merge_cf(&mut self, _: u32, _: &[u8], _: &[u8]) -> Result<(), Error> {
        panic!("unexpected Merge");
    }
    fn log_data(&mut self, _: &[u8]) {
        panic!("unexpected LogData");
    }
    fn should_continue(&mut self) -> bool {
        self.num_seen < self.limit
    }
}

// It requires more than 30GB of memory to run the test. With single memory
// allocation of more than 30GB.
// Not all platform can run it. Also it runs a long time. So disable it.
#[test]
#[ignore = "needs more than 30 GB of memory, as RocksDB's DISABLED_ManyUpdates"]
fn disabled_many_updates() {
    // Insert key and value of 3GB and push total batch size to 12GB.
    const KEY_VALUE_SIZE: usize = 4;
    const NUM_UPDATES: u32 = 3 << 30;
    let mut raw = vec![b'A'; KEY_VALUE_SIZE];
    let mut batch =
        WriteBatch::with_max_bytes(NUM_UPDATES as usize * (4 + KEY_VALUE_SIZE * 2) + 1024);
    let mut c = b'A';
    for _ in 0..NUM_UPDATES {
        if c > b'Z' {
            c = b'A';
        }
        raw[0] = c;
        raw[KEY_VALUE_SIZE - 1] = c;
        c += 1;
        batch.put(&raw, &raw).unwrap();
    }

    assert_eq!(NUM_UPDATES, batch.count());

    let mut expected_char = b'A';
    let mut handler = NoopHandler {
        num_seen: 0,
        limit: NUM_UPDATES,
        check: |key: &[u8], value: &[u8]| {
            assert_eq!(KEY_VALUE_SIZE, key.len());
            assert_eq!(KEY_VALUE_SIZE, value.len());
            assert_eq!(expected_char, key[0]);
            assert_eq!(expected_char, value[0]);
            assert_eq!(expected_char, key[KEY_VALUE_SIZE - 1]);
            assert_eq!(expected_char, value[KEY_VALUE_SIZE - 1]);
            expected_char += 1;
            if expected_char > b'Z' {
                expected_char = b'A';
            }
        },
    };
    batch.iterate(&mut handler).unwrap();
    assert_eq!(NUM_UPDATES, handler.num_seen);
}

// The test requires more than 18GB memory to run it, with single memory
// allocation of more than 12GB. Not all the platform can run it. So disable it.
#[test]
#[ignore = "needs more than 18 GB of memory, as RocksDB's DISABLED_LargeKeyValue"]
fn disabled_large_key_value() {
    // Insert key and value of 3GB and push total batch size to 12GB.
    const KEY_VALUE_SIZE: usize = 3_221_225_472;
    let mut raw = vec![b'A'; KEY_VALUE_SIZE];
    let mut batch = WriteBatch::with_max_bytes(12_884_901_888 + 1024);
    for i in 0..2u8 {
        raw[0] = b'A' + i;
        raw[KEY_VALUE_SIZE - 1] = b'A' - i;
        batch.put(&raw, &raw).unwrap();
    }

    assert_eq!(2, batch.count());

    let mut n = 0u8;
    let mut handler = NoopHandler {
        num_seen: 0,
        limit: 2,
        check: |key: &[u8], value: &[u8]| {
            assert_eq!(KEY_VALUE_SIZE, key.len());
            assert_eq!(KEY_VALUE_SIZE, value.len());
            assert_eq!(b'A' + n, key[0]);
            assert_eq!(b'A' + n, value[0]);
            assert_eq!(b'A' - n, key[KEY_VALUE_SIZE - 1]);
            assert_eq!(b'A' - n, value[KEY_VALUE_SIZE - 1]);
            n += 1;
        },
    };
    batch.iterate(&mut handler).unwrap();
    assert_eq!(2, handler.num_seen);
}

/// `test::HasBigMem` [R test_util/testharness.cc:106-124]: at least 128 GiB of physical memory.
/// RocksDB also takes `ROCKSDB_BIGMEM_TESTS`; the port's tests read no environment, so the host's
/// memory alone decides.
fn has_big_mem() -> bool {
    let physical = if cfg!(target_os = "macos") {
        std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
    } else {
        std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| {
            m.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
    };
    physical.is_some_and(|b| b >= 128 << 30)
}

// A zeroed `vec!` is allocated lazily zeroed (calloc), so the large inputs themselves take no
// physical memory; only the batch's copy does (~4 GB at the accepted limit).
#[test]
fn large_key_value_size_limit() {
    if !has_big_mem() {
        eprintln!("bypassed: insufficient memory for reliable continuous testing");
        return;
    }
    const MAX_KEY_SIZE: usize = u32::MAX as usize - 8;
    const MAX_VALUE_SIZE: usize = u32::MAX as usize;
    let invalid = |r: Result<(), Error>| matches!(r, Err(Error::InvalidArgument { .. }));

    let mut batch = WriteBatch::new();

    // --- Large key ---
    {
        let mm = vec![0u8; MAX_KEY_SIZE + 1];

        // A key at the limit should be accepted
        batch.put(&mm[..MAX_KEY_SIZE], b"val").unwrap();
        batch.clear();

        // A key one byte over the limit should be rejected
        assert!(invalid(batch.put(&mm, b"val")));
        assert!(invalid(batch.merge(&mm, b"val")));
        assert!(invalid(batch.delete(&mm)));
        assert!(invalid(batch.single_delete(&mm)));
        assert!(invalid(batch.delete_range(&mm, &mm)));
    }

    // --- Large value ---
    {
        let mm = vec![0u8; MAX_VALUE_SIZE + 1];

        // A value at the limit should be accepted
        batch.put(b"key", &mm[..MAX_VALUE_SIZE]).unwrap();
        batch.clear();

        // A value one byte over the limit should be rejected
        assert!(invalid(batch.put(b"key", &mm)));
        assert!(invalid(batch.merge(b"key", &mm)));
    }
}

/// `Continue`'s handler: a `TestHandler` that stops after five calls.
#[derive(Default)]
struct ContinueHandler {
    inner: TestHandler,
    num_seen: u32,
}

impl Handler for ContinueHandler {
    fn put_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.num_seen += 1;
        self.inner.put_cf(cf, key, value)
    }
    fn delete_cf(&mut self, cf: u32, key: &[u8]) -> Result<(), Error> {
        self.num_seen += 1;
        self.inner.delete_cf(cf, key)
    }
    fn single_delete_cf(&mut self, cf: u32, key: &[u8]) -> Result<(), Error> {
        self.num_seen += 1;
        self.inner.single_delete_cf(cf, key)
    }
    fn merge_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.num_seen += 1;
        self.inner.merge_cf(cf, key, value)
    }
    fn log_data(&mut self, blob: &[u8]) {
        self.num_seen += 1;
        self.inner.log_data(blob);
    }
    fn should_continue(&mut self) -> bool {
        self.num_seen < 5
    }
}

#[test]
fn continue_() {
    let mut batch = WriteBatch::new();
    let mut handler = ContinueHandler::default();

    batch.put(b"k1", b"v1").unwrap();
    batch.put(b"k2", b"v2").unwrap();
    batch.put_log_data(b"blob1").unwrap();
    batch.delete(b"k1").unwrap();
    batch.single_delete(b"k2").unwrap();
    batch.put_log_data(b"blob2").unwrap();
    batch.merge(b"foo", b"bar").unwrap();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(
        "Put(k1, v1)\
         Put(k2, v2)\
         LogData(blob1)\
         Delete(k1)\
         SingleDelete(k2)",
        handler.inner.seen
    );
}

fn two_columns<'a>(a: (&'a str, &'a str), b: (&'a str, &'a str)) -> Vec<WideColumn<'a>> {
    vec![WideColumn::new(a.0, a.1), WideColumn::new(b.0, b.1)]
}

#[test]
fn attribute_group_test() {
    let mut batch = WriteBatch::new();
    let zero_col_1_col_2 = two_columns(("0_c_1_n", "0_c_1_v"), ("0_c_2_n", "0_c_2_v"));
    let two_col_1_col_2 = two_columns(("2_c_1_n", "2_c_1_v"), ("2_c_2_n", "2_c_2_v"));
    let foo_ags = vec![
        AttributeGroup::new(0, zero_col_1_col_2),
        AttributeGroup::new(2, two_col_1_col_2),
    ];

    batch.put_entity_attribute_groups(b"foo", &foo_ags).unwrap();

    let mut handler = TestHandler::default();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(
        "PutEntity(foo, 0_c_1_n:0_c_1_v \
         0_c_2_n:0_c_2_v)\
         PutEntityCF(2, foo, 2_c_1_n:2_c_1_v \
         2_c_2_n:2_c_2_v)",
        handler.seen
    );
}

#[test]
fn attribute_group_save_point_test() {
    let mut batch = WriteBatch::new();
    batch.set_save_point().unwrap();

    let zero_col_1_col_2 = two_columns(("0_c_1_n", "0_c_1_v"), ("0_c_2_n", "0_c_2_v"));
    let two_col_1_col_2 = two_columns(("2_c_1_n", "2_c_1_v"), ("2_c_2_n", "2_c_2_v"));
    let foo_ags = vec![
        AttributeGroup::new(0, zero_col_1_col_2.clone()),
        AttributeGroup::new(2, two_col_1_col_2),
    ];

    let three_col_1_col_2 = two_columns(("3_c_1_n", "3_c_1_v"), ("3_c_2_n", "3_c_2_v"));
    let bar_ags = vec![
        AttributeGroup::new(0, zero_col_1_col_2),
        AttributeGroup::new(3, three_col_1_col_2),
    ];

    batch.put_entity_attribute_groups(b"foo", &foo_ags).unwrap();
    batch.set_save_point().unwrap();

    batch.put_entity_attribute_groups(b"bar", &bar_ags).unwrap();

    let mut handler = TestHandler::default();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(
        "PutEntity(foo, 0_c_1_n:0_c_1_v 0_c_2_n:0_c_2_v)\
         PutEntityCF(2, foo, 2_c_1_n:2_c_1_v 2_c_2_n:2_c_2_v)\
         PutEntity(bar, 0_c_1_n:0_c_1_v 0_c_2_n:0_c_2_v)\
         PutEntityCF(3, bar, 3_c_1_n:3_c_1_v 3_c_2_n:3_c_2_v)",
        handler.seen
    );

    assert!(batch.rollback_to_save_point().unwrap());

    handler.seen.clear();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(
        "PutEntity(foo, 0_c_1_n:0_c_1_v 0_c_2_n:0_c_2_v)\
         PutEntityCF(2, foo, 2_c_1_n:2_c_1_v 2_c_2_n:2_c_2_v)",
        handler.seen
    );
}

/// `IterateCanRebuildSerializedV2Entity`'s handler: rebuilds the batch from its records.
#[derive(Default)]
struct RebuildHandler {
    rebuilt: WriteBatch,
}

impl Handler for RebuildHandler {
    fn put_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.rebuilt.put_cf(cf, key, value)
    }
    fn put_entity_cf(&mut self, cf: u32, key: &[u8], entity: &[u8]) -> Result<(), Error> {
        self.rebuilt.put_entity_serialized(cf, key, entity)
    }
}

#[test]
fn iterate_can_rebuild_serialized_v2_entity() {
    let mut batch = WriteBatch::new();

    let mut encoded_blob_index = Vec::new();
    encode_blob(
        &mut encoded_blob_index,
        9,
        123,
        456,
        0, /* kNoCompression */
    );
    let blob_index = BlobIndex::decode_from(&encoded_blob_index).unwrap();

    let columns = [
        WideColumn::new("", "default_inline"),
        WideColumn::new("ttl", "00000001"),
    ];
    let mut serialized_entity = Vec::new();
    serialize_v2(&columns, &[(0, blob_index)], &mut serialized_entity).unwrap();
    batch
        .put_entity_serialized(7, b"key", &serialized_entity)
        .unwrap();

    let mut handler = RebuildHandler::default();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(handler.rebuilt.count(), batch.count());
    assert_eq!(handler.rebuilt.data(), batch.data());
}

#[test]
fn column_families_batch_test() {
    let mut batch = WriteBatch::new();
    let (zero, two, three, eight) = (0, 2, 3, 8);
    batch.put_cf(zero, b"foo", b"bar").unwrap();
    batch.put_cf(two, b"twofoo", b"bar2").unwrap();
    batch.put_cf(eight, b"eightfoo", b"bar8").unwrap();
    batch.delete_cf(eight, b"eightfoo").unwrap();
    batch.single_delete_cf(two, b"twofoo").unwrap();
    batch.delete_range_cf(two, b"3foo", b"4foo").unwrap();
    batch.merge_cf(three, b"threethree", b"3three").unwrap();
    batch.put_cf(zero, b"foo", b"bar").unwrap();
    batch.merge(b"omom", b"nom").unwrap();
    batch.timed_put(zero, b"foo", b"bar", 0).unwrap();

    let mut handler = TestHandler::default();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(
        "Put(foo, bar)\
         PutCF(2, twofoo, bar2)\
         PutCF(8, eightfoo, bar8)\
         DeleteCF(8, eightfoo)\
         SingleDeleteCF(2, twofoo)\
         DeleteRangeCF(2, 3foo, 4foo)\
         MergeCF(3, threethree, 3three)\
         Put(foo, bar)\
         Merge(omom, nom)\
         TimedPut(foo, bar, 0)",
        handler.seen
    );
}

#[test]
fn memory_limit_test() {
    // The header size is 12 bytes. The two Puts take 8 bytes which gives total
    // of 12 + 8 * 2 = 28 bytes.
    let mut batch = WriteBatch::with_max_bytes(28);

    batch.put(b"a", b"....").unwrap();
    batch.put(b"b", b"....").unwrap();
    let s = batch.put(b"c", b"....");
    assert!(matches!(s, Err(Error::LimitExceeded { .. })));
}

#[test]
fn commit_with_timestamp() {
    let mut wb = WriteBatch::new();
    let txn_name = b"xid1";
    let mut ts = Vec::new();
    let commit_ts: u64 = 23;
    put_fixed64(&mut ts, commit_ts);
    wb.mark_commit_with_timestamp(txn_name, &ts).unwrap();
    let mut handler = TestHandler::default();
    wb.iterate(&mut handler).unwrap();
    assert_eq!(
        format!("MarkCommitWithTimestamp({}, {})", s(txn_name), hex(&ts)),
        handler.seen
    );
}

#[test]
fn empty() {
    let batch = WriteBatch::new();
    assert_eq!("", pc(&batch));
    assert_eq!(0, batch.count());
}

#[test]
fn multiple() {
    let mut batch = WriteBatch::new();
    batch.put(b"foo", b"bar").unwrap();
    batch.delete(b"box").unwrap();
    batch.delete_range(b"bar", b"foo").unwrap();
    batch.put(b"baz", b"boo").unwrap();
    batch.set_sequence(100);
    assert_eq!(100, batch.sequence());
    assert_eq!(4, batch.count());
    assert_eq!(
        "Put(baz, boo)@103\
         Delete(box)@101\
         Put(foo, bar)@100\
         DeleteRange(bar, foo)@102",
        pc(&batch)
    );
    assert_eq!(4, batch.count());
}

#[test]
fn corruption() {
    let mut batch = WriteBatch::new();
    batch.put(b"foo", b"bar").unwrap();
    batch.delete(b"box").unwrap();
    batch.set_sequence(200);
    let contents = batch.data().to_vec();
    batch.set_contents(&contents[..contents.len() - 1]).unwrap();
    // RocksDB: "Corruption: bad WriteBatch Delete".
    let bad_delete = Error::Corruption {
        what: "WriteBatch Delete",
        why: Malformed::Truncated,
    };
    assert_eq!(format!("Put(foo, bar)@200{bad_delete}"), pc(&batch));
}

#[test]
fn append() {
    let mut b1 = WriteBatch::new();
    let mut b2 = WriteBatch::new();
    b1.set_sequence(200);
    b2.set_sequence(300);
    WriteBatch::append(&mut b1, &b2, false).unwrap();
    assert_eq!("", pc(&b1));
    assert_eq!(0, b1.count());
    b2.put(b"a", b"va").unwrap();
    WriteBatch::append(&mut b1, &b2, false).unwrap();
    assert_eq!("Put(a, va)@200", pc(&b1));
    assert_eq!(1, b1.count());
    b2.clear();
    b2.put(b"b", b"vb").unwrap();
    WriteBatch::append(&mut b1, &b2, false).unwrap();
    assert_eq!(
        "Put(a, va)@200\
         Put(b, vb)@201",
        pc(&b1)
    );
    assert_eq!(2, b1.count());
    b2.delete(b"foo").unwrap();
    WriteBatch::append(&mut b1, &b2, false).unwrap();
    assert_eq!(
        "Put(a, va)@200\
         Put(b, vb)@202\
         Put(b, vb)@201\
         Delete(foo)@203",
        pc(&b1)
    );
    assert_eq!(4, b1.count());
    b2.clear();
    b2.put(b"c", b"cc").unwrap();
    b2.put(b"d", b"dd").unwrap();
    b2.mark_wal_termination_point();
    b2.put(b"e", b"ee").unwrap();
    WriteBatch::append(&mut b1, &b2, /*wal only*/ true).unwrap();
    assert_eq!(
        "Put(a, va)@200\
         Put(b, vb)@202\
         Put(b, vb)@201\
         Put(c, cc)@204\
         Put(d, dd)@205\
         Delete(foo)@203",
        pc(&b1)
    );
    assert_eq!(6, b1.count());
    assert_eq!(
        "Put(c, cc)@0\
         Put(d, dd)@1\
         Put(e, ee)@2",
        pc(&b2)
    );
    assert_eq!(3, b2.count());
}

#[test]
fn single_deletion() {
    let mut batch = WriteBatch::new();
    batch.set_sequence(100);
    assert_eq!("", pc(&batch));
    assert_eq!(0, batch.count());
    batch.put(b"a", b"va").unwrap();
    assert_eq!("Put(a, va)@100", pc(&batch));
    assert_eq!(1, batch.count());
    batch.single_delete(b"a").unwrap();
    assert_eq!(
        "SingleDelete(a)@101\
         Put(a, va)@100",
        pc(&batch)
    );
    assert_eq!(2, batch.count());
}

#[test]
fn put_not_implemented() {
    let mut batch = WriteBatch::new();
    batch.put(b"k1", b"v1").unwrap();
    assert_eq!(1, batch.count());
    assert_eq!("Put(k1, v1)@0", pc(&batch));

    batch.iterate(&mut DefaultHandler).unwrap();
}

#[test]
fn timed_put_not_implemented() {
    let mut batch = WriteBatch::new();
    batch
        .timed_put(0, b"k1", b"v1", /*write_unix_time=*/ 30)
        .unwrap();
    assert_eq!(1, batch.count());
    assert_eq!("TimedPut(k1, v1, 30)@0", pc(&batch));

    assert!(matches!(
        batch.iterate(&mut DefaultHandler),
        Err(Error::InvalidArgument { .. })
    ));

    batch.clear();
    batch.timed_put(0, b"k1", b"v1", u64::MAX).unwrap();
    assert_eq!(1, batch.count());
    assert_eq!("Put(k1, v1)@0", pc(&batch));
}

#[test]
fn delete_not_implemented() {
    let mut batch = WriteBatch::new();
    batch.delete(b"k2").unwrap();
    assert_eq!(1, batch.count());
    assert_eq!("Delete(k2)@0", pc(&batch));

    batch.iterate(&mut DefaultHandler).unwrap();
}

#[test]
fn single_delete_not_implemented() {
    let mut batch = WriteBatch::new();
    batch.single_delete(b"k2").unwrap();
    assert_eq!(1, batch.count());
    assert_eq!("SingleDelete(k2)@0", pc(&batch));

    batch.iterate(&mut DefaultHandler).unwrap();
}

#[test]
fn merge_not_implemented() {
    let mut batch = WriteBatch::new();
    batch.merge(b"foo", b"bar").unwrap();
    assert_eq!(1, batch.count());
    assert_eq!("Merge(foo, bar)@0", pc(&batch));

    batch.iterate(&mut DefaultHandler).unwrap();
}

#[test]
fn merge_without_operator_insertion_failure() {
    let mut batch = WriteBatch::new();
    batch.merge(b"foo", b"bar").unwrap();
    assert_eq!(1, batch.count());
    let refused = Error::InvalidArgument {
        what: "Merge requires `ColumnFamilyOptions::merge_operator != nullptr`",
    };
    assert_eq!(
        refused.to_string(),
        print_contents(&batch, false /* merge_operator_supported */)
    );
}

#[test]
fn blob() {
    let mut batch = WriteBatch::new();
    batch.put(b"k1", b"v1").unwrap();
    batch.put(b"k2", b"v2").unwrap();
    batch.put(b"k3", b"v3").unwrap();
    batch.put_log_data(b"blob1").unwrap();
    batch.delete(b"k2").unwrap();
    batch.single_delete(b"k3").unwrap();
    batch.put_log_data(b"blob2").unwrap();
    batch.merge(b"foo", b"bar").unwrap();
    assert_eq!(6, batch.count());
    assert_eq!(
        "Merge(foo, bar)@5\
         Put(k1, v1)@0\
         Delete(k2)@3\
         Put(k2, v2)@1\
         SingleDelete(k3)@4\
         Put(k3, v3)@2",
        pc(&batch)
    );

    let mut handler = TestHandler::default();
    batch.iterate(&mut handler).unwrap();
    assert_eq!(
        "Put(k1, v1)\
         Put(k2, v2)\
         Put(k3, v3)\
         LogData(blob1)\
         Delete(k2)\
         SingleDelete(k3)\
         LogData(blob2)\
         Merge(foo, bar)",
        handler.seen
    );
}

#[test]
fn put_gather_slices() {
    let mut batch = WriteBatch::new();
    batch.put(b"foo", b"bar").unwrap();

    {
        // Try a write where the key is one slice but the value is two
        let key_slice: [&[u8]; 1] = [b"baz"];
        let value_slices: [&[u8]; 2] = [b"header", b"payload"];
        batch.put_parts(0, &key_slice, &value_slices).unwrap();
    }

    {
        // One where the key is composite but the value is a single slice
        let key_slices: [&[u8]; 3] = [b"key", b"part2", b"part3"];
        let value_slice: [&[u8]; 1] = [b"value"];
        batch.put_parts(0, &key_slices, &value_slice).unwrap();
    }

    batch.set_sequence(100);
    assert_eq!(
        "Put(baz, headerpayload)@101\
         Put(foo, bar)@100\
         Put(keypart2part3, value)@102",
        pc(&batch)
    );
    assert_eq!(3, batch.count());
}

#[test]
fn save_point_test() {
    let mut batch = WriteBatch::new();
    batch.set_save_point().unwrap();

    batch.put(b"A", b"a").unwrap();
    batch.put(b"B", b"b").unwrap();
    batch.set_save_point().unwrap();

    batch.put(b"C", b"c").unwrap();
    batch.delete(b"A").unwrap();
    batch.set_save_point().unwrap();
    batch.set_save_point().unwrap();

    assert!(batch.rollback_to_save_point().unwrap());
    assert_eq!(
        "Delete(A)@3\
         Put(A, a)@0\
         Put(B, b)@1\
         Put(C, c)@2",
        pc(&batch)
    );

    assert!(batch.rollback_to_save_point().unwrap());
    assert!(batch.rollback_to_save_point().unwrap());
    assert_eq!(
        "Put(A, a)@0\
         Put(B, b)@1",
        pc(&batch)
    );

    batch.delete(b"A").unwrap();
    batch.put(b"B", b"bb").unwrap();

    assert!(batch.rollback_to_save_point().unwrap());
    assert_eq!("", pc(&batch));

    // NotFound.
    assert!(!batch.rollback_to_save_point().unwrap());
    assert_eq!("", pc(&batch));

    batch.put(b"D", b"d").unwrap();
    batch.delete(b"A").unwrap();

    batch.set_save_point().unwrap();

    batch.put(b"A", b"aaa").unwrap();

    assert!(batch.rollback_to_save_point().unwrap());
    assert_eq!(
        "Delete(A)@1\
         Put(D, d)@0",
        pc(&batch)
    );

    batch.set_save_point().unwrap();

    batch.put(b"D", b"d").unwrap();
    batch.delete(b"A").unwrap();

    assert!(batch.rollback_to_save_point().unwrap());
    assert_eq!(
        "Delete(A)@1\
         Put(D, d)@0",
        pc(&batch)
    );

    assert!(!batch.rollback_to_save_point().unwrap());
    assert_eq!(
        "Delete(A)@1\
         Put(D, d)@0",
        pc(&batch)
    );

    let mut batch2 = WriteBatch::new();

    assert!(!batch2.rollback_to_save_point().unwrap());
    assert_eq!("", pc(&batch2));

    batch2.delete(b"A").unwrap();
    batch2.set_save_point().unwrap();

    assert!(batch2.rollback_to_save_point().unwrap());
    assert_eq!("Delete(A)@0", pc(&batch2));

    batch2.clear();
    assert_eq!("", pc(&batch2));

    batch2.set_save_point().unwrap();

    batch2.delete(b"B").unwrap();
    assert_eq!("Delete(B)@0", pc(&batch2));

    batch2.set_save_point().unwrap();
    assert!(batch2.rollback_to_save_point().unwrap());
    assert_eq!("Delete(B)@0", pc(&batch2));

    assert!(batch2.rollback_to_save_point().unwrap());
    assert_eq!("", pc(&batch2));

    assert!(!batch2.rollback_to_save_point().unwrap());
    assert_eq!("", pc(&batch2));

    let mut batch3 = WriteBatch::new();

    assert!(!batch3.pop_save_point());
    assert_eq!("", pc(&batch3));

    batch3.set_save_point().unwrap();
    batch3.delete(b"A").unwrap();

    assert!(batch3.pop_save_point());
    assert_eq!("Delete(A)@0", pc(&batch3));
}

// The port's own: applying a two-phase-commit marker to a memtable is refused
// (docs/research/24 §1.4 DECISION), each marker by its tag, after the records before it.
#[test]
fn memtable_insert_refuses_two_phase_commit_markers() {
    let refused = |tag: ValueType| Error::Unsupported {
        feature: "two-phase commit",
        value: u64::from(tag.as_u8()),
    };
    let mut prepared = WriteBatch::new();
    prepared.insert_noop();
    prepared.put(b"k1", b"v1").unwrap();
    prepared.mark_end_prepare(b"xid1", true, false).unwrap();
    assert_eq!(
        format!("{}", refused(ValueType::BeginPrepareXid)),
        pc(&prepared)
    );

    let mut unprepared = WriteBatch::new();
    unprepared.insert_noop();
    unprepared.mark_end_prepare(b"xid1", false, true).unwrap();
    assert_eq!(
        format!("{}", refused(ValueType::BeginUnprepareXid)),
        pc(&unprepared)
    );

    for (build, tag) in [
        (
            (|b: &mut WriteBatch| b.mark_commit(b"x")) as fn(&mut WriteBatch) -> Result<(), Error>,
            ValueType::CommitXid,
        ),
        (|b| b.mark_rollback(b"x"), ValueType::RollbackXid),
        (
            |b| b.mark_commit_with_timestamp(b"x", b"ts"),
            ValueType::CommitXidAndTimestamp,
        ),
    ] {
        let mut b = WriteBatch::new();
        b.put(b"a", b"1").unwrap();
        build(&mut b).unwrap();
        assert_eq!(format!("Put(a, 1)@0{}", refused(tag)), pc(&b));
    }

    // A Noop alone is accepted, as RocksDB accepts it.
    let mut noop = WriteBatch::new();
    noop.insert_noop();
    noop.put(b"a", b"1").unwrap();
    assert_eq!("Put(a, 1)@0", pc(&noop));
}

// The port's own: a record for a column family the inserter has no memtable for is refused,
// as RocksDB refuses it without `ignore_missing_column_families`.
#[test]
fn memtable_insert_refuses_unknown_column_family() {
    let mut b = WriteBatch::new();
    b.put(b"a", b"1").unwrap();
    b.put_cf(3, b"b", b"2").unwrap();
    let refused = Error::InvalidArgument {
        what: "Invalid column family specified in write batch",
    };
    assert_eq!(format!("Put(a, 1)@0{refused}"), pc(&b));
}
