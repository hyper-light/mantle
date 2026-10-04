//! RocksDB's db/memtable_list_test.cc, test for test: 6 of its 8 test definitions, with the
//! same literals.
//!
//! Left out: `MemTableListWithTimestampTest.GetTableNewestUDT` and
//! `.ConcurrentGetTableNewestUDT`, which test user-defined timestamps (P16).
//!
//! Changed: RocksDB's `Mock_InstallMemtableFlushResults` builds a `VersionSet` and writes the
//! flush's MANIFEST edit before removing the memtables; the port's list does the removal only
//! (db/memtable_list.rs), the MANIFEST write being P6–P7's, so the mocks call the removal
//! directly and every assertion about the list is kept. The C++ checks that each deleted
//! memtable's reference count reached zero by `Ref`/`Unref`; the port returns deleted memtables
//! by value, so their count is checked instead. `GetTest` merges with RocksDB's
//! `StringAppendOperator` (delimiter ','), re-implemented here: the port's `Get` returns the
//! operands and the base, and merging is P12's.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::sync::atomic::{AtomicU64, Ordering};

use mantle_engine::db::dbformat::{
    InternalKeyComparator, LookupKey, MAX_SEQUENCE_NUMBER, SequenceNumber, ValueType,
};
use mantle_engine::db::memtable::{Found, Hit, MemTable, MemTableOptions, MergeContext};
use mantle_engine::db::memtable_list::{MemTableList, install_memtable_atomic_flush_results};
use mantle_engine::memory::arena::INLINE_SIZE;
use mantle_engine::util::comparator::Comparator;

fn value_with_write_time(value: &str, write_time: u64) -> Vec<u8> {
    let mut result = value.as_bytes().to_vec();
    result.extend_from_slice(&write_time.to_le_bytes());
    result
}

fn new_mem() -> MemTable {
    MemTable::new(
        InternalKeyComparator::new(Comparator::Bytewise),
        MemTableOptions::default(),
        MAX_SEQUENCE_NUMBER,
    )
    .unwrap()
}

/// The C++ fixture's file-number counter.
static FILE_NUMBER: AtomicU64 = AtomicU64::new(1);

/// `Mock_InstallMemtableFlushResults`, less its `VersionSet` (see the file header).
fn mock_install_memtable_flush_results(list: &mut MemTableList, m: &[u64]) -> Vec<MemTable> {
    let file_num = FILE_NUMBER.fetch_add(1, Ordering::Relaxed);
    list.try_install_memtable_flush_results(m, file_num)
}

/// The outcome of a `Get`, as the C++ reads `found` and the status: `(found, value or
/// NotFound)`, merging with the string-append operator.
fn outcome(hit: Option<Hit>, merge_context: &MergeContext) -> (bool, Option<String>) {
    let merge = |base: Option<&[u8]>| {
        let mut out = base.map(|b| String::from_utf8(b.to_vec()).unwrap());
        for op in merge_context.operands().iter().rev() {
            let op = std::str::from_utf8(op).unwrap();
            out = Some(match out {
                Some(s) => format!("{s},{op}"),
                None => op.to_string(),
            });
        }
        out
    };
    match hit {
        None => (false, None),
        Some(Hit {
            found: Found::Value(v),
            ..
        }) => (true, merge(Some(&v))),
        Some(Hit {
            found: Found::Deleted,
            ..
        }) => {
            if merge_context.num_operands() > 0 {
                (true, merge(None))
            } else {
                (true, None)
            }
        }
        Some(other) => panic!("unexpected {other:?}"),
    }
}

#[test]
fn empty() {
    // Create an empty MemTableList and validate basic functions.
    let mut list = MemTableList::new(1, 0);

    assert_eq!(0, list.num_not_flushed());
    assert!(!list.imm_flush_needed());
    assert!(!list.is_flush_pending());

    let mems = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(0, mems.len());

    assert_eq!(
        0,
        list.current().num_not_flushed() + list.current().num_flushed()
    );
}

#[test]
fn get_test() {
    // Create MemTableList
    let min_write_buffer_number_to_merge = 2;
    let max_write_buffer_size_to_maintain = 0;
    let mut list = MemTableList::new(
        min_write_buffer_number_to_merge,
        max_write_buffer_size_to_maintain,
    );

    let mut seq: SequenceNumber = 1;
    let mut merge_context = MergeContext::new();
    let mut max_covering_tombstone_seq = 0;

    let lkey = LookupKey::new(b"key1", seq).unwrap();
    let found = list
        .current()
        .get(&lkey, &mut merge_context, &mut max_covering_tombstone_seq)
        .unwrap();
    assert!(found.is_none());

    // Create a MemTable
    let mut mem = new_mem();

    // Write some keys to this memtable.
    seq += 1;
    mem.add(seq, ValueType::Deletion, b"key1", b"").unwrap();
    seq += 1;
    mem.add(seq, ValueType::Value, b"key2", b"value2").unwrap();
    seq += 1;
    mem.add(seq, ValueType::Value, b"key1", b"value1").unwrap();
    seq += 1;
    mem.add(seq, ValueType::Value, b"key2", b"value2.2")
        .unwrap();
    seq += 1;
    mem.add(
        seq,
        ValueType::ValuePreferredSeqno,
        b"key3",
        &value_with_write_time("value3.1", 20),
    )
    .unwrap();

    // Fetch the newly written keys
    let mut get = |mem: &MemTable, key: &[u8], s: SequenceNumber| {
        merge_context.clear();
        let hit = mem
            .get(
                &LookupKey::new(key, s).unwrap(),
                &mut merge_context,
                &mut max_covering_tombstone_seq,
            )
            .unwrap();
        outcome(hit, &merge_context)
    };
    assert_eq!(get(&mem, b"key1", seq), (true, Some("value1".into())));
    // MemTable found out that this key is *not* found (at this sequence#)
    assert_eq!(get(&mem, b"key1", 2), (true, None));
    assert_eq!(get(&mem, b"key2", seq), (true, Some("value2.2".into())));
    assert_eq!(get(&mem, b"key3", seq), (true, Some("value3.1".into())));

    assert_eq!(5, mem.num_entries());
    assert_eq!(1, mem.num_deletes());

    // Add memtable to list
    mem.set_id(1);
    assert!(list.add(mem).unwrap().is_empty());

    let saved_seq = seq;

    // Create another memtable and write some keys to it
    let mut mem2 = new_mem();
    mem2.set_id(2);

    seq += 1;
    mem2.add(seq, ValueType::Deletion, b"key1", b"").unwrap();
    seq += 1;
    mem2.add(seq, ValueType::Value, b"key2", b"value2.3")
        .unwrap();
    seq += 1;
    mem2.add(seq, ValueType::Merge, b"key3", b"value3.2")
        .unwrap();

    // Add second memtable to list
    assert!(list.add(mem2).unwrap().is_empty());

    // Fetch keys via MemTableList
    let mut list_get = |list: &MemTableList, key: &[u8], s: SequenceNumber| {
        merge_context.clear();
        let hit = list
            .current()
            .get(
                &LookupKey::new(key, s).unwrap(),
                &mut merge_context,
                &mut max_covering_tombstone_seq,
            )
            .unwrap();
        outcome(hit, &merge_context)
    };
    assert_eq!(list_get(&list, b"key1", seq), (true, None));
    assert_eq!(
        list_get(&list, b"key1", saved_seq),
        (true, Some("value1".into()))
    );
    assert_eq!(
        list_get(&list, b"key2", seq),
        (true, Some("value2.3".into()))
    );
    assert_eq!(list_get(&list, b"key2", 1), (false, None));
    assert_eq!(
        list_get(&list, b"key3", seq),
        (true, Some("value3.1,value3.2".into()))
    );

    assert_eq!(2, list.num_not_flushed());
}

#[test]
fn get_from_history_test() {
    // Create MemTableList
    let min_write_buffer_number_to_merge = 2;
    let max_write_buffer_size_to_maintain = 2 * INLINE_SIZE as i64;
    let mut list = MemTableList::new(
        min_write_buffer_number_to_merge,
        max_write_buffer_size_to_maintain,
    );

    let mut seq: SequenceNumber = 1;
    let mut merge_context = MergeContext::new();
    let mut max_covering_tombstone_seq = 0;

    let lkey = LookupKey::new(b"key1", seq).unwrap();
    let found = list
        .current()
        .get(&lkey, &mut merge_context, &mut max_covering_tombstone_seq)
        .unwrap();
    assert!(found.is_none());

    // Create a MemTable
    let mut mem = new_mem();

    // Write some keys to this memtable.
    seq += 1;
    mem.add(seq, ValueType::Deletion, b"key1", b"").unwrap();
    seq += 1;
    mem.add(seq, ValueType::Value, b"key2", b"value2").unwrap();
    seq += 1;
    mem.add(seq, ValueType::Value, b"key2", b"value2.2")
        .unwrap();

    // Fetch the newly written keys
    let mut get = |mem: &MemTable, key: &[u8]| {
        merge_context.clear();
        let hit = mem
            .get(
                &LookupKey::new(key, seq).unwrap(),
                &mut merge_context,
                &mut max_covering_tombstone_seq,
            )
            .unwrap();
        outcome(hit, &merge_context)
    };
    // MemTable found out that this key is *not* found (at this sequence#)
    assert_eq!(get(&mem, b"key1"), (true, None));
    assert_eq!(get(&mem, b"key2"), (true, Some("value2.2".into())));

    // Add memtable to list
    let to_delete = list.add(mem).unwrap();
    assert_eq!(0, to_delete.len());

    let lookup = |list: &MemTableList, key: &[u8], seq: SequenceNumber, history: bool| {
        let mut merge_context = MergeContext::new();
        let mut max_cov = 0;
        let lk = LookupKey::new(key, seq).unwrap();
        let v = list.current();
        let hit = if history {
            v.get_from_history(&lk, &mut merge_context, &mut max_cov)
        } else {
            v.get(&lk, &mut merge_context, &mut max_cov)
        }
        .unwrap();
        outcome(hit, &merge_context)
    };

    // Fetch keys via MemTableList
    assert_eq!(lookup(&list, b"key1", seq, false), (true, None));
    assert_eq!(
        lookup(&list, b"key2", seq, false),
        (true, Some("value2.2".into()))
    );

    // Flush this memtable from the list.
    // (It will then be a part of the memtable history).
    let to_flush = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(1, to_flush.len());

    let to_delete = mock_install_memtable_flush_results(&mut list, &to_flush);
    assert_eq!(0, list.num_not_flushed());
    assert_eq!(1, list.num_flushed());
    assert_eq!(0, to_delete.len());

    // Verify keys are no longer in MemTableList
    assert_eq!(lookup(&list, b"key1", seq, false), (false, None));
    assert_eq!(lookup(&list, b"key2", seq, false), (false, None));

    // Verify keys are present in history
    assert_eq!(lookup(&list, b"key1", seq, true), (true, None));
    assert_eq!(
        lookup(&list, b"key2", seq, true),
        (true, Some("value2.2".into()))
    );

    // Create another memtable and write some keys to it
    let mut mem2 = new_mem();
    seq += 1;
    mem2.add(seq, ValueType::Deletion, b"key1", b"").unwrap();
    seq += 1;
    mem2.add(seq, ValueType::Value, b"key3", b"value3").unwrap();

    // Add second memtable to list
    let to_delete = list.add(mem2).unwrap();
    assert_eq!(0, to_delete.len());

    let to_flush = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(1, to_flush.len());

    // Flush second memtable
    let to_delete = mock_install_memtable_flush_results(&mut list, &to_flush);
    assert_eq!(0, list.num_not_flushed());
    assert_eq!(2, list.num_flushed());
    assert_eq!(0, to_delete.len());

    // Add a third memtable to push the first memtable out of the history
    let mem3 = new_mem();
    let to_delete = list.add(mem3).unwrap();
    assert_eq!(1, list.num_not_flushed());
    assert_eq!(1, list.num_flushed());
    assert_eq!(1, to_delete.len());

    // Verify keys are no longer in MemTableList
    assert_eq!(lookup(&list, b"key1", seq, false), (false, None));
    assert_eq!(lookup(&list, b"key2", seq, false), (false, None));
    assert_eq!(lookup(&list, b"key3", seq, false), (false, None));

    // Verify that the second memtable's keys are in the history
    assert_eq!(lookup(&list, b"key1", seq, true), (true, None));
    assert_eq!(
        lookup(&list, b"key3", seq, true),
        (true, Some("value3".into()))
    );

    // Verify that key2 from the first memtable is no longer in the history
    assert_eq!(lookup(&list, b"key2", seq, false), (false, None));

    // Cleanup: `Unref(&to_delete)` adds what the list still holds to the one trimmed above.
    assert_eq!(
        3,
        list.current().num_not_flushed() + list.current().num_flushed() + to_delete.len()
    );
}

/// Five entries per memtable, as `FlushPendingTest` and `AtomicFlushTest` write them.
fn fill(mem: &mut MemTable, seq: &mut SequenceNumber, i: usize) {
    let mut add = |t, k: String, v: String| {
        *seq += 1;
        mem.add(*seq, t, k.as_bytes(), v.as_bytes()).unwrap();
    };
    add(ValueType::Value, "key1".into(), i.to_string());
    add(ValueType::Value, format!("keyN{i}"), "valueN".into());
    add(ValueType::Value, format!("keyX{i}"), "value".into());
    add(ValueType::Value, format!("keyM{i}"), "valueM".into());
    add(ValueType::Deletion, format!("keyX{i}"), String::new());
}

#[test]
fn flush_pending_test() {
    let num_tables = 6;
    let mut seq: SequenceNumber = 1;
    let write_buffer_size = MemTableOptions::default().write_buffer_size;

    // Create MemTableList
    let min_write_buffer_number_to_merge = 3;
    let max_write_buffer_size_to_maintain = 7 * write_buffer_size as i64;
    let mut list = MemTableList::new(
        min_write_buffer_number_to_merge,
        max_write_buffer_size_to_maintain,
    );

    // Create some MemTables
    let mut tables: Vec<Option<MemTable>> = Vec::new();
    for i in 0..num_tables {
        let mut mem = new_mem();
        mem.set_id(i as u64);
        fill(&mut mem, &mut seq, i);
        tables.push(Some(mem));
    }
    let mut take = |i: usize| tables[i].take().unwrap();

    // Nothing to flush
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());
    let to_flush = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(0, to_flush.len());

    // Request a flush even though there is nothing to flush
    list.flush_requested();
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Attempt to 'flush' to clear request for flush
    let to_flush = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(0, to_flush.len());
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Request a flush again
    list.flush_requested();
    // No flush pending since the list is empty.
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Add 2 tables
    let mut to_delete = list.add(take(0)).unwrap();
    to_delete.extend(list.add(take(1)).unwrap());
    assert_eq!(2, list.num_not_flushed());
    assert_eq!(0, to_delete.len());

    // Even though we have less than the minimum to flush, a flush is
    // pending since we had previously requested a flush and never called
    // PickMemtablesToFlush() to clear the flush.
    assert!(list.is_flush_pending());
    assert!(list.imm_flush_needed());

    // Pick tables to flush
    let to_flush = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(2, to_flush.len());
    assert_eq!(2, list.num_not_flushed());
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Revert flush
    list.rollback_memtable_flush(&to_flush, false);
    assert!(!list.is_flush_pending());
    assert!(list.imm_flush_needed());

    // Add another table
    to_delete.extend(list.add(take(2)).unwrap());
    // We now have the minimum to flush regardles of whether FlushRequested()
    // was called.
    assert!(list.is_flush_pending());
    assert!(list.imm_flush_needed());
    assert_eq!(0, to_delete.len());

    // Pick tables to flush
    let to_flush = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(3, to_flush.len());
    assert_eq!(3, list.num_not_flushed());
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Pick tables to flush again
    let to_flush2 = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(0, to_flush2.len());
    assert_eq!(3, list.num_not_flushed());
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Add another table
    to_delete.extend(list.add(take(3)).unwrap());
    assert!(!list.is_flush_pending());
    assert!(list.imm_flush_needed());
    assert_eq!(0, to_delete.len());

    // Request a flush again
    list.flush_requested();
    assert!(list.is_flush_pending());
    assert!(list.imm_flush_needed());

    // Pick tables to flush again
    let to_flush2 = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(1, to_flush2.len());
    assert_eq!(4, list.num_not_flushed());
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Rollback first pick of tables
    list.rollback_memtable_flush(&to_flush, false);
    assert!(list.is_flush_pending());
    assert!(list.imm_flush_needed());

    // Add another tables
    to_delete.extend(list.add(take(4)).unwrap());
    assert_eq!(5, list.num_not_flushed());
    // We now have the minimum to flush regardles of whether FlushRequested()
    assert!(list.is_flush_pending());
    assert!(list.imm_flush_needed());
    assert_eq!(0, to_delete.len());

    // Pick tables to flush
    let to_flush = list.pick_memtables_to_flush(u64::MAX);
    // Picks three oldest memtables. The fourth oldest is picked in `to_flush2` so
    // must be excluded. The newest (fifth oldest) is non-consecutive with the
    // three oldest due to omitting the fourth oldest so must not be picked.
    assert_eq!(3, to_flush.len());
    assert_eq!(5, list.num_not_flushed());
    assert!(!list.is_flush_pending());
    assert!(list.imm_flush_needed());

    // Pick tables to flush again
    let to_flush3 = list.pick_memtables_to_flush(u64::MAX);
    // Picks newest (fifth oldest)
    assert_eq!(1, to_flush3.len());
    assert_eq!(5, list.num_not_flushed());
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Nothing left to flush
    let to_flush4 = list.pick_memtables_to_flush(u64::MAX);
    assert_eq!(0, to_flush4.len());
    assert_eq!(5, list.num_not_flushed());
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Flush the 3 memtables that were picked in to_flush
    let to_delete = mock_install_memtable_flush_results(&mut list, &to_flush);

    // Note:  now to_flush contains tables[0,1,2].  to_flush2 contains
    // tables[3]. to_flush3 contains tables[4].
    // Current implementation will only commit memtables in the order they were
    // created. So TryInstallMemtableFlushResults will install the first 3 tables
    // in to_flush and stop when it encounters a table not yet flushed.
    assert_eq!(2, list.num_not_flushed());
    let num_in_history =
        3.min(usize::try_from(max_write_buffer_size_to_maintain).unwrap() / write_buffer_size);
    assert_eq!(num_in_history, list.num_flushed());
    assert_eq!(5 - list.num_not_flushed() - num_in_history, to_delete.len());

    // Request a flush again. Should be nothing to flush
    list.flush_requested();
    assert!(!list.is_flush_pending());
    assert!(!list.imm_flush_needed());

    // Flush the 1 memtable (tables[4]) that was picked in to_flush3
    let to_delete = mock_install_memtable_flush_results(&mut list, &to_flush3);

    // This will install 0 tables since tables[4] flushed while tables[3] has not
    // yet flushed.
    assert_eq!(2, list.num_not_flushed());
    assert_eq!(0, to_delete.len());

    // Flush the 1 memtable (tables[3]) that was picked in to_flush2
    let to_delete = mock_install_memtable_flush_results(&mut list, &to_flush2);

    // This will actually install 2 tables.  The 1 we told it to flush, and also
    // tables[4] which has been waiting for tables[3] to commit.
    assert_eq!(0, list.num_not_flushed());
    let num_in_history =
        5.min(usize::try_from(max_write_buffer_size_to_maintain).unwrap() / write_buffer_size);
    assert_eq!(num_in_history, list.num_flushed());
    assert_eq!(5 - list.num_not_flushed() - num_in_history, to_delete.len());

    // Add another table
    assert!(list.add(take(5)).unwrap().is_empty());
    assert_eq!(1, list.num_not_flushed());
    assert_eq!(5, list.latest_memtable_id());
    let memtable_id = 4;
    // Pick tables to flush. The tables to pick must have ID smaller than or
    // equal to 4. Therefore, no table will be selected in this case.
    list.flush_requested();
    assert!(list.has_flush_requested());
    let to_flush5 = list.pick_memtables_to_flush(memtable_id);
    assert!(to_flush5.is_empty());
    assert_eq!(1, list.num_not_flushed());
    assert!(list.imm_flush_needed());
    assert!(!list.is_flush_pending());
    assert!(!list.has_flush_requested());

    // Pick tables to flush. The tables to pick must have ID smaller than or
    // equal to 5. Therefore, only tables[5] will be selected.
    let memtable_id = 5;
    list.flush_requested();
    let to_flush5 = list.pick_memtables_to_flush(memtable_id);
    assert_eq!(1, to_flush5.len());
    assert_eq!(1, list.num_not_flushed());
    assert!(!list.imm_flush_needed());
    assert!(!list.is_flush_pending());

    // `list.current()->Unref(&to_delete)`: everything the list still holds.
    let to_delete_size = num_tables
        .min(usize::try_from(max_write_buffer_size_to_maintain).unwrap() / write_buffer_size);
    assert_eq!(
        to_delete_size,
        list.current().num_not_flushed() + list.current().num_flushed()
    );
}

#[test]
fn empty_atomic_flush_test() {
    let to_delete = install_memtable_atomic_flush_results(&mut [], &[], &[]).unwrap();
    assert!(to_delete.is_empty());
}

#[test]
fn atomic_flush_test() {
    let num_cfs = 3;
    let num_tables_per_cf = 2;
    let mut seq: SequenceNumber = 1;
    let write_buffer_size = MemTableOptions::default().write_buffer_size;

    // Create MemTableLists
    let min_write_buffer_number_to_merge = 3;
    let max_write_buffer_size_to_maintain = 7 * write_buffer_size as i64;
    let mut lists: Vec<MemTableList> = (0..num_cfs)
        .map(|_| {
            MemTableList::new(
                min_write_buffer_number_to_merge,
                max_write_buffer_size_to_maintain,
            )
        })
        .collect();

    let mut tables: Vec<Vec<MemTable>> = Vec::new();
    for _ in 0..num_cfs {
        let mut elem = Vec::new();
        for i in 0..num_tables_per_cf {
            let mut mem = new_mem();
            mem.set_id(i as u64);
            fill(&mut mem, &mut seq, i);
            elem.push(mem);
        }
        tables.push(elem);
    }

    // Nothing to flush
    for list in &mut lists {
        assert!(!list.is_flush_pending());
        assert!(!list.imm_flush_needed());
        assert_eq!(0, list.pick_memtables_to_flush(u64::MAX).len());
    }
    // Request flush even though there is nothing to flush
    for list in &mut lists {
        list.flush_requested();
        assert!(!list.is_flush_pending());
        assert!(!list.imm_flush_needed());
    }
    // Add tables to the immutable memtalbe lists associated with column families
    for (list, elem) in lists.iter_mut().zip(tables.drain(..)) {
        for mem in elem {
            assert!(list.add(mem).unwrap().is_empty());
        }
        assert_eq!(num_tables_per_cf, list.num_not_flushed());
        assert!(list.is_flush_pending());
        assert!(list.imm_flush_needed());
    }
    let flush_memtable_ids: [u64; 3] = [1, 1, 0];
    //          +----+
    // list[0]: |0  1|
    // list[1]: |0  1|
    //          | +--+
    // list[2]: |0| 1
    //          +-+
    // Pick memtables to flush
    let mut flush_candidates = Vec::new();
    for (list, &id) in lists.iter_mut().zip(&flush_memtable_ids) {
        let picked = list.pick_memtables_to_flush(id);
        assert_eq!(id + 1, picked.len() as u64);
        flush_candidates.push(picked);
    }
    let file_numbers: Vec<u64> = (0..num_cfs)
        .map(|_| FILE_NUMBER.fetch_add(1, Ordering::Relaxed))
        .collect();
    let mut refs: Vec<&mut MemTableList> = lists.iter_mut().collect();
    let to_delete =
        install_memtable_atomic_flush_results(&mut refs, &flush_candidates, &file_numbers).unwrap();

    for (i, list) in lists.iter().enumerate() {
        for m in list.current().history() {
            assert!(m.id() <= flush_memtable_ids[i]);
            assert!(0 < m.file_number());
        }
        assert_eq!(
            num_tables_per_cf - flush_candidates[i].len(),
            list.num_not_flushed()
        );
        assert_eq!(flush_candidates[i].len(), list.num_flushed());
    }
    assert!(to_delete.is_empty());

    // All memtables in tables array are still held by the lists, to be deleted with them.
    let held: usize = lists
        .iter()
        .map(|l| l.current().num_not_flushed() + l.current().num_flushed())
        .sum();
    assert_eq!(held, num_cfs * num_tables_per_cf);
}
