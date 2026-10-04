//! The port parses batches the oracle wrote (docs/research/24 §3.1 P2 differential): RocksDB
//! 11.8.1's `ldb` writes a WAL, the port reads it with its log reader, decodes every record as a
//! `WriteBatch` and prints it as `ldb dump_wal --header --print_value` does; the two outputs must
//! be equal line for line, header, sequence numbers, counts, byte sizes, physical offsets and
//! every key and value.
//!
//! The oracle's WALs and dumps are fixtures (`tests/golden/p3_wal/`), written by
//! `tests/golden/p3_wal_gen.sh` with the C++ build of `ldb` outside the repository, as the golden
//! programs are; the test reads them on every run, so it never passes without comparing.
//!
//! Each scenario is a fresh DB: RocksDB flushes a recovered WAL when it reopens, so only the last
//! command's WAL survives, and `ldb query` puts many batches into one WAL.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::Error;
use mantle_engine::db::log_format::WalRecoveryMode;
use mantle_engine::db::log_reader::{DropReason, Reader, Reporter};
use mantle_engine::db::wide::wide_columns_helper::dump_slice_as_wide_columns;
use mantle_engine::db::write_batch::{Handler, WriteBatch};
use mantle_engine::file::{ReadFailure, SequentialFile};

/// A WAL held in memory.
struct Bytes {
    data: Vec<u8>,
    pos: usize,
}

impl SequentialFile for Bytes {
    fn read(&mut self, scratch: &mut [u8]) -> Result<usize, ReadFailure> {
        let n = scratch.len().min(self.data.len() - self.pos);
        scratch[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
    fn file_name(&self) -> &str {
        "oracle.log"
    }
}

/// A reporter that fails the test on any dropped byte: the oracle's WALs are well formed.
struct Strict;

impl Reporter for Strict {
    fn corruption(&mut self, bytes: usize, reason: &DropReason, _: Option<u64>) {
        panic!("{bytes} bytes dropped: {reason}");
    }
}

fn to_hex(b: &[u8]) -> String {
    let mut s = String::from("0x");
    for x in b {
        s += &format!("{x:02X}");
    }
    s
}

/// ldb's `InMemoryHandler` with `print_values` [R tools/ldb_cmd.cc:2975-3112], keys printed as
/// hex since no column family is opened.
struct DumpHandler<'a>(&'a mut String);

impl DumpHandler<'_> {
    fn put_merge(&mut self, key: &[u8], value: &[u8]) {
        *self.0 += &format!("{} : {} ", to_hex(key), to_hex(value));
    }
}

impl Handler for DumpHandler<'_> {
    fn put_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        *self.0 += &format!("PUT({cf}) : ");
        self.put_merge(key, value);
        Ok(())
    }
    fn put_entity_cf(&mut self, cf: u32, key: &[u8], entity: &[u8]) -> Result<(), Error> {
        *self.0 += &format!("PUT_ENTITY({cf}) : {} : ", to_hex(key));
        dump_slice_as_wide_columns(entity, self.0, true)?;
        *self.0 += " ";
        Ok(())
    }
    fn merge_cf(&mut self, cf: u32, key: &[u8], value: &[u8]) -> Result<(), Error> {
        *self.0 += &format!("MERGE({cf}) : ");
        self.put_merge(key, value);
        Ok(())
    }
    fn mark_noop(&mut self, _: bool) -> Result<(), Error> {
        *self.0 += "NOOP ";
        Ok(())
    }
    fn delete_cf(&mut self, cf: u32, key: &[u8]) -> Result<(), Error> {
        *self.0 += &format!("DELETE({cf}) : {} ", to_hex(key));
        Ok(())
    }
    fn single_delete_cf(&mut self, cf: u32, key: &[u8]) -> Result<(), Error> {
        *self.0 += &format!("SINGLE_DELETE({cf}) : {} ", to_hex(key));
        Ok(())
    }
    fn delete_range_cf(&mut self, cf: u32, begin: &[u8], end: &[u8]) -> Result<(), Error> {
        *self.0 += &format!("DELETE_RANGE({cf}) : {} {} ", to_hex(begin), to_hex(end));
        Ok(())
    }
}

/// The port's `dump_wal` rows of one WAL file.
fn port_dump(wal: Vec<u8>) -> Vec<String> {
    let mut reader = Reader::new(Bytes { data: wal, pos: 0 }, Strict, true, 0);
    let mut rows = vec!["Sequence,Count,ByteSize,Physical Offset,Key(s) : value ".to_owned()];
    let mut record = Vec::new();
    while reader.read_record(&mut record, WalRecoveryMode::TolerateCorruptedTailRecords) {
        let batch = WriteBatch::from_rep(record.clone()).unwrap();
        let mut row = format!(
            "{},{},{},{},",
            batch.sequence(),
            batch.count(),
            batch.data_size(),
            reader.last_record_offset()
        );
        batch.iterate(&mut DumpHandler(&mut row)).unwrap();
        rows.push(row);
    }
    rows
}

/// Each scenario's WAL and its oracle dump, as `p3_wal_gen.sh` names them.
const SCENARIOS: [(&str, &[u8], &str); 6] = [
    (
        "query",
        include_bytes!("golden/p3_wal/query-0.log"),
        include_str!("golden/p3_wal/query-0.dump"),
    ),
    (
        "batchput",
        include_bytes!("golden/p3_wal/batchput-0.log"),
        include_str!("golden/p3_wal/batchput-0.dump"),
    ),
    (
        "singledelete",
        include_bytes!("golden/p3_wal/singledelete-0.log"),
        include_str!("golden/p3_wal/singledelete-0.dump"),
    ),
    (
        "deleterange",
        include_bytes!("golden/p3_wal/deleterange-0.log"),
        include_str!("golden/p3_wal/deleterange-0.dump"),
    ),
    (
        "put_entity",
        include_bytes!("golden/p3_wal/put_entity-0.log"),
        include_str!("golden/p3_wal/put_entity-0.dump"),
    ),
    (
        "fragmented",
        include_bytes!("golden/p3_wal/fragmented-0.log"),
        include_str!("golden/p3_wal/fragmented-0.dump"),
    ),
];

/// The port reads every oracle WAL and prints the oracle's dump line for line: 300 batches
/// through ldb's REPL, a batch of 64 puts, one each of single delete, range delete and entity,
/// and a 100,000-byte value fragmented over four 32 KiB log blocks.
#[test]
fn port_parses_batches_the_oracle_wrote() {
    let mut batches = 0;
    for (name, wal, dump) in SCENARIOS {
        let expected: Vec<String> = dump.lines().map(str::to_owned).collect();
        let ours = port_dump(wal.to_vec());
        assert_eq!(expected, ours, "{name}");
        batches += ours.len() - 1;
    }
    assert_eq!(batches, 300 + 5, "{batches} batches compared");
}
