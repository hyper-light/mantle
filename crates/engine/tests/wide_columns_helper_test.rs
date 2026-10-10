//! RocksDB's db/wide/wide_columns_helper_test.cc, test for test: its 2 tests with the same
//! literals. The C++ writes to an `std::ostream`; the port appends to a `String`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::db::wide::wide_column_serialization::serialize;
use mantle_engine::db::wide::wide_columns::WideColumn;
use mantle_engine::db::wide::wide_columns_helper::{dump_slice_as_wide_columns, dump_wide_columns};

#[test]
fn dump_wide_columns_test() {
    let columns = [
        WideColumn::new("foo", "bar"),
        WideColumn::new("hello", "world"),
    ];
    let mut oss = String::new();
    dump_wide_columns(&columns, &mut oss, false /* hex */);
    assert_eq!("foo:bar hello:world", oss);
}

#[test]
fn dump_slice_as_wide_columns_test() {
    let columns = [
        WideColumn::new("foo", "bar"),
        WideColumn::new("hello", "world"),
    ];
    let mut output = Vec::new();
    serialize(&columns, &mut output).unwrap();

    let mut oss = String::new();
    dump_slice_as_wide_columns(&output, &mut oss, false /* hex */).unwrap();

    assert_eq!("foo:bar hello:world", oss);
}
