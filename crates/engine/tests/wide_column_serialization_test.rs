//! RocksDB's db/wide/wide_column_serialization_test.cc, test for test: its 29 test definitions
//! (22 `WideColumnSerializationTest`, 7 `PinnableWideColumnsTest`) with the same literals.
//!
//! RocksDB checks an error's message with `strstr`; the port's corruption error carries the
//! field RocksDB names as its `what`, and these check that the error is a corruption whose text
//! contains the same word ("version", "number", "name", "value size", "payload", "order",
//! "wide column ValueType"). `RandomizedSerializeDeserializeRoundTrip` seeds RocksDB's `Random`
//! from the clock; here it runs over fixed SplitMix64 seeds, so a failure replays. The C++
//! `SerializeV2` overload over string pairs is the `WideColumn` one here, since a column is two
//! borrowed byte strings either way. `PinnableWideColumnsHelper::ResolveColumns` takes the
//! resolved columns as positions in the buffers the columns hold or are handed, which is how a
//! column refers to a buffer without a pointer; the pointer-stability checks compare
//! `as_ptr()` before and after a move, as the C++ compares `data()`.
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
mod common;

use common::SplitMix64;
use mantle_engine::Error;
use mantle_engine::db::blob::blob_index::{
    BlobIndex, encode_blob, encode_blob_ttl, encode_inlined_ttl,
};
use mantle_engine::db::dbformat::ValueType;
use mantle_engine::db::wide::wide_column_serialization::{
    VERSION1, VERSION2, deserialize, deserialize_simple, for_each_blob_file_number,
    get_value_of_default_column, get_version, has_blob_columns,
    resolve_default_column_blob_reference, resolve_entity_for_merge, serialize, serialize_v2,
    serialized_size_v1,
};
use mantle_engine::db::wide::wide_columns::{
    DEFAULT_WIDE_COLUMN_NAME, PinnableWideColumns, ResolvedBytes, WideColumn, WideColumns,
};
use mantle_engine::db::wide::wide_columns_helper::find;
use mantle_engine::util::coding::{get_varint32, put_length_prefixed_slice, put_varint32};

/// `kNoCompression`, `kSnappyCompression`, `kZlibCompression` [R
/// include/rocksdb/compression_type.h].
const NO_COMPRESSION: u8 = 0x0;
const SNAPPY_COMPRESSION: u8 = 0x1;
const ZLIB_COMPRESSION: u8 = 0x2;

fn is_corruption_about(e: &Error, word: &str) -> bool {
    matches!(e, Error::Corruption { .. }) && e.to_string().contains(word)
}

fn wc<'a>(pairs: &'a [(&'a str, &'a str)]) -> WideColumns<'a> {
    pairs.iter().map(|(n, v)| WideColumn::new(*n, *v)).collect()
}

fn owned_wc(pairs: &[(Vec<u8>, Vec<u8>)]) -> WideColumns<'_> {
    pairs
        .iter()
        .map(|(n, v)| WideColumn::new(n.as_slice(), v.as_slice()))
        .collect()
}

#[test]
fn construct() {
    let foo = "foo";
    let bar = "bar";

    let foo_str = String::from(foo);
    let bar_str = String::from(bar);

    let foo_slice: &[u8] = foo_str.as_bytes();
    let bar_slice: &[u8] = bar_str.as_bytes();

    {
        let column = WideColumn::new(foo, bar);
        assert_eq!(column.name(), foo.as_bytes());
        assert_eq!(column.value(), bar.as_bytes());
    }
    {
        let column = WideColumn::new(&foo_str, bar);
        assert_eq!(column.name(), foo_str.as_bytes());
        assert_eq!(column.value(), bar.as_bytes());
    }
    {
        let column = WideColumn::new(foo_slice, bar);
        assert_eq!(column.name(), foo_slice);
        assert_eq!(column.value(), bar.as_bytes());
    }
    {
        let column = WideColumn::new(foo, &bar_str);
        assert_eq!(column.name(), foo.as_bytes());
        assert_eq!(column.value(), bar_str.as_bytes());
    }
    {
        let column = WideColumn::new(&foo_str, &bar_str);
        assert_eq!(column.name(), foo_str.as_bytes());
        assert_eq!(column.value(), bar_str.as_bytes());
    }
    {
        let column = WideColumn::new(foo_slice, &bar_str);
        assert_eq!(column.name(), foo_slice);
        assert_eq!(column.value(), bar_str.as_bytes());
    }
    {
        let column = WideColumn::new(foo, bar_slice);
        assert_eq!(column.name(), foo.as_bytes());
        assert_eq!(column.value(), bar_slice);
    }
    {
        let column = WideColumn::new(&foo_str, bar_slice);
        assert_eq!(column.name(), foo_str.as_bytes());
        assert_eq!(column.value(), bar_slice);
    }
    {
        let column = WideColumn::new(foo_slice, bar_slice);
        assert_eq!(column.name(), foo_slice);
        assert_eq!(column.value(), bar_slice);
    }
    {
        // The piecewise constructor: a prefix of each buffer.
        let foo_name = "foo_name";
        let bar_value = "bar_value";
        let column = WideColumn::new(
            &foo_name.as_bytes()[..foo.len()],
            &bar_value.as_bytes()[..bar.len()],
        );
        assert_eq!(column.name(), foo.as_bytes());
        assert_eq!(column.value(), bar.as_bytes());
    }
}

#[test]
fn serialize_deserialize() {
    let columns = wc(&[("foo", "bar"), ("hello", "world")]);
    let mut output = Vec::new();

    serialize(&columns, &mut output).unwrap();

    let deserialized_columns = deserialize_simple(&output).unwrap();
    assert_eq!(columns, deserialized_columns);

    {
        let it = find(&deserialized_columns, b"foo").unwrap();
        assert_eq!(deserialized_columns[it], deserialized_columns[0]);
    }
    {
        let it = find(&deserialized_columns, b"hello").unwrap();
        assert_eq!(
            deserialized_columns[it],
            *deserialized_columns.last().unwrap()
        );
    }
    assert!(find(&deserialized_columns, b"fubar").is_none());
    assert!(find(&deserialized_columns, b"snafu").is_none());
}

#[test]
fn serialize_duplicate_error() {
    let columns = wc(&[("foo", "bar"), ("foo", "baz")]);
    let mut output = Vec::new();
    let e = serialize(&columns, &mut output).unwrap_err();
    assert!(matches!(e, Error::Corruption { .. }));
}

#[test]
fn serialize_out_of_order_error() {
    let columns = wc(&[("hello", "world"), ("foo", "bar")]);
    let mut output = Vec::new();
    let e = serialize(&columns, &mut output).unwrap_err();
    assert!(matches!(e, Error::Corruption { .. }));
}

#[test]
fn deserialize_version_error() {
    // Can't decode version
    let buf = Vec::new();
    let s = deserialize_simple(&buf).unwrap_err();
    assert!(is_corruption_about(&s, "version"));
}

#[test]
fn deserialize_unsupported_version() {
    // A version newer than kVersion2 is reported as Corruption.
    let future_version = 1000;
    let mut buf = Vec::new();
    put_varint32(&mut buf, future_version);

    let s = deserialize_simple(&buf).unwrap_err();
    assert!(is_corruption_about(&s, "version"));
}

#[test]
fn future_version_rejected_consistently() {
    // Every entry point must reject a future/unknown version as Corruption.
    let future_version = 1000;
    let mut buf = Vec::new();
    put_varint32(&mut buf, future_version);

    assert!(matches!(
        deserialize_simple(&buf),
        Err(Error::Corruption { .. })
    ));
    assert!(matches!(
        get_value_of_default_column(&buf),
        Err(Error::Corruption { .. })
    ));
    assert!(matches!(
        has_blob_columns(&buf),
        Err(Error::Corruption { .. })
    ));
    assert!(matches!(
        for_each_blob_file_number(&buf, |_| Ok(())),
        Err(Error::Corruption { .. })
    ));
}

#[test]
fn deserialize_number_of_columns_error() {
    // Can't decode number of columns
    let mut buf = Vec::new();
    put_varint32(&mut buf, VERSION1);

    let s = deserialize_simple(&buf).unwrap_err();
    assert!(is_corruption_about(&s, "number"));
}

#[test]
fn deserialize_v2_error() {
    let mut buf = Vec::new();
    put_varint32(&mut buf, VERSION1);
    let num_columns = 2;
    put_varint32(&mut buf, num_columns);

    // Can't decode the first column name
    assert!(is_corruption_about(
        &deserialize_simple(&buf).unwrap_err(),
        "name"
    ));

    put_length_prefixed_slice(&mut buf, b"foo").unwrap();

    // Can't decode the size of the first column value
    assert!(is_corruption_about(
        &deserialize_simple(&buf).unwrap_err(),
        "value size"
    ));

    let first_value_size = 16;
    put_varint32(&mut buf, first_value_size);

    // Can't decode the second column name
    assert!(is_corruption_about(
        &deserialize_simple(&buf).unwrap_err(),
        "name"
    ));

    put_length_prefixed_slice(&mut buf, b"hello").unwrap();

    // Can't decode the size of the second column value
    assert!(is_corruption_about(
        &deserialize_simple(&buf).unwrap_err(),
        "value size"
    ));

    let second_value_size = 64;
    put_varint32(&mut buf, second_value_size);

    // Can't decode the payload of the first column
    assert!(is_corruption_about(
        &deserialize_simple(&buf).unwrap_err(),
        "payload"
    ));

    buf.extend(std::iter::repeat_n(b'0', first_value_size as usize));

    // Can't decode the payload of the second column
    assert!(is_corruption_about(
        &deserialize_simple(&buf).unwrap_err(),
        "payload"
    ));

    buf.extend(std::iter::repeat_n(b'x', second_value_size as usize));

    // Success
    deserialize_simple(&buf).unwrap();
}

#[test]
fn deserialize_v2_out_of_order() {
    let mut buf = Vec::new();
    put_varint32(&mut buf, VERSION1);
    put_varint32(&mut buf, 2);
    put_length_prefixed_slice(&mut buf, b"b").unwrap();
    put_varint32(&mut buf, 16);
    put_length_prefixed_slice(&mut buf, b"a").unwrap();

    let s = deserialize_simple(&buf).unwrap_err();
    assert!(is_corruption_about(&s, "order"));
}

#[test]
fn deserialize_v2_rejects_recursive_type() {
    // A V2 entity where one column has type kTypeWideColumnEntity. The bytes are the C++
    // test's, which put the types before the skip info; either way a type byte that is not
    // inline or blob index is found.
    let mut buf = Vec::new();
    put_varint32(&mut buf, VERSION2);
    put_varint32(&mut buf, 2);

    buf.push(ValueType::Value.as_u8());
    buf.push(ValueType::WideColumnEntity.as_u8());

    put_varint32(&mut buf, 2); // name_sizes_bytes
    put_varint32(&mut buf, 2); // value_sizes_bytes
    put_varint32(&mut buf, 2); // names_bytes

    put_varint32(&mut buf, 1);
    put_varint32(&mut buf, 1);

    put_varint32(&mut buf, 3);
    put_varint32(&mut buf, 5);

    buf.extend_from_slice(b"ab");
    buf.extend(std::iter::repeat_n(b'x', 8));

    {
        let mut blob_columns = Vec::new();
        let s = deserialize(&buf, Some(&mut blob_columns)).unwrap_err();
        assert!(is_corruption_about(&s, "wide column ValueType"));
    }
    {
        let s = deserialize_simple(&buf).unwrap_err();
        assert!(matches!(s, Error::Corruption { .. }));
    }
}

#[test]
fn fast_paths_reject_unsupported_column_type() {
    let mut buf = Vec::new();
    put_varint32(&mut buf, VERSION2);
    put_varint32(&mut buf, 1); // num_columns

    put_varint32(&mut buf, 1); // name_sizes_bytes
    put_varint32(&mut buf, 1); // value_sizes_bytes
    put_varint32(&mut buf, 0); // names_bytes

    buf.push(ValueType::WideColumnEntity.as_u8());

    put_varint32(&mut buf, 0); // empty default column name
    put_varint32(&mut buf, 3);
    buf.extend_from_slice(b"xyz");

    {
        let s = get_value_of_default_column(&buf).unwrap_err();
        assert!(is_corruption_about(&s, "wide column ValueType"));
    }
    {
        let s = for_each_blob_file_number(&buf, |_| Ok(())).unwrap_err();
        assert!(is_corruption_about(&s, "wide column ValueType"));
    }
}

/// `MakeBlobIndex`: a blob index built by `EncodeBlob` and decoded back.
fn make_blob_index(file_number: u64, offset: u64, size: u64) -> BlobIndex<'static> {
    make_blob_index_with(file_number, offset, size, NO_COMPRESSION)
}

fn make_blob_index_with(
    file_number: u64,
    offset: u64,
    size: u64,
    compression: u8,
) -> BlobIndex<'static> {
    let mut encoded = Vec::new();
    encode_blob(&mut encoded, file_number, offset, size, compression);
    let decoded = BlobIndex::decode_from(&encoded).unwrap();
    // A reference borrows nothing from its encoding.
    BlobIndex::Blob(decoded.reference().unwrap())
}

/// `MakeRandomBlobIndex`: Blob or BlobTTL, never inlined.
fn make_random_blob_index(rng: &mut SplitMix64) -> BlobIndex<'static> {
    let mut bi = Vec::new();
    if rng.next().is_multiple_of(2) {
        encode_blob(
            &mut bi,
            rng.next() % 1000,
            rng.next() % 10000,
            rng.next() % 5000,
            NO_COMPRESSION,
        );
    } else {
        encode_blob_ttl(
            &mut bi,
            rng.next() % 1_000_000,
            rng.next() % 1000,
            rng.next() % 10000,
            rng.next() % 5000,
            SNAPPY_COMPRESSION,
        );
    }
    match BlobIndex::decode_from(&bi).unwrap() {
        BlobIndex::Blob(r) => BlobIndex::Blob(r),
        BlobIndex::BlobTtl {
            expiration,
            reference,
        } => BlobIndex::BlobTtl {
            expiration,
            reference,
        },
        BlobIndex::InlinedTtl { .. } => {
            panic!("an inlined TTL value has no blob reference to replace")
        }
    }
}

/// `V2SerializeAndDeserialize`, in two steps: `v2_serialize` then this, which deserializes
/// and checks the names.
fn v2_deserialize<'a>(
    columns: &[WideColumn<'_>],
    serialized: &'a [u8],
) -> (WideColumns<'a>, Vec<(usize, BlobIndex<'a>)>) {
    let mut blob_columns_out = Vec::new();
    let deserialized = deserialize(serialized, Some(&mut blob_columns_out)).unwrap();
    assert_eq!(deserialized.len(), columns.len());
    for (d, c) in deserialized.iter().zip(columns) {
        assert_eq!(d.name, c.name);
    }
    (deserialized, blob_columns_out)
}

/// `VerifyDeserialize`: names and values as expected.
fn verify_deserialize(serialized: &[u8], expected: &[WideColumn<'_>]) {
    let deserialized = deserialize_simple(serialized).unwrap();
    assert_eq!(deserialized.len(), expected.len());
    for (d, e) in deserialized.iter().zip(expected) {
        assert_eq!(d.name, e.name);
        assert_eq!(d.value, e.value);
    }
}

/// `VerifyGetDefaultColumn`.
fn verify_get_default_column(columns: &[(&str, &str)], expected_value: &[u8]) {
    let mut serialized = Vec::new();
    serialize_v2(&wc(columns), &[], &mut serialized).unwrap();
    let (value, is_blob_reference) = get_value_of_default_column(&serialized).unwrap();
    assert!(!is_blob_reference);
    assert_eq!(value, expected_value);
}

#[test]
fn v2_get_value_of_default_column() {
    // V2 with default column present
    verify_get_default_column(
        &[("", "default_value"), ("col1", "value1")],
        b"default_value",
    );
    // V2 without default column
    verify_get_default_column(&[("col1", "value1"), ("col2", "value2")], b"");
    // V2 with zero columns
    verify_get_default_column(&[], b"");

    // V1 fallback
    {
        let columns = wc(&[("", "v1_default"), ("col1", "v1")]);
        let mut serialized = Vec::new();
        serialize(&columns, &mut serialized).unwrap();
        let (value, is_blob_reference) = get_value_of_default_column(&serialized).unwrap();
        assert!(!is_blob_reference);
        assert_eq!(value, b"v1_default");
    }
}

#[test]
fn v2_blob_column_rejects_deserialize() {
    let columns = wc(&[("a", "inline"), ("b", "placeholder")]);
    let blob_columns = [(1, make_blob_index(1, 2, 3))];

    let mut serialized = Vec::new();
    serialize_v2(&columns, &blob_columns, &mut serialized).unwrap();

    assert!(matches!(
        deserialize_simple(&serialized),
        Err(Error::Corruption { .. })
    ));
}

#[test]
fn pinnable_wide_columns_fallbacks_to_v2() {
    let columns = wc(&[("", "placeholder"), ("ttl", "00000001"), ("type", "cold")]);
    let blob_columns = [(0, make_blob_index(10, 20, 30))];

    let mut serialized = Vec::new();
    serialize_v2(&columns, &blob_columns, &mut serialized).unwrap();

    let mut expected_blob_columns = Vec::new();
    let expected_columns = deserialize(&serialized, Some(&mut expected_blob_columns)).unwrap();

    let mut result = PinnableWideColumns::new();
    result.set_wide_column_value(serialized.clone()).unwrap();

    assert_eq!(result.columns().len(), expected_columns.len());
    for (r, e) in result.columns().iter().zip(&expected_columns) {
        assert_eq!(r.name, e.name);
        assert_eq!(r.value, e.value);
    }
}

#[test]
fn v2_get_value_of_default_column_blob_ref() {
    let columns = wc(&[("", "placeholder"), ("col1", "value1")]);
    let blob_columns = [(0, make_blob_index(10, 100, 500))];

    let mut serialized = Vec::new();
    serialize_v2(&columns, &blob_columns, &mut serialized).unwrap();

    let (value, is_blob_reference) = get_value_of_default_column(&serialized).unwrap();
    assert!(is_blob_reference);
    assert!(!value.is_empty());

    // Resolving a non-inlined reference without a blob fetcher is Corruption.
    assert!(matches!(
        resolve_default_column_blob_reference(value, b"user_key", None),
        Err(Error::Corruption { .. })
    ));
}

#[test]
fn serialize_v2_errors() {
    // Blob column index out of range
    {
        let columns = wc(&[("a", "val")]);
        let blob_columns = [(5, make_blob_index(1, 2, 3))];
        let mut output = Vec::new();
        assert!(matches!(
            serialize_v2(&columns, &blob_columns, &mut output),
            Err(Error::InvalidArgument { .. })
        ));
    }
    // Columns out of order (V2)
    {
        let columns = wc(&[("b", "val_b"), ("a", "val_a")]);
        let mut output = Vec::new();
        assert!(matches!(
            serialize_v2(&columns, &[], &mut output),
            Err(Error::Corruption { .. })
        ));
    }
    // Duplicate column names (V2)
    {
        let columns = wc(&[("a", "val1"), ("a", "val2")]);
        let mut output = Vec::new();
        assert!(matches!(
            serialize_v2(&columns, &[], &mut output),
            Err(Error::Corruption { .. })
        ));
    }
}

#[test]
fn blob_index_encode_to_round_trip() {
    let verify_encode_to = |encoded_static: &[u8]| {
        let bi = BlobIndex::decode_from(encoded_static).unwrap();
        let mut encoded_instance = Vec::new();
        bi.encode_to(&mut encoded_instance);
        assert_eq!(encoded_static, encoded_instance.as_slice());
    };

    let mut blob_str = Vec::new();
    let mut blob_ttl_str = Vec::new();
    let mut inlined_str = Vec::new();
    encode_blob(&mut blob_str, 42, 1024, 2048, SNAPPY_COMPRESSION);
    encode_blob_ttl(&mut blob_ttl_str, 9999, 10, 200, 3000, ZLIB_COMPRESSION);
    encode_inlined_ttl(&mut inlined_str, 12345, b"inline_data");

    verify_encode_to(&blob_str);
    verify_encode_to(&blob_ttl_str);
    verify_encode_to(&inlined_str);
}

#[test]
fn v2_layout_structure_verification() {
    let columns = wc(&[("aa", "val_aa"), ("bbb", "val_bbb")]);
    let mut serialized = Vec::new();
    serialize_v2(&columns, &[], &mut serialized).unwrap();

    let mut data: &[u8] = &serialized;

    // Section 1: HEADER
    assert_eq!(get_varint32(&mut data).unwrap(), VERSION2);
    assert_eq!(get_varint32(&mut data).unwrap(), 2);

    // Section 2: SKIP INFO (3 varints)
    assert_eq!(get_varint32(&mut data).unwrap(), 2); // name sizes: varint(2) + varint(3)
    assert_eq!(get_varint32(&mut data).unwrap(), 2); // value sizes: varint(6) + varint(7)
    assert_eq!(get_varint32(&mut data).unwrap(), 5); // names: "aa" + "bbb"

    // Section 3: COLUMN TYPES (2 bytes, both inline)
    assert!(data.len() >= 2);
    assert_eq!(data[0], ValueType::Value.as_u8());
    assert_eq!(data[1], ValueType::Value.as_u8());
    data = &data[2..];

    // Section 4: NAME SIZES
    assert_eq!(get_varint32(&mut data).unwrap(), 2);
    assert_eq!(get_varint32(&mut data).unwrap(), 3);

    // Section 5: VALUE SIZES
    assert_eq!(get_varint32(&mut data).unwrap(), 6); // "val_aa"
    assert_eq!(get_varint32(&mut data).unwrap(), 7); // "val_bbb"

    // Section 6: COLUMN NAMES
    assert!(data.len() >= 5);
    assert_eq!(&data[..2], b"aa");
    assert_eq!(&data[2..5], b"bbb");
    data = &data[5..];

    // Section 7: COLUMN VALUES
    assert!(data.len() >= 13);
    assert_eq!(&data[..6], b"val_aa");
    assert_eq!(&data[6..13], b"val_bbb");
}

#[test]
fn randomized_serialize_deserialize_round_trip() {
    const NUM_ITERATIONS: usize = 100;
    for seed in [1u64, 2, 3, 0x5eed] {
        let mut rng = SplitMix64(seed);
        for _ in 0..NUM_ITERATIONS {
            let num_cols = (rng.next() % 17) as usize; // 0..16
            let name_sz = 1 + (rng.next() % 64) as usize; // 1..64
            let val_sz = (rng.next() % 1025) as usize; // 0..1024

            // Sorted, unique names of exactly name_sz bytes: a hex index prefix, padded.
            let mut columns: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            for c in 0..num_cols {
                let mut name = format!("{c:04x}").into_bytes();
                if name.len() < name_sz {
                    let pad = b'a' + (rng.next() % 26) as u8;
                    name.extend(std::iter::repeat_n(pad, name_sz - name.len()));
                }
                if name.len() > name_sz {
                    name = name[name.len() - name_sz..].to_vec();
                }
                let value = (0..val_sz).map(|_| (rng.next() % 256) as u8).collect();
                columns.push((name, value));
            }

            let mut blob_columns = Vec::new();
            for c in 0..num_cols {
                if rng.next().is_multiple_of(3) {
                    blob_columns.push((c, make_random_blob_index(&mut rng)));
                }
            }

            let wide = owned_wc(&columns);
            let mut serialized = Vec::new();
            serialize_v2(&wide, &blob_columns, &mut serialized).unwrap();
            let (deserialized, blob_out) = v2_deserialize(&wide, &serialized);

            assert_eq!(get_version(&serialized).unwrap(), VERSION2);
            assert_eq!(
                has_blob_columns(&serialized).unwrap(),
                !blob_columns.is_empty()
            );

            assert_eq!(blob_out.len(), blob_columns.len());
            for ((oi, orig), (di, decoded)) in blob_columns.iter().zip(&blob_out) {
                assert_eq!(di, oi);
                assert_eq!(decoded.is_inlined(), orig.is_inlined());
                assert_eq!(decoded.has_ttl(), orig.has_ttl());
                if !decoded.is_inlined() {
                    let (d, o) = (decoded.reference().unwrap(), orig.reference().unwrap());
                    assert_eq!(d.file_number, o.file_number);
                    assert_eq!(d.offset, o.offset);
                    assert_eq!(d.size, o.size);
                }
            }

            let mut blob_idx = 0;
            for (c, (_, value)) in columns.iter().enumerate() {
                if blob_idx < blob_columns.len() && blob_columns[blob_idx].0 == c {
                    blob_idx += 1;
                } else {
                    assert_eq!(deserialized[c].value, value.as_slice());
                }
            }

            if blob_columns.is_empty() {
                verify_deserialize(&serialized, &wide);
            }

            // V1 Serialize round-trip
            let mut serialized_v1 = Vec::new();
            serialize(&wide, &mut serialized_v1).unwrap();
            assert_eq!(get_version(&serialized_v1).unwrap(), VERSION1);
            verify_deserialize(&serialized_v1, &wide);
        }
    }
}

#[test]
fn resolve_entity_for_merge_null_blob_fetcher() {
    let columns = wc(&[("", "default_val"), ("col1", "inline_val")]);
    let blob_columns = [(0, make_blob_index(42, 100, 50))];

    let mut serialized = Vec::new();
    serialize_v2(&columns, &blob_columns, &mut serialized).unwrap();

    assert!(has_blob_columns(&serialized).unwrap());

    // With no blob fetcher: an error, not a crash.
    assert!(matches!(
        resolve_entity_for_merge(&serialized, b"user_key", None),
        Err(Error::Corruption { .. })
    ));
}

#[test]
fn deserialize_rejects_trailing_data() {
    let columns = [
        WideColumn::new(DEFAULT_WIDE_COLUMN_NAME, "d"),
        WideColumn::new("attr", "val"),
    ];

    let check = |serialized: &[u8]| {
        let mut blob_columns = Vec::new();
        deserialize(serialized, Some(&mut blob_columns)).unwrap();

        // A trailing byte must be rejected as Corruption.
        let with_trailing = [serialized, b"x"].concat();
        let mut blob_columns = Vec::new();
        assert!(matches!(
            deserialize(&with_trailing, Some(&mut blob_columns)),
            Err(Error::Corruption { .. })
        ));
    };

    // V1 layout
    {
        let mut serialized = Vec::new();
        serialize(&columns, &mut serialized).unwrap();
        check(&serialized);
    }
    // V2 layout with no blob columns.
    {
        let mut serialized = Vec::new();
        serialize_v2(&columns, &[], &mut serialized).unwrap();
        check(&serialized);
    }
    // Empty (zero-column) entities must reject trailing data too.
    {
        let mut serialized = Vec::new();
        serialize(&[], &mut serialized).unwrap();
        check(&serialized);
    }
    {
        let mut serialized = Vec::new();
        serialize_v2(&[], &[], &mut serialized).unwrap();
        check(&serialized);
    }
}

fn expected_payload_size(columns: &[WideColumn<'_>]) -> usize {
    columns.iter().map(|c| c.name.len() + c.value.len()).sum()
}

#[test]
fn pinnable_wide_columns_test_set_plain_value() {
    let mut columns = PinnableWideColumns::new();
    columns.set_plain_value(&b"hello"[..]);

    assert_eq!(columns.columns().len(), 1);
    assert_eq!(columns.columns()[0].name, DEFAULT_WIDE_COLUMN_NAME);
    assert_eq!(columns.columns()[0].value, b"hello");

    // For a plain value, payload_size() equals the raw value size.
    assert_eq!(columns.payload_size(), 5);
}

#[test]
fn pinnable_wide_columns_test_set_wide_column_value_and_payload_size() {
    // Columns must be sorted (default/empty name first).
    let entity_columns = [
        WideColumn::new(DEFAULT_WIDE_COLUMN_NAME, "default"),
        WideColumn::new("a", "bb"),
        WideColumn::new("ccc", "dddd"),
    ];

    let mut serialized = Vec::new();
    serialize(&entity_columns, &mut serialized).unwrap();

    let mut columns = PinnableWideColumns::new();
    columns.set_wide_column_value(serialized).unwrap();

    assert_eq!(columns.columns(), entity_columns);
    assert_eq!(
        columns.payload_size(),
        expected_payload_size(&entity_columns)
    );
}

#[test]
fn pinnable_wide_columns_test_serialized_size_matches_serialized_size_v1() {
    {
        // Plain value: default column with the raw value.
        let mut columns = PinnableWideColumns::new();
        columns.set_plain_value(&b"plain-value"[..]);
        assert_eq!(
            columns.serialized_size(),
            serialized_size_v1(&columns.columns())
        );
    }
    {
        // V1 entity: serialized_size() equals the length Serialize() produced.
        let entity_columns = [
            WideColumn::new(DEFAULT_WIDE_COLUMN_NAME, "default"),
            WideColumn::new("attr1", "val1"),
            WideColumn::new("attr2", ""),
        ];
        let mut serialized = Vec::new();
        serialize(&entity_columns, &mut serialized).unwrap();
        let serialized_len = serialized.len();

        let mut columns = PinnableWideColumns::new();
        columns.set_wide_column_value(serialized).unwrap();

        assert_eq!(
            columns.serialized_size(),
            serialized_size_v1(&columns.columns())
        );
        assert_eq!(columns.serialized_size(), serialized_len);
    }
}

#[test]
fn pinnable_wide_columns_test_reset() {
    let mut columns = PinnableWideColumns::new();
    columns.set_plain_value(&b"something"[..]);
    assert!(!columns.columns().is_empty());

    columns.reset();
    assert!(columns.columns().is_empty());
    assert_eq!(columns.payload_size(), 0);

    // Reusable after Reset.
    columns.set_plain_value(&b"again"[..]);
    assert_eq!(columns.columns().len(), 1);
    assert_eq!(columns.columns()[0].value, b"again");
}

#[test]
fn pinnable_wide_columns_test_move_plain_value_self_pinned_stable() {
    // The backing buffer does not relocate on move, so the value's address is kept.
    let mut columns = PinnableWideColumns::new();
    columns.set_plain_value(String::from("tiny"));

    let value_data = columns.columns()[0].value.as_ptr();

    let moved = columns;
    assert_eq!(moved.columns().len(), 1);
    assert_eq!(moved.columns()[0].name, DEFAULT_WIDE_COLUMN_NAME);
    assert_eq!(moved.columns()[0].value, b"tiny");
    assert_eq!(moved.columns()[0].value.as_ptr(), value_data);
}

/// Resolves column 1 of `columns` into its own buffer holding `payload`, keeping column 0 in
/// the entity's buffer, as the C++ tests build their `resolved_columns`.
fn resolve_second_column(columns: &mut PinnableWideColumns, payload: &[u8]) {
    let spans = columns.column_spans().to_vec();
    let extra: Box<[u8]> = payload.into();
    let len = extra.len();
    let resolved = [
        (
            ResolvedBytes::Held(spans[0].0),
            ResolvedBytes::Held(spans[0].1),
        ),
        (
            ResolvedBytes::Held(spans[1].0),
            ResolvedBytes::Extra {
                buffer: 0,
                start: 0,
                len,
            },
        ),
    ];
    columns.resolve_columns(&resolved, vec![extra]).unwrap();
}

#[test]
fn pinnable_wide_columns_test_resolve_columns_zero_copy() {
    let entity_columns = [
        WideColumn::new(DEFAULT_WIDE_COLUMN_NAME, "default-value"),
        WideColumn::new("blob_col", "encoded-blob-index"),
    ];
    let mut serialized = Vec::new();
    serialize(&entity_columns, &mut serialized).unwrap();

    let mut columns = PinnableWideColumns::new();
    columns.set_wide_column_value(serialized).unwrap();

    let inline_value_data = columns.columns()[0].value.as_ptr();

    // Resolve "blob_col" into its own backing buffer; the buffer's bytes do not move when it
    // is handed over.
    let extra: Box<[u8]> = (&b"resolved-blob-payload"[..]).into();
    let resolved_data = extra.as_ptr();
    let spans = columns.column_spans().to_vec();
    let len = extra.len();
    columns
        .resolve_columns(
            &[
                (
                    ResolvedBytes::Held(spans[0].0),
                    ResolvedBytes::Held(spans[0].1),
                ),
                (
                    ResolvedBytes::Held(spans[1].0),
                    ResolvedBytes::Extra {
                        buffer: 0,
                        start: 0,
                        len,
                    },
                ),
            ],
            vec![extra],
        )
        .unwrap();

    assert_eq!(columns.columns().len(), 2);

    // Inline column still points into the original entity buffer.
    assert_eq!(columns.columns()[0].value, b"default-value");
    assert_eq!(columns.columns()[0].value.as_ptr(), inline_value_data);

    // Resolved blob column points directly into the handed-over buffer.
    assert_eq!(columns.columns()[1].name, b"blob_col");
    assert_eq!(columns.columns()[1].value, b"resolved-blob-payload");
    assert_eq!(columns.columns()[1].value.as_ptr(), resolved_data);
}

#[test]
fn pinnable_wide_columns_test_multi_buffer_move_preserves_pointers() {
    let entity_columns = [
        WideColumn::new(DEFAULT_WIDE_COLUMN_NAME, "default-value"),
        WideColumn::new("blob_col", "encoded-blob-index"),
    ];
    let mut serialized = Vec::new();
    serialize(&entity_columns, &mut serialized).unwrap();

    let mut columns = PinnableWideColumns::new();
    columns.set_wide_column_value(serialized).unwrap();
    resolve_second_column(&mut columns, b"a-fairly-long-resolved-blob-payload");

    let inline_data = columns.columns()[0].value.as_ptr();
    let resolved_data = columns.columns()[1].value.as_ptr();

    // Move construction: buffers are taken, not relocated.
    let moved = columns;
    assert_eq!(moved.columns().len(), 2);
    assert_eq!(moved.columns()[0].value, b"default-value");
    assert_eq!(
        moved.columns()[1].value,
        b"a-fairly-long-resolved-blob-payload"
    );
    assert_eq!(moved.columns()[0].value.as_ptr(), inline_data);
    assert_eq!(moved.columns()[1].value.as_ptr(), resolved_data);

    // Move assignment: same guarantee.
    let mut move_assigned = PinnableWideColumns::new();
    assert!(move_assigned.columns().is_empty());
    move_assigned = moved;
    assert_eq!(move_assigned.columns().len(), 2);
    assert_eq!(move_assigned.columns()[0].value, b"default-value");
    assert_eq!(
        move_assigned.columns()[1].value,
        b"a-fairly-long-resolved-blob-payload"
    );
    assert_eq!(move_assigned.columns()[0].value.as_ptr(), inline_data);
    assert_eq!(move_assigned.columns()[1].value.as_ptr(), resolved_data);
}
