//! Properties of the wide-column and blob-index encodings beyond RocksDB's own tests: every
//! entity of sorted unique columns round-trips through both versions, with and without blob
//! columns; the hex dump matches RocksDB's `operator<<`; and no input, arbitrary or a mutation
//! of a valid entity, makes a decoder panic or return a column outside its input.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::BTreeMap;

use mantle_engine::db::blob::blob_index::{BlobIndex, BlobReference};
use mantle_engine::db::wide::wide_column_serialization::{
    deserialize, for_each_blob_file_number, get_value_of_default_column, has_blob_columns,
    serialize, serialize_v2, serialized_size_v1,
};
use mantle_engine::db::wide::wide_columns::{PinnableWideColumns, WideColumn};
use mantle_engine::db::wide::wide_columns_helper::dump_wide_columns;
use proptest::prelude::*;

fn entity() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
    prop::collection::btree_map(
        prop::collection::vec(any::<u8>(), 0..12),
        prop::collection::vec(any::<u8>(), 0..40),
        0..10,
    )
}

fn within(input: &[u8], part: &[u8]) -> bool {
    let (start, end) = (
        input.as_ptr() as usize,
        input.as_ptr() as usize + input.len(),
    );
    let p = part.as_ptr() as usize;
    part.is_empty() || (p >= start && p + part.len() <= end)
}

fn decode_all(input: &[u8]) {
    let mut blobs = Vec::new();
    if let Ok(columns) = deserialize(input, Some(&mut blobs)) {
        for c in &columns {
            assert!(within(input, c.name) && within(input, c.value));
        }
    }
    let _ = has_blob_columns(input);
    let _ = for_each_blob_file_number(input, |_| Ok(()));
    if let Ok((v, _)) = get_value_of_default_column(input) {
        assert!(within(input, v));
    }
    if let Ok(BlobIndex::InlinedTtl { value, .. }) = BlobIndex::decode_from(input) {
        assert!(within(input, value));
    }
    let _ = PinnableWideColumns::new().set_wide_column_value(input.to_vec());
}

proptest! {
    #[test]
    fn round_trips(e in entity(), blob_mask in any::<u16>(), r in any::<(u64, u64, u64, u8)>()) {
        let columns: Vec<WideColumn<'_>> =
            e.iter().map(|(n, v)| WideColumn::new(n.as_slice(), v.as_slice())).collect();
        let mut v1 = Vec::new();
        serialize(&columns, &mut v1).unwrap();
        prop_assert_eq!(v1.len(), serialized_size_v1(&columns));
        prop_assert_eq!(deserialize(&v1, None).unwrap(), columns.clone());

        let reference = BlobReference { file_number: r.0, offset: r.1, size: r.2, compression: r.3 };
        let blobs: Vec<(usize, BlobIndex<'_>)> = (0..columns.len())
            .filter(|i| blob_mask >> i & 1 == 1)
            .map(|i| (i, BlobIndex::Blob(reference)))
            .collect();
        let mut v2 = Vec::new();
        serialize_v2(&columns, &blobs, &mut v2).unwrap();
        let mut out = Vec::new();
        let back = deserialize(&v2, Some(&mut out)).unwrap();
        prop_assert_eq!(out, blobs.clone());
        prop_assert_eq!(has_blob_columns(&v2).unwrap(), !blobs.is_empty());
        for (i, c) in back.iter().enumerate() {
            prop_assert_eq!(c.name, columns[i].name);
            if !blobs.iter().any(|(b, _)| *b == i) {
                prop_assert_eq!(c.value, columns[i].value);
            }
        }
        let default = columns.first().filter(|c| c.name.is_empty());
        let (value, is_blob) = get_value_of_default_column(&v2).unwrap();
        prop_assert_eq!(is_blob, default.is_some() && blobs.iter().any(|(b, _)| *b == 0));
        if !is_blob {
            prop_assert_eq!(value, default.map_or(&[][..], |c| c.value));
        }
    }

    #[test]
    fn hex_dump_matches_operator(name in prop::collection::vec(any::<u8>(), 0..8),
                                 value in prop::collection::vec(any::<u8>(), 0..8)) {
        let mut out = String::new();
        dump_wide_columns(&[WideColumn::new(name.as_slice(), value.as_slice())], &mut out, true);
        let hex = |b: &[u8]| if b.is_empty() { String::new() } else {
            format!("0x{}", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        };
        prop_assert_eq!(out, format!("{}:{}", hex(&name), hex(&value)));
    }

    #[test]
    fn arbitrary_bytes_never_panic(input in prop::collection::vec(any::<u8>(), 0..64)) {
        decode_all(&input);
    }

    #[test]
    fn mutated_entities_never_panic(e in entity(), blob_mask in any::<u16>(),
                                    at in any::<usize>(), byte in any::<u8>(), cut in any::<usize>()) {
        let columns: Vec<WideColumn<'_>> =
            e.iter().map(|(n, v)| WideColumn::new(n.as_slice(), v.as_slice())).collect();
        let reference = BlobReference { file_number: 7, offset: 9, size: 11, compression: 0 };
        let blobs: Vec<(usize, BlobIndex<'_>)> = (0..columns.len())
            .filter(|i| blob_mask >> i & 1 == 1)
            .map(|i| (i, BlobIndex::Blob(reference)))
            .collect();
        for mut bytes in [
            { let mut v = Vec::new(); serialize(&columns, &mut v).unwrap(); v },
            { let mut v = Vec::new(); serialize_v2(&columns, &blobs, &mut v).unwrap(); v },
        ] {
            let i = at % bytes.len();
            bytes[i] = byte;
            decode_all(&bytes);
            bytes.truncate(cut % (bytes.len() + 1));
            decode_all(&bytes);
        }
    }
}

/// The version boundaries of docs/research/24 §1.17: 3, the first version RocksDB does not
/// define, is corruption; 0, below the first, parses as version 1 [R
/// db/wide/wide_column_serialization.cc:470-477].
#[test]
fn version_boundaries() {
    use mantle_engine::db::wide::wide_column_serialization::deserialize_simple;
    use mantle_engine::util::coding::put_varint32;
    for (version, ok) in [(0u32, true), (1, true), (2, true), (3, false)] {
        let mut buf = Vec::new();
        put_varint32(&mut buf, version);
        put_varint32(&mut buf, 0); // no columns
        if version >= 2 {
            // Version 2's skip info: three empty sections, so that only the version decides
            // whether version 3 parses.
            for _ in 0..3 {
                put_varint32(&mut buf, 0);
            }
        }
        assert_eq!(deserialize_simple(&buf).is_ok(), ok, "version {version}");
    }
}

/// A blob reference ends with exactly one compression byte [R db/blob/blob_index.h:117-122]:
/// none is truncation, two are trailing bytes.
#[test]
fn blob_reference_ends_with_one_byte() {
    use mantle_engine::db::blob::blob_index::{encode_blob, encode_blob_ttl};
    let mut blob = Vec::new();
    encode_blob(&mut blob, 7, 100, 12, 1);
    let mut ttl = Vec::new();
    encode_blob_ttl(&mut ttl, 99, 7, 100, 12, 1);
    for encoded in [blob, ttl] {
        assert!(BlobIndex::decode_from(&encoded).is_ok());
        assert!(BlobIndex::decode_from(&encoded[..encoded.len() - 1]).is_err());
        let mut longer = encoded.clone();
        longer.push(0);
        assert!(BlobIndex::decode_from(&longer).is_err());
    }
}
