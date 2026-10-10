//! The port's wide-column entities and blob indexes against RocksDB 11.8.1 itself: 4,096
//! random entities serialized in version 1 and in version 2 with random blob columns, their
//! default columns and blob flags, their blob-index encodings, and their deserialization,
//! compared with the digests RocksDB's own code printed (`tests/golden/p2_wide_gen.cc`, output
//! `tests/golden/p2_wide.txt`); and the first 16 entities RocksDB wrote, in full, parsed by the
//! port and re-serialized to the same bytes.
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

use std::collections::BTreeMap;

use common::SplitMix64;
use mantle_engine::db::blob::blob_index::{
    BlobIndex, encode_blob, encode_blob_ttl, encode_inlined_ttl,
};
use mantle_engine::db::wide::wide_column_serialization::{
    VERSION1, deserialize, get_value_of_default_column, get_version, has_blob_columns, serialize,
    serialize_v2,
};
use mantle_engine::db::wide::wide_columns::WideColumn;

const GOLDEN: &str = include_str!("golden/p2_wide.txt");
const DRAWS: usize = 4096;
const HEX_DRAWS: usize = 16;
const WINDOW: usize = 32;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;
const ALPHABET: [u8; 4] = [b'a', b'b', 0x00, 0xff];

struct Record {
    count: usize,
    d: u64,
    digests: Vec<u64>,
}

impl Record {
    fn new() -> Self {
        Self {
            count: 0,
            d: FNV_OFFSET,
            digests: Vec::new(),
        }
    }
    fn bytes(&mut self, b: &[u8]) {
        for &x in b {
            self.d ^= u64::from(x);
            self.d = self.d.wrapping_mul(FNV_PRIME);
        }
    }
    fn u8(&mut self, v: u8) {
        self.bytes(&[v]);
    }
    fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }
    fn str(&mut self, s: &[u8]) {
        self.u64(s.len() as u64);
        self.bytes(s);
    }
    fn end_item(&mut self) {
        self.count += 1;
        if self.count.is_multiple_of(WINDOW) {
            self.digests.push(self.d);
            self.d = FNV_OFFSET;
        }
    }
    fn finish(mut self) -> (usize, Vec<u64>) {
        if !self.count.is_multiple_of(WINDOW) {
            self.digests.push(self.d);
        }
        (self.count, self.digests)
    }
}

/// Each golden record's name with its count and digests, and the encoded pairs.
type Computed = (BTreeMap<String, (usize, Vec<u64>)>, Vec<(Vec<u8>, Vec<u8>)>);

fn compute() -> Computed {
    let mut v1 = Record::new();
    let mut v2 = Record::new();
    let mut dflt = Record::new();
    let mut blobs = Record::new();
    let mut parsed = Record::new();
    let mut hex = Vec::new();
    let mut r = SplitMix64(0x5749_4445_4330_4C53);
    for draw in 0..DRAWS {
        let mut entity: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let n = r.next() % 9;
        for _ in 0..n {
            let name_len = r.next() % 6;
            let name: Vec<u8> = (0..name_len)
                .map(|_| ALPHABET[(r.next() % 4) as usize])
                .collect();
            let value_len = r.next() % 300;
            let value: Vec<u8> = (0..value_len).map(|_| (r.next() & 0xff) as u8).collect();
            entity.insert(name, value);
        }
        let columns: Vec<WideColumn<'_>> = entity
            .iter()
            .map(|(k, v)| WideColumn::new(k.as_slice(), v.as_slice()))
            .collect();

        let mut s1 = Vec::new();
        serialize(&columns, &mut s1).unwrap();
        v1.str(&s1);

        let mask = r.next();
        let mut encodings: Vec<(usize, Vec<u8>)> = Vec::new();
        for i in 0..columns.len() {
            if (mask >> i) & 1 == 0 {
                continue;
            }
            let kind = r.next() % 3;
            let mut enc = Vec::new();
            if kind == 0 {
                let file = r.next() % 1_000_000;
                let offset = r.next();
                let size = r.next() % 1_000_000;
                let comp = (r.next() % 8) as u8;
                encode_blob(&mut enc, file, offset, size, comp);
            } else if kind == 1 {
                let exp = r.next();
                let file = r.next() % 1_000_000;
                let offset = r.next();
                let size = r.next() % 1_000_000;
                let comp = (r.next() % 8) as u8;
                encode_blob_ttl(&mut enc, exp, file, offset, size, comp);
            } else {
                let exp = r.next();
                let len = (r.next() % 20) as usize;
                encode_inlined_ttl(&mut enc, exp, &vec![b'i'; len]);
            }
            blobs.str(&enc);
            encodings.push((i, enc));
        }
        blobs.end_item();
        let blob_columns: Vec<(usize, BlobIndex<'_>)> = encodings
            .iter()
            .map(|(i, enc)| (*i, BlobIndex::decode_from(enc).unwrap()))
            .collect();

        let mut s2 = Vec::new();
        serialize_v2(&columns, &blob_columns, &mut s2).unwrap();
        v2.str(&s2);

        for s in [&s1, &s2] {
            match get_value_of_default_column(s) {
                Ok((value, is_blob)) => {
                    dflt.u8(1);
                    dflt.str(value);
                    dflt.u8(u8::from(is_blob));
                }
                Err(_) => {
                    dflt.u8(0);
                    dflt.str(&[]);
                    dflt.u8(0);
                }
            }
            match has_blob_columns(s) {
                Ok(has) => {
                    dflt.u8(1);
                    dflt.u8(u8::from(has));
                }
                Err(_) => {
                    dflt.u8(0);
                    dflt.u8(0);
                }
            }
        }

        let mut back_blobs = Vec::new();
        let back = deserialize(&s2, Some(&mut back_blobs)).unwrap();
        for c in &back {
            parsed.str(c.name);
            parsed.str(c.value);
        }
        for (i, bi) in &back_blobs {
            parsed.u64(*i as u64);
            let mut enc = Vec::new();
            bi.encode_to(&mut enc);
            parsed.str(&enc);
        }

        v1.end_item();
        v2.end_item();
        dflt.end_item();
        parsed.end_item();
        if draw < HEX_DRAWS {
            hex.push((s1, s2));
        }
    }
    let records = [
        ("serialize_v1", v1),
        ("serialize_v2", v2),
        ("default_column", dflt),
        ("blob_index_encode", blobs),
        ("deserialize_v2", parsed),
    ]
    .into_iter()
    .map(|(name, rec)| (name.to_string(), rec.finish()))
    .collect();
    (records, hex)
}

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn wide_columns_match_rocksdb() {
    let mut golden = BTreeMap::new();
    let mut golden_hex: Vec<(String, Vec<u8>)> = Vec::new();
    for line in GOLDEN.lines() {
        let mut fields = line.split_whitespace();
        let name = fields.next().unwrap();
        if name == "hex" {
            let version = fields.next().unwrap().to_string();
            golden_hex.push((version, unhex(fields.next().unwrap_or(""))));
            continue;
        }
        let count: usize = fields.next().unwrap().parse().unwrap();
        let digests: Vec<u64> = fields
            .map(|d| u64::from_str_radix(d, 16).unwrap())
            .collect();
        golden.insert(name.to_string(), (count, digests));
    }
    let (ours, hex) = compute();
    assert_eq!(
        golden.keys().collect::<Vec<_>>(),
        ours.keys().collect::<Vec<_>>()
    );
    for (name, (count, digests)) in &golden {
        let (our_count, our_digests) = &ours[name];
        assert_eq!(count, our_count, "{name}: count");
        for (w, (g, o)) in digests.iter().zip(our_digests).enumerate() {
            assert_eq!(
                g,
                o,
                "{name}: draws {}..{} differ",
                w * WINDOW,
                (w + 1) * WINDOW
            );
        }
    }

    // RocksDB's bytes, in full: equal to the port's, and parsed and re-serialized to the same.
    assert_eq!(golden_hex.len(), 2 * HEX_DRAWS);
    for (pair, (s1, s2)) in golden_hex.chunks(2).zip(&hex) {
        assert_eq!(pair[0].0, "v1");
        assert_eq!(&pair[0].1, s1);
        assert_eq!(pair[1].0, "v2");
        assert_eq!(&pair[1].1, s2);

        let cpp_v1 = &pair[0].1;
        assert_eq!(get_version(cpp_v1).unwrap(), VERSION1);
        let columns = deserialize(cpp_v1, None).unwrap();
        let mut again = Vec::new();
        serialize(&columns, &mut again).unwrap();
        assert_eq!(&again, cpp_v1);

        let cpp_v2 = &pair[1].1;
        let mut blob_columns = Vec::new();
        let columns = deserialize(cpp_v2, Some(&mut blob_columns)).unwrap();
        let mut again = Vec::new();
        serialize_v2(&columns, &blob_columns, &mut again).unwrap();
        assert_eq!(&again, cpp_v2);
    }
}
