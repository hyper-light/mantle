//! Row keys as bytes whose order is the one the design needs (docs/design/metadata.md §1).
//!
//! A string is a FoundationDB tuple-layer byte string without its type code: each 0x00 byte
//! written as 0x00 0xFF, and the string ended by 0x00 ("FDB Tuple Layer Typecodes", byte
//! string). A string then sorts before every longer string it begins, and so long as the
//! component after its end never begins with 0xFF, which would read as an escaped 0x00, keys
//! sort and decode as their components. Every component that can follow a string begins with
//! a small marker byte. Numbers are fixed-width and big-endian.

/// A range's rows about itself: its applied index and descriptor.
pub const LOCAL: u8 = 0x00;
/// The rows of the range's layer.
pub const DATA: u8 = 0x01;

/// Name-layer rows under `(bucket, key)`, in the order they sort.
const NULL: u8 = 1;
const VERSION: u8 = 2;
const UPLOAD: u8 = 3;
/// After an upload's ID: one of its parts.
const PART: u8 = 1;

/// A Name-layer row of one object key. Rows sort null pointer, then versions newest first,
/// then uploads by ID, each followed by its parts in number order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameRow {
    /// Where the key's null version is.
    Null,
    /// A version, at its order: newest first.
    Version(u64),
    /// A multipart upload in progress.
    Upload(Vec<u8>),
    /// A part of an upload.
    Part(Vec<u8>, u16),
}

/// The key of a Name-layer row.
pub fn name(bucket: &str, key: &str, row: &NameRow) -> Vec<u8> {
    let mut out = object(bucket, key);
    match row {
        NameRow::Null => out.push(NULL),
        NameRow::Version(order) => {
            out.push(VERSION);
            out.extend_from_slice(&order.to_be_bytes());
        }
        NameRow::Upload(upload) => {
            out.push(UPLOAD);
            put_string(&mut out, upload);
        }
        NameRow::Part(upload, part) => {
            out.push(UPLOAD);
            put_string(&mut out, upload);
            out.push(PART);
            out.extend_from_slice(&part.to_be_bytes());
        }
    }
    out
}

/// The prefix every row of one object key begins with.
pub fn object(bucket: &str, key: &str) -> Vec<u8> {
    let mut out = objects(bucket);
    put_string(&mut out, key.as_bytes());
    out
}

/// The prefix every row of a bucket's objects begins with.
pub fn objects(bucket: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len().saturating_add(2));
    out.push(DATA);
    put_string(&mut out, bucket.as_bytes());
    out
}

/// Where in the bucket's rows the object keys at or after `from` begin. `from` is any byte
/// string, as a listing's seek positions are (s3 list.rs), not only a key: escaping keeps
/// order and prefixes, so the first row at or after this is the first object key at or after
/// `from`.
pub fn position(bucket: &str, from: &[u8]) -> Vec<u8> {
    let mut out = objects(bucket);
    escape(&mut out, from);
    out
}

/// The bucket, object key and row a Name-layer key names; `None` if it is not one.
pub fn decode_name(k: &[u8]) -> Option<(String, String, NameRow)> {
    let rest = k.strip_prefix(&[DATA])?;
    let (bucket, rest) = take_string(rest)?;
    let (key, rest) = take_string(rest)?;
    let (&kind, rest) = rest.split_first()?;
    let row = match kind {
        NULL if rest.is_empty() => NameRow::Null,
        VERSION => NameRow::Version(u64::from_be_bytes(rest.try_into().ok()?)),
        UPLOAD => {
            let (upload, rest) = take_string(rest)?;
            match rest.split_first() {
                None => NameRow::Upload(upload),
                Some((&PART, part)) => {
                    NameRow::Part(upload, u16::from_be_bytes(part.try_into().ok()?))
                }
                Some(_) => return None,
            }
        }
        _ => return None,
    };
    Some((
        String::from_utf8(bucket).ok()?,
        String::from_utf8(key).ok()?,
        row,
    ))
}

/// Appends `s` escaped and ended.
fn put_string(out: &mut Vec<u8>, s: &[u8]) {
    escape(out, s);
    out.push(0x00);
}

/// Appends `s` with each 0x00 written as 0x00 0xFF.
fn escape(out: &mut Vec<u8>, s: &[u8]) {
    for &b in s {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
}

/// The string at the start of `k`, and what follows its end.
fn take_string(k: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut rest = k;
    loop {
        let (&b, tail) = rest.split_first()?;
        if b != 0x00 {
            out.push(b);
            rest = tail;
            continue;
        }
        match tail.split_first() {
            Some((&0xFF, tail)) => {
                out.push(0x00);
                rest = tail;
            }
            _ => return Some((out, tail)),
        }
    }
}

/// Crockford's base-32 alphabet, which ascends in ASCII.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// The version ID S3 shows for the null version (05 §7.1).
pub const NULL_VERSION: &str = "null";

/// A version's ID: its order in 13 characters of Crockford's base 32. The alphabet ascends,
/// so IDs sort as their versions do, and it holds no `+`, `/` or `=`, so an ID needs no
/// escaping in a URL (05 §7.1).
pub fn version_id(order: u64) -> String {
    (0..13u32)
        .rev()
        .map(|i| {
            let digit = order.checked_shr(i.saturating_mul(5)).unwrap_or(0) & 31;
            usize::try_from(digit)
                .ok()
                .and_then(|d| ALPHABET.get(d))
                .map_or('0', |&c| char::from(c))
        })
        .collect()
}

/// The order a version ID names; `None` unless it is 13 characters of the alphabet that fit
/// in 64 bits.
pub fn parse_version_id(id: &str) -> Option<u64> {
    if id.len() != 13 {
        return None;
    }
    id.bytes().try_fold(0u64, |order, c| {
        let digit = ALPHABET.iter().position(|&a| a == c)?;
        order
            .checked_mul(32)?
            .checked_add(u64::try_from(digit).ok()?)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn row() -> impl Strategy<Value = NameRow> {
        let bytes = prop::collection::vec(any::<u8>(), 0..6);
        prop_oneof![
            Just(NameRow::Null),
            any::<u64>().prop_map(NameRow::Version),
            bytes.clone().prop_map(NameRow::Upload),
            (bytes, any::<u16>()).prop_map(|(u, p)| NameRow::Part(u, p)),
        ]
    }

    fn text() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop_oneof![Just('\0'), Just('a'), Just('b'), Just('é')],
            0..5,
        )
        .prop_map(|c| c.into_iter().collect())
    }

    proptest! {
        /// Keys sort as their `(bucket, key, row)` tuples do, and decode to them.
        #[test]
        fn keys_sort_as_their_components(
            a in ("[a-c]{1,3}", text(), row()),
            b in ("[a-c]{1,3}", text(), row()),
        ) {
            let (ka, kb) = (name(&a.0, &a.1, &a.2), name(&b.0, &b.1, &b.2));
            // The order the design gives rows (NameRow's doc comment).
            let rank = |r: &NameRow| match r {
                NameRow::Null => (0, 0, Vec::new(), None),
                NameRow::Version(o) => (1, *o, Vec::new(), None),
                NameRow::Upload(u) => (2, 0, u.clone(), None),
                NameRow::Part(u, p) => (2, 0, u.clone(), Some(*p)),
            };
            let tuple = |t: &(String, String, NameRow)| {
                (t.0.as_bytes().to_vec(), t.1.as_bytes().to_vec(), rank(&t.2))
            };
            prop_assert_eq!(ka.cmp(&kb), tuple(&a).cmp(&tuple(&b)));
            prop_assert_eq!(decode_name(&ka), Some(a.clone()));
            prop_assert!(ka.starts_with(&object(&a.0, &a.1)));
        }

        /// A seek position in object-key space lands before exactly the keys at or after it.
        #[test]
        fn positions_order_as_object_keys(
            key in text(),
            from in prop::collection::vec(prop_oneof![Just(0u8), Just(b'a'), Just(0xFF)], 0..5),
            r in row(),
        ) {
            let k = name("b", &key, &r);
            prop_assert_eq!(k >= position("b", &from), key.as_bytes() >= from.as_slice());
        }

        #[test]
        fn version_ids_round_trip_and_sort(a in any::<u64>(), b in any::<u64>()) {
            prop_assert_eq!(parse_version_id(&version_id(a)), Some(a));
            prop_assert_eq!(version_id(a).cmp(&version_id(b)), a.cmp(&b));
        }
    }

    #[test]
    fn version_ids_are_url_safe_and_exclude_null() {
        let id = version_id(u64::MAX);
        assert_eq!(id.len(), 13);
        assert!(id.bytes().all(|c| c.is_ascii_alphanumeric()));
        assert_eq!(parse_version_id(NULL_VERSION), None);
        assert_eq!(parse_version_id("ZZZZZZZZZZZZZ"), None);
        assert_eq!(parse_version_id("0000000000001"), Some(1));
        assert_eq!(decode_name(&[DATA, b'b', 0, b'k', 0, 9]), None);
        assert_eq!(decode_name(&[LOCAL]), None);
    }
}
