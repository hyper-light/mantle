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

/// The byte after `LOCAL` that says which kind of a range's own rows a key holds. Each kind is
/// read by scanning its marker, so every kind has a marker of its own: two kinds that shared
/// one would each read the other's rows as their own.
pub mod marker {
    /// The Bucket range's creates and deletes in progress.
    pub const ATTEMPT: u8 = b'a';
    pub const CLOCK: u8 = b'c';
    /// A session's place in the order of last use.
    pub const EXPIRY: u8 = b'e';
    /// The Name range's gate floor.
    pub const FLOOR: u8 = b'f';
    pub const GATE: u8 = b'g';
    /// The Name range's lineage: its descriptor and the child of its last split.
    pub const LINEAGE: u8 = b'l';
    /// How many sessions the range holds.
    pub const SESSIONS: u8 = b'n';
    /// The last snapshot a member installed.
    pub const INSTALLED: u8 = b'p';
    /// The Name range's queue of released files.
    pub const RELEASED: u8 = b'q';
    /// The group's configuration.
    pub const CONFIGURATION: u8 = b'r';
    pub const SESSION: u8 = b's';
    /// The File range's files, and the Block range's blocks, whose handover the sweep has
    /// not yet settled.
    pub const UNSETTLED: u8 = b'u';

    /// Every marker in use.
    pub const ALL: [u8; 12] = [
        ATTEMPT,
        CLOCK,
        EXPIRY,
        FLOOR,
        GATE,
        LINEAGE,
        UNSETTLED,
        SESSIONS,
        INSTALLED,
        RELEASED,
        CONFIGURATION,
        SESSION,
    ];
}
/// The rows of the range's layer.
pub const DATA: u8 = 0x01;
/// Reverse rows: an index kept in the range of the rows it indexes and sorted by another of
/// their fields, the Block layer's by volume and the Bucket layer's by owner.
pub const REVERSE: u8 = 0x02;
/// The Name range's marks of the files it took, until the sweep settles them, ordered by
/// object key as its rows are: a split cuts them at the same key, and listings never see them
/// (docs/design/metadata.md §2).
pub const MARKS: u8 = 0x03;

/// After an object key's routing key in the marks' space: one of its marks. A file ID may
/// begin with 0xFF, which would read as an escaped 0x00 of the key.
const MARK: u8 = 1;

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

/// The first key of a bucket's objects' rows, and a key past the last. After the bucket's
/// name ends, an object key's first byte is never 0xFF, which UTF-8 never holds and escaping
/// only writes after a 0x00, so every row of the bucket sorts before the second key and every
/// row of a longer bucket name after it.
pub fn bucket_span(bucket: &str) -> (Vec<u8>, Vec<u8>) {
    let from = objects(bucket);
    let mut past = from.clone();
    past.push(0xFF);
    (from, past)
}

/// The key of a Name range's gate for a bucket (docs/design/metadata.md §2).
pub fn gate(bucket: &str) -> Vec<u8> {
    let mut out = vec![LOCAL, marker::GATE];
    put_string(&mut out, bucket.as_bytes());
    out
}

/// A key past every gate.
pub const GATES_END: [u8; 3] = [LOCAL, marker::GATE, 0xFF];

/// The bucket a gate's key names; `None` if it is not one.
pub fn decode_gate(k: &[u8]) -> Option<String> {
    let rest = k.strip_prefix(&[LOCAL, marker::GATE])?;
    let (name, rest) = take_string(rest)?;
    if !rest.is_empty() {
        return None;
    }
    String::from_utf8(name).ok()
}

/// The routing key of an object key: its bucket, then its key. Every row of the key, in each
/// space that holds them, is the space's byte, this, and a suffix whose first byte is below
/// 0xFF, so a span between two routing keys bounds the rows of its keys by bytes in every
/// space, and a Name range's span is such a pair (docs/design/metadata.md §3).
pub fn route(bucket: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len().saturating_add(key.len()).saturating_add(2));
    put_string(&mut out, bucket.as_bytes());
    put_string(&mut out, key.as_bytes());
    out
}

/// The bucket and key a routing key names; `None` if it is not one.
pub fn decode_route(r: &[u8]) -> Option<(String, String)> {
    let (bucket, rest) = take_string(r)?;
    let (key, rest) = take_string(rest)?;
    if !rest.is_empty() {
        return None;
    }
    Some((
        String::from_utf8(bucket).ok()?,
        String::from_utf8(key).ok()?,
    ))
}

/// The routing keys of a bucket's object keys lie in `[from, past)`: `from` is its empty
/// key's, the least, and no routing key of another bucket falls between.
pub fn bucket_routes(bucket: &str) -> (Vec<u8>, Vec<u8>) {
    let mut from = Vec::with_capacity(bucket.len().saturating_add(2));
    put_string(&mut from, bucket.as_bytes());
    let mut past = from.clone();
    from.push(0x00);
    past.push(0xFF);
    (from, past)
}

/// The rows of the Name layer's spaces, its objects' and its marks', whose routing keys lie in
/// `[from, to)`, or from `from` on when `to` is `None`.
pub fn name_spans(from: &[u8], to: Option<&[u8]>) -> [(Vec<u8>, Vec<u8>); 2] {
    [DATA, MARKS].map(|space| {
        let mut lo = vec![space];
        lo.extend_from_slice(from);
        // A routing key never begins with 0xFF, so this bounds the whole space.
        let mut hi = vec![space];
        hi.extend_from_slice(to.unwrap_or(&[0xFF]));
        (lo, hi)
    })
}

/// The key of a bucket's row in the Bucket range's index of creates and deletes in progress,
/// which the collector reads for attempts to take over (docs/design/metadata.md §2).
pub fn attempt(bucket: &str) -> Vec<u8> {
    let mut out = vec![LOCAL, marker::ATTEMPT];
    put_string(&mut out, bucket.as_bytes());
    out
}

/// The keys of the index of attempts in progress after `after`, or from its first, and a key
/// past its last.
pub fn attempts_after(after: Option<&str>) -> (Vec<u8>, Vec<u8>) {
    let from = match after {
        Some(bucket) => {
            let mut k = attempt(bucket);
            k.push(0);
            k
        }
        None => vec![LOCAL, marker::ATTEMPT],
    };
    (from, vec![LOCAL, marker::ATTEMPT.saturating_add(1)])
}

/// The bucket an attempt's index key names; `None` if it is not one.
pub fn decode_attempt(k: &[u8]) -> Option<String> {
    let rest = k.strip_prefix(&[LOCAL, marker::ATTEMPT])?;
    let (name, rest) = take_string(rest)?;
    if !rest.is_empty() {
        return None;
    }
    String::from_utf8(name).ok()
}

/// The key of the Name range's mark that it took `file`, made for `key` in `bucket`.
pub fn mark(bucket: &str, key: &str, file: u128) -> Vec<u8> {
    let mut out = marks_of(bucket);
    put_string(&mut out, key.as_bytes());
    out.push(MARK);
    out.extend_from_slice(&file.to_be_bytes());
    out
}

/// The first key of a bucket's marks, and a key past its last.
pub fn marks_span(bucket: &str) -> (Vec<u8>, Vec<u8>) {
    let from = marks_of(bucket);
    let mut past = from.clone();
    past.push(0xFF);
    (from, past)
}

/// The file a mark's key names; `None` if it is not one.
pub fn decode_mark(k: &[u8]) -> Option<(String, String, u128)> {
    let rest = k.strip_prefix(&[MARKS])?;
    let (bucket, rest) = take_string(rest)?;
    let (key, rest) = take_string(rest)?;
    let rest = rest.strip_prefix(&[MARK])?;
    Some((
        String::from_utf8(bucket).ok()?,
        String::from_utf8(key).ok()?,
        u128::from_be_bytes(rest.try_into().ok()?),
    ))
}

fn marks_of(bucket: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len().saturating_add(2));
    out.push(MARKS);
    put_string(&mut out, bucket.as_bytes());
    out
}

/// The key of a file in the File range's queue of files whose handover is not yet settled, or
/// of a block in the Block range's: its handover deadline, then its ID, so the sweep takes them
/// as their deadlines pass (docs/design/metadata.md §2).
pub fn unsettled(deadline_ns: u64, id: u128) -> Vec<u8> {
    let mut out = Vec::with_capacity(26);
    out.extend_from_slice(&[LOCAL, marker::UNSETTLED]);
    out.extend_from_slice(&deadline_ns.to_be_bytes());
    out.extend_from_slice(&id.to_be_bytes());
    out
}

/// The first key of the unsettled queue, and the first of those whose deadline is at or after
/// `before_ns`.
pub fn unsettled_before(before_ns: u64) -> (Vec<u8>, Vec<u8>) {
    let mut past = vec![LOCAL, marker::UNSETTLED];
    past.extend_from_slice(&before_ns.to_be_bytes());
    (vec![LOCAL, marker::UNSETTLED], past)
}

/// The deadline and ID an unsettled file's or block's key names; `None` if it is not one.
pub fn decode_unsettled(k: &[u8]) -> Option<(u64, u128)> {
    let rest = k.strip_prefix(&[LOCAL, marker::UNSETTLED])?;
    let (deadline, file) = rest.split_first_chunk::<8>()?;
    Some((
        u64::from_be_bytes(*deadline),
        u128::from_be_bytes(file.try_into().ok()?),
    ))
}

/// The range's queue of the files it released, oldest first (docs/design/metadata.md §2).
const RELEASED: [u8; 2] = [LOCAL, marker::RELEASED];

/// The key of a released file's row: the range's time when it was released, then the file,
/// so the collector takes them in the order they were released.
pub fn released(time_ns: u64, file: u128) -> Vec<u8> {
    let mut out = Vec::with_capacity(26);
    out.extend_from_slice(&RELEASED);
    out.extend_from_slice(&time_ns.to_be_bytes());
    out.extend_from_slice(&file.to_be_bytes());
    out
}

/// The first key of the released queue, and the first of those released at or after
/// `before_ns`.
pub fn released_before(before_ns: u64) -> (Vec<u8>, Vec<u8>) {
    let mut past = RELEASED.to_vec();
    past.extend_from_slice(&before_ns.to_be_bytes());
    (RELEASED.to_vec(), past)
}

/// Every key of the released queue: its first, and a key past its last.
pub fn released_all() -> (Vec<u8>, Vec<u8>) {
    let mut past = released(u64::MAX, u128::MAX);
    past.push(0);
    (RELEASED.to_vec(), past)
}

/// The time and file a released file's key names; `None` if it is not one.
pub fn decode_released(k: &[u8]) -> Option<(u64, u128)> {
    let rest = k.strip_prefix(&RELEASED)?;
    let (time, file) = rest.split_first_chunk::<8>()?;
    Some((
        u64::from_be_bytes(*time),
        u128::from_be_bytes(file.try_into().ok()?),
    ))
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

/// File-layer rows under a file's ID.
const HEADER: u8 = 1;
const EXTENT: u8 = 2;
/// Block-layer rows under a block's ID, after its header.
const CHUNK: u8 = 2;
const ORIGIN: u8 = 3;

/// The key of a file's header row.
pub fn file_header(file: u128) -> Vec<u8> {
    id_row(file, HEADER)
}

/// The key of a file's extent that ends at byte `end` of the file: keyed by its end, a seek
/// forward from any offset lands on the extent that holds it.
pub fn file_extent(file: u128, end: u64) -> Vec<u8> {
    let mut out = id_row(file, EXTENT);
    out.extend_from_slice(&end.to_be_bytes());
    out
}

/// The end an extent's key names.
pub fn extent_end(k: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(id_row_tail(k, EXTENT)?.try_into().ok()?))
}

/// The key of a block's header row.
pub fn block_header(block: u128) -> Vec<u8> {
    id_row(block, HEADER)
}

/// The key of the row locating chunk `index` of a block.
pub fn block_chunk(block: u128, index: u16) -> Vec<u8> {
    let mut out = id_row(block, CHUNK);
    out.extend_from_slice(&index.to_be_bytes());
    out
}

/// The key of the row recording where a block came from.
pub fn block_origin(block: u128) -> Vec<u8> {
    id_row(block, ORIGIN)
}

/// The first and last possible keys of every row of one file or block.
pub fn id_rows(id: u128) -> (Vec<u8>, Vec<u8>) {
    (id_row(id, 0), id_row(id, u8::MAX))
}

fn id_row(id: u128, kind: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(18);
    out.push(DATA);
    out.extend_from_slice(&id.to_be_bytes());
    out.push(kind);
    out
}

/// What follows the ID in a File- or Block-layer key of `kind`.
fn id_row_tail(k: &[u8], kind: u8) -> Option<&[u8]> {
    let (&prefix, rest) = k.split_first()?;
    let (&found, tail) = rest.get(16..)?.split_first()?;
    if prefix == DATA && found == kind {
        Some(tail)
    } else {
        None
    }
}

/// The key of the reverse row saying `block` has a chunk on `volume`. It sorts by volume, then
/// block, so one scan lists a range's blocks on a volume, as Tectonic keys its reverse index
/// `(disk_id, blk_id)` and shards it by block [01 Table 1]; it is kept in the range of its
/// block, so it changes in the same transaction as the block's rows.
pub fn reverse(volume: u128, block: u128) -> Vec<u8> {
    let mut out = Vec::with_capacity(33);
    out.push(REVERSE);
    out.extend_from_slice(&volume.to_be_bytes());
    out.extend_from_slice(&block.to_be_bytes());
    out
}

/// The first key of `volume`'s reverse rows, and a key past its last.
pub fn reverse_rows(volume: u128) -> (Vec<u8>, Vec<u8>) {
    let mut past = reverse(volume, u128::MAX);
    past.push(0);
    (reverse(volume, 0), past)
}

/// The volume and block a reverse row's key names.
pub fn decode_reverse(k: &[u8]) -> Option<(u128, u128)> {
    let rest = k.strip_prefix(&[REVERSE])?;
    let volume = u128::from_be_bytes(rest.get(..16)?.try_into().ok()?);
    let block = u128::from_be_bytes(rest.get(16..)?.try_into().ok()?);
    Some((volume, block))
}

/// The key of a bucket's row in the Bucket layer.
pub fn bucket(name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len().saturating_add(2));
    out.push(DATA);
    put_string(&mut out, name.as_bytes());
    out
}

/// The key of an owner's row, which counts its buckets. The reverse rows of its buckets
/// follow it.
pub fn owner(owner: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(owner.len().saturating_add(2));
    out.push(REVERSE);
    put_string(&mut out, owner.as_bytes());
    out
}

/// The key of the reverse row saying `owner` owns `bucket`.
pub fn owned(owner: &str, bucket: &str) -> Vec<u8> {
    let mut out = self::owner(owner);
    put_string(&mut out, bucket.as_bytes());
    out
}

/// The first key of `owner`'s reverse rows after bucket `after`, or of all of them, and a key
/// past the last.
pub fn owned_rows(owner: &str, after: Option<&str>) -> (Vec<u8>, Vec<u8>) {
    let head = self::owner(owner);
    let mut past = head.clone();
    past.push(0xFF);
    let mut from = match after {
        Some(bucket) => owned(owner, bucket),
        None => head,
    };
    from.push(0);
    (from, past)
}

/// The owner and bucket a reverse row's key names.
pub fn decode_owned(k: &[u8]) -> Option<(String, String)> {
    let rest = k.strip_prefix(&[REVERSE])?;
    let (owner, rest) = take_string(rest)?;
    let (bucket, rest) = take_string(rest)?;
    if !rest.is_empty() {
        return None;
    }
    Some((
        String::from_utf8(owner).ok()?,
        String::from_utf8(bucket).ok()?,
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

    #[test]
    fn every_kind_of_a_ranges_own_rows_has_its_own_marker() {
        let mut seen = marker::ALL.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), marker::ALL.len());
    }

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
    fn a_bucket_span_holds_its_rows_and_no_other_buckets() {
        let (from, past) = bucket_span("b");
        for inside in [
            name("b", "", &NameRow::Null),
            name("b", "\0k", &NameRow::Version(0)),
            name("b", "\u{10FFFF}", &NameRow::Part(vec![0xFF], u16::MAX)),
        ] {
            assert!(from <= inside && inside < past, "{inside:?}");
        }
        for outside in [
            name("b\0", "", &NameRow::Null),
            name("b\0\0", "k", &NameRow::Null),
            name("ba", "", &NameRow::Null),
            name("a", "\u{10FFFF}", &NameRow::Null),
        ] {
            assert!(outside < from || past <= outside, "{outside:?}");
        }
    }

    fn route_text() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop_oneof![Just('\0'), Just('a'), Just('b'), Just('\u{7F}'), Just('é')],
            0..4,
        )
        .prop_map(|c| c.into_iter().collect())
    }

    proptest! {
        /// A span between routing keys holds, in each space, the rows of exactly the object
        /// keys whose routing keys it holds: a mark whose file ID begins with 0xFF included.
        #[test]
        fn a_routing_span_bounds_every_row_of_its_keys(
            bucket in route_text(),
            key in route_text(),
            r in row(),
            file in prop_oneof![Just(u128::MAX), Just(0), any::<u128>()],
            lo in (route_text(), route_text()),
            hi in prop::option::of((route_text(), route_text())),
        ) {
            let at = route(&bucket, &key);
            let (lo, hi) = (route(&lo.0, &lo.1), hi.map(|(b, k)| route(&b, &k)));
            let inside = lo <= at && hi.as_ref().is_none_or(|hi| at < *hi);
            let [data, marks] = name_spans(&lo, hi.as_deref());
            for (row, (from, to)) in [(name(&bucket, &key, &r), data), (mark(&bucket, &key, file), marks)] {
                prop_assert_eq!(from <= row && row < to, inside);
            }
            prop_assert_eq!(decode_route(&at), Some((bucket.clone(), key.clone())));
            let (first, past) = bucket_routes(&bucket);
            prop_assert!(first <= at && at < past);
            prop_assert_eq!(decode_mark(&mark(&bucket, &key, file)), Some((bucket, key, file)));
        }

        /// Routing keys order as their `(bucket, key)` pairs, and a bucket's routes hold only
        /// its own keys.
        #[test]
        fn routing_keys_order_as_their_pairs(
            a in (route_text(), route_text()),
            b in (route_text(), route_text()),
        ) {
            let pair = |p: &(String, String)| (p.0.as_bytes().to_vec(), p.1.as_bytes().to_vec());
            prop_assert_eq!(route(&a.0, &a.1).cmp(&route(&b.0, &b.1)), pair(&a).cmp(&pair(&b)));
            let (first, past) = bucket_routes(&a.0);
            let at = route(&b.0, &b.1);
            prop_assert_eq!(first <= at && at < past, a.0 == b.0);
        }
    }

    #[test]
    fn gate_keys_decode_to_their_bucket() {
        assert_eq!(decode_gate(&gate("b\0c")), Some("b\0c".into()));
        assert!(gate("\u{10FFFF}") < GATES_END.to_vec());
        assert_eq!(decode_gate(&GATES_END), None);
        assert_eq!(decode_route(&route("b", "k")[..3]), None);
    }

    #[test]
    fn owner_rows_list_its_buckets_after_its_count() {
        let (from, past) = owned_rows("o", None);
        let head = owner("o");
        assert!(head < from);
        for bucket in ["a", "b\0", "zz"] {
            let k = owned("o", bucket);
            assert!(from <= k && k < past);
            assert_eq!(decode_owned(&k), Some(("o".into(), bucket.into())));
        }
        assert!(owned("o\0", "a") >= past);
        let (after_a, _) = owned_rows("o", Some("a"));
        assert!(owned("o", "a") < after_a && after_a < owned("o", "a\0"));
        assert_eq!(decode_owned(&head), None);
    }

    #[test]
    fn file_and_block_keys_decode_only_as_their_kind() {
        assert_eq!(extent_end(&file_extent(7, 35)), Some(35));
        assert_eq!(extent_end(&block_chunk(7, 3)), None);
        assert_eq!(extent_end(&file_header(7)), None);
        let (from, past) = reverse_rows(4);
        let k = reverse(4, u128::MAX);
        assert!(from <= k && k < past && past < reverse(5, 0));
        assert_eq!(decode_reverse(&k), Some((4, u128::MAX)));
        assert_eq!(decode_reverse(&past), None);
        assert_eq!(decode_reverse(&file_header(4)), None);
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
