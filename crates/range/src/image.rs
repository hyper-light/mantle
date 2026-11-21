//! A snapshot's data: every row of the range as of its index, checked by a CRC-32C. The
//! rows travel inside the snapshot message for now; the production engine will move its
//! files out of band instead (docs/design/replica.md §6; 12 §6.3).

use mantle_codec::Reader;
use mantle_meta::engine::Row;

const FORMAT: u8 = 1;

/// The bytes before an image's first row: its format and its count of rows.
pub(crate) const HEAD: usize = 1 + 8;

/// Begins an image in `into`, which is empty: its head, whose count [`finish`] sets. The rows
/// follow a slice at a time ([`push_row`]), as a range images its rows a page a drive on a node's
/// shard (docs/design/replica.md §4).
pub fn begin(into: &mut Vec<u8>) {
    into.push(FORMAT);
    into.extend_from_slice(&0u64.to_le_bytes());
}

/// Appends a row: its key's length, its key, its value's length and its value. None for a key or
/// a value past a u32 of bytes, nothing appended.
pub fn push_row(into: &mut Vec<u8>, key: &[u8], value: &[u8]) -> Option<()> {
    let (k, v) = (
        u32::try_from(key.len()).ok()?,
        u32::try_from(value.len()).ok()?,
    );
    into.extend_from_slice(&k.to_le_bytes());
    into.extend_from_slice(key);
    into.extend_from_slice(&v.to_le_bytes());
    into.extend_from_slice(value);
    Some(())
}

/// Ends the image of `rows` rows [`begin`] began in `into`, whose bytes past the head have the
/// CRC-32C `rows_crc`, kept as each slice was written (`mantle_crc::crc32c_extend`): sets the
/// count and appends the checksum of the whole, combined from the head's and the rows'
/// (`mantle_crc::crc32c_combine`), so no step reads the image again. None for an `into` no image
/// was begun in.
pub fn finish(into: &mut Vec<u8>, rows: u64, rows_crc: u32) -> Option<()> {
    let count = into.get_mut(1..HEAD)?;
    for (to, byte) in count.iter_mut().zip(rows.to_le_bytes()) {
        *to = byte;
    }
    let head = mantle_crc::crc32c(into.get(..HEAD)?);
    let len = u64::try_from(into.len().checked_sub(HEAD)?).ok()?;
    let crc = mantle_crc::crc32c_combine(head, rows_crc, len);
    into.extend_from_slice(&crc.to_le_bytes());
    Some(())
}

/// The bytes an image of `rows` rows whose keys and values take `bytes` holds, without writing
/// it: the format byte, the count, each row's two lengths and its key and value, and the
/// checksum.
pub fn encoded_len_of(rows: u64, bytes: u64) -> Option<u64> {
    rows.checked_mul(4 + 4)?
        .checked_add(bytes)?
        .checked_add(1 + 8 + 4)
}

/// An image that fails a check (docs/design/replica.md §4): the check, and what the image
/// states of itself, so a rare failure identifies itself. The checksum carried and the one its
/// bytes give tell an image whose bytes changed after it was made (they differ) from one made
/// with a head that disagrees with its rows (they agree, and a later check failed).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageError {
    /// The check it failed.
    pub check: &'static str,
    /// The point the image is of, its index and term, where its holder knows it.
    pub at: Option<(u64, u64)>,
    /// Its bytes.
    pub len: usize,
    /// The rows its head states, where it holds a head.
    pub count: Option<u64>,
    /// The checksum it carries and the one its bytes give, where it is long enough to carry one.
    pub crcs: Option<(u32, u32)>,
}

impl ImageError {
    /// What `bytes` states of itself, failing `check`.
    fn of(bytes: &[u8], check: &'static str) -> Self {
        let count = bytes
            .get(1..HEAD)
            .and_then(|count| <[u8; 8]>::try_from(count).ok())
            .map(u64::from_le_bytes);
        let crcs = bytes
            .len()
            .checked_sub(4)
            .and_then(|body| bytes.split_at_checked(body))
            .and_then(|(body, crc)| {
                let carried = <[u8; 4]>::try_from(crc).ok().map(u32::from_le_bytes)?;
                Some((carried, mantle_crc::crc32c(body)))
            });
        Self {
            check,
            at: None,
            len: bytes.len(),
            count,
            crcs,
        }
    }

    /// The same, of the image of `index` and `term`.
    pub fn at(self, index: u64, term: u64) -> Self {
        Self {
            at: Some((index, term)),
            ..self
        }
    }
}

impl std::fmt::Display for ImageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "an image {}: ", self.check)?;
        if let Some((index, term)) = self.at {
            write!(f, "of index {index} and term {term}, ")?;
        }
        write!(f, "{} bytes, ", self.len)?;
        match self.count {
            Some(count) => write!(f, "{count} rows stated, ")?,
            None => write!(f, "no count of rows, ")?,
        }
        match self.crcs {
            Some((carried, computed)) => write!(
                f,
                "checksum {carried:#010x} carried and {computed:#010x} computed"
            ),
            None => write!(f, "no checksum"),
        }
    }
}

impl std::error::Error for ImageError {}

/// Checks `bytes` as an image, handing `each` its rows in order: the checksum over everything
/// before it first, then the format, the count against what the bytes can hold, each row, and
/// no byte past the last. The rows it holds.
fn walk(bytes: &[u8], each: &mut dyn FnMut(&[u8], &[u8])) -> Result<u64, ImageError> {
    let fail = |check| ImageError::of(bytes, check);
    let body_len = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| fail("is shorter than its checksum"))?;
    let (body, crc) = bytes.split_at(body_len);
    let carried = <[u8; 4]>::try_from(crc)
        .map(u32::from_le_bytes)
        .map_err(|_| fail("is shorter than its checksum"))?;
    if mantle_crc::crc32c(body) != carried {
        return Err(fail("does not match its checksum"));
    }
    let mut r = Reader::new(body);
    if r.u8() != Some(FORMAT) {
        return Err(fail("is of an unknown format"));
    }
    let count = r.u64().ok_or_else(|| fail("has no count of rows"))?;
    let most = u64::try_from(r.remaining() / 8).unwrap_or(u64::MAX);
    if count > most {
        return Err(fail("states more rows than its bytes hold"));
    }
    for _ in 0..count {
        let (key, value) = row(&mut r).ok_or_else(|| fail("ends inside a row"))?;
        each(key, value);
    }
    if r.remaining() != 0 {
        return Err(fail("holds bytes past its rows"));
    }
    Ok(count)
}

/// Checks `bytes` as an image without taking its rows ([`walk`]): the rows it holds.
pub fn check(bytes: &[u8]) -> Result<u64, ImageError> {
    walk(bytes, &mut |_, _| {})
}

/// The rows `bytes` holds, checked as [`walk`] checks them.
pub fn decode(bytes: &[u8]) -> Result<Vec<Row>, ImageError> {
    let mut rows = Vec::new();
    walk(bytes, &mut |key, value| {
        rows.push((key.to_vec(), value.to_vec()))
    })?;
    Ok(rows)
}

/// The row `bytes` begins with, its key and value, and the bytes it takes: none where they do
/// not hold one whole.
pub(crate) fn next_row(bytes: &[u8]) -> Option<(&[u8], &[u8], usize)> {
    let mut r = Reader::new(bytes);
    let (key, value) = row(&mut r)?;
    Some((key, value, bytes.len().checked_sub(r.remaining())?))
}

/// The key of the last row `bytes` holds, which are whole rows and at least one: none
/// otherwise. A walk of its rows, each at least the 8 bytes of its lengths.
pub(crate) fn last_key(bytes: &[u8]) -> Option<&[u8]> {
    let mut r = Reader::new(bytes);
    let mut last = None;
    while r.remaining() > 0 {
        last = Some(row(&mut r)?.0);
    }
    last
}

/// The next row `r` holds: its key, then its value, each after its length.
fn row<'a>(r: &mut Reader<'a>) -> Option<(&'a [u8], &'a [u8])> {
    let k = usize::try_from(r.u32()?).ok()?;
    let key = r.take(k)?;
    let v = usize::try_from(r.u32()?).ok()?;
    Some((key, r.take(v)?))
}

#[cfg(test)]
mod tests {
    use mantle_codec::Writer;

    use super::*;

    /// An image of `rows`, written as a range writes one, `per` rows a slice, its checksum kept a
    /// slice at a time.
    fn encode(rows: &[Row], per: usize) -> Vec<u8> {
        let mut into = Vec::new();
        begin(&mut into);
        let mut crc = 0;
        for slice in rows.chunks(per.max(1)) {
            let start = into.len();
            for (k, v) in slice {
                push_row(&mut into, k, v).unwrap();
            }
            crc = mantle_crc::crc32c_extend(crc, &into[start..]);
        }
        assert_eq!(mantle_crc::crc32c(&into[HEAD..]), crc);
        finish(&mut into, rows.len() as u64, crc).unwrap();
        into
    }

    #[test]
    fn rows_round_trip_and_damage_is_refused() {
        let rows = vec![(b"a".to_vec(), b"1".to_vec()), (Vec::new(), Vec::new())];
        let bytes = encode(&rows, 1);
        assert_eq!(decode(&bytes), Ok(rows));
        let mut bad = bytes.clone();
        bad[9] ^= 1;
        let failed = |bytes: &[u8]| decode(bytes).unwrap_err().check;
        assert_eq!(failed(&bad), "does not match its checksum");
        let error = decode(&bad).unwrap_err();
        assert_eq!(error.len, bytes.len());
        assert_eq!(error.count, Some(2));
        let (carried, computed) = error.crcs.unwrap();
        assert_ne!(carried, computed);
        assert_eq!(
            error.at(7, 3).to_string(),
            format!(
                "an image does not match its checksum: of index 7 and term 3, {} bytes, 2 rows \
                 stated, checksum {carried:#010x} carried and {computed:#010x} computed",
                bytes.len()
            )
        );
        assert_eq!(
            failed(&bytes[..bytes.len() - 1]),
            "does not match its checksum"
        );
        assert_eq!(failed(&bytes[..3]), "is shorter than its checksum");
        assert_eq!(check(&bytes), Ok(2));
        // Bodies whose checksum holds but whose head disagrees with their rows: each names the
        // check it failed.
        let sealed = |body: &[u8]| {
            let mut sealed = body.to_vec();
            sealed.extend_from_slice(&mantle_crc::crc32c(body).to_le_bytes());
            sealed
        };
        let body = &bytes[..bytes.len() - 4];
        let mut format = body.to_vec();
        format[0] = FORMAT + 1;
        assert_eq!(failed(&sealed(&format)), "is of an unknown format");
        // The last row's value said to be longer than the bytes left.
        let mut short = body.to_vec();
        let at = short.len() - 4;
        short[at..].copy_from_slice(&5u32.to_le_bytes());
        assert_eq!(failed(&sealed(&short)), "ends inside a row");
        let mut many = body.to_vec();
        many[1..HEAD].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(
            failed(&sealed(&many)),
            "states more rows than its bytes hold"
        );
        let mut fewer = body.to_vec();
        fewer[1..HEAD].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(failed(&sealed(&fewer)), "holds bytes past its rows");
    }

    /// The image as the encoder before slices wrote it, in one step.
    fn reference(rows: &[Row]) -> Vec<u8> {
        let mut w = Writer::default();
        w.u8(FORMAT);
        w.u64(rows.len() as u64);
        for (k, v) in rows {
            w.u32(u32::try_from(k.len()).unwrap());
            w.bytes(k);
            w.u32(u32::try_from(v.len()).unwrap());
            w.bytes(v);
        }
        let crc = mantle_crc::crc32c(w.as_slice());
        w.u32(crc);
        w.into_vec()
    }

    /// An image written a slice at a time, its checksum combined from the slices', is the bytes
    /// the one-step encoder wrote, at every slice: no rows, one, ten thousand of varied lengths a
    /// row, a hundred and a whole at a time, and one row of 64 MiB.
    #[test]
    fn an_image_in_slices_is_the_image_made_whole() {
        let many: Vec<Row> = (0..10_000usize)
            .map(|i| (format!("key/{i}").into_bytes(), vec![9u8; i % 1000]))
            .collect();
        let cases = [
            ("none", Vec::new()),
            ("one", vec![(b"a".to_vec(), b"1".to_vec())]),
            ("many", many),
            (
                "a large row",
                vec![(b"large".to_vec(), vec![5u8; 64 << 20])],
            ),
        ];
        for (what, rows) in cases {
            let expected = reference(&rows);
            for per in [1, 100, rows.len()] {
                let bytes = encode(&rows, per);
                assert_eq!(bytes, expected, "{what}, {per} a slice");
            }
            assert_eq!(decode(&expected), Ok(rows), "{what}");
        }
    }

    /// `encoded_len_of` counts what an image of the rows holds, for no rows, empty rows and rows
    /// of every length a byte's worth apart.
    #[test]
    fn the_encoded_length_is_what_an_image_holds() {
        let mut rows = Vec::new();
        for n in 0..40usize {
            let len = encode(&rows, 7).len() as u64;
            let bytes: u64 = rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
            assert_eq!(
                encoded_len_of(rows.len() as u64, bytes),
                Some(len),
                "{n} rows"
            );
            rows.push((vec![7u8; n % 5], vec![9u8; n * 13]));
        }
    }
}
