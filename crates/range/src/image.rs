//! A snapshot's data: every row of the range as of its index, checked by a CRC-32C. The
//! rows travel inside the snapshot message for now; the production engine will move its
//! files out of band instead (docs/design/replica.md §6; 12 §6.3).

use mantle_codec::Reader;
use mantle_meta::engine::Row;

const FORMAT: u8 = 1;

/// The bytes before an image's first row: its format and its count of rows.
const HEAD: usize = 1 + 8;

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

pub fn decode(bytes: &[u8]) -> Option<Vec<Row>> {
    let body_len = bytes.len().checked_sub(4)?;
    let (body, crc) = bytes.split_at(body_len);
    if mantle_crc::crc32c(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let mut r = Reader::new(body);
    if r.u8()? != FORMAT {
        return None;
    }
    let count = usize::try_from(r.u64()?).ok()?;
    // A row takes eight bytes at least.
    if count > r.remaining() / 8 {
        return None;
    }
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let k = usize::try_from(r.u32()?).ok()?;
        let key = r.take(k)?.to_vec();
        let v = usize::try_from(r.u32()?).ok()?;
        rows.push((key, r.take(v)?.to_vec()));
    }
    if r.remaining() != 0 {
        return None;
    }
    Some(rows)
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
        assert_eq!(decode(&bytes), Some(rows));
        let mut bad = bytes.clone();
        bad[9] ^= 1;
        assert_eq!(decode(&bad), None);
        assert_eq!(decode(&bytes[..bytes.len() - 1]), None);
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
            assert_eq!(decode(&expected), Some(rows), "{what}");
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
