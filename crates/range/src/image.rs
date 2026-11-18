//! A snapshot's data: every row of the range as of its index, checked by a CRC-32C. The
//! rows travel inside the snapshot message for now; the production engine will move its
//! files out of band instead (docs/design/replica.md §6; 12 §6.3).

use mantle_codec::{Reader, Writer};
use mantle_meta::engine::Row;

const FORMAT: u8 = 1;

/// The rows' bytes, made at their length ([`encoded_len_of`]), so the buffer never grows.
pub fn encode(rows: &[Row]) -> Option<Vec<u8>> {
    let bytes = rows.iter().try_fold(0u64, |sum, (k, v)| {
        sum.checked_add(u64::try_from(k.len()).ok()?)?
            .checked_add(u64::try_from(v.len()).ok()?)
    })?;
    let len = encoded_len_of(u64::try_from(rows.len()).ok()?, bytes)?;
    let mut w = Writer::with_capacity(usize::try_from(len).ok()?);
    w.u8(FORMAT);
    w.u64(u64::try_from(rows.len()).ok()?);
    for (k, v) in rows {
        w.u32(u32::try_from(k.len()).ok()?);
        w.bytes(k);
        w.u32(u32::try_from(v.len()).ok()?);
        w.bytes(v);
    }
    let crc = mantle_crc::crc32c(w.as_slice());
    w.u32(crc);
    Some(w.into_vec())
}

/// The bytes [`encode`] writes for `rows` rows whose keys and values take `bytes`, without
/// writing them: the format byte, the count, each row's two lengths and its key and value, and
/// the checksum.
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
    use super::*;

    #[test]
    fn rows_round_trip_and_damage_is_refused() {
        let rows = vec![(b"a".to_vec(), b"1".to_vec()), (Vec::new(), Vec::new())];
        let bytes = encode(&rows).unwrap();
        assert_eq!(decode(&bytes), Some(rows));
        let mut bad = bytes.clone();
        bad[9] ^= 1;
        assert_eq!(decode(&bad), None);
        assert_eq!(decode(&bytes[..bytes.len() - 1]), None);
    }

    /// `encode` as it was before Lean doors step 7, in a writer that grows.
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

    /// An image is one buffer made at its length, the bytes the encoder before step 7 wrote: no
    /// rows, one, ten thousand of varied lengths, and one row of 64 MiB. Its bound, a u32 of bytes
    /// (hyper-raft's wire), is past what a unit test holds twice over; the length is computed the
    /// same way at every size.
    #[test]
    fn an_image_is_written_at_its_length() {
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
        let mut wrong = Vec::new();
        for (what, rows) in cases {
            let (expected, before) = crate::counted(|| reference(&rows));
            let (bytes, made) = crate::counted(|| encode(&rows).unwrap());
            println!(
                "image {what}: before {before:?}, now {made:?}, {} bytes",
                bytes.len()
            );
            if made != (1, 0) || bytes.capacity() != bytes.len() {
                wrong.push(format!(
                    "{what}: (allocations, reallocations) {made:?}, capacity {} for {} bytes",
                    bytes.capacity(),
                    bytes.len()
                ));
            }
            assert_eq!(bytes, expected, "{what}");
            assert_eq!(decode(&bytes), Some(rows), "{what}");
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// `encoded_len_of` counts what `encode` writes, for no rows, empty rows and rows of every
    /// length a byte's worth apart.
    #[test]
    fn the_encoded_length_is_what_encode_writes() {
        let mut rows = Vec::new();
        for n in 0..40usize {
            let len = encode(&rows).unwrap().len() as u64;
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
