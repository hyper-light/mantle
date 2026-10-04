//! A snapshot's data: every row of the range as of its index, checked by a CRC-32C. The
//! rows travel inside the snapshot message for now; the production engine will move its
//! files out of band instead (docs/design/replica.md §6; 12 §6.3).

use mantle_codec::{Reader, Writer};
use mantle_meta::engine::Row;

const FORMAT: u8 = 1;

pub fn encode(rows: &[Row]) -> Option<Vec<u8>> {
    let mut w = Writer::default();
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

/// The bytes [`encode`] writes for `rows`, without writing them: the format byte, the count, each
/// row's two lengths and its key and value, and the checksum.
pub fn encoded_len(rows: &[Row]) -> Option<u64> {
    rows.iter().try_fold(1u64 + 8 + 4, |bytes, (k, v)| {
        let row = u64::try_from(k.len())
            .ok()?
            .checked_add(u64::try_from(v.len()).ok()?)?
            .checked_add(4 + 4)?;
        bytes.checked_add(row)
    })
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

    /// `encoded_len` counts what `encode` writes, for no rows, empty rows and rows of every
    /// length a byte's worth apart.
    #[test]
    fn the_encoded_length_is_what_encode_writes() {
        let mut rows = Vec::new();
        for n in 0..40usize {
            let len = encode(&rows).unwrap().len() as u64;
            assert_eq!(encoded_len(&rows), Some(len), "{n} rows");
            rows.push((vec![7u8; n % 5], vec![9u8; n * 13]));
        }
    }
}
