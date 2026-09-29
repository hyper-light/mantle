//! The group's configuration as the engine keeps it, applied at the entry that changed it so
//! that a restart tells the core the configuration of the state it opens at
//! (docs/design/replica.md §4).

use focal_raft::proto::ConfState;
use mantle_codec::{Reader, Writer};
use mantle_meta::key::LOCAL;

/// The row that holds the configuration: `[LOCAL, 'r']`.
pub const ROW: &[u8] = &[LOCAL, b'r'];

/// The row that holds the index and term of the last snapshot installed: `[LOCAL, 'p']`.
pub const INSTALLED: &[u8] = &[LOCAL, b'p'];

/// A snapshot point's bytes: its index and term, and their CRC-32C.
pub fn encode_point(index: u64, term: u64) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(index);
    w.u64(term);
    let crc = mantle_crc::crc32c(w.as_slice());
    w.u32(crc);
    w.into_vec()
}

pub fn decode_point(bytes: &[u8]) -> Option<(u64, u64)> {
    let body_len = bytes.len().checked_sub(4)?;
    let (body, crc) = bytes.split_at(body_len);
    if mantle_crc::crc32c(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let mut r = Reader::new(body);
    let point = (r.u64()?, r.u64()?);
    if r.remaining() != 0 {
        return None;
    }
    Some(point)
}

const FORMAT: u8 = 1;

pub fn encode(conf: &ConfState) -> Option<Vec<u8>> {
    let mut w = Writer::default();
    w.u8(FORMAT);
    for list in [
        &conf.voters,
        &conf.learners,
        &conf.voters_outgoing,
        &conf.learners_next,
    ] {
        w.u32(u32::try_from(list.len()).ok()?);
        for id in list {
            w.u64(*id);
        }
    }
    w.u8(u8::from(conf.auto_leave));
    let crc = mantle_crc::crc32c(w.as_slice());
    w.u32(crc);
    Some(w.into_vec())
}

pub fn decode(bytes: &[u8]) -> Option<ConfState> {
    let body_len = bytes.len().checked_sub(4)?;
    let (body, crc) = bytes.split_at(body_len);
    if mantle_crc::crc32c(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let mut r = Reader::new(body);
    if r.u8()? != FORMAT {
        return None;
    }
    let mut lists: [Vec<u64>; 4] = Default::default();
    for list in &mut lists {
        let count = usize::try_from(r.u32()?).ok()?;
        if count > r.remaining() / 8 {
            return None;
        }
        for _ in 0..count {
            list.push(r.u64()?);
        }
    }
    let auto_leave = match r.u8()? {
        0 => false,
        1 => true,
        _ => return None,
    };
    if r.remaining() != 0 {
        return None;
    }
    let [voters, learners, voters_outgoing, learners_next] = lists;
    Some(ConfState {
        voters,
        learners,
        voters_outgoing,
        learners_next,
        auto_leave,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_round_trips_and_damage_is_refused() {
        let conf = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![4],
            voters_outgoing: vec![1, 2],
            learners_next: vec![],
            auto_leave: true,
        };
        let bytes = encode(&conf).unwrap();
        assert_eq!(decode(&bytes), Some(conf));
        let mut bad = bytes;
        bad[3] ^= 1;
        assert_eq!(decode(&bad), None);
    }
}
