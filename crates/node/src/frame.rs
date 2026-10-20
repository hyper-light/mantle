//! A range's Raft message as a node sends it, on a lane or on the plane (docs/design/node.md
//! §3.2): the CRC-32C of everything after it, the range's group, then the message as hyper-raft's
//! wire writes it, which carries its own CRC-32C. Every network payload carries a checksum verified
//! on read (CLAUDE.md §6); this one also covers the group, which the message does not name.
//!
//! | Bytes | Field |
//! |---|---|
//! | 0..4 | CRC-32C of bytes 4.., little-endian |
//! | 4..20 | the range's group, u128, little-endian |
//! | 20.. | the message (`hyper_raft::wire`) |
//!
//! On a lane the transport verifies the first field itself ([`EMBEDDED`]); on the plane the
//! datagram's own checksum and seal cover it too, and it is checked again here.

use hyper_raft::proto::Message;
use hyper_raft::wire::Record;
use hyper_transport::Embedded;

/// Where a frame's own checksum lies, and what it covers: the transport checks it on a lane.
pub const EMBEDDED: Embedded = Embedded {
    covered_from: CRC_BYTES,
    at: 0,
};

/// The bytes the checksum takes.
const CRC_BYTES: usize = 4;
/// The bytes before the message: the checksum and the group.
pub const HEADER_BYTES: usize = CRC_BYTES + GROUP_BYTES;
/// The bytes the group takes.
const GROUP_BYTES: usize = 16;

/// Why a frame was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// Shorter than its header.
    #[error("a frame of {0} bytes, shorter than its header")]
    Short(usize),
    /// Its checksum does not match its bytes: damaged on the way.
    #[error("a frame whose checksum does not match")]
    Checksum,
    /// The message inside does not decode.
    #[error("a frame's message does not decode: {0}")]
    Message(String),
}

/// The frame of `message` for `group`, appended to `out`.
pub fn encode(out: &mut Vec<u8>, group: u128, message: &Message) {
    let start = out.len();
    out.extend_from_slice(&[0; CRC_BYTES]);
    out.extend_from_slice(&group.to_le_bytes());
    message.encode(out);
    let crc = out
        .get(start.saturating_add(CRC_BYTES)..)
        .map_or(0, mantle_crc::crc32c);
    if let Some(at) = out.get_mut(start..start.saturating_add(CRC_BYTES)) {
        at.copy_from_slice(&crc.to_le_bytes());
    }
}

/// The bytes `message`'s frame takes.
pub fn encoded_len(message: &Message) -> usize {
    HEADER_BYTES.saturating_add(message.encoded_len())
}

/// The group and message a frame carries, its checksum verified first.
pub fn decode(frame: &[u8]) -> Result<(u128, Message), FrameError> {
    let short = || FrameError::Short(frame.len());
    if frame.len() < HEADER_BYTES {
        return Err(short());
    }
    let (crc, rest) = frame.split_first_chunk::<CRC_BYTES>().ok_or_else(short)?;
    if mantle_crc::crc32c(rest) != u32::from_le_bytes(*crc) {
        return Err(FrameError::Checksum);
    }
    let (group, record) = rest.split_first_chunk::<GROUP_BYTES>().ok_or_else(short)?;
    let message = Message::decode(record).map_err(|e| FrameError::Message(format!("{e:?}")))?;
    Ok((u128::from_le_bytes(*group), message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper_raft::proto::{Entry, MessageType};

    fn append() -> Message {
        Message {
            msg_type: MessageType::MsgAppend,
            to: 2,
            from: 1,
            term: 7,
            log_term: 6,
            index: 41,
            commit: 40,
            entries: vec![Entry {
                term: 7,
                index: 42,
                data: b"put k v".to_vec(),
                ..Entry::default()
            }],
            ..Message::default()
        }
    }

    #[test]
    fn a_frame_reads_back_its_group_and_message() {
        let mut out = vec![0xaa];
        encode(&mut out, 0x0072_616e_6765, &append());
        assert_eq!(out.len(), 1 + encoded_len(&append()));
        let (group, message) = decode(&out[1..]).unwrap();
        assert_eq!(group, 0x0072_616e_6765);
        assert_eq!(message, append());
    }

    #[test]
    fn a_frame_damaged_anywhere_is_refused() {
        let mut out = Vec::new();
        encode(&mut out, 3, &append());
        for at in 0..out.len() {
            let mut damaged = out.clone();
            damaged[at] ^= 0x40;
            assert!(decode(&damaged).is_err(), "byte {at} flipped");
        }
        assert_eq!(decode(&out[..HEADER_BYTES - 1]), Err(FrameError::Short(19)));
    }

    /// The transport verifies a lane frame's checksum where [`EMBEDDED`] says it is: the CRC-32C
    /// of the bytes from `covered_from`, stated little-endian at `at`.
    #[test]
    fn the_checksum_lies_where_the_transport_is_told() {
        let mut out = Vec::new();
        encode(&mut out, 9, &append());
        let stated = u32::from_le_bytes(out[EMBEDDED.at..EMBEDDED.at + 4].try_into().unwrap());
        assert_eq!(stated, mantle_crc::crc32c(&out[EMBEDDED.covered_from..]));
    }
}
