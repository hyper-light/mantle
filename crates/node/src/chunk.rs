//! A chunk's exchanges, on the transfer class (docs/design/node.md §3.1, §5.2): a put carries a
//! chunk's bytes and the CRC-32C its sender computed to the node that holds the chunk's volume,
//! which checks them before it writes and answers once they are durable; a get asks for a range of
//! a chunk's bytes, which the volume verifies against the record's checksums before they are sent.
//! The transport checks each body's CRC-32C on the way (CLAUDE.md §6).
//!
//! | Message | Head | Body |
//! |---|---|---|
//! | put | the volume, u128; the chunk's key (`mantle_chunk::ChunkKey`, 22 bytes); the chunk's CRC-32C, u32; little-endian | the chunk's bytes |
//! | get | the volume, u128; the chunk's key; the offset and the length of the bytes asked, u64 each; little-endian | none |
//! | reply | the status, u8 | a get's bytes, when read |

use mantle_chunk::{ChunkError, ChunkKey};
use mantle_codec::{Reader, Writer};

/// The bytes of a put's head: the volume, the key and the CRC-32C.
pub const PUT_HEAD_BYTES: usize = 16 + ChunkKey::ENCODED_LEN + 4;
/// The bytes of a get's head: the volume, the key, the offset and the length.
pub const GET_HEAD_BYTES: usize = 16 + ChunkKey::ENCODED_LEN + 8 + 8;
/// The bytes of a reply's head: the status.
pub const REPLY_HEAD_BYTES: usize = 1;

/// What became of a chunk's exchange: the volume's own answer (node.md §5.2), or why the request
/// never reached one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// A put's chunk is durable on its volume.
    Stored,
    /// A get's bytes follow, verified.
    Read,
    /// The volume's writer queue is full, or its reads are: another copy or volume serves it.
    Busy,
    /// The volume has no room, and cleaning can make none.
    Full,
    /// The volume is fenced after a device error.
    Fenced,
    /// The volume is closing.
    Closed,
    /// This node holds no such volume.
    NoVolume,
    /// The bytes do not match the CRC-32C their sender computed: changed on the way.
    Checksum,
    /// Larger than a record holds, or than the transfer class carries.
    TooLarge,
    /// The volume holds no such chunk.
    NotFound,
    /// The bytes asked lie past the chunk's end.
    Range,
    /// The chunk's bytes failed verification, or the device failed the read: another copy
    /// serves it, and the volume reports the chunk for repair.
    Corrupt,
    /// The volume holds other bytes under the key: a retry never changes what a write stored.
    Exists,
    /// The head does not decode.
    Malformed,
    /// The volume failed otherwise.
    Failed,
}

const STATUSES: [Status; 15] = [
    Status::Stored,
    Status::Read,
    Status::Busy,
    Status::Full,
    Status::Fenced,
    Status::Closed,
    Status::NoVolume,
    Status::Checksum,
    Status::TooLarge,
    Status::NotFound,
    Status::Range,
    Status::Corrupt,
    Status::Exists,
    Status::Malformed,
    Status::Failed,
];

impl Status {
    fn code(self) -> u8 {
        match self {
            Self::Stored => 0,
            Self::Read => 1,
            Self::Busy => 2,
            Self::Full => 3,
            Self::Fenced => 4,
            Self::Closed => 5,
            Self::NoVolume => 6,
            Self::Checksum => 7,
            Self::TooLarge => 8,
            Self::NotFound => 9,
            Self::Range => 10,
            Self::Corrupt => 11,
            Self::Exists => 12,
            Self::Malformed => 13,
            Self::Failed => 14,
        }
    }

    fn of(code: u8) -> Option<Self> {
        STATUSES.into_iter().find(|s| s.code() == code)
    }

    /// The status a volume's refusal is answered with.
    pub fn of_error(error: &ChunkError) -> Self {
        match error {
            ChunkError::Busy => Self::Busy,
            ChunkError::Full => Self::Full,
            ChunkError::Fenced | ChunkError::Device(_) => Self::Fenced,
            ChunkError::Closed => Self::Closed,
            ChunkError::Checksum { .. } => Self::Checksum,
            ChunkError::TooLarge { .. } | ChunkError::TooManyFragments(_) => Self::TooLarge,
            ChunkError::NotFound(_) => Self::NotFound,
            ChunkError::Range { .. } => Self::Range,
            ChunkError::Corrupt { .. } | ChunkError::CorruptLog { .. } => Self::Corrupt,
            ChunkError::Exists(_)
            | ChunkError::Sealed(_)
            | ChunkError::Gap { .. }
            | ChunkError::Conflict { .. } => Self::Exists,
            ChunkError::Format(_) | ChunkError::Config(_) | ChunkError::Internal(_) => Self::Failed,
        }
    }
}

/// Why a chunk's message did not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChunkMessageError {
    #[error("a chunk request's head of {0} bytes")]
    Head(usize),
    #[error("a chunk reply's status {0} names none")]
    Status(u8),
    #[error("a chunk reply's head of {0} bytes")]
    ReplyHead(usize),
}

/// A put's head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Put {
    pub volume: u128,
    pub key: ChunkKey,
    pub crc32c: u32,
}

impl Put {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(PUT_HEAD_BYTES);
        w.u128(self.volume);
        self.key.encode(&mut w);
        w.u32(self.crc32c);
        w.into_vec()
    }

    pub fn decode(head: &[u8]) -> Result<Self, ChunkMessageError> {
        let malformed = || ChunkMessageError::Head(head.len());
        if head.len() != PUT_HEAD_BYTES {
            return Err(malformed());
        }
        let mut r = Reader::new(head);
        let volume = r.u128().ok_or_else(malformed)?;
        let key = ChunkKey::decode(&mut r).ok_or_else(malformed)?;
        let crc32c = r.u32().ok_or_else(malformed)?;
        Ok(Self {
            volume,
            key,
            crc32c,
        })
    }
}

/// A get's head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Get {
    pub volume: u128,
    pub key: ChunkKey,
    pub offset: u64,
    pub len: u64,
}

impl Get {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(GET_HEAD_BYTES);
        w.u128(self.volume);
        self.key.encode(&mut w);
        w.u64(self.offset);
        w.u64(self.len);
        w.into_vec()
    }

    pub fn decode(head: &[u8]) -> Result<Self, ChunkMessageError> {
        let malformed = || ChunkMessageError::Head(head.len());
        if head.len() != GET_HEAD_BYTES {
            return Err(malformed());
        }
        let mut r = Reader::new(head);
        let volume = r.u128().ok_or_else(malformed)?;
        let key = ChunkKey::decode(&mut r).ok_or_else(malformed)?;
        let offset = r.u64().ok_or_else(malformed)?;
        let len = r.u64().ok_or_else(malformed)?;
        Ok(Self {
            volume,
            key,
            offset,
            len,
        })
    }
}

/// A reply's head.
pub fn reply_head(status: Status) -> [u8; REPLY_HEAD_BYTES] {
    [status.code()]
}

/// The status a reply's head states.
pub fn read_reply_head(head: &[u8]) -> Result<Status, ChunkMessageError> {
    let [code] = <[u8; REPLY_HEAD_BYTES]>::try_from(head)
        .map_err(|_| ChunkMessageError::ReplyHead(head.len()))?;
    Status::of(code).ok_or(ChunkMessageError::Status(code))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ChunkKey {
        ChunkKey {
            block: 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210,
            epoch: 7,
            index: 3,
        }
    }

    #[test]
    fn heads_read_back_as_written_and_no_other_length_reads() {
        let put = Put {
            volume: 9,
            key: key(),
            crc32c: 0xdead_beef,
        };
        let bytes = put.encode();
        assert_eq!(bytes.len(), PUT_HEAD_BYTES);
        assert_eq!(Put::decode(&bytes), Ok(put));
        assert!(Put::decode(&bytes[1..]).is_err());
        let get = Get {
            volume: 9,
            key: key(),
            offset: 4096,
            len: 1 << 20,
        };
        let bytes = get.encode();
        assert_eq!(bytes.len(), GET_HEAD_BYTES);
        assert_eq!(Get::decode(&bytes), Ok(get));
        assert!(Get::decode(&[bytes.as_slice(), &[0]].concat()).is_err());
    }

    #[test]
    fn every_status_reads_back_and_no_other_code_does() {
        for status in STATUSES {
            assert_eq!(read_reply_head(&reply_head(status)), Ok(status));
        }
        let codes: std::collections::BTreeSet<u8> = STATUSES.iter().map(|s| s.code()).collect();
        assert_eq!(codes.len(), STATUSES.len());
        assert_eq!(
            read_reply_head(&[u8::try_from(STATUSES.len()).unwrap()]),
            Err(ChunkMessageError::Status(15))
        );
        assert!(read_reply_head(&[]).is_err());
    }
}
