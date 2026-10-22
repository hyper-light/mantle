//! An object's exchanges, the native protocol's first operations (docs/design/node.md §4.1;
//! gateway.md §2–§3): a client, or a gateway, asks a node to create a bucket, to put an object,
//! its bytes streamed on the exchange, or to get a range of one, the bytes streamed on the reply.
//! The node runs the bucket's coordinator and the gateway's drivers and serves their requests
//! itself (node.md §5). The transport checks each body's CRC-32C on the way (CLAUDE.md §6); the
//! object's bytes are sealed under their file's key before any leaves the node.
//!
//! A head is held for its exchange's whole life and a body is read a piece at a time, so each head
//! is a few fixed fields and every name rides in the body, ahead of the object's bytes.
//!
//! | Message | Head | Body |
//! |---|---|---|
//! | request | the kind (u8: 0 create, 1 put, 2 get); the bucket's and the key's lengths (u32 each); the operation's identity (u128), which every attempt of a put carries (gateway.md §2.1), else 0; a put's length, or a get's offset, else 0 (u64); a get's length, else 0 (u64) | the bucket's name and the key, UTF-8; then a put's bytes |
//! | reply | the status (u8); the object's size, a get's (u64); the version id's and the ETag's lengths (u32 each) | the version id and the ETag; then a get's bytes |
//!
//! Numbers are little-endian; names are within S3's bounds on a bucket's name and a key.

use mantle_codec::{Reader, Writer};
use mantle_meta::wire::{MAX_BUCKET, MAX_KEY};

const CREATE: u8 = 0;
const PUT: u8 = 1;
const GET: u8 = 2;

/// The longest version id or ETag a reply carries: a multipart object's ETag, bare of quotes, an
/// MD5's 32 hex digits, `-` and its count of parts, at most `MAX_PARTS`'s five digits
/// (`mantle_s3::checksum::multipart_etag`); a version's id is 13 characters
/// (`mantle_meta::key::version_id`).
pub const MAX_TAG: usize = "d41d8cd98f00b204e9800998ecf8427e-10000".len();

/// The bytes of an object's request head: the kind, the names' lengths, the identity and two
/// numbers.
pub const HEAD_BYTES: usize = 1 + 4 + 4 + 16 + 8 + 8;
/// The bytes of a reply's head: the status, the size and the tags' lengths.
pub const REPLY_HEAD_BYTES: usize = 1 + 8 + 4 + 4;

/// What an object's request asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// CreateBucket, owned by the asker.
    Create { bucket: String },
    /// PutObject: `length` bytes follow the names.
    Put {
        bucket: String,
        key: String,
        id: u128,
        length: u64,
    },
    /// GetObject: bytes `[offset, offset + len)`, or to the object's end, of its current version.
    Get {
        bucket: String,
        key: String,
        offset: u64,
        len: u64,
    },
}

/// A request's head: what it is and how long its names are, before they are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    kind: u8,
    bucket: u32,
    key: u32,
    id: u128,
    a: u64,
    b: u64,
}

impl Head {
    /// The bytes of the names that open the body.
    pub fn names(&self) -> u64 {
        u64::from(self.bucket).saturating_add(u64::from(self.key))
    }

    /// The request, once its names are read.
    pub fn op(&self, names: &[u8]) -> Result<Op, ObjectError> {
        let bad = || ObjectError::Names(names.len());
        let bucket = usize::try_from(self.bucket).map_err(|_| bad())?;
        let (bucket, key) = names.split_at_checked(bucket).ok_or_else(bad)?;
        if u64::try_from(key.len()).ok() != Some(u64::from(self.key)) {
            return Err(bad());
        }
        let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).map_err(|_| bad());
        let bucket = text(bucket)?;
        Ok(match self.kind {
            CREATE => Op::Create { bucket },
            PUT => Op::Put {
                bucket,
                key: text(key)?,
                id: self.id,
                length: self.a,
            },
            _ => Op::Get {
                bucket,
                key: text(key)?,
                offset: self.a,
                len: self.b,
            },
        })
    }

    /// The head an exchange carries, its body `body` bytes: refused for an unknown kind, names
    /// past S3's bounds, or a body that is not the names and a put's length.
    pub fn decode(head: &[u8], body: Option<u64>) -> Result<Self, ObjectError> {
        let mut r = Reader::new(head);
        let decoded = (|| {
            Some(Self {
                kind: r.u8()?,
                bucket: r.u32()?,
                key: r.u32()?,
                id: r.u128()?,
                a: r.u64()?,
                b: r.u64()?,
            })
        })()
        .filter(|_| r.remaining() == 0);
        let head_of = || ObjectError::Head(head.len());
        let decoded = decoded.ok_or_else(head_of)?;
        let within = usize::try_from(decoded.bucket).is_ok_and(|b| b <= MAX_BUCKET)
            && usize::try_from(decoded.key).is_ok_and(|k| k <= MAX_KEY)
            && matches!(decoded.kind, CREATE | PUT | GET)
            && (decoded.kind != CREATE || decoded.key == 0);
        let length = match decoded.kind {
            PUT => decoded.names().checked_add(decoded.a),
            _ => Some(decoded.names()),
        };
        if !within || length != Some(body.unwrap_or(0)) {
            return Err(head_of());
        }
        Ok(decoded)
    }
}

impl Op {
    /// The request's head and the names that open its body.
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), ObjectError> {
        let (kind, bucket, key, id, a, b) = match self {
            Self::Create { bucket } => (CREATE, bucket, "", 0, 0, 0),
            Self::Put {
                bucket,
                key,
                id,
                length,
            } => (PUT, bucket, key.as_str(), *id, *length, 0),
            Self::Get {
                bucket,
                key,
                offset,
                len,
            } => (GET, bucket, key.as_str(), 0, *offset, *len),
        };
        if bucket.len() > MAX_BUCKET {
            return Err(ObjectError::TooLong(bucket.len()));
        }
        if key.len() > MAX_KEY {
            return Err(ObjectError::TooLong(key.len()));
        }
        let mut w = Writer::with_capacity(HEAD_BYTES);
        w.u8(kind);
        w.u32(u32::try_from(bucket.len()).map_err(|_| ObjectError::TooLong(bucket.len()))?);
        w.u32(u32::try_from(key.len()).map_err(|_| ObjectError::TooLong(key.len()))?);
        w.u128(id);
        w.u64(a);
        w.u64(b);
        let mut names = Vec::with_capacity(bucket.len().saturating_add(key.len()));
        names.extend_from_slice(bucket.as_bytes());
        names.extend_from_slice(key.as_bytes());
        Ok((w.into_vec(), names))
    }

    /// The bytes of the request's body: its names and a put's object.
    pub fn body(&self) -> u64 {
        match self {
            Self::Create { bucket } => u64::try_from(bucket.len()).unwrap_or(u64::MAX),
            Self::Put {
                bucket,
                key,
                length,
                ..
            } => u64::try_from(bucket.len().saturating_add(key.len()))
                .unwrap_or(u64::MAX)
                .saturating_add(*length),
            Self::Get { bucket, key, .. } => {
                u64::try_from(bucket.len().saturating_add(key.len())).unwrap_or(u64::MAX)
            }
        }
    }
}

/// What became of an object's request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Done: the bucket made, the object committed, or the bytes follow.
    Done,
    /// S3's `404 NoSuchBucket`.
    NoSuchBucket,
    /// S3's `404 NoSuchKey`.
    NoSuchKey,
    /// S3's `409 BucketAlreadyExists`.
    BucketAlreadyExists,
    /// S3's `409 BucketAlreadyOwnedByYou`.
    BucketAlreadyOwnedByYou,
    /// S3's `409 OperationAborted`: the bucket is being created or deleted.
    OperationAborted,
    /// S3's `400 TooManyBuckets`.
    TooManyBuckets,
    /// S3's `416 InvalidRange`.
    InvalidRange,
    /// S3's `400 EntityTooLarge`.
    EntityTooLarge,
    /// S3's `400 IncompleteBody`: the body ended before its length.
    IncompleteBody,
    /// S3's `503 SlowDown`: the node holds as many operations as it keeps; nothing was done.
    SlowDown,
    /// The head does not decode, or names a bucket or key past S3's bounds.
    Malformed,
    /// S3's `500 InternalError`: the operation failed past its start; a put may not have
    /// committed, and asking again with the same identity is answered from what it made.
    Failed,
}

const STATUSES: [Status; 13] = [
    Status::Done,
    Status::NoSuchBucket,
    Status::NoSuchKey,
    Status::BucketAlreadyExists,
    Status::BucketAlreadyOwnedByYou,
    Status::OperationAborted,
    Status::TooManyBuckets,
    Status::InvalidRange,
    Status::EntityTooLarge,
    Status::IncompleteBody,
    Status::SlowDown,
    Status::Malformed,
    Status::Failed,
];

impl Status {
    fn code(self) -> u8 {
        match self {
            Self::Done => 0,
            Self::NoSuchBucket => 1,
            Self::NoSuchKey => 2,
            Self::BucketAlreadyExists => 3,
            Self::BucketAlreadyOwnedByYou => 4,
            Self::OperationAborted => 5,
            Self::TooManyBuckets => 6,
            Self::InvalidRange => 7,
            Self::EntityTooLarge => 8,
            Self::IncompleteBody => 9,
            Self::SlowDown => 10,
            Self::Malformed => 11,
            Self::Failed => 12,
        }
    }

    fn of(code: u8) -> Option<Self> {
        STATUSES.into_iter().find(|s| s.code() == code)
    }
}

/// A reply: its head's status and size, and the version id and ETag its body opens with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: Status,
    /// The version's id: a put's, or the one a get read; empty where none applies.
    pub version: String,
    pub etag: String,
    /// The object's size, a get's.
    pub size: u64,
}

/// A reply's head: its status and size, and how long the tags that open its body are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplyHead {
    pub status: Status,
    pub size: u64,
    version: u32,
    etag: u32,
}

impl ReplyHead {
    /// The bytes of the tags that open the body.
    pub fn tags(&self) -> u64 {
        u64::from(self.version).saturating_add(u64::from(self.etag))
    }

    pub fn decode(head: &[u8]) -> Result<Self, ObjectError> {
        let mut r = Reader::new(head);
        let reply = (|| {
            Some(Self {
                status: Status::of(r.u8()?)?,
                size: r.u64()?,
                version: r.u32()?,
                etag: r.u32()?,
            })
        })();
        reply
            .filter(|h| {
                r.remaining() == 0
                    && usize::try_from(h.version).is_ok_and(|v| v <= MAX_TAG)
                    && usize::try_from(h.etag).is_ok_and(|e| e <= MAX_TAG)
            })
            .ok_or(ObjectError::Reply(head.len()))
    }

    /// The reply, once its tags are read.
    pub fn reply(&self, tags: &[u8]) -> Result<Reply, ObjectError> {
        let bad = || ObjectError::Names(tags.len());
        let version = usize::try_from(self.version).map_err(|_| bad())?;
        let (version, etag) = tags.split_at_checked(version).ok_or_else(bad)?;
        if u64::try_from(etag.len()).ok() != Some(u64::from(self.etag)) {
            return Err(bad());
        }
        let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).map_err(|_| bad());
        Ok(Reply {
            status: self.status,
            version: text(version)?,
            etag: text(etag)?,
            size: self.size,
        })
    }
}

impl Reply {
    /// A reply of `status` alone.
    pub fn status(status: Status) -> Self {
        Self {
            status,
            version: String::new(),
            etag: String::new(),
            size: 0,
        }
    }

    /// The reply's head and the tags that open its body.
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), ObjectError> {
        for tag in [&self.version, &self.etag] {
            if tag.len() > MAX_TAG {
                return Err(ObjectError::TooLong(tag.len()));
            }
        }
        let mut w = Writer::with_capacity(REPLY_HEAD_BYTES);
        w.u8(self.status.code());
        w.u64(self.size);
        w.u32(u32::try_from(self.version.len()).map_err(|_| ObjectError::TooLong(0))?);
        w.u32(u32::try_from(self.etag.len()).map_err(|_| ObjectError::TooLong(0))?);
        let mut tags = Vec::with_capacity(self.version.len().saturating_add(self.etag.len()));
        tags.extend_from_slice(self.version.as_bytes());
        tags.extend_from_slice(self.etag.as_bytes());
        Ok((w.into_vec(), tags))
    }
}

/// Why an object's message did not encode or decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ObjectError {
    #[error("an object's request head of {0} bytes")]
    Head(usize),
    #[error("an object's reply head of {0} bytes")]
    Reply(usize),
    #[error("names or tags of {0} bytes that are not the head's")]
    Names(usize),
    #[error("a name or tag of {0} bytes, past its bound")]
    TooLong(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_replies_read_back_and_nothing_past_s3s_bounds_does() {
        for op in [
            Op::Create { bucket: "b".into() },
            Op::Put {
                bucket: "b".into(),
                key: "k/ü".into(),
                id: u128::MAX,
                length: 1 << 30,
            },
            Op::Get {
                bucket: "b".into(),
                key: "k".repeat(MAX_KEY),
                offset: 3,
                len: u64::MAX,
            },
        ] {
            let (head, names) = op.encode().unwrap();
            assert_eq!(head.len(), HEAD_BYTES);
            let decoded = Head::decode(&head, Some(op.body())).unwrap();
            assert_eq!(decoded.names(), names.len() as u64);
            assert_eq!(decoded.op(&names), Ok(op));
        }
        // A put whose body is not its names and its length, a key past S3's bound, names that are
        // not the head's, and a head cut short: each refused.
        let put = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: 10,
        };
        let (head, names) = put.encode().unwrap();
        assert!(Head::decode(&head, Some(put.body() - 1)).is_err());
        let decoded = Head::decode(&head, Some(put.body())).unwrap();
        assert!(decoded.op(&names[1..]).is_err());
        let long = Op::Get {
            bucket: "b".into(),
            key: "k".repeat(MAX_KEY + 1),
            offset: 0,
            len: 1,
        };
        assert_eq!(long.encode(), Err(ObjectError::TooLong(MAX_KEY + 1)));
        assert!(Head::decode(&head[1..], Some(put.body())).is_err());
        for status in STATUSES {
            let reply = Reply {
                status,
                version: "v".into(),
                etag: format!("\"{}\"", "0".repeat(32)),
                size: 7,
            };
            let (head, tags) = reply.encode().unwrap();
            assert_eq!(head.len(), REPLY_HEAD_BYTES);
            let decoded = ReplyHead::decode(&head).unwrap();
            assert_eq!(decoded.tags(), tags.len() as u64);
            assert_eq!(decoded.reply(&tags), Ok(reply));
        }
        assert!(ReplyHead::decode(&[13]).is_err());
    }
}
