//! An object's exchanges, the native protocol's operations (docs/design/node.md §4.1–§4.2;
//! gateway.md §2–§3): a client, or a gateway, asks a node for S3's bucket and object operations,
//! a put's bytes streamed on the exchange and a get's on the reply. The node runs the bucket's
//! coordinator, the gateway's drivers and the ranges' reads, and serves their requests itself
//! (node.md §5). The transport checks each body's CRC-32C on the way (CLAUDE.md §6); the object's
//! bytes are sealed under their file's key before any leaves the node.
//!
//! A head is held for its exchange's whole life and a body is read a piece at a time, so each head
//! is a few fixed fields: a request's fields ride in its body ahead of a put's bytes, and a
//! reply's ahead of a get's.
//!
//! | Message | Head | Body |
//! |---|---|---|
//! | request | the kind (u8); the fields' length (u32); the operation's identity (u128), which every attempt of a mutation carries (gateway.md §2.1), else 0; a put's length, else 0 (u64) | the fields; then a put's bytes |
//! | reply | the status (u8); the object's size, a get's or a head's, else 0 (u64); the fields' length (u32) | the fields; then a get's bytes |
//!
//! A request's fields, by its kind. A text is UTF-8 behind its length (u32); an optional field is
//! behind a byte, 0 for none and 1 for one.
//!
//! | Kind | Operation | Fields |
//! |---|---|---|
//! | 0 | CreateBucket | the bucket |
//! | 1 | PutObject | the bucket and the key; the headers; the checksum, optional: its algorithm's code (u8) and its value, its length (u8) before it; `Content-MD5`, optional, 16 bytes |
//! | 2 | GetObject | the bucket and the key; the headers |
//! | 3 | HeadObject | the bucket and the key; the headers |
//! | 4 | DeleteObject | the bucket and the key; the headers |
//! | 5 | ListObjectsV2 | the bucket; the prefix; the delimiter, empty for none; where the page starts, optional: 0 after a key or common prefix, 1 just past a key a page passed, then the key; the most keys (u32); whether each key's owner is listed (u8) |
//! | 6 | HeadBucket | the bucket |
//! | 7 | ListBuckets | the prefix; the bucket after which the page starts, optional; the most buckets (u32) |
//! | 8 | DeleteBucket | the bucket |
//! | 9 | PutBucketVersioning | the bucket; its state (u8): 2 enabled, 3 suspended |
//! | 10 | GetBucketVersioning | the bucket |
//!
//! The headers are the values of S3's request headers an object's operation takes as text, each
//! optional: `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` and `Range`,
//! together within S3's 8 KB of headers (05 §10.2). The node judges them as the HTTP/1.1
//! listener's are judged, by `mantle_s3::conditional` and `mantle_s3::range`, so the two doors
//! cannot differ: a read's preconditions in RFC 9110's order against the version it reads, a
//! write's and a delete's at the Name range's commit (05 §2), and a range against the version's
//! size once it is read (05 §12). An operation takes the headers S3 takes for it and leaves the
//! others: a put `If-Match` and `If-None-Match`, a delete `If-Match`, a get and a head all five.
//!
//! A reply's fields: the version's ID and the ETag, bare of quotes, each a text; Last-Modified,
//! whole seconds since the Unix epoch (u64), the one value a header, a listing and a condition all
//! take (s3-protocol.md §4); the checksum, optional: its algorithm's code (u8), the parts it
//! combines (u16, 0 for a full-object value) and its value, its length (u8) before it; whether the
//! version is a delete marker (u8); the bytes a range served, optional: its first and last (u64
//! each), S3's `206 Partial Content`; the bucket's versioning, GetBucketVersioning's (u8): 0 none
//! asked, 1 never set, 2 enabled, 3 suspended; then the listing, behind a byte: 0 none, 1 a page of
//! keys, 2 a page of buckets. A page of keys: whether it is truncated (u8); where the next page starts,
//! optional, as a request names it; the count of keys (u32), each its key, ETag, size (u64),
//! Last-Modified (u64), checksum algorithm, optional, its code (u8) and parts (u16), and owner,
//! optional, a text; the count of common prefixes (u32), each a text. A page of buckets: the
//! count (u32), each its name and creation (u64, whole seconds); the bucket after which the next
//! page starts, optional.
//!
//! Numbers are little-endian. Every text is within S3's bound on what it names, and every count
//! within S3's page: a message past them is refused whole.

use mantle_codec::{Reader, Writer};
use mantle_meta::record::{self, Versioning};
use mantle_meta::wire::{MAX_BUCKET, MAX_HEADERS, MAX_KEY, MAX_OWNER};
use mantle_s3::checksum::{Algorithm, Checksum};
use mantle_s3::list::{MAX_KEYS, Resume};

const CREATE: u8 = 0;
const PUT: u8 = 1;
const GET: u8 = 2;
const HEAD: u8 = 3;
const DELETE: u8 = 4;
const LIST: u8 = 5;
const HEAD_BUCKET: u8 = 6;
const LIST_BUCKETS: u8 = 7;
const DELETE_BUCKET: u8 = 8;
const PUT_VERSIONING: u8 = 9;
const GET_VERSIONING: u8 = 10;

/// The longest version id or ETag a reply carries: a multipart object's ETag, bare of quotes, an
/// MD5's 32 hex digits, `-` and its count of parts, at most `MAX_PARTS`'s five digits
/// (`mantle_s3::checksum::multipart_etag`); a version's id is 13 characters
/// (`mantle_meta::key::version_id`).
pub const MAX_TAG: usize = "d41d8cd98f00b204e9800998ecf8427e-10000".len();

/// The most buckets a page of ListBuckets lists: S3's `max-buckets`, "1–10000" (05 §10.4).
pub const MAX_BUCKETS: usize = 10_000;

/// The longest checksum value: SHA-512's 64 bytes (05 §3.1).
const MAX_CHECKSUM: usize = Algorithm::Sha512.width();

/// The bytes of an object's request head: the kind, the fields' length, the identity and a put's
/// length.
pub const HEAD_BYTES: usize = 1 + 4 + 16 + 8;
/// The bytes of a reply's head: the status, the size and the fields' length.
pub const REPLY_HEAD_BYTES: usize = 1 + 8 + 4;

/// A request's fields at most: the longest of a put's (its names, its headers within S3's 8 KB,
/// a checksum and an MD5) and a listing's (its bucket, prefix, delimiter and start).
pub const MAX_FIELDS: usize = {
    // Each text behind its length, a u32.
    let names = (4 + MAX_BUCKET) + (4 + MAX_KEY);
    // Five optional texts within S3's headers.
    let headers = 5 * (1 + 4) + MAX_HEADERS;
    let put = names + headers + (1 + 1 + 1 + MAX_CHECKSUM) + (1 + 16);
    let list = (4 + MAX_BUCKET) + 2 * (4 + MAX_KEY) + (1 + 1 + 4 + MAX_KEY) + 4 + 1;
    if put > list { put } else { list }
};

/// A key's bytes at most in a page of keys: its key, ETag, size, Last-Modified, checksum
/// algorithm and owner.
pub(crate) const LISTED_BYTES: usize =
    (4 + MAX_KEY) + (4 + MAX_TAG) + 8 + 8 + (1 + 1 + 2) + (1 + 4 + MAX_OWNER);

/// A reply's fields at most: the version's facts and the longest listing, a page of S3's 1,000
/// keys at their longest or of 10,000 buckets.
pub const MAX_REPLY_FIELDS: usize = {
    let facts = 2 * (4 + MAX_TAG) + 8 + (1 + 1 + 2 + 1 + MAX_CHECKSUM) + 1 + (1 + 8 + 8) + 1;
    let keys = PAGE_BYTES + MAX_KEYS * LISTED_BYTES;
    let buckets = 4 + MAX_BUCKETS * (4 + MAX_BUCKET + 8) + (1 + 4 + MAX_BUCKET);
    facts + 1 + if keys > buckets { keys } else { buckets }
};

/// The values of S3's request headers an object's operation takes as text, as a request sends
/// them: its preconditions (05 §2) and a read's range (05 §12).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<String>,
    pub if_unmodified_since: Option<String>,
    pub range: Option<String>,
}

impl Headers {
    /// The preconditions as `mantle_s3::conditional` judges them.
    pub fn conditions(&self) -> mantle_s3::conditional::Conditions<'_> {
        mantle_s3::conditional::Conditions {
            if_match: self.if_match.as_deref(),
            if_none_match: self.if_none_match.as_deref(),
            if_modified_since: self.if_modified_since.as_deref(),
            if_unmodified_since: self.if_unmodified_since.as_deref(),
        }
    }

    fn each(&self) -> [&Option<String>; 5] {
        [
            &self.if_match,
            &self.if_none_match,
            &self.if_modified_since,
            &self.if_unmodified_since,
            &self.range,
        ]
    }
}

/// ListObjectsV2's request (05 §6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct List {
    pub bucket: String,
    pub prefix: String,
    /// No delimiter when empty.
    pub delimiter: String,
    /// Where the page starts: after `start-after` or a page's last item, or past a key a page
    /// passed, as a continuation token names it; `None` for the first page.
    pub start: Option<Resume>,
    pub max_keys: u32,
    pub fetch_owner: bool,
}

/// What an object's request asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// CreateBucket, owned by the asker.
    Create { bucket: String, id: u128 },
    /// PutObject: `length` bytes follow the fields, checked against `checksum` and `md5` before
    /// anything commits.
    Put {
        bucket: String,
        key: String,
        id: u128,
        length: u64,
        headers: Headers,
        checksum: Option<Checksum>,
        md5: Option<[u8; 16]>,
    },
    /// GetObject: the current version's bytes, or the range its `Range` names.
    Get {
        bucket: String,
        key: String,
        headers: Headers,
    },
    /// HeadObject: the current version's facts.
    Head {
        bucket: String,
        key: String,
        headers: Headers,
    },
    /// DeleteObject: the key's object removed, or a delete marker made where versioning is on.
    Delete {
        bucket: String,
        key: String,
        id: u128,
        headers: Headers,
    },
    /// ListObjectsV2.
    List(List),
    /// HeadBucket.
    HeadBucket { bucket: String },
    /// ListBuckets: the asker's buckets whose names begin with `prefix`, after `after`.
    ListBuckets {
        prefix: String,
        after: Option<String>,
        max: u32,
    },
    /// DeleteBucket.
    DeleteBucket { bucket: String, id: u128 },
    /// PutBucketVersioning: enabled or suspended, never unversioned again (05 §7.1).
    PutVersioning {
        bucket: String,
        id: u128,
        versioning: Versioning,
    },
    /// GetBucketVersioning.
    GetVersioning { bucket: String },
}

/// A versioning state as a message carries it.
fn versioning_code(v: Versioning) -> u8 {
    match v {
        Versioning::Unversioned => 1,
        Versioning::Enabled => 2,
        Versioning::Suspended => 3,
    }
}

fn versioning_of(code: u8) -> Option<Versioning> {
    match code {
        1 => Some(Versioning::Unversioned),
        2 => Some(Versioning::Enabled),
        3 => Some(Versioning::Suspended),
        _ => None,
    }
}

fn put_text(w: &mut Writer, text: &str, most: usize) -> Result<(), ObjectError> {
    if text.len() > most {
        return Err(ObjectError::TooLong(text.len()));
    }
    w.u32(u32::try_from(text.len()).map_err(|_| ObjectError::TooLong(text.len()))?);
    w.bytes(text.as_bytes());
    Ok(())
}

fn take_text(r: &mut Reader<'_>, most: usize) -> Option<String> {
    let len = usize::try_from(r.u32()?).ok()?;
    if len > most {
        return None;
    }
    String::from_utf8(r.take(len)?.to_vec()).ok()
}

fn put_flag(w: &mut Writer, flag: bool) {
    w.u8(u8::from(flag));
}

fn take_flag(r: &mut Reader<'_>) -> Option<bool> {
    match r.u8()? {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    }
}

fn put_optional_text(w: &mut Writer, text: Option<&str>, most: usize) -> Result<(), ObjectError> {
    put_flag(w, text.is_some());
    match text {
        Some(text) => put_text(w, text, most),
        None => Ok(()),
    }
}

fn take_optional_text(r: &mut Reader<'_>, most: usize) -> Option<Option<String>> {
    if take_flag(r)? {
        Some(Some(take_text(r, most)?))
    } else {
        Some(None)
    }
}

fn put_headers(w: &mut Writer, c: &Headers) -> Result<(), ObjectError> {
    let total = c
        .each()
        .iter()
        .filter_map(|v| v.as_ref())
        .fold(0usize, |sum, v| sum.saturating_add(v.len()));
    if total > MAX_HEADERS {
        return Err(ObjectError::TooLong(total));
    }
    for value in c.each() {
        put_optional_text(w, value.as_deref(), MAX_HEADERS)?;
    }
    Ok(())
}

fn take_headers(r: &mut Reader<'_>) -> Option<Headers> {
    let c = Headers {
        if_match: take_optional_text(r, MAX_HEADERS)?,
        if_none_match: take_optional_text(r, MAX_HEADERS)?,
        if_modified_since: take_optional_text(r, MAX_HEADERS)?,
        if_unmodified_since: take_optional_text(r, MAX_HEADERS)?,
        range: take_optional_text(r, MAX_HEADERS)?,
    };
    let total = c
        .each()
        .iter()
        .filter_map(|v| v.as_ref())
        .try_fold(0usize, |sum, v| sum.checked_add(v.len()))?;
    if total <= MAX_HEADERS { Some(c) } else { None }
}

fn put_resume(w: &mut Writer, start: Option<&Resume>) -> Result<(), ObjectError> {
    put_flag(w, start.is_some());
    match start {
        None => Ok(()),
        Some(Resume::After(key)) => {
            w.u8(0);
            put_text(w, key, MAX_KEY)
        }
        Some(Resume::Past(key)) => {
            w.u8(1);
            put_text(w, key, MAX_KEY)
        }
    }
}

fn take_resume(r: &mut Reader<'_>) -> Option<Option<Resume>> {
    if !take_flag(r)? {
        return Some(None);
    }
    Some(Some(match r.u8()? {
        0 => Resume::After(take_text(r, MAX_KEY)?),
        1 => Resume::Past(take_text(r, MAX_KEY)?),
        _ => return None,
    }))
}

/// A request's head: what it is and how long its fields are, before they are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    kind: u8,
    fields: u32,
    id: u128,
    length: u64,
}

impl Head {
    /// The bytes of the fields that open the body.
    pub fn fields(&self) -> u64 {
        u64::from(self.fields)
    }

    /// The request, once its fields are read.
    pub fn op(&self, fields: &[u8]) -> Result<Op, ObjectError> {
        let bad = || ObjectError::Fields(fields.len());
        if u64::try_from(fields.len()).ok() != Some(self.fields()) {
            return Err(bad());
        }
        let mut r = Reader::new(fields);
        let object = |r: &mut Reader<'_>| Some((take_text(r, MAX_BUCKET)?, take_text(r, MAX_KEY)?));
        let op = (|| {
            Some(match self.kind {
                CREATE => Op::Create {
                    bucket: take_text(&mut r, MAX_BUCKET)?,
                    id: self.id,
                },
                PUT => {
                    let (bucket, key) = object(&mut r)?;
                    let headers = take_headers(&mut r)?;
                    let checksum = if take_flag(&mut r)? {
                        let algorithm = Algorithm::from_code(r.u8()?)?;
                        let len = usize::from(r.u8()?);
                        if len != algorithm.width() {
                            return None;
                        }
                        Some(Checksum {
                            algorithm,
                            bytes: r.take(len)?.to_vec(),
                        })
                    } else {
                        None
                    };
                    let md5 = if take_flag(&mut r)? {
                        Some(<[u8; 16]>::try_from(r.take(16)?).ok()?)
                    } else {
                        None
                    };
                    Op::Put {
                        bucket,
                        key,
                        id: self.id,
                        length: self.length,
                        headers,
                        checksum,
                        md5,
                    }
                }
                GET => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Get {
                        bucket,
                        key,
                        headers: take_headers(&mut r)?,
                    }
                }
                HEAD => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Head {
                        bucket,
                        key,
                        headers: take_headers(&mut r)?,
                    }
                }
                DELETE => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Delete {
                        bucket,
                        key,
                        id: self.id,
                        headers: take_headers(&mut r)?,
                    }
                }
                LIST => Op::List(take_list(&mut r)?),
                HEAD_BUCKET => Op::HeadBucket {
                    bucket: take_text(&mut r, MAX_BUCKET)?,
                },
                LIST_BUCKETS => Op::ListBuckets {
                    prefix: take_text(&mut r, MAX_BUCKET)?,
                    after: take_optional_text(&mut r, MAX_BUCKET)?,
                    max: r.u32().filter(|m| {
                        usize::try_from(*m).is_ok_and(|m| (1..=MAX_BUCKETS).contains(&m))
                    })?,
                },
                DELETE_BUCKET => Op::DeleteBucket {
                    bucket: take_text(&mut r, MAX_BUCKET)?,
                    id: self.id,
                },
                PUT_VERSIONING => Op::PutVersioning {
                    bucket: take_text(&mut r, MAX_BUCKET)?,
                    id: self.id,
                    versioning: versioning_of(r.u8()?).filter(|v| *v != Versioning::Unversioned)?,
                },
                GET_VERSIONING => Op::GetVersioning {
                    bucket: take_text(&mut r, MAX_BUCKET)?,
                },
                _ => return None,
            })
        })();
        op.filter(|_| r.remaining() == 0).ok_or_else(bad)
    }

    /// The head an exchange carries, its body `body` bytes: refused for an unknown kind, fields
    /// past their bound, a length on any but a put, or a body that is not the fields and a put's
    /// length.
    pub fn decode(head: &[u8], body: Option<u64>) -> Result<Self, ObjectError> {
        let mut r = Reader::new(head);
        let decoded = (|| {
            Some(Self {
                kind: r.u8()?,
                fields: r.u32()?,
                id: r.u128()?,
                length: r.u64()?,
            })
        })()
        .filter(|_| r.remaining() == 0);
        let head_of = || ObjectError::Head(head.len());
        let decoded = decoded.ok_or_else(head_of)?;
        let within = usize::try_from(decoded.fields).is_ok_and(|f| f <= MAX_FIELDS)
            && decoded.kind <= GET_VERSIONING
            && (decoded.kind == PUT || decoded.length == 0);
        if !within || decoded.fields().checked_add(decoded.length) != Some(body.unwrap_or(0)) {
            return Err(head_of());
        }
        Ok(decoded)
    }
}

impl Op {
    /// The request's head and the fields that open its body.
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), ObjectError> {
        let mut f = Writer::default();
        let object = |f: &mut Writer, bucket: &str, key: &str| -> Result<(), ObjectError> {
            put_text(f, bucket, MAX_BUCKET)?;
            put_text(f, key, MAX_KEY)
        };
        let (kind, id, length) = match self {
            Self::Create { bucket, id } => {
                put_text(&mut f, bucket, MAX_BUCKET)?;
                (CREATE, *id, 0)
            }
            Self::Put {
                bucket,
                key,
                id,
                length,
                headers,
                checksum,
                md5,
            } => {
                object(&mut f, bucket, key)?;
                put_headers(&mut f, headers)?;
                put_flag(&mut f, checksum.is_some());
                if let Some(c) = checksum {
                    if c.bytes.len() != c.algorithm.width() {
                        return Err(ObjectError::TooLong(c.bytes.len()));
                    }
                    f.u8(c.algorithm.code());
                    f.u8(u8::try_from(c.bytes.len()).map_err(|_| ObjectError::TooLong(0))?);
                    f.bytes(&c.bytes);
                }
                put_flag(&mut f, md5.is_some());
                if let Some(md5) = md5 {
                    f.bytes(md5);
                }
                (PUT, *id, *length)
            }
            Self::Get {
                bucket,
                key,
                headers,
            } => {
                object(&mut f, bucket, key)?;
                put_headers(&mut f, headers)?;
                (GET, 0, 0)
            }
            Self::Head {
                bucket,
                key,
                headers,
            } => {
                object(&mut f, bucket, key)?;
                put_headers(&mut f, headers)?;
                (HEAD, 0, 0)
            }
            Self::Delete {
                bucket,
                key,
                id,
                headers,
            } => {
                object(&mut f, bucket, key)?;
                put_headers(&mut f, headers)?;
                (DELETE, *id, 0)
            }
            Self::List(list) => {
                put_list(&mut f, list)?;
                (LIST, 0, 0)
            }
            Self::HeadBucket { bucket } => {
                put_text(&mut f, bucket, MAX_BUCKET)?;
                (HEAD_BUCKET, 0, 0)
            }
            Self::ListBuckets { prefix, after, max } => {
                put_text(&mut f, prefix, MAX_BUCKET)?;
                put_optional_text(&mut f, after.as_deref(), MAX_BUCKET)?;
                if !usize::try_from(*max).is_ok_and(|m| (1..=MAX_BUCKETS).contains(&m)) {
                    return Err(ObjectError::TooLong(usize::try_from(*max).unwrap_or(0)));
                }
                f.u32(*max);
                (LIST_BUCKETS, 0, 0)
            }
            Self::DeleteBucket { bucket, id } => {
                put_text(&mut f, bucket, MAX_BUCKET)?;
                (DELETE_BUCKET, *id, 0)
            }
            Self::PutVersioning {
                bucket,
                id,
                versioning,
            } => {
                if *versioning == Versioning::Unversioned {
                    return Err(ObjectError::Unversioned);
                }
                put_text(&mut f, bucket, MAX_BUCKET)?;
                f.u8(versioning_code(*versioning));
                (PUT_VERSIONING, *id, 0)
            }
            Self::GetVersioning { bucket } => {
                put_text(&mut f, bucket, MAX_BUCKET)?;
                (GET_VERSIONING, 0, 0)
            }
        };
        let fields = f.into_vec();
        if fields.len() > MAX_FIELDS {
            return Err(ObjectError::TooLong(fields.len()));
        }
        let mut w = Writer::with_capacity(HEAD_BYTES);
        w.u8(kind);
        w.u32(u32::try_from(fields.len()).map_err(|_| ObjectError::TooLong(fields.len()))?);
        w.u128(id);
        w.u64(length);
        Ok((w.into_vec(), fields))
    }

    /// The bytes of the request's body: its fields and a put's object.
    pub fn body(&self, fields: &[u8]) -> u64 {
        let fields = u64::try_from(fields.len()).unwrap_or(u64::MAX);
        match self {
            Self::Put { length, .. } => fields.saturating_add(*length),
            _ => fields,
        }
    }
}

/// What became of an object's request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Done: the bucket made or removed, the object committed or removed, the facts or the
    /// bytes follow.
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
    /// S3's `304 Not Modified`: a read's `If-None-Match` or `If-Modified-Since` held; the
    /// version's facts follow, its ETag among them (05 §2.4).
    NotModified,
    /// S3's `412 PreconditionFailed`.
    PreconditionFailed,
    /// S3's `400 BadDigest`: the body is not what its checksum or `Content-MD5` says (05 §3.2).
    BadDigest,
    /// S3's `409 BucketNotEmpty`: the bucket holds a version, a delete marker or an upload.
    BucketNotEmpty,
    /// S3's `403 AccessDenied`: another principal's bucket, or a write that would remove or
    /// overwrite a version Object Lock holds (18 §5).
    AccessDenied,
    /// S3's `409 InvalidBucketState`: versioning suspended on a bucket Object Lock holds, which
    /// "can't ... suspend versioning" (18 §2.1).
    InvalidBucketState,
}

const STATUSES: [Status; 19] = [
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
    Status::NotModified,
    Status::PreconditionFailed,
    Status::BadDigest,
    Status::BucketNotEmpty,
    Status::AccessDenied,
    Status::InvalidBucketState,
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
            Self::NotModified => 13,
            Self::PreconditionFailed => 14,
            Self::BadDigest => 15,
            Self::BucketNotEmpty => 16,
            Self::AccessDenied => 17,
            Self::InvalidBucketState => 18,
        }
    }

    fn of(code: u8) -> Option<Self> {
        STATUSES.into_iter().find(|s| s.code() == code)
    }
}

/// A key a page lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    /// Bare of quotes.
    pub etag: String,
    pub size: u64,
    /// Last-Modified, whole seconds.
    pub modified: u64,
    /// The algorithm of the checksum the version keeps, and the parts a composite value
    /// combines (0 for a full-object value).
    pub checksum: Option<(Algorithm, u16)>,
    /// The version's owner, when the request asked for owners.
    pub owner: Option<String>,
}

/// A page of ListObjectsV2.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Page {
    pub contents: Vec<Listed>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// Where the next page starts, on a truncated page.
    pub next: Option<Resume>,
}

/// A page of ListBuckets: each bucket's name and creation, whole seconds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Buckets {
    pub buckets: Vec<(String, u64)>,
    /// The bucket after which the next page starts, when more follow.
    pub next: Option<String>,
}

/// What a reply lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Listing {
    Keys(Page),
    Buckets(Buckets),
}

/// A reply: its head's status and size, and the fields its body opens with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: Status,
    /// The object's size, a get's or a head's.
    pub size: u64,
    /// The version's id: a put's, a delete's marker's, or the one a get or head read; empty
    /// where none applies.
    pub version: String,
    /// Bare of quotes.
    pub etag: String,
    /// Last-Modified, whole seconds.
    pub modified: u64,
    pub checksum: Option<record::Checksum>,
    /// The version a delete made or removed, or the key's current one a get or head found, is a
    /// delete marker.
    pub marker: bool,
    /// The bytes a get's or head's range served, first and last: S3's `206 Partial Content`.
    pub range: Option<(u64, u64)>,
    /// The bucket's versioning, GetBucketVersioning's.
    pub versioning: Option<Versioning>,
    pub listing: Option<Listing>,
}

fn put_listed(w: &mut Writer, l: &Listed) -> Result<(), ObjectError> {
    put_text(w, &l.key, MAX_KEY)?;
    put_text(w, &l.etag, MAX_TAG)?;
    w.u64(l.size);
    w.u64(l.modified);
    put_flag(w, l.checksum.is_some());
    if let Some((algorithm, parts)) = l.checksum {
        w.u8(algorithm.code());
        w.u16(parts);
    }
    put_optional_text(w, l.owner.as_deref(), MAX_OWNER)
}

fn take_listed(r: &mut Reader<'_>) -> Option<Listed> {
    Some(Listed {
        key: take_text(r, MAX_KEY)?,
        etag: take_text(r, MAX_TAG)?,
        size: r.u64()?,
        modified: r.u64()?,
        checksum: if take_flag(r)? {
            Some((Algorithm::from_code(r.u8()?)?, r.u16()?))
        } else {
            None
        },
        owner: take_optional_text(r, MAX_OWNER)?,
    })
}

/// The bytes a key takes in a page of keys: its key, ETag, size, Last-Modified, checksum
/// algorithm and owner, as [`put_page`] writes it.
pub(crate) fn listed_bytes(key: &str, etag: &str, checksum: bool, owner: Option<&str>) -> usize {
    let checksum = if checksum { 1 + 1 + 2 } else { 1 };
    let owner = owner.map_or(1, |o| 1usize.saturating_add(4).saturating_add(o.len()));
    4usize
        .saturating_add(key.len())
        .saturating_add(4)
        .saturating_add(etag.len())
        .saturating_add(8 + 8)
        .saturating_add(checksum)
        .saturating_add(owner)
}

/// The bytes a common prefix takes in a page of keys.
pub(crate) fn prefix_bytes(prefix: &str) -> usize {
    4usize.saturating_add(prefix.len())
}

/// The bytes a page of keys takes beside its keys and common prefixes, at most: whether it is
/// truncated, where the next page starts and the two counts.
pub(crate) const PAGE_BYTES: usize = 1 + (1 + 1 + 4 + MAX_KEY) + 4 + 4;

/// Whole seconds of `ns` nanoseconds since the Unix epoch: the one time an object's
/// Last-Modified header, listing and conditions take (s3-protocol.md §4).
pub(crate) fn seconds(ns: u64) -> u64 {
    ns / 1_000_000_000
}

pub(crate) fn put_list(w: &mut Writer, list: &List) -> Result<(), ObjectError> {
    put_text(w, &list.bucket, MAX_BUCKET)?;
    put_text(w, &list.prefix, MAX_KEY)?;
    put_text(w, &list.delimiter, MAX_KEY)?;
    put_resume(w, list.start.as_ref())?;
    w.u32(list.max_keys);
    put_flag(w, list.fetch_owner);
    Ok(())
}

pub(crate) fn take_list(r: &mut Reader<'_>) -> Option<List> {
    Some(List {
        bucket: take_text(r, MAX_BUCKET)?,
        prefix: take_text(r, MAX_KEY)?,
        delimiter: take_text(r, MAX_KEY)?,
        start: take_resume(r)?,
        max_keys: r.u32()?,
        fetch_owner: take_flag(r)?,
    })
}

pub(crate) fn put_page(w: &mut Writer, page: &Page) -> Result<(), ObjectError> {
    if page
        .contents
        .len()
        .saturating_add(page.common_prefixes.len())
        > MAX_KEYS
    {
        return Err(ObjectError::TooLong(page.contents.len()));
    }
    put_flag(w, page.truncated);
    put_resume(w, page.next.as_ref())?;
    put_count(w, page.contents.len())?;
    for listed in &page.contents {
        put_listed(w, listed)?;
    }
    put_count(w, page.common_prefixes.len())?;
    for prefix in &page.common_prefixes {
        put_text(w, prefix, MAX_KEY)?;
    }
    Ok(())
}

pub(crate) fn take_page(r: &mut Reader<'_>) -> Option<Page> {
    let truncated = take_flag(r)?;
    let next = take_resume(r)?;
    // A key takes 38 bytes at least, a common prefix 4.
    let count = take_count(r, MAX_KEYS, 38)?;
    let mut contents = Vec::with_capacity(count);
    for _ in 0..count {
        contents.push(take_listed(r)?);
    }
    let prefixes = take_count(r, MAX_KEYS.checked_sub(count)?, 4)?;
    let mut common_prefixes = Vec::with_capacity(prefixes);
    for _ in 0..prefixes {
        common_prefixes.push(take_text(r, MAX_KEY)?);
    }
    Some(Page {
        contents,
        common_prefixes,
        truncated,
        next,
    })
}

pub(crate) fn put_buckets(w: &mut Writer, page: &Buckets) -> Result<(), ObjectError> {
    if page.buckets.len() > MAX_BUCKETS {
        return Err(ObjectError::TooLong(page.buckets.len()));
    }
    put_count(w, page.buckets.len())?;
    for (name, created) in &page.buckets {
        put_text(w, name, MAX_BUCKET)?;
        w.u64(*created);
    }
    put_optional_text(w, page.next.as_deref(), MAX_BUCKET)
}

pub(crate) fn take_buckets(r: &mut Reader<'_>) -> Option<Buckets> {
    // A bucket takes 12 bytes at least.
    let count = take_count(r, MAX_BUCKETS, 12)?;
    let mut buckets = Vec::with_capacity(count);
    for _ in 0..count {
        buckets.push((take_text(r, MAX_BUCKET)?, r.u64()?));
    }
    let next = take_optional_text(r, MAX_BUCKET)?;
    Some(Buckets { buckets, next })
}

/// A count within `most`, read as a u32, and no more than the rest holds at `least` bytes each.
fn take_count(r: &mut Reader<'_>, most: usize, least: usize) -> Option<usize> {
    let count = usize::try_from(r.u32()?).ok()?;
    let fits = r.remaining().checked_div(least)?;
    if count <= most && count <= fits {
        Some(count)
    } else {
        None
    }
}

fn put_count(w: &mut Writer, count: usize) -> Result<(), ObjectError> {
    w.u32(u32::try_from(count).map_err(|_| ObjectError::TooLong(count))?);
    Ok(())
}

impl Reply {
    /// A reply of `status` alone.
    pub fn status(status: Status) -> Self {
        Self {
            status,
            size: 0,
            version: String::new(),
            etag: String::new(),
            modified: 0,
            checksum: None,
            marker: false,
            range: None,
            versioning: None,
            listing: None,
        }
    }

    /// The reply's head and the fields that open its body.
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), ObjectError> {
        let mut f = Writer::default();
        put_text(&mut f, &self.version, MAX_TAG)?;
        put_text(&mut f, &self.etag, MAX_TAG)?;
        f.u64(self.modified);
        put_flag(&mut f, self.checksum.is_some());
        if let Some(c) = &self.checksum {
            if c.value.len() > MAX_CHECKSUM {
                return Err(ObjectError::TooLong(c.value.len()));
            }
            f.u8(c.algorithm);
            f.u16(c.parts);
            f.u8(u8::try_from(c.value.len()).map_err(|_| ObjectError::TooLong(0))?);
            f.bytes(&c.value);
        }
        put_flag(&mut f, self.marker);
        put_flag(&mut f, self.range.is_some());
        if let Some((first, last)) = self.range {
            f.u64(first);
            f.u64(last);
        }
        f.u8(self.versioning.map_or(0, versioning_code));
        match &self.listing {
            None => f.u8(0),
            Some(Listing::Keys(page)) => {
                f.u8(1);
                put_page(&mut f, page)?;
            }
            Some(Listing::Buckets(page)) => {
                f.u8(2);
                put_buckets(&mut f, page)?;
            }
        }
        let fields = f.into_vec();
        let mut w = Writer::with_capacity(REPLY_HEAD_BYTES);
        w.u8(self.status.code());
        w.u64(self.size);
        w.u32(u32::try_from(fields.len()).map_err(|_| ObjectError::TooLong(fields.len()))?);
        Ok((w.into_vec(), fields))
    }
}

/// A reply's head: its status and size, and how long the fields that open its body are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplyHead {
    pub status: Status,
    pub size: u64,
    fields: u32,
}

impl ReplyHead {
    /// The bytes of the fields that open the body.
    pub fn fields(&self) -> u64 {
        u64::from(self.fields)
    }

    pub fn decode(head: &[u8]) -> Result<Self, ObjectError> {
        let mut r = Reader::new(head);
        let reply = (|| {
            Some(Self {
                status: Status::of(r.u8()?)?,
                size: r.u64()?,
                fields: r.u32()?,
            })
        })();
        reply
            .filter(|h| {
                r.remaining() == 0 && usize::try_from(h.fields).is_ok_and(|f| f <= MAX_REPLY_FIELDS)
            })
            .ok_or(ObjectError::Reply(head.len()))
    }

    /// The reply, once its fields are read.
    pub fn reply(&self, fields: &[u8]) -> Result<Reply, ObjectError> {
        let bad = || ObjectError::Fields(fields.len());
        if u64::try_from(fields.len()).ok() != Some(self.fields()) {
            return Err(bad());
        }
        let mut r = Reader::new(fields);
        let reply = (|| {
            let version = take_text(&mut r, MAX_TAG)?;
            let etag = take_text(&mut r, MAX_TAG)?;
            let modified = r.u64()?;
            let checksum = if take_flag(&mut r)? {
                let algorithm = r.u8()?;
                let parts = r.u16()?;
                let len = usize::from(r.u8()?);
                if len > MAX_CHECKSUM {
                    return None;
                }
                Some(record::Checksum {
                    algorithm,
                    parts,
                    value: r.take(len)?.to_vec(),
                })
            } else {
                None
            };
            let marker = take_flag(&mut r)?;
            let range = if take_flag(&mut r)? {
                Some((r.u64()?, r.u64()?))
            } else {
                None
            };
            let versioning = match r.u8()? {
                0 => None,
                code => Some(versioning_of(code)?),
            };
            let listing = match r.u8()? {
                0 => None,
                1 => Some(Listing::Keys(take_page(&mut r)?)),
                2 => Some(Listing::Buckets(take_buckets(&mut r)?)),
                _ => return None,
            };
            Some(Reply {
                status: self.status,
                size: self.size,
                version,
                etag,
                modified,
                checksum,
                marker,
                range,
                versioning,
                listing,
            })
        })();
        reply.filter(|_| r.remaining() == 0).ok_or_else(bad)
    }
}

/// Why an object's message did not encode or decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ObjectError {
    #[error("an object's request head of {0} bytes")]
    Head(usize),
    #[error("an object's reply head of {0} bytes")]
    Reply(usize),
    #[error("fields of {0} bytes that are not the head's")]
    Fields(usize),
    #[error("a field of {0} bytes, past its bound")]
    TooLong(usize),
    #[error("versioning, once set, is enabled or suspended, never unversioned again")]
    Unversioned,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> Headers {
        Headers {
            if_match: Some("\"a\", \"b\"".into()),
            if_none_match: Some("*".into()),
            if_modified_since: Some("Tue, 24 Sep 2024 12:00:00 GMT".into()),
            if_unmodified_since: None,
            range: Some("bytes=3-".into()),
        }
    }

    fn every_op() -> Vec<Op> {
        vec![
            Op::Create {
                bucket: "b".into(),
                id: 2,
            },
            Op::Put {
                bucket: "b".into(),
                key: "k/ü".into(),
                id: u128::MAX,
                length: 1 << 30,
                headers: headers(),
                checksum: Some(Checksum {
                    algorithm: Algorithm::Sha512,
                    bytes: vec![7; 64],
                }),
                md5: Some([9; 16]),
            },
            Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: 1,
                length: 0,
                headers: Headers::default(),
                checksum: None,
                md5: None,
            },
            Op::Get {
                bucket: "b".into(),
                key: "k".repeat(MAX_KEY),
                headers: headers(),
            },
            Op::Head {
                bucket: "b".repeat(MAX_BUCKET),
                key: "k".into(),
                headers: Headers::default(),
            },
            Op::Delete {
                bucket: "b".into(),
                key: "k".into(),
                id: 3,
                headers: headers(),
            },
            Op::List(List {
                bucket: "b".into(),
                prefix: "p/".into(),
                delimiter: "/".into(),
                start: Some(Resume::Past("p/q".into())),
                max_keys: 1000,
                fetch_owner: true,
            }),
            Op::List(List {
                bucket: "b".into(),
                prefix: String::new(),
                delimiter: String::new(),
                start: None,
                max_keys: 0,
                fetch_owner: false,
            }),
            Op::HeadBucket { bucket: "b".into() },
            Op::ListBuckets {
                prefix: "a".into(),
                after: Some("ab".into()),
                max: u32::try_from(MAX_BUCKETS).unwrap(),
            },
            Op::DeleteBucket {
                bucket: "b".into(),
                id: 4,
            },
            Op::PutVersioning {
                bucket: "b".into(),
                id: 5,
                versioning: Versioning::Suspended,
            },
            Op::GetVersioning { bucket: "b".into() },
        ]
    }

    #[test]
    fn every_request_reads_back_within_its_bounds() {
        for op in every_op() {
            let (head, fields) = op.encode().unwrap();
            assert_eq!(head.len(), HEAD_BYTES);
            assert!(fields.len() <= MAX_FIELDS, "{op:?}");
            let decoded = Head::decode(&head, Some(op.body(&fields))).unwrap();
            assert_eq!(decoded.fields(), fields.len() as u64);
            assert_eq!(decoded.op(&fields), Ok(op));
        }
    }

    #[test]
    fn a_request_past_its_bounds_or_not_its_heads_is_refused() {
        let put = every_op().swap_remove(1);
        let (head, fields) = put.encode().unwrap();
        // A body that is not the fields and the put's length; fields cut short; a head cut short.
        assert!(Head::decode(&head, Some(put.body(&fields) - 1)).is_err());
        let decoded = Head::decode(&head, Some(put.body(&fields))).unwrap();
        assert!(decoded.op(&fields[1..]).is_err());
        assert!(Head::decode(&head[1..], Some(put.body(&fields))).is_err());
        // A length on any kind but a put's.
        let (mut get_head, get_fields) = every_op().swap_remove(3).encode().unwrap();
        get_head[HEAD_BYTES - 8..].copy_from_slice(&5u64.to_le_bytes());
        assert!(Head::decode(&get_head, Some(get_fields.len() as u64 + 5)).is_err());
        // An unknown kind, and versioning set back to never set.
        let mut unknown = head.clone();
        unknown[0] = GET_VERSIONING + 1;
        let unset = Op::PutVersioning {
            bucket: "b".into(),
            id: 1,
            versioning: Versioning::Unversioned,
        };
        assert_eq!(unset.encode(), Err(ObjectError::Unversioned));
        assert!(Head::decode(&unknown, Some(put.body(&fields))).is_err());
        // A key past S3's bound, headers past S3's 8 KB, a checksum of the wrong width.
        let long = Op::Head {
            bucket: "b".into(),
            key: "k".repeat(MAX_KEY + 1),
            headers: Headers::default(),
        };
        assert_eq!(long.encode(), Err(ObjectError::TooLong(MAX_KEY + 1)));
        let crowded = Op::Get {
            bucket: "b".into(),
            key: "k".into(),
            headers: Headers {
                if_match: Some("m".repeat(MAX_HEADERS / 2)),
                range: Some("r".repeat(MAX_HEADERS / 2 + 1)),
                ..Headers::default()
            },
        };
        assert_eq!(crowded.encode(), Err(ObjectError::TooLong(MAX_HEADERS + 1)));
        let narrow = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: 1,
            headers: Headers::default(),
            checksum: Some(Checksum {
                algorithm: Algorithm::Crc32c,
                bytes: vec![1; 8],
            }),
            md5: None,
        };
        assert!(narrow.encode().is_err());
        // ListBuckets asks one bucket at least and S3's 10,000 at most.
        for max in [0, u32::try_from(MAX_BUCKETS).unwrap() + 1] {
            let op = Op::ListBuckets {
                prefix: String::new(),
                after: None,
                max,
            };
            assert!(op.encode().is_err(), "{max}");
        }
    }

    #[test]
    fn every_reply_reads_back_and_nothing_past_s3s_bounds_does() {
        let listed = |n: usize| Listed {
            key: format!("k{n}"),
            etag: "0".repeat(32),
            size: n as u64,
            modified: 1_727_179_200,
            checksum: if n.is_multiple_of(2) {
                Some((Algorithm::Crc64Nvme, 0))
            } else {
                None
            },
            owner: if n.is_multiple_of(3) {
                Some("o".repeat(MAX_OWNER))
            } else {
                None
            },
        };
        let mut replies = Vec::new();
        for status in STATUSES {
            replies.push(Reply::status(status));
        }
        replies.push(Reply {
            status: Status::Done,
            size: 7,
            version: "0123456789ABC".into(),
            etag: "d41d8cd98f00b204e9800998ecf8427e-10000".into(),
            modified: u64::MAX,
            checksum: Some(record::Checksum {
                algorithm: Algorithm::Sha512.code(),
                parts: 3,
                value: vec![1; 64],
            }),
            marker: true,
            range: Some((3, 6)),
            versioning: Some(Versioning::Suspended),
            listing: None,
        });
        replies.push(Reply {
            listing: Some(Listing::Keys(Page {
                contents: (0..999).map(listed).collect(),
                common_prefixes: vec!["p/".into()],
                truncated: true,
                next: Some(Resume::After("p/".into())),
            })),
            ..Reply::status(Status::Done)
        });
        replies.push(Reply {
            listing: Some(Listing::Buckets(Buckets {
                buckets: (0..MAX_BUCKETS)
                    .map(|n| (format!("{n:0>63}"), n as u64))
                    .collect(),
                next: Some("z".into()),
            })),
            ..Reply::status(Status::Done)
        });
        for reply in replies {
            let (head, fields) = reply.encode().unwrap();
            assert_eq!(head.len(), REPLY_HEAD_BYTES);
            assert!(fields.len() <= MAX_REPLY_FIELDS);
            let decoded = ReplyHead::decode(&head).unwrap();
            assert_eq!(decoded.fields(), fields.len() as u64);
            assert_eq!(decoded.reply(&fields), Ok(reply));
        }
        // A page of more than S3's 1,000 keys and common prefixes, an unknown status, a head
        // naming more fields than any reply holds, and fields cut short: each refused.
        let over = Reply {
            listing: Some(Listing::Keys(Page {
                contents: (0..1000).map(listed).collect(),
                common_prefixes: vec!["p/".into()],
                ..Page::default()
            })),
            ..Reply::status(Status::Done)
        };
        assert!(over.encode().is_err());
        assert!(ReplyHead::decode(&[19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
        let mut long = Reply::status(Status::Done).encode().unwrap().0;
        long[9..].copy_from_slice(&(u32::try_from(MAX_REPLY_FIELDS).unwrap() + 1).to_le_bytes());
        assert!(ReplyHead::decode(&long).is_err());
        let (head, fields) = Reply::status(Status::NotModified).encode().unwrap();
        let decoded = ReplyHead::decode(&head).unwrap();
        assert!(decoded.reply(&fields[1..]).is_err());
    }
}
