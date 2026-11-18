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
//! | request | the kind (u8); the fields' length (u32); the operation's identity (u128), which every attempt of a mutation carries (gateway.md §2.1), else 0; the time its client will still wait, nanoseconds from its sending (u64), 0 for none (research/77 D2); a put's length, else 0 (u64); the algorithm of the checksum a put's trailer brings after its bytes (u8), 0 for none, else its code (`mantle_s3::checksum::Algorithm::code`) and one | the fields; then a put's bytes; then its trailer, the checksum's value |
//! | reply | the status (u8); the object's size, a get's or a head's, else 0 (u64); the fields' length (u32) | the fields; then a get's bytes |
//!
//! A request's fields, by its kind. A text is UTF-8 behind its length (u32); an optional field is
//! behind a byte, 0 for none and 1 for one.
//!
//! | Kind | Operation | Fields |
//! |---|---|---|
//! | 0 | CreateBucket | the bucket |
//! | 1 | PutObject | the bucket and the key; the headers; the checksum, optional: its algorithm's code (u8) and its value, its length (u8) before it; `Content-MD5`, optional, 16 bytes; the metadata |
//! | 2 | GetObject | the bucket and the key; the headers; the version, optional, a text |
//! | 3 | HeadObject | the bucket and the key; the headers; the version, optional, a text |
//! | 4 | DeleteObject | the bucket and the key; the headers; the version, optional, a text |
//! | 5 | ListObjectsV2 | the bucket; the prefix; the delimiter, empty for none; where the page starts, optional: 0 after a key or common prefix, 1 just past a key a page passed, then the key; the most keys (u32); whether each key's owner is listed (u8) |
//! | 6 | HeadBucket | the bucket |
//! | 7 | ListBuckets | the prefix; the bucket after which the page starts, optional; the most buckets (u32) |
//! | 8 | DeleteBucket | the bucket |
//! | 9 | PutBucketVersioning | the bucket; its state (u8): 2 enabled, 3 suspended |
//! | 10 | GetBucketVersioning | the bucket |
//! | 11 | CreateMultipartUpload | the bucket and the key; the checksum its parts carry, optional: its algorithm's code (u8) and whether its values combine as one full-object value (u8); the metadata |
//! | 12 | UploadPart | the bucket and the key; the upload's ID; the part's number (u16); the checksum, optional, and `Content-MD5`, optional, as a put's |
//! | 13 | CompleteMultipartUpload | the bucket and the key; the upload's ID; the headers; the parts it lists (a u32 count), each its number (u16) and the ETag its upload answered, bare of quotes, in ascending order |
//! | 14 | AbortMultipartUpload | the bucket and the key; the upload's ID |
//! | 15 | ListParts | the bucket and the key; the upload's ID; the part number after which the page starts (u16); the most parts (u32) |
//! | 16 | ListMultipartUploads | the bucket; the prefix; the delimiter, empty for none; the key marker and the upload ID marker, each optional; the most uploads (u32) |
//! | 17 | Open, a native upload (gateway.md §2.1) | the bucket and the key; the object's length, optional (u64); the client's buffer, optional (u64); the full-object checksum's algorithm's code (u8); the metadata |
//! | 18 | Append, a native upload's run | the bucket and the key; the upload's ID; the offset it begins at (u64); whether it ends the object (u8) |
//! | 19 | Offset, a native upload's state | the bucket and the key; the upload's ID |
//! | 20 | Finish, a native upload's end | the bucket and the key; the upload's ID; the headers; the object's length (u64); its full-object checksum: its algorithm's code (u8) and its value, its length (u8) before it |
//! | 21 | CopyObject (research/89 §3) | the bucket and the key; the source's bucket and key, and its version, optional; the source's conditions, as headers, `x-amz-copy-source-if-*` in the precondition slots and no range; the headers; the metadata directive (u8): 0 COPY, 1 REPLACE; the metadata; the checksum's algorithm, optional, its code (u8) |
//! | 22 | UploadPartCopy (research/89 §4) | the bucket and the key; the upload's ID; the part's number (u16); the source's bucket and key, and its version, optional; the source's conditions, as headers, `x-amz-copy-source-range` in the range slot |
//! | 23 | DeleteObjects (research/89 §5) | the bucket; the objects (a u32 count, at most S3's 1,000), each its key, its version, optional, and the ETag its delete is conditioned on, optional; whether the reply is quiet (u8) |
//! | 24 | ListObjectVersions (research/89 §2.3) | the bucket; the prefix; the delimiter, empty for none; the key marker and the version ID marker, each optional; the most versions (u32) |
//! | 25 | CreatePrincipal (docs/design/principals.md) | the principal's name; its identity policy as written |
//! | 26 | PutPolicy | the principal's name; its identity policy as written |
//! | 27 | DeletePrincipal | the principal's name |
//! | 28 | ListPrincipals | the principal after which the page starts, optional; the most principals (u32) |
//! | 29 | GetPrincipal | the principal's name |
//! | 30 | CreateKey | the principal's name; whether it rotates the principal's one key (u8) |
//! | 31 | RevokeKey | the key's id |
//!
//! The metadata, a version's content headers and user-defined metadata (`mantle_s3::metadata`): a
//! count (u32), then each pair, its lowercase name and its value, each a text, together within
//! S3's 8 KB of headers. A version is a text within S3's 1,024 bytes of a version ID
//! (`mantle_s3::body::MAX_VERSION_ID`): `null`, or one of mantle's, which the node parses.
//!
//! The principals' kinds are an operator's, and every client of the native protocol acts for the
//! cell's account, its root (node.md §3.6), which IAM lets manage its users and their keys.
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
//! each), S3's `206 Partial Content`; the bucket's versioning, GetBucketVersioning's (u8): 0 none,
//! never set or not asked, 2 enabled, 3 suspended; the upload's ID, CreateMultipartUpload's, a text;
//! then the listing, behind a byte: 0 none, 1 a page of keys, 2 a page of buckets, 3 a page of
//! parts, 4 a page of uploads, 5 a page of versions, 6 a delete's results, 7 a page of principals,
//! 8 a key made; then a native upload's progress, optional: its durable offset (u64), its runs
//! (u16), the length of every run but its last (u64), whether its last run committed (u8) and a
//! run's checksum, optional, its length (u8) before it; then the version's metadata, a get's or a
//! head's; then the version a copy read of its source, a text, empty where none applies. A page of
//! keys: whether it is truncated (u8); where the next page starts,
//! optional, as a request names it; the count of keys (u32), each its key, ETag, size (u64),
//! Last-Modified (u64), checksum algorithm, optional, its code (u8) and parts (u16), and owner,
//! optional, a text; the count of common prefixes (u32), each a text. A page of buckets: the
//! count (u32), each its name and creation (u64, whole seconds); the bucket after which the next
//! page starts, optional. A page of parts: whether it is truncated (u8); the part number the next
//! page starts after, optional (u16); the count (u32), each its number (u16), ETag, size (u64),
//! Last-Modified (u64) and checksum value, optional, its length (u8) before it. A page of uploads:
//! whether it is truncated (u8); where the next page starts, optional, a key and an upload's ID;
//! the count of uploads (u32), each its key, ID, initiation (u64, whole seconds) and owner; the
//! count of common prefixes (u32), each a text. A page of versions: whether it is truncated (u8);
//! where the next page starts, optional, a key and a version ID marker; the count of versions
//! (u32), each its key, version ID, flags (u8: 1 the key's latest, 2 a delete marker), ETag, size
//! (u64), Last-Modified (u64), checksum algorithm, optional, its code (u8) and parts (u16), and
//! owner; the count of common prefixes (u32), each a text. A delete's results, in the order the
//! request named its objects: the count (u32), each the key, the version the request named,
//! optional, the status of its delete (u8), and the version ID of the delete marker the delete
//! made or removed, optional. A page of principals: the count (u32), each its name, its unique ID,
//! its creation (u64, whole seconds), its identity policy, optional, and its keys (a u32 count),
//! each its id and creation (u64, whole seconds); the principal after which the next page starts,
//! optional. A key made: its id, its secret and the key it replaces, optional.
//!
//! Numbers are little-endian. Every text is within S3's bound on what it names, and every count
//! within S3's page: a message past them is refused whole.

use std::time::Duration;

use mantle_codec::{Reader, Writer};
use mantle_s3::account::MAX_OWNER;
use mantle_s3::body::MAX_VERSION_ID;
use mantle_s3::body::Versioning;
use mantle_s3::checksum::{Algorithm, Checksum};
use mantle_s3::copy::{Directive, Source};
use mantle_s3::list::{MAX_KEYS, Resume};
use mantle_s3::route::{MAX_BUCKET, MAX_HEADERS, MAX_KEY};

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
const CREATE_UPLOAD: u8 = 11;
const UPLOAD_PART: u8 = 12;
const COMPLETE: u8 = 13;
const ABORT: u8 = 14;
const LIST_PARTS: u8 = 15;
const LIST_UPLOADS: u8 = 16;
const OPEN: u8 = 17;
const APPEND: u8 = 18;
const OFFSET: u8 = 19;
const FINISH: u8 = 20;
const COPY: u8 = 21;
const COPY_PART: u8 = 22;
const DELETE_OBJECTS: u8 = 23;
const LIST_VERSIONS: u8 = 24;
const CREATE_PRINCIPAL: u8 = 25;
const PUT_POLICY: u8 = 26;
const DELETE_PRINCIPAL: u8 = 27;
const LIST_PRINCIPALS: u8 = 28;
const GET_PRINCIPAL: u8 = 29;
const CREATE_KEY: u8 = 30;
const REVOKE_KEY: u8 = 31;

/// The most principals a page of ListPrincipals lists: IAM's ListUsers, `MaxItems` "Maximum value
/// of 1000" (docs/research/91 §2).
pub const MAX_PAGE_PRINCIPALS: usize = 1000;

/// A principal's bytes at most in a page of principals, its policy left out: its name, its unique
/// ID, its creation, the policy's flag, and its keys' count and each key's id and creation.
pub const PRINCIPAL_LISTED_BYTES: usize = (4 + mantle_s3::account::MAX_NAME)
    + (4 + mantle_s3::account::KEY_ID_CHARS)
    + 8
    + 1
    + 4
    + mantle_s3::account::MAX_KEYS * ((4 + mantle_s3::account::KEY_ID_CHARS) + 8);

/// A page of principals' bytes beside its principals: the count, and the principal after which
/// the next page starts behind its flag.
pub const PRINCIPALS_HEAD_BYTES: usize = 4 + 1 + 4 + mantle_s3::account::MAX_NAME;

/// A principal's identity policy as a reply carries it at most, in octets: its compact form's
/// 2,048 characters, each at most four octets in UTF-8 (RFC 3629 §3).
pub const MAX_POLICY_BYTES: usize = 4 * mantle_s3::policy::IDENTITY_CHARS;

/// The longest version id or ETag a reply carries: a multipart object's ETag, bare of quotes, an
/// MD5's 32 hex digits, `-` and its count of parts, at most `MAX_PARTS`'s five digits
/// (`mantle_s3::checksum::multipart_etag`); a version's id is 13 characters (mantle-meta's
/// `key::version_id`).
pub const MAX_TAG: usize = "d41d8cd98f00b204e9800998ecf8427e-10000".len();

/// The most buckets a page of ListBuckets lists: S3's `max-buckets`, "1–10000" (05 §10.4).
pub const MAX_BUCKETS: usize = 10_000;

/// The longest checksum value: SHA-512's 64 bytes (05 §3.1).
pub const MAX_CHECKSUM: usize = Algorithm::Sha512.width();

/// The most parts a page of ListParts lists, and the most uploads a page of ListMultipartUploads
/// does: S3's 1,000 for each (05 §4.1, §4.7).
pub const MAX_PAGE_PARTS: usize = 1000;

/// The most parts an upload has and a completion lists: S3's 10,000 (05 §4.1).
const MAX_PARTS: usize = mantle_s3::body::MAX_PARTS as usize;

/// The most objects a DeleteObjects names and its reply's results: "a list of up to 1,000 keys"
/// (research/89 §5.1).
pub const MAX_OBJECTS: usize = mantle_s3::body::MAX_OBJECTS;

/// The longest ETag a delete of DeleteObjects is conditioned on: mantle's longest, quoted and
/// weak (`W/""`). A longer one names no version mantle holds, and is carried as the empty tag,
/// which names none either, an ETag never being empty (research/89 §5.1).
pub const MAX_CONDITION_TAG: usize = MAX_TAG + "W/\"\"".len();

/// The bytes of a version's metadata at most, its names and values: within S3's 8 KB of request
/// headers, which carry them (research/05 §10.2; `mantle_s3::metadata`).
pub const MAX_METADATA: usize = MAX_HEADERS;

/// A version's metadata in a message at most: its count, and each pair's two lengths beside the
/// bytes, each pair at least one byte of name.
const METADATA_BYTES: usize = 4 + MAX_METADATA + 2 * 4 * MAX_METADATA;

/// The bytes of an object's request head: the kind, the fields' length, the identity, the time
/// its client will still wait, a put's length and its trailer's algorithm.
pub const HEAD_BYTES: usize = 1 + 4 + 16 + 8 + 8 + 1;
/// Where a request's head keeps the time its client will still wait: past the kind, the fields'
/// length and the identity.
const REMAINING_AT: usize = 1 + 4 + 16;
/// The bytes of a reply's head: the status, a `SlowDown`'s cause, the size and the fields'
/// length.
pub const REPLY_HEAD_BYTES: usize = 1 + 1 + 8 + 4;
/// The longer of the two, the longest head a client of objects sends or receives.
pub const MAX_HEAD: usize = if HEAD_BYTES > REPLY_HEAD_BYTES {
    HEAD_BYTES
} else {
    REPLY_HEAD_BYTES
};

/// An object's largest message, head and body: a put of S3's largest single upload behind the
/// longest fields a request opens its body with and a trailer's checksum, or a get's reply of S3's
/// largest single GET behind the longest fields a reply opens its body with (05 §4.1). An object
/// put in runs or parts passes a single upload's 5 GiB, and one GET returns it whole to 5 TB. The
/// transfer class's bound at the least, at a node and at its clients.
pub const MAX_MESSAGE: u64 = {
    let request = mantle_s3::body::MAX_UPLOAD + (HEAD_BYTES + MAX_FIELDS + MAX_CHECKSUM) as u64;
    let reply = mantle_s3::body::MAX_GET + (REPLY_HEAD_BYTES + MAX_REPLY_FIELDS) as u64;
    if request > reply { request } else { reply }
};

/// A request's fields at most: the longest of a put's or a part's (its names, an upload's ID and a
/// part's number, its headers within S3's 8 KB, a checksum and an MD5), a listing's (its bucket,
/// prefix, delimiter and start) and a completion's (its names, the upload's ID, its headers and
/// S3's 10,000 parts, each its number and ETag).
pub const MAX_FIELDS: usize = {
    // Each text behind its length, a u32.
    let names = (4 + MAX_BUCKET) + (4 + MAX_KEY);
    // Five optional texts within S3's headers.
    let headers = 5 * (1 + 4) + MAX_HEADERS;
    let version = 1 + 4 + MAX_VERSION_ID;
    let put = names
        + (4 + MAX_TAG)
        + 2
        + headers
        + (1 + 1 + 1 + MAX_CHECKSUM)
        + (1 + 16)
        + METADATA_BYTES;
    let list = (4 + MAX_BUCKET) + 2 * (4 + MAX_KEY) + (1 + 1 + 4 + MAX_KEY) + 4 + 1;
    let complete = names + (4 + MAX_TAG) + headers + 4 + MAX_PARTS * (2 + 4 + MAX_TAG);
    // A copy names its source beside its own, two headers and its metadata.
    let copy = 2 * names + version + 2 * headers + 1 + METADATA_BYTES + (1 + 1);
    let deletes = (4 + MAX_BUCKET)
        + 4
        + MAX_OBJECTS * ((4 + MAX_KEY) + version + (1 + 4 + MAX_CONDITION_TAG))
        + 1;
    let principal = (4 + mantle_s3::account::MAX_NAME) + (4 + mantle_s3::policy::DOCUMENT_BYTES);
    let mut longest = if put > list { put } else { list };
    if complete > longest {
        longest = complete;
    }
    if copy > longest {
        longest = copy;
    }
    if deletes > longest {
        longest = deletes;
    }
    if principal > longest {
        longest = principal;
    }
    longest
};

/// A key's bytes at most in a page of keys: its key, ETag, size, Last-Modified, checksum
/// algorithm and owner.
pub const LISTED_BYTES: usize =
    (4 + MAX_KEY) + (4 + MAX_TAG) + 8 + 8 + (1 + 1 + 2) + (1 + 4 + MAX_OWNER);

/// A reply's fields at most: the version's facts and the longest listing, a page of S3's 1,000
/// keys, 1,000 parts or 1,000 uploads at their longest, or of 10,000 buckets.
pub const MAX_REPLY_FIELDS: usize = {
    let facts = 2 * (4 + MAX_TAG)
        + 8
        + (1 + 1 + 2 + 1 + MAX_CHECKSUM)
        + 1
        + (1 + 8 + 8)
        + 1
        + (4 + MAX_TAG);
    let keys = PAGE_BYTES + MAX_KEYS * LISTED_BYTES;
    let buckets = 4 + MAX_BUCKETS * (4 + MAX_BUCKET + 8) + (1 + 4 + MAX_BUCKET);
    let parts = PARTS_HEAD_BYTES + MAX_PAGE_PARTS * PART_BYTES;
    let uploads = UPLOADS_HEAD_BYTES + MAX_PAGE_PARTS * UPLOAD_BYTES;
    let versions = VERSIONS_HEAD_BYTES + MAX_KEYS * VERSION_BYTES;
    let deleted = 4 + MAX_OBJECTS * DELETED_BYTES;
    let principals = PRINCIPALS_HEAD_BYTES + MAX_PAGE_PRINCIPALS * PRINCIPAL_LISTED_BYTES;
    let principal = PRINCIPALS_HEAD_BYTES + PRINCIPAL_LISTED_BYTES + 4 + MAX_POLICY_BYTES;
    let mut longest = if keys > buckets { keys } else { buckets };
    if parts > longest {
        longest = parts;
    }
    if uploads > longest {
        longest = uploads;
    }
    if versions > longest {
        longest = versions;
    }
    if deleted > longest {
        longest = deleted;
    }
    if principals > longest {
        longest = principals;
    }
    if principal > longest {
        longest = principal;
    }
    if ISSUED_BYTES > longest {
        longest = ISSUED_BYTES;
    }
    // The progress, the metadata and a copy's source version follow the listing.
    let progress = 1 + 8 + 2 + 8 + 1 + (1 + 1 + MAX_CHECKSUM);
    facts + 1 + longest + progress + METADATA_BYTES + (4 + MAX_TAG)
};

/// A key made as a reply lists it: its id, its secret, and the key it replaces behind its flag.
const ISSUED_BYTES: usize = (4 + mantle_s3::account::KEY_ID_CHARS)
    + (4 + mantle_s3::account::SECRET_CHARS)
    + (1 + 4 + mantle_s3::account::KEY_ID_CHARS);

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

/// A version's metadata: its content headers and user-defined metadata, each a lowercase name and
/// its value, sorted by name (`mantle_s3::metadata`).
pub type Metadata = Vec<(String, String)>;

/// CopyObject (research/89 §3): `source`'s version copied to `bucket`'s `key` by the node, no byte
/// through the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub bucket: String,
    pub key: String,
    pub id: u128,
    pub source: Source,
    /// `x-amz-copy-source-if-*`, judged against the source's version (research/89 §3.3), in the
    /// precondition slots; no range.
    pub conditions: Headers,
    /// The destination's `If-Match` and `If-None-Match`, judged as its version commits.
    pub headers: Headers,
    /// Where the copy's metadata comes from: the source's, or `metadata`.
    pub directive: Directive,
    /// `REPLACE`'s metadata; empty under `COPY`.
    pub metadata: Metadata,
    /// `x-amz-checksum-algorithm`: the full-object checksum the copy keeps; none for the source's
    /// algorithm, or CRC-64/NVME where the source keeps none (research/89 §3.4).
    pub checksum: Option<Algorithm>,
}

/// UploadPartCopy (research/89 §4): part `number` of `upload` copied by the node from `source`'s
/// version, or the range `conditions.range` names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyPart {
    pub bucket: String,
    pub key: String,
    pub upload: String,
    pub number: u16,
    pub id: u128,
    pub source: Source,
    /// `x-amz-copy-source-if-*` in the precondition slots and `x-amz-copy-source-range` in the
    /// range's.
    pub conditions: Headers,
}

/// An object DeleteObjects names (research/89 §5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub key: String,
    /// The version to remove; none for the key's simple delete.
    pub version: Option<String>,
    /// The ETag its delete is conditioned on, as `If-Match` conditions a DeleteObject's.
    pub etag: Option<String>,
}

/// ListObjectVersions' request (research/89 §2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListVersions {
    pub bucket: String,
    pub prefix: String,
    /// No delimiter when empty.
    pub delimiter: String,
    pub key_marker: Option<String>,
    pub id_marker: Option<String>,
    pub max: u32,
}

/// What an object's request asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// CreateBucket, owned by the asker.
    Create { bucket: String, id: u128 },
    /// PutObject: `length` bytes follow the fields, checked against `checksum` and `md5` before
    /// anything commits; with `trailing`, its checksum's value follows them, so a client that
    /// streams its body from a source it reads once states it at the end, as aws-chunked's
    /// trailer does (05 §3.2).
    Put {
        bucket: String,
        key: String,
        id: u128,
        length: u64,
        headers: Headers,
        checksum: Option<Checksum>,
        md5: Option<[u8; 16]>,
        trailing: Option<Algorithm>,
        /// The version's content headers and user-defined metadata (research/89 §1).
        metadata: Metadata,
    },
    /// GetObject: the current version's bytes, or the named version's, or the range its `Range`
    /// names.
    Get {
        bucket: String,
        key: String,
        headers: Headers,
        /// `versionId`, as sent: `null` or one of mantle's (research/89 §2.1).
        version: Option<String>,
    },
    /// HeadObject: the current version's facts, or the named version's.
    Head {
        bucket: String,
        key: String,
        headers: Headers,
        version: Option<String>,
    },
    /// DeleteObject: the key's object removed, or a delete marker made where versioning is on; or
    /// the named version removed (research/89 §2.2).
    Delete {
        bucket: String,
        key: String,
        id: u128,
        headers: Headers,
        version: Option<String>,
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
    /// CreateMultipartUpload, with the checksum algorithm its parts carry and whether its values
    /// combine as one full-object value or a composite (05 §3.3).
    CreateUpload {
        bucket: String,
        key: String,
        id: u128,
        checksum: Option<(Algorithm, bool)>,
        /// The completed version's metadata (research/89 §1).
        metadata: Metadata,
    },
    /// UploadPart: part `number` of `upload`, `length` bytes following the fields, checked as a
    /// put's are, a trailer's checksum after them as a put's.
    UploadPart {
        bucket: String,
        key: String,
        upload: String,
        number: u16,
        id: u128,
        length: u64,
        checksum: Option<Checksum>,
        md5: Option<[u8; 16]>,
        trailing: Option<Algorithm>,
    },
    /// CompleteMultipartUpload: the parts it lists, each its number and the ETag its upload
    /// answered, bare of quotes, in ascending order (05 §4.4).
    Complete {
        bucket: String,
        key: String,
        upload: String,
        id: u128,
        headers: Headers,
        parts: Vec<(u16, String)>,
    },
    /// AbortMultipartUpload.
    Abort {
        bucket: String,
        key: String,
        upload: String,
        id: u128,
    },
    /// ListParts: the upload's parts numbered after `after`, at most `max` (05 §4.6).
    ListParts {
        bucket: String,
        key: String,
        upload: String,
        after: u16,
        max: u32,
    },
    /// ListMultipartUploads (05 §4.7).
    ListUploads {
        bucket: String,
        prefix: String,
        /// No delimiter when empty.
        delimiter: String,
        key_marker: Option<String>,
        id_marker: Option<String>,
        max: u32,
    },
    /// A native upload (gateway.md §2.1): a multipart upload whose runs the node cuts, of
    /// `length` bytes where the client knows them, held at its end to a full-object checksum in
    /// `checksum`'s algorithm, a CRC, which combines from the runs'; `buffer`, where the
    /// client's source cannot be read again, the most of it the client holds, which no run
    /// passes. Its reply carries the upload's ID and its progress, the length of its runs.
    Open {
        bucket: String,
        key: String,
        id: u128,
        length: Option<u64>,
        buffer: Option<u64>,
        checksum: Algorithm,
        /// The object's metadata (research/89 §1).
        metadata: Metadata,
    },
    /// A native upload's next run: `length` bytes from `offset`, the upload's durable offset,
    /// `last` where they end the object; its checksum, in the upload's algorithm, in a trailer
    /// after them. Its reply is the run's progress frame: the durable offset after it, its
    /// number and its checksum. One at another offset is refused `Offset`, the progress naming
    /// the upload's.
    Append {
        bucket: String,
        key: String,
        upload: String,
        id: u128,
        offset: u64,
        length: u64,
        last: bool,
        trailing: Option<Algorithm>,
    },
    /// A native upload's progress: its durable offset, its runs and their length, which a client
    /// resuming appends from (gateway.md §2.1).
    Offset {
        bucket: String,
        key: String,
        upload: String,
    },
    /// A native upload's end: its object committed, under the preconditions `headers` name, if
    /// its runs are `length` bytes whose full-object checksum is `checksum`; otherwise
    /// `BadDigest`, and nothing is committed.
    Finish {
        bucket: String,
        key: String,
        upload: String,
        id: u128,
        headers: Headers,
        length: u64,
        checksum: Checksum,
    },
    /// CopyObject (research/89 §3).
    Copy(Box<Copy>),
    /// UploadPartCopy (research/89 §4).
    CopyPart(Box<CopyPart>),
    /// DeleteObjects (research/89 §5): each object's delete, its result in its place.
    DeleteObjects {
        bucket: String,
        id: u128,
        objects: Vec<Target>,
        /// Report only the deletes that failed.
        quiet: bool,
    },
    /// ListObjectVersions (research/89 §2.3).
    ListVersions(ListVersions),
    /// A principal of the cell's account made with its identity policy, as IAM's CreateUser and
    /// PutUserPolicy make a user (docs/design/principals.md): the policy as its operator wrote it,
    /// which the node checks as IAM checks one (`mantle_s3::policy::identity`).
    CreatePrincipal { name: String, policy: String },
    /// A principal's identity policy replaced (PutUserPolicy): every node judges by it from the
    /// reply on.
    PutPolicy { name: String, policy: String },
    /// A principal deleted (DeleteUser), refused while it holds a key.
    DeletePrincipal { name: String },
    /// A page of the account's principals after `after`, at most `max`, each with its keys
    /// (ListUsers, ListAccessKeys).
    ListPrincipals { after: Option<String>, max: u32 },
    /// A principal, its policy and its keys (GetUser, GetUserPolicy).
    GetPrincipal { name: String },
    /// An access key made for `principal` (CreateAccessKey); with `rotating`, only while the
    /// principal holds one key, which the new one replaces once that one is revoked. The reply
    /// carries the key's id and its secret, the one time the secret leaves the cell.
    CreateKey { principal: String, rotating: bool },
    /// An access key revoked (DeleteAccessKey): every node refuses it from the reply on.
    RevokeKey { id: String },
}

/// A versioning state as a message carries it: 2 enabled, 3 suspended.
fn versioning_code(v: Versioning) -> u8 {
    match v {
        Versioning::Enabled => 2,
        Versioning::Suspended => 3,
    }
}

fn versioning_of(code: u8) -> Option<Versioning> {
    match code {
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

fn put_optional_u64(w: &mut Writer, value: Option<u64>) {
    put_flag(w, value.is_some());
    if let Some(value) = value {
        w.u64(value);
    }
}

fn take_optional_u64(r: &mut Reader<'_>) -> Option<Option<u64>> {
    if take_flag(r)? {
        Some(Some(r.u64()?))
    } else {
        Some(None)
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

/// A body's checksum and `Content-MD5`, each optional, as a put or a part states them.
fn put_digests(
    w: &mut Writer,
    checksum: Option<&Checksum>,
    md5: Option<&[u8; 16]>,
) -> Result<(), ObjectError> {
    put_flag(w, checksum.is_some());
    if let Some(c) = checksum {
        if c.bytes.len() != c.algorithm.width() {
            return Err(ObjectError::TooLong(c.bytes.len()));
        }
        w.u8(c.algorithm.code());
        w.u8(u8::try_from(c.bytes.len()).map_err(|_| ObjectError::TooLong(0))?);
        w.bytes(&c.bytes);
    }
    put_flag(w, md5.is_some());
    if let Some(md5) = md5 {
        w.bytes(md5);
    }
    Ok(())
}

type Digests = (Option<Checksum>, Option<[u8; 16]>);

fn take_digests(r: &mut Reader<'_>) -> Option<Digests> {
    let checksum = if take_flag(r)? {
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
    let md5 = if take_flag(r)? {
        Some(<[u8; 16]>::try_from(r.take(16)?).ok()?)
    } else {
        None
    };
    Some((checksum, md5))
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

/// A version's metadata: its count, then each name and value, together within
/// [`MAX_METADATA`].
fn put_metadata(w: &mut Writer, pairs: &[(String, String)]) -> Result<(), ObjectError> {
    let total = pairs.iter().fold(0usize, |sum, (n, v)| {
        sum.saturating_add(n.len()).saturating_add(v.len())
    });
    if total > MAX_METADATA {
        return Err(ObjectError::TooLong(total));
    }
    put_count(w, pairs.len())?;
    for (name, value) in pairs {
        put_text(w, name, MAX_METADATA)?;
        put_text(w, value, MAX_METADATA)?;
    }
    Ok(())
}

fn take_metadata(r: &mut Reader<'_>) -> Option<Metadata> {
    // A pair takes 8 bytes at least, its two lengths.
    let count = take_count(r, MAX_METADATA, 8)?;
    let mut pairs = Vec::with_capacity(count);
    let mut total = 0usize;
    for _ in 0..count {
        let (name, value) = (take_text(r, MAX_METADATA)?, take_text(r, MAX_METADATA)?);
        total = total.checked_add(name.len())?.checked_add(value.len())?;
        if total > MAX_METADATA {
            return None;
        }
        pairs.push((name, value));
    }
    Some(pairs)
}

/// A copy's source: its bucket and key, and its version, optional.
fn put_source(w: &mut Writer, source: &Source) -> Result<(), ObjectError> {
    put_text(w, &source.bucket, MAX_BUCKET)?;
    put_text(w, &source.key, MAX_KEY)?;
    put_optional_text(w, source.version.as_deref(), MAX_VERSION_ID)
}

fn take_source(r: &mut Reader<'_>) -> Option<Source> {
    Some(Source {
        bucket: take_text(r, MAX_BUCKET)?,
        key: take_text(r, MAX_KEY)?,
        version: take_optional_text(r, MAX_VERSION_ID)?,
    })
}

/// A copy's metadata directive as a message carries it: 0 COPY, 1 REPLACE.
fn directive_code(d: Directive) -> u8 {
    match d {
        Directive::Copy => 0,
        Directive::Replace => 1,
    }
}

fn directive_of(code: u8) -> Option<Directive> {
    match code {
        0 => Some(Directive::Copy),
        1 => Some(Directive::Replace),
        _ => None,
    }
}

/// A request's head: what it is and how long its fields are, before they are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    kind: u8,
    fields: u32,
    id: u128,
    remaining: u64,
    length: u64,
    trailing: Option<Algorithm>,
}

impl Head {
    /// The bytes of the fields that open the body.
    pub fn fields(&self) -> u64 {
        u64::from(self.fields)
    }

    /// The time the request's client will still wait from its sending; none where it gave none.
    pub fn remaining(&self) -> Option<Duration> {
        if self.remaining == 0 {
            None
        } else {
            Some(Duration::from_nanos(self.remaining))
        }
    }

    /// The bytes of object data the request writes, known from its head before its fields are
    /// read: a put's, a part's or a native upload's run's body; none for a request that writes
    /// none.
    pub fn written(&self) -> Option<u64> {
        matches!(self.kind, PUT | UPLOAD_PART | APPEND).then(|| self.length)
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
                    let (checksum, md5) = take_digests(&mut r)?;
                    Op::Put {
                        bucket,
                        key,
                        id: self.id,
                        length: self.length,
                        headers,
                        checksum,
                        md5,
                        trailing: self.trailing,
                        metadata: take_metadata(&mut r)?,
                    }
                }
                GET => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Get {
                        bucket,
                        key,
                        headers: take_headers(&mut r)?,
                        version: take_optional_text(&mut r, MAX_VERSION_ID)?,
                    }
                }
                HEAD => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Head {
                        bucket,
                        key,
                        headers: take_headers(&mut r)?,
                        version: take_optional_text(&mut r, MAX_VERSION_ID)?,
                    }
                }
                DELETE => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Delete {
                        bucket,
                        key,
                        id: self.id,
                        headers: take_headers(&mut r)?,
                        version: take_optional_text(&mut r, MAX_VERSION_ID)?,
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
                    versioning: versioning_of(r.u8()?)?,
                },
                GET_VERSIONING => Op::GetVersioning {
                    bucket: take_text(&mut r, MAX_BUCKET)?,
                },
                CREATE_UPLOAD => {
                    let (bucket, key) = object(&mut r)?;
                    let checksum = if take_flag(&mut r)? {
                        Some((Algorithm::from_code(r.u8()?)?, take_flag(&mut r)?))
                    } else {
                        None
                    };
                    Op::CreateUpload {
                        bucket,
                        key,
                        id: self.id,
                        checksum,
                        metadata: take_metadata(&mut r)?,
                    }
                }
                UPLOAD_PART => {
                    let (bucket, key) = object(&mut r)?;
                    let upload = take_text(&mut r, MAX_TAG)?;
                    let number = r.u16()?;
                    let (checksum, md5) = take_digests(&mut r)?;
                    Op::UploadPart {
                        bucket,
                        key,
                        upload,
                        number,
                        id: self.id,
                        length: self.length,
                        checksum,
                        md5,
                        trailing: self.trailing,
                    }
                }
                COMPLETE => {
                    let (bucket, key) = object(&mut r)?;
                    let upload = take_text(&mut r, MAX_TAG)?;
                    let headers = take_headers(&mut r)?;
                    // A part takes 6 bytes at least.
                    let count = take_count(&mut r, MAX_PARTS, 6)?;
                    let mut parts = Vec::with_capacity(count);
                    for _ in 0..count {
                        parts.push((r.u16()?, take_text(&mut r, MAX_TAG)?));
                    }
                    Op::Complete {
                        bucket,
                        key,
                        upload,
                        id: self.id,
                        headers,
                        parts,
                    }
                }
                ABORT => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Abort {
                        bucket,
                        key,
                        upload: take_text(&mut r, MAX_TAG)?,
                        id: self.id,
                    }
                }
                LIST_PARTS => {
                    let (bucket, key) = object(&mut r)?;
                    Op::ListParts {
                        bucket,
                        key,
                        upload: take_text(&mut r, MAX_TAG)?,
                        after: r.u16()?,
                        max: r.u32()?,
                    }
                }
                LIST_UPLOADS => Op::ListUploads {
                    bucket: take_text(&mut r, MAX_BUCKET)?,
                    prefix: take_text(&mut r, MAX_KEY)?,
                    delimiter: take_text(&mut r, MAX_KEY)?,
                    key_marker: take_optional_text(&mut r, MAX_KEY)?,
                    id_marker: take_optional_text(&mut r, MAX_TAG)?,
                    max: r.u32()?,
                },
                OPEN => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Open {
                        bucket,
                        key,
                        id: self.id,
                        length: take_optional_u64(&mut r)?,
                        buffer: take_optional_u64(&mut r)?,
                        checksum: Algorithm::from_code(r.u8()?)?,
                        metadata: take_metadata(&mut r)?,
                    }
                }
                APPEND => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Append {
                        bucket,
                        key,
                        upload: take_text(&mut r, MAX_TAG)?,
                        id: self.id,
                        offset: r.u64()?,
                        length: self.length,
                        last: take_flag(&mut r)?,
                        trailing: self.trailing,
                    }
                }
                OFFSET => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Offset {
                        bucket,
                        key,
                        upload: take_text(&mut r, MAX_TAG)?,
                    }
                }
                FINISH => {
                    let (bucket, key) = object(&mut r)?;
                    let upload = take_text(&mut r, MAX_TAG)?;
                    let headers = take_headers(&mut r)?;
                    let length = r.u64()?;
                    let algorithm = Algorithm::from_code(r.u8()?)?;
                    let width = usize::from(r.u8()?);
                    if width != algorithm.width() {
                        return None;
                    }
                    Op::Finish {
                        bucket,
                        key,
                        upload,
                        id: self.id,
                        headers,
                        length,
                        checksum: Checksum {
                            algorithm,
                            bytes: r.take(width)?.to_vec(),
                        },
                    }
                }
                COPY => {
                    let (bucket, key) = object(&mut r)?;
                    Op::Copy(Box::new(Copy {
                        bucket,
                        key,
                        id: self.id,
                        source: take_source(&mut r)?,
                        conditions: take_headers(&mut r)?,
                        headers: take_headers(&mut r)?,
                        directive: directive_of(r.u8()?)?,
                        metadata: take_metadata(&mut r)?,
                        checksum: if take_flag(&mut r)? {
                            Some(Algorithm::from_code(r.u8()?)?)
                        } else {
                            None
                        },
                    }))
                }
                COPY_PART => {
                    let (bucket, key) = object(&mut r)?;
                    Op::CopyPart(Box::new(CopyPart {
                        bucket,
                        key,
                        upload: take_text(&mut r, MAX_TAG)?,
                        number: r.u16()?,
                        id: self.id,
                        source: take_source(&mut r)?,
                        conditions: take_headers(&mut r)?,
                    }))
                }
                DELETE_OBJECTS => {
                    let bucket = take_text(&mut r, MAX_BUCKET)?;
                    // An object takes 6 bytes at least: its key's length and two flags.
                    let count = take_count(&mut r, MAX_OBJECTS, 6)?;
                    let mut objects = Vec::with_capacity(count);
                    for _ in 0..count {
                        objects.push(Target {
                            key: take_text(&mut r, MAX_KEY)?,
                            version: take_optional_text(&mut r, MAX_VERSION_ID)?,
                            etag: take_optional_text(&mut r, MAX_CONDITION_TAG)?,
                        });
                    }
                    Op::DeleteObjects {
                        bucket,
                        id: self.id,
                        objects,
                        quiet: take_flag(&mut r)?,
                    }
                }
                LIST_VERSIONS => Op::ListVersions(take_list_versions(&mut r)?),
                CREATE_PRINCIPAL => Op::CreatePrincipal {
                    name: take_text(&mut r, mantle_s3::account::MAX_NAME)?,
                    policy: take_text(&mut r, mantle_s3::policy::DOCUMENT_BYTES)?,
                },
                PUT_POLICY => Op::PutPolicy {
                    name: take_text(&mut r, mantle_s3::account::MAX_NAME)?,
                    policy: take_text(&mut r, mantle_s3::policy::DOCUMENT_BYTES)?,
                },
                DELETE_PRINCIPAL => Op::DeletePrincipal {
                    name: take_text(&mut r, mantle_s3::account::MAX_NAME)?,
                },
                LIST_PRINCIPALS => Op::ListPrincipals {
                    after: take_optional_text(&mut r, mantle_s3::account::MAX_NAME)?,
                    max: r.u32()?,
                },
                GET_PRINCIPAL => Op::GetPrincipal {
                    name: take_text(&mut r, mantle_s3::account::MAX_NAME)?,
                },
                CREATE_KEY => Op::CreateKey {
                    principal: take_text(&mut r, mantle_s3::account::MAX_NAME)?,
                    rotating: take_flag(&mut r)?,
                },
                REVOKE_KEY => Op::RevokeKey {
                    id: take_text(&mut r, mantle_s3::account::KEY_ID_CHARS)?,
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
                remaining: r.u64()?,
                length: r.u64()?,
                trailing: match r.u8()? {
                    0 => None,
                    code => Some(Algorithm::from_code(code.checked_sub(1)?)?),
                },
            })
        })()
        .filter(|_| r.remaining() == 0);
        let head_of = || ObjectError::Head(head.len());
        let decoded = decoded.ok_or_else(head_of)?;
        let puts = matches!(decoded.kind, PUT | UPLOAD_PART | APPEND);
        let within = usize::try_from(decoded.fields).is_ok_and(|f| f <= MAX_FIELDS)
            && decoded.kind <= REVOKE_KEY
            && (puts || (decoded.length == 0 && decoded.trailing.is_none()));
        let trailer = decoded
            .trailing
            .map_or(0, |a| u64::try_from(a.width()).unwrap_or(u64::MAX));
        let stated = decoded
            .fields()
            .checked_add(decoded.length)
            .and_then(|b| b.checked_add(trailer));
        if !within || stated != Some(body.unwrap_or(0)) {
            return Err(head_of());
        }
        Ok(decoded)
    }
}

/// Sets in a request's `head`, as [`Op::encode`] made it, the time its client will still wait,
/// `remaining` from its sending, none where it gives none (research/77 D2): set at each attempt,
/// as the time left changes between them.
pub fn within(head: &mut [u8], remaining: Option<Duration>) -> Result<(), ObjectError> {
    let len = head.len();
    let at = head
        .get_mut(REMAINING_AT..REMAINING_AT + 8)
        .filter(|_| len == HEAD_BYTES)
        .ok_or(ObjectError::Head(len))?;
    // A time too long to count in nanoseconds is one no wait reaches; a zero one is none.
    let ns = remaining.map_or(0, |r| {
        u64::try_from(r.as_nanos()).unwrap_or(u64::MAX).max(1)
    });
    at.copy_from_slice(&ns.to_le_bytes());
    Ok(())
}

impl Op {
    /// The bytes of object data the operation writes: a put's, a part's or a native upload's
    /// run's body; none for one that writes none.
    pub fn written(&self) -> Option<u64> {
        match self {
            Self::Put { length, .. }
            | Self::UploadPart { length, .. }
            | Self::Append { length, .. } => Some(*length),
            _ => None,
        }
    }

    /// The request's head and the fields that open its body: the fields counted first, then
    /// written into one buffer at their length (`mantle_codec::encode`'s rule), so it never grows.
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), ObjectError> {
        let mut count = Writer::counting();
        self.put_fields(&mut count)?;
        if count.len() > MAX_FIELDS {
            return Err(ObjectError::TooLong(count.len()));
        }
        let mut f = Writer::with_capacity(count.len());
        let (kind, id, length, trailing) = self.put_fields(&mut f)?;
        let fields = f.into_vec();
        let mut w = Writer::with_capacity(HEAD_BYTES);
        w.u8(kind);
        w.u32(u32::try_from(fields.len()).map_err(|_| ObjectError::TooLong(fields.len()))?);
        w.u128(id);
        w.u64(0);
        w.u64(length);
        w.u8(trailing.map_or(0, |a| a.code().saturating_add(1)));
        Ok((w.into_vec(), fields))
    }

    /// Writes the request's fields into `f`, and gives its kind, its identity, its body's length
    /// and its trailer's algorithm.
    fn put_fields(
        &self,
        f: &mut Writer,
    ) -> Result<(u8, u128, u64, Option<Algorithm>), ObjectError> {
        let object = |f: &mut Writer, bucket: &str, key: &str| -> Result<(), ObjectError> {
            put_text(f, bucket, MAX_BUCKET)?;
            put_text(f, key, MAX_KEY)
        };
        Ok(match self {
            Self::Create { bucket, id } => {
                put_text(f, bucket, MAX_BUCKET)?;
                (CREATE, *id, 0, None)
            }
            Self::Put {
                bucket,
                key,
                id,
                length,
                headers,
                checksum,
                md5,
                trailing,
                metadata,
            } => {
                object(f, bucket, key)?;
                put_headers(f, headers)?;
                put_digests(f, checksum.as_ref(), md5.as_ref())?;
                put_metadata(f, metadata)?;
                (PUT, *id, *length, *trailing)
            }
            Self::Get {
                bucket,
                key,
                headers,
                version,
            } => {
                object(f, bucket, key)?;
                put_headers(f, headers)?;
                put_optional_text(f, version.as_deref(), MAX_VERSION_ID)?;
                (GET, 0, 0, None)
            }
            Self::Head {
                bucket,
                key,
                headers,
                version,
            } => {
                object(f, bucket, key)?;
                put_headers(f, headers)?;
                put_optional_text(f, version.as_deref(), MAX_VERSION_ID)?;
                (HEAD, 0, 0, None)
            }
            Self::Delete {
                bucket,
                key,
                id,
                headers,
                version,
            } => {
                object(f, bucket, key)?;
                put_headers(f, headers)?;
                put_optional_text(f, version.as_deref(), MAX_VERSION_ID)?;
                (DELETE, *id, 0, None)
            }
            Self::List(list) => {
                put_list(f, list)?;
                (LIST, 0, 0, None)
            }
            Self::HeadBucket { bucket } => {
                put_text(f, bucket, MAX_BUCKET)?;
                (HEAD_BUCKET, 0, 0, None)
            }
            Self::ListBuckets { prefix, after, max } => {
                put_text(f, prefix, MAX_BUCKET)?;
                put_optional_text(f, after.as_deref(), MAX_BUCKET)?;
                if !usize::try_from(*max).is_ok_and(|m| (1..=MAX_BUCKETS).contains(&m)) {
                    return Err(ObjectError::TooLong(usize::try_from(*max).unwrap_or(0)));
                }
                f.u32(*max);
                (LIST_BUCKETS, 0, 0, None)
            }
            Self::DeleteBucket { bucket, id } => {
                put_text(f, bucket, MAX_BUCKET)?;
                (DELETE_BUCKET, *id, 0, None)
            }
            Self::PutVersioning {
                bucket,
                id,
                versioning,
            } => {
                put_text(f, bucket, MAX_BUCKET)?;
                f.u8(versioning_code(*versioning));
                (PUT_VERSIONING, *id, 0, None)
            }
            Self::GetVersioning { bucket } => {
                put_text(f, bucket, MAX_BUCKET)?;
                (GET_VERSIONING, 0, 0, None)
            }
            Self::CreateUpload {
                bucket,
                key,
                id,
                checksum,
                metadata,
            } => {
                object(f, bucket, key)?;
                put_flag(f, checksum.is_some());
                if let Some((algorithm, full_object)) = checksum {
                    f.u8(algorithm.code());
                    put_flag(f, *full_object);
                }
                put_metadata(f, metadata)?;
                (CREATE_UPLOAD, *id, 0, None)
            }
            Self::UploadPart {
                bucket,
                key,
                upload,
                number,
                id,
                length,
                checksum,
                md5,
                trailing,
            } => {
                object(f, bucket, key)?;
                put_text(f, upload, MAX_TAG)?;
                f.u16(*number);
                put_digests(f, checksum.as_ref(), md5.as_ref())?;
                (UPLOAD_PART, *id, *length, *trailing)
            }
            Self::Complete {
                bucket,
                key,
                upload,
                id,
                headers,
                parts,
            } => {
                object(f, bucket, key)?;
                put_text(f, upload, MAX_TAG)?;
                put_headers(f, headers)?;
                if parts.len() > MAX_PARTS {
                    return Err(ObjectError::TooLong(parts.len()));
                }
                put_count(f, parts.len())?;
                for (number, etag) in parts {
                    f.u16(*number);
                    put_text(f, etag, MAX_TAG)?;
                }
                (COMPLETE, *id, 0, None)
            }
            Self::Abort {
                bucket,
                key,
                upload,
                id,
            } => {
                object(f, bucket, key)?;
                put_text(f, upload, MAX_TAG)?;
                (ABORT, *id, 0, None)
            }
            Self::ListParts {
                bucket,
                key,
                upload,
                after,
                max,
            } => {
                object(f, bucket, key)?;
                put_text(f, upload, MAX_TAG)?;
                f.u16(*after);
                f.u32(*max);
                (LIST_PARTS, 0, 0, None)
            }
            Self::ListUploads {
                bucket,
                prefix,
                delimiter,
                key_marker,
                id_marker,
                max,
            } => {
                put_text(f, bucket, MAX_BUCKET)?;
                put_text(f, prefix, MAX_KEY)?;
                put_text(f, delimiter, MAX_KEY)?;
                put_optional_text(f, key_marker.as_deref(), MAX_KEY)?;
                put_optional_text(f, id_marker.as_deref(), MAX_TAG)?;
                f.u32(*max);
                (LIST_UPLOADS, 0, 0, None)
            }
            Self::Open {
                bucket,
                key,
                id,
                length,
                buffer,
                checksum,
                metadata,
            } => {
                object(f, bucket, key)?;
                put_optional_u64(f, *length);
                put_optional_u64(f, *buffer);
                f.u8(checksum.code());
                put_metadata(f, metadata)?;
                (OPEN, *id, 0, None)
            }
            Self::Append {
                bucket,
                key,
                upload,
                id,
                offset,
                length,
                last,
                trailing,
            } => {
                object(f, bucket, key)?;
                put_text(f, upload, MAX_TAG)?;
                f.u64(*offset);
                put_flag(f, *last);
                (APPEND, *id, *length, *trailing)
            }
            Self::Offset {
                bucket,
                key,
                upload,
            } => {
                object(f, bucket, key)?;
                put_text(f, upload, MAX_TAG)?;
                (OFFSET, 0, 0, None)
            }
            Self::Finish {
                bucket,
                key,
                upload,
                id,
                headers,
                length,
                checksum,
            } => {
                object(f, bucket, key)?;
                put_text(f, upload, MAX_TAG)?;
                put_headers(f, headers)?;
                f.u64(*length);
                if checksum.bytes.len() != checksum.algorithm.width() {
                    return Err(ObjectError::TooLong(checksum.bytes.len()));
                }
                f.u8(checksum.algorithm.code());
                f.u8(u8::try_from(checksum.bytes.len()).map_err(|_| ObjectError::TooLong(0))?);
                f.bytes(&checksum.bytes);
                (FINISH, *id, 0, None)
            }
            Self::Copy(copy) => {
                object(f, &copy.bucket, &copy.key)?;
                put_source(f, &copy.source)?;
                put_headers(f, &copy.conditions)?;
                put_headers(f, &copy.headers)?;
                f.u8(directive_code(copy.directive));
                put_metadata(f, &copy.metadata)?;
                put_flag(f, copy.checksum.is_some());
                if let Some(algorithm) = copy.checksum {
                    f.u8(algorithm.code());
                }
                (COPY, copy.id, 0, None)
            }
            Self::CopyPart(part) => {
                object(f, &part.bucket, &part.key)?;
                put_text(f, &part.upload, MAX_TAG)?;
                f.u16(part.number);
                put_source(f, &part.source)?;
                put_headers(f, &part.conditions)?;
                (COPY_PART, part.id, 0, None)
            }
            Self::DeleteObjects {
                bucket,
                id,
                objects,
                quiet,
            } => {
                put_text(f, bucket, MAX_BUCKET)?;
                if objects.len() > MAX_OBJECTS {
                    return Err(ObjectError::TooLong(objects.len()));
                }
                put_count(f, objects.len())?;
                for target in objects {
                    put_text(f, &target.key, MAX_KEY)?;
                    put_optional_text(f, target.version.as_deref(), MAX_VERSION_ID)?;
                    put_optional_text(f, target.etag.as_deref(), MAX_CONDITION_TAG)?;
                }
                put_flag(f, *quiet);
                (DELETE_OBJECTS, *id, 0, None)
            }
            Self::ListVersions(list) => {
                put_list_versions(f, list)?;
                (LIST_VERSIONS, 0, 0, None)
            }
            Self::CreatePrincipal { name, policy } => {
                put_text(f, name, mantle_s3::account::MAX_NAME)?;
                put_text(f, policy, mantle_s3::policy::DOCUMENT_BYTES)?;
                (CREATE_PRINCIPAL, 0, 0, None)
            }
            Self::PutPolicy { name, policy } => {
                put_text(f, name, mantle_s3::account::MAX_NAME)?;
                put_text(f, policy, mantle_s3::policy::DOCUMENT_BYTES)?;
                (PUT_POLICY, 0, 0, None)
            }
            Self::DeletePrincipal { name } => {
                put_text(f, name, mantle_s3::account::MAX_NAME)?;
                (DELETE_PRINCIPAL, 0, 0, None)
            }
            Self::ListPrincipals { after, max } => {
                put_optional_text(f, after.as_deref(), mantle_s3::account::MAX_NAME)?;
                f.u32(*max);
                (LIST_PRINCIPALS, 0, 0, None)
            }
            Self::GetPrincipal { name } => {
                put_text(f, name, mantle_s3::account::MAX_NAME)?;
                (GET_PRINCIPAL, 0, 0, None)
            }
            Self::CreateKey {
                principal,
                rotating,
            } => {
                put_text(f, principal, mantle_s3::account::MAX_NAME)?;
                put_flag(f, *rotating);
                (CREATE_KEY, 0, 0, None)
            }
            Self::RevokeKey { id } => {
                put_text(f, id, mantle_s3::account::KEY_ID_CHARS)?;
                (REVOKE_KEY, 0, 0, None)
            }
        })
    }

    /// The bytes of the request's body: its fields, and a put's or a part's bytes and trailer.
    pub fn body(&self, fields: &[u8]) -> u64 {
        let fields = u64::try_from(fields.len()).unwrap_or(u64::MAX);
        match self {
            Self::Put {
                length, trailing, ..
            }
            | Self::UploadPart {
                length, trailing, ..
            }
            | Self::Append {
                length, trailing, ..
            } => fields.saturating_add(*length).saturating_add(
                trailing.map_or(0, |a| u64::try_from(a.width()).unwrap_or(u64::MAX)),
            ),
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
    /// S3's `503 SlowDown`: a cause that passes, which clients ask again; nothing was done. A
    /// node's reply says which beside it ([`Reply::slowed`]).
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
    /// S3's `404 NoSuchUpload`: no such multipart upload, or it was aborted (05 §4.4).
    NoSuchUpload,
    /// S3's `400 InvalidPart`: a listed part was never uploaded or its ETag differs (05 §4.4).
    InvalidPart,
    /// S3's `400 InvalidPartOrder`: parts not listed in ascending order, or none (05 §4.4).
    InvalidPartOrder,
    /// S3's `400 EntityTooSmall`: a part other than the last under 5 MiB (05 §4.4).
    EntityTooSmall,
    /// S3's `400 InvalidArgument`: a part number outside 1 to 10,000, for which AWS names no
    /// code (05 §4.3).
    InvalidArgument,
    /// S3's `400 InvalidRequest`: a checksum type its algorithm does not have, a full-object
    /// hash or a composite CRC-64/NVME (05 §3.3).
    InvalidRequest,
    /// A native upload's run that does not begin at the upload's durable offset, or follows its
    /// last run: the reply's progress names the offset, as the resumable-upload draft's `409
    /// Conflict` names it (research/30 §3.5).
    Offset,
    /// HTTP's `507 Insufficient Storage` (RFC 4918 §11.5): every volume a block's chunks were
    /// offered refused them as full, so the cell has no room for the object, a condition that
    /// outlasts a client's retries; nothing of the object commits (node.md §4.4, research/75).
    InsufficientStorage,
    /// S3's `404 NoSuchVersion`: the version named does not exist (research/89 §2.1).
    NoSuchVersion,
    /// S3's `405 MethodNotAllowed`: the version a get or head names is a delete marker; the
    /// reply states the marker's facts, its Last-Modified among them (research/89 §2.1).
    MethodNotAllowed,
    /// S3's `400 MetadataTooLarge`: past S3's 2 KB of user-defined or system metadata
    /// (research/89 §1.4).
    MetadataTooLarge,
    /// S3's `400 InvalidArgument`, "Invalid version id specified": a version ID that is neither
    /// `null` nor one mantle makes (research/89 §2.1).
    InvalidVersion,
    /// S3's `400 InvalidRequest`: a copy onto its own source's current version that changes
    /// nothing (research/89 §3.5).
    CopyToItself,
    /// S3's `400 InvalidRequest`: a CopyObject's source past one PUT's 5 GiB (research/89 §3.1).
    CopySourceTooLarge,
    /// S3's `400 InvalidRequest`: a byte range of a source of at most 5 MiB, "You can copy a range
    /// only if the source object is greater than 5 MB" (research/89 §4.1).
    CopyRangeSource,
    /// S3's `400 InvalidRequest`: a copy whose named source version is a delete marker
    /// (research/89 §4.1).
    CopyMarker,
    /// S3's `400 InvalidArgument`: an `x-amz-copy-source-range` not of the form
    /// `bytes=first-last` (research/89 §4.1).
    InvalidCopyRange,
    /// IAM's `409 EntityAlreadyExists`: a principal of that name, its case aside (research/91 §2).
    EntityAlreadyExists,
    /// IAM's `404 NoSuchEntity`: no principal of that name, or no key of that id.
    NoSuchEntity,
    /// IAM's `409 LimitExceeded`: the account's 5,000 principals, or a principal's two keys.
    LimitExceeded,
    /// IAM's `409 DeleteConflict`: the principal holds keys, which are revoked first.
    DeleteConflict,
    /// IAM's `400 MalformedPolicyDocument`: a policy IAM refuses (`mantle_s3::policy::identity`).
    MalformedPolicy,
    /// A rotation asked of a principal that holds no key: there is none to rotate.
    NothingToRotate,
}

const STATUSES: [Status; 42] = [
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
    Status::NoSuchUpload,
    Status::InvalidPart,
    Status::InvalidPartOrder,
    Status::EntityTooSmall,
    Status::InvalidArgument,
    Status::InvalidRequest,
    Status::Offset,
    Status::InsufficientStorage,
    Status::NoSuchVersion,
    Status::MethodNotAllowed,
    Status::MetadataTooLarge,
    Status::InvalidVersion,
    Status::CopyToItself,
    Status::CopySourceTooLarge,
    Status::CopyRangeSource,
    Status::CopyMarker,
    Status::InvalidCopyRange,
    Status::EntityAlreadyExists,
    Status::NoSuchEntity,
    Status::LimitExceeded,
    Status::DeleteConflict,
    Status::MalformedPolicy,
    Status::NothingToRotate,
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
            Self::NoSuchUpload => 19,
            Self::InvalidPart => 20,
            Self::InvalidPartOrder => 21,
            Self::EntityTooSmall => 22,
            Self::InvalidArgument => 23,
            Self::InvalidRequest => 24,
            Self::Offset => 25,
            Self::InsufficientStorage => 26,
            Self::NoSuchVersion => 27,
            Self::MethodNotAllowed => 28,
            Self::MetadataTooLarge => 29,
            Self::InvalidVersion => 30,
            Self::CopyToItself => 31,
            Self::CopySourceTooLarge => 32,
            Self::CopyRangeSource => 33,
            Self::CopyMarker => 34,
            Self::InvalidCopyRange => 35,
            Self::EntityAlreadyExists => 36,
            Self::NoSuchEntity => 37,
            Self::LimitExceeded => 38,
            Self::DeleteConflict => 39,
            Self::MalformedPolicy => 40,
            Self::NothingToRotate => 41,
        }
    }

    fn of(code: u8) -> Option<Self> {
        STATUSES.into_iter().find(|s| s.code() == code)
    }
}

/// Why a node answered a request `SlowDown`, each a cause that passes, which clients ask again
/// (node.md §4.5): carried beside the status in a reply's head on the native door, and in the
/// `<Message>` of the S3 door's `503 SlowDown`, so a refusal on either door names the path that
/// answered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slowed {
    /// Past the connections the S3 listener serves at once.
    Connections,
    /// The S3 listener's door held as many requests as it keeps, the node was gone, or its
    /// channels for the request could not be made.
    Door,
    /// The node held as many key lookups as its door holds requests.
    Lookups,
    /// The node was stopping, or had no room for an operation its door handed it.
    Stopping,
    /// The access key's read of the root range waited its range's budget without a leader that
    /// could serve it (node.md §5.4).
    KeyLeaderless,
    /// A read or command of the operation waited its range's budget without a leader that could
    /// serve it (node.md §5.4).
    Leaderless,
    /// Fewer of a block's chunks could be read than rebuild it.
    Unreadable,
    /// No volume the placement offered took one of a block's chunks.
    Unplaced,
    /// The operation's work was held past a stock client's patience (research/77 D2).
    Patience,
    /// A native request the host had no room for, while as many waited for room as it parks.
    Queued,
    /// A native request that waited for the host's room past its client's deadline or patience.
    Waited,
    /// A native request whose operation, learned from its fields, holds more bytes than its head
    /// charged and the host has room for.
    Crowded,
}

impl Slowed {
    /// Every cause, in the order of their codes.
    pub const ALL: [Self; 12] = [
        Self::Connections,
        Self::Door,
        Self::Lookups,
        Self::Stopping,
        Self::KeyLeaderless,
        Self::Leaderless,
        Self::Unreadable,
        Self::Unplaced,
        Self::Patience,
        Self::Queued,
        Self::Waited,
        Self::Crowded,
    ];

    /// The cause's code beside a reply's status: its place in [`Slowed::ALL`] from one, zero
    /// standing for none.
    fn code(self) -> u8 {
        match self {
            Self::Connections => 1,
            Self::Door => 2,
            Self::Lookups => 3,
            Self::Stopping => 4,
            Self::KeyLeaderless => 5,
            Self::Leaderless => 6,
            Self::Unreadable => 7,
            Self::Unplaced => 8,
            Self::Patience => 9,
            Self::Queued => 10,
            Self::Waited => 11,
            Self::Crowded => 12,
        }
    }

    fn of(code: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.code() == code)
    }

    /// The cause's place in [`Slowed::ALL`], where a count of each keeps it.
    pub fn index(self) -> usize {
        usize::from(self.code()).saturating_sub(1)
    }

    /// The cause's name in a node's lines (`slowed NAME COUNT`).
    pub fn name(self) -> &'static str {
        match self {
            Self::Connections => "connections",
            Self::Door => "door",
            Self::Lookups => "lookups",
            Self::Stopping => "stopping",
            Self::KeyLeaderless => "key-leaderless",
            Self::Leaderless => "leaderless",
            Self::Unreadable => "unreadable",
            Self::Unplaced => "unplaced",
            Self::Patience => "patience",
            Self::Queued => "queued",
            Self::Waited => "waited",
            Self::Crowded => "crowded",
        }
    }

    /// What the cause says, a sentence: after `SlowDown: ` in a native client's failure, and
    /// after S3's own words in the S3 door's `<Message>`.
    pub fn words(self) -> &'static str {
        match self {
            Self::Connections => "Past the connections the node serves at once.",
            Self::Door => "The node's door was full, or the node gone.",
            Self::Lookups => "The node held as many key lookups as it keeps.",
            Self::Stopping => "The node was stopping.",
            Self::KeyLeaderless => {
                "The access key's read of the root range found no leader that serves it within its budget."
            }
            Self::Leaderless => {
                "A range's read or command found no leader that serves it within its budget."
            }
            Self::Unreadable => "Fewer of a block's chunks could be read than rebuild it.",
            Self::Unplaced => "No volume offered took a block's chunk.",
            Self::Patience => "The operation's work was held past a client's patience.",
            Self::Queued => {
                "The node had no room for the request, and as many waited for room as it keeps."
            }
            Self::Waited => {
                "The request waited for the node's room past its client's deadline or patience."
            }
            Self::Crowded => {
                "The operation's bytes, learned from its fields, passed the room the node had."
            }
        }
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

/// A part a page of ListParts lists (05 §4.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartListed {
    pub number: u16,
    /// Bare of quotes.
    pub etag: String,
    pub size: u64,
    /// Last-Modified, whole seconds.
    pub modified: u64,
    /// The part's checksum value, of the algorithm its upload names.
    pub checksum: Option<Vec<u8>>,
}

/// A page of ListParts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Parts {
    pub parts: Vec<PartListed>,
    pub truncated: bool,
    /// The part number the next page starts after, on a truncated page: `NextPartNumberMarker`.
    pub next: Option<u16>,
}

/// An upload a page of ListMultipartUploads lists (05 §4.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadListed {
    pub key: String,
    pub id: String,
    /// When it was initiated, whole seconds.
    pub initiated: u64,
    pub owner: String,
}

/// A page of ListMultipartUploads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Uploads {
    pub uploads: Vec<UploadListed>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// Where the next page starts, on a truncated page: a key marker and an upload ID marker.
    pub next: Option<(String, String)>,
}

/// A version or delete marker a page of ListObjectVersions lists (research/89 §2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionListed {
    pub key: String,
    /// Its version ID: `null`, or one of mantle's.
    pub id: String,
    /// The key's current version.
    pub latest: bool,
    /// A delete marker, which has no ETag, size or checksum.
    pub marker: bool,
    /// Bare of quotes.
    pub etag: String,
    pub size: u64,
    /// Last-Modified, whole seconds.
    pub modified: u64,
    pub checksum: Option<(Algorithm, u16)>,
    pub owner: String,
}

/// A page of ListObjectVersions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Versions {
    pub versions: Vec<VersionListed>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// Where the next page starts, on a truncated page: a key marker and a version ID marker.
    pub next: Option<(String, String)>,
}

/// What became of one object a DeleteObjects named (research/89 §5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deleted {
    pub key: String,
    /// The version the request named, as sent.
    pub version: Option<String>,
    /// `Done`, or the refusal its delete earned.
    pub status: Status,
    /// The version ID of the delete marker its delete made, or of the one it removed.
    pub marker: Option<String>,
}

/// A principal a page of principals lists (docs/design/principals.md): its name, its unique ID, its
/// creation, its identity policy where it was asked for (GetPrincipal), and each of its keys' ids
/// and creations, whole seconds. No secret is ever listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalListed {
    pub name: String,
    pub id: String,
    pub created: u64,
    pub policy: Option<String>,
    pub keys: Vec<(String, u64)>,
}

/// A page of principals.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principals {
    pub principals: Vec<PrincipalListed>,
    /// The principal after which the next page starts, when more follow.
    pub next: Option<String>,
}

/// What a reply lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Listing {
    Keys(Page),
    Buckets(Buckets),
    Parts(Parts),
    Uploads(Uploads),
    Versions(Versions),
    Deleted(Vec<Deleted>),
    Principals(Principals),
    /// A key made, CreateKey's: what its reply carries, the one time its secret leaves the cell.
    Issued(Box<Issued>),
}

/// An access key made (CreateKey): its id, its secret, the one time it leaves the cell, and the key
/// a rotation replaces, which stays until it is revoked. Its secret is wiped when dropped and never
/// shown by `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct Issued {
    pub id: String,
    pub secret: mantle_s3::account::Secret,
    pub replacing: Option<String>,
}

impl std::fmt::Debug for Issued {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issued")
            .field("id", &self.id)
            .field("replacing", &self.replacing)
            .finish_non_exhaustive()
    }
}

/// A version's checksum as a reply carries it (05 §3.3): its algorithm's code, as
/// `mantle_s3::checksum::Algorithm::code` numbers them; the parts a composite value combines, zero
/// for a full-object value; and its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub algorithm: u8,
    pub parts: u16,
    pub value: Vec<u8>,
}

/// A reply: its head's status, cause and size, and the fields its body opens with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: Status,
    /// Why it is `SlowDown`, beside that status alone; none beside any other.
    pub slowed: Option<Slowed>,
    /// The object's size, a get's or a head's.
    pub size: u64,
    /// The version's id: a put's, a delete's marker's, or the one a get or head read; empty
    /// where none applies.
    pub version: String,
    /// Bare of quotes.
    pub etag: String,
    /// Last-Modified, whole seconds.
    pub modified: u64,
    pub checksum: Option<Stored>,
    /// The version a delete made or removed, or the key's current one a get or head found, is a
    /// delete marker.
    pub marker: bool,
    /// The bytes a get's or head's range served, first and last: S3's `206 Partial Content`.
    pub range: Option<(u64, u64)>,
    /// The bucket's versioning, GetBucketVersioning's: none for a bucket never versioned.
    pub versioning: Option<Versioning>,
    /// The upload's ID, CreateMultipartUpload's and Open's; empty where none applies.
    pub upload: String,
    pub listing: Option<Listing>,
    /// A native upload's progress: Open's, Offset's, a run's frame, and an append refused
    /// `Offset`. Boxed: few replies carry one, and every reply is moved whole.
    pub progress: Option<Box<Progress>>,
    /// The version's content headers and user-defined metadata, a get's or a head's
    /// (research/89 §1).
    pub metadata: Metadata,
    /// The version a copy read of its source; empty where none applies (research/89 §3.6).
    pub source_version: String,
}

/// A native upload's progress (gateway.md §2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    /// The durable offset: the bytes of its committed runs, which the next run begins at.
    pub offset: u64,
    /// Its committed runs, the last one's number.
    pub runs: u16,
    /// The length of every run but the last, which the node cut.
    pub run: u64,
    /// Its last run committed: it takes no more, and is finished.
    pub ended: bool,
    /// A run's frame: the run's checksum, as the node kept it.
    pub checksum: Option<Vec<u8>>,
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
pub fn listed_bytes(key: &str, etag: &str, checksum: bool, owner: Option<&str>) -> usize {
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
pub fn prefix_bytes(prefix: &str) -> usize {
    4usize.saturating_add(prefix.len())
}

/// The bytes a page of keys takes beside its keys and common prefixes, at most: whether it is
/// truncated, where the next page starts and the two counts.
pub const PAGE_BYTES: usize = 1 + (1 + 1 + 4 + MAX_KEY) + 4 + 4;

/// Whole seconds of `ns` nanoseconds since the Unix epoch: the one time an object's
/// Last-Modified header, listing and conditions take (s3-protocol.md §4).
pub fn seconds(ns: u64) -> u64 {
    ns / 1_000_000_000
}

pub fn put_list(w: &mut Writer, list: &List) -> Result<(), ObjectError> {
    put_text(w, &list.bucket, MAX_BUCKET)?;
    put_text(w, &list.prefix, MAX_KEY)?;
    put_text(w, &list.delimiter, MAX_KEY)?;
    put_resume(w, list.start.as_ref())?;
    w.u32(list.max_keys);
    put_flag(w, list.fetch_owner);
    Ok(())
}

pub fn put_list_versions(w: &mut Writer, list: &ListVersions) -> Result<(), ObjectError> {
    put_text(w, &list.bucket, MAX_BUCKET)?;
    put_text(w, &list.prefix, MAX_KEY)?;
    put_text(w, &list.delimiter, MAX_KEY)?;
    put_optional_text(w, list.key_marker.as_deref(), MAX_KEY)?;
    put_optional_text(w, list.id_marker.as_deref(), MAX_VERSION_ID)?;
    w.u32(list.max);
    Ok(())
}

pub fn take_list_versions(r: &mut Reader<'_>) -> Option<ListVersions> {
    Some(ListVersions {
        bucket: take_text(r, MAX_BUCKET)?,
        prefix: take_text(r, MAX_KEY)?,
        delimiter: take_text(r, MAX_KEY)?,
        key_marker: take_optional_text(r, MAX_KEY)?,
        id_marker: take_optional_text(r, MAX_VERSION_ID)?,
        max: r.u32()?,
    })
}

pub fn take_list(r: &mut Reader<'_>) -> Option<List> {
    Some(List {
        bucket: take_text(r, MAX_BUCKET)?,
        prefix: take_text(r, MAX_KEY)?,
        delimiter: take_text(r, MAX_KEY)?,
        start: take_resume(r)?,
        max_keys: r.u32()?,
        fetch_owner: take_flag(r)?,
    })
}

pub fn put_page(w: &mut Writer, page: &Page) -> Result<(), ObjectError> {
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

pub fn take_page(r: &mut Reader<'_>) -> Option<Page> {
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

/// A part in a page of parts at its longest: its number, its ETag behind its length, its size and
/// Last-Modified, and its checksum's flag, length and value.
pub const PART_BYTES: usize = 2 + (4 + MAX_TAG) + 8 + 8 + (1 + 1 + MAX_CHECKSUM);

/// A page of parts' bytes beside its parts: whether it is truncated, the next marker behind its
/// flag, and the count.
pub const PARTS_HEAD_BYTES: usize = 1 + (1 + 2) + 4;

pub fn put_parts(w: &mut Writer, page: &Parts) -> Result<(), ObjectError> {
    if page.parts.len() > MAX_PAGE_PARTS {
        return Err(ObjectError::TooLong(page.parts.len()));
    }
    put_flag(w, page.truncated);
    put_flag(w, page.next.is_some());
    if let Some(next) = page.next {
        w.u16(next);
    }
    put_count(w, page.parts.len())?;
    for part in &page.parts {
        w.u16(part.number);
        put_text(w, &part.etag, MAX_TAG)?;
        w.u64(part.size);
        w.u64(part.modified);
        put_flag(w, part.checksum.is_some());
        if let Some(c) = &part.checksum {
            if c.len() > MAX_CHECKSUM {
                return Err(ObjectError::TooLong(c.len()));
            }
            w.u8(u8::try_from(c.len()).map_err(|_| ObjectError::TooLong(c.len()))?);
            w.bytes(c);
        }
    }
    Ok(())
}

pub fn take_parts(r: &mut Reader<'_>) -> Option<Parts> {
    let truncated = take_flag(r)?;
    let next = if take_flag(r)? { Some(r.u16()?) } else { None };
    // A part takes 27 bytes at least.
    let count = take_count(r, MAX_PAGE_PARTS, 27)?;
    let mut parts = Vec::with_capacity(count);
    for _ in 0..count {
        parts.push(PartListed {
            number: r.u16()?,
            etag: take_text(r, MAX_TAG)?,
            size: r.u64()?,
            modified: r.u64()?,
            checksum: if take_flag(r)? {
                let len = usize::from(r.u8()?);
                if len > MAX_CHECKSUM {
                    return None;
                }
                Some(r.take(len)?.to_vec())
            } else {
                None
            },
        });
    }
    Some(Parts {
        parts,
        truncated,
        next,
    })
}

/// The bytes an upload takes in a page of uploads, as [`put_uploads`] writes it.
pub fn upload_bytes(key: &str, id: &str, owner: &str) -> usize {
    4usize
        .saturating_add(key.len())
        .saturating_add(4)
        .saturating_add(id.len())
        .saturating_add(8)
        .saturating_add(4)
        .saturating_add(owner.len())
}

/// An upload in a page of uploads at its longest: its key, ID, initiation and owner.
pub const UPLOAD_BYTES: usize = (4 + MAX_KEY) + (4 + MAX_TAG) + 8 + (4 + MAX_OWNER);

/// A page of uploads' bytes beside its uploads and common prefixes, at most: whether it is
/// truncated, where the next page starts (its flag, a key and an ID), and the two counts.
pub const UPLOADS_HEAD_BYTES: usize = 1 + (1 + 4 + MAX_KEY + 4 + MAX_TAG) + 4 + 4;

pub fn put_uploads(w: &mut Writer, page: &Uploads) -> Result<(), ObjectError> {
    if page
        .uploads
        .len()
        .saturating_add(page.common_prefixes.len())
        > MAX_PAGE_PARTS
    {
        return Err(ObjectError::TooLong(page.uploads.len()));
    }
    put_flag(w, page.truncated);
    put_flag(w, page.next.is_some());
    if let Some((key, id)) = &page.next {
        put_text(w, key, MAX_KEY)?;
        put_text(w, id, MAX_TAG)?;
    }
    put_count(w, page.uploads.len())?;
    for upload in &page.uploads {
        put_text(w, &upload.key, MAX_KEY)?;
        put_text(w, &upload.id, MAX_TAG)?;
        w.u64(upload.initiated);
        put_text(w, &upload.owner, MAX_OWNER)?;
    }
    put_count(w, page.common_prefixes.len())?;
    for prefix in &page.common_prefixes {
        put_text(w, prefix, MAX_KEY)?;
    }
    Ok(())
}

pub fn take_uploads(r: &mut Reader<'_>) -> Option<Uploads> {
    let truncated = take_flag(r)?;
    let next = if take_flag(r)? {
        Some((take_text(r, MAX_KEY)?, take_text(r, MAX_TAG)?))
    } else {
        None
    };
    // An upload takes 20 bytes at least, a common prefix 4.
    let count = take_count(r, MAX_PAGE_PARTS, 20)?;
    let mut uploads = Vec::with_capacity(count);
    for _ in 0..count {
        uploads.push(UploadListed {
            key: take_text(r, MAX_KEY)?,
            id: take_text(r, MAX_TAG)?,
            initiated: r.u64()?,
            owner: take_text(r, MAX_OWNER)?,
        });
    }
    let prefixes = take_count(r, MAX_PAGE_PARTS.checked_sub(count)?, 4)?;
    let mut common_prefixes = Vec::with_capacity(prefixes);
    for _ in 0..prefixes {
        common_prefixes.push(take_text(r, MAX_KEY)?);
    }
    Some(Uploads {
        uploads,
        common_prefixes,
        truncated,
        next,
    })
}

pub fn put_buckets(w: &mut Writer, page: &Buckets) -> Result<(), ObjectError> {
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

pub fn take_buckets(r: &mut Reader<'_>) -> Option<Buckets> {
    // A bucket takes 12 bytes at least.
    let count = take_count(r, MAX_BUCKETS, 12)?;
    let mut buckets = Vec::with_capacity(count);
    for _ in 0..count {
        buckets.push((take_text(r, MAX_BUCKET)?, r.u64()?));
    }
    let next = take_optional_text(r, MAX_BUCKET)?;
    Some(Buckets { buckets, next })
}

/// A version in a page of versions at its longest: its key and version ID behind their lengths,
/// its flags, its ETag behind its length, its size and Last-Modified, its checksum algorithm's
/// flag, code and parts, and its owner behind its length.
pub const VERSION_BYTES: usize =
    (4 + MAX_KEY) + (4 + MAX_TAG) + 1 + (4 + MAX_TAG) + 8 + 8 + (1 + 1 + 2) + (4 + MAX_OWNER);

/// A page of versions' bytes beside its versions and common prefixes, at most: whether it is
/// truncated, where the next page starts (its flag, a key and a version ID), and the two counts.
pub const VERSIONS_HEAD_BYTES: usize = 1 + (1 + 4 + MAX_KEY + 4 + MAX_VERSION_ID) + 4 + 4;

/// The bytes a version takes in a page of versions, as [`put_versions`] writes it.
pub fn version_bytes(key: &str, id: &str, etag: &str, checksum: bool, owner: &str) -> usize {
    let checksum = if checksum { 1 + 1 + 2 } else { 1 };
    4usize
        .saturating_add(key.len())
        .saturating_add(4)
        .saturating_add(id.len())
        .saturating_add(1)
        .saturating_add(4)
        .saturating_add(etag.len())
        .saturating_add(8 + 8)
        .saturating_add(checksum)
        .saturating_add(4)
        .saturating_add(owner.len())
}

const LATEST: u8 = 1;
const MARKER: u8 = 2;

pub fn put_versions(w: &mut Writer, page: &Versions) -> Result<(), ObjectError> {
    if page
        .versions
        .len()
        .saturating_add(page.common_prefixes.len())
        > MAX_KEYS
    {
        return Err(ObjectError::TooLong(page.versions.len()));
    }
    put_flag(w, page.truncated);
    put_flag(w, page.next.is_some());
    if let Some((key, id)) = &page.next {
        put_text(w, key, MAX_KEY)?;
        put_text(w, id, MAX_VERSION_ID)?;
    }
    put_count(w, page.versions.len())?;
    for v in &page.versions {
        put_text(w, &v.key, MAX_KEY)?;
        put_text(w, &v.id, MAX_TAG)?;
        let latest = if v.latest { LATEST } else { 0 };
        let marker = if v.marker { MARKER } else { 0 };
        w.u8(latest | marker);
        put_text(w, &v.etag, MAX_TAG)?;
        w.u64(v.size);
        w.u64(v.modified);
        put_flag(w, v.checksum.is_some());
        if let Some((algorithm, parts)) = v.checksum {
            w.u8(algorithm.code());
            w.u16(parts);
        }
        put_text(w, &v.owner, MAX_OWNER)?;
    }
    put_count(w, page.common_prefixes.len())?;
    for prefix in &page.common_prefixes {
        put_text(w, prefix, MAX_KEY)?;
    }
    Ok(())
}

pub fn take_versions(r: &mut Reader<'_>) -> Option<Versions> {
    let truncated = take_flag(r)?;
    let next = if take_flag(r)? {
        Some((take_text(r, MAX_KEY)?, take_text(r, MAX_VERSION_ID)?))
    } else {
        None
    };
    // A version takes 38 bytes at least, a common prefix 4.
    let count = take_count(r, MAX_KEYS, 38)?;
    let mut versions = Vec::with_capacity(count);
    for _ in 0..count {
        let key = take_text(r, MAX_KEY)?;
        let id = take_text(r, MAX_TAG)?;
        let flags = r.u8()?;
        if flags & !(LATEST | MARKER) != 0 {
            return None;
        }
        versions.push(VersionListed {
            key,
            id,
            latest: flags & LATEST != 0,
            marker: flags & MARKER != 0,
            etag: take_text(r, MAX_TAG)?,
            size: r.u64()?,
            modified: r.u64()?,
            checksum: if take_flag(r)? {
                Some((Algorithm::from_code(r.u8()?)?, r.u16()?))
            } else {
                None
            },
            owner: take_text(r, MAX_OWNER)?,
        });
    }
    let prefixes = take_count(r, MAX_KEYS.checked_sub(count)?, 4)?;
    let mut common_prefixes = Vec::with_capacity(prefixes);
    for _ in 0..prefixes {
        common_prefixes.push(take_text(r, MAX_KEY)?);
    }
    Some(Versions {
        versions,
        common_prefixes,
        truncated,
        next,
    })
}

/// A delete's result at its longest: its key, the version it named, its status and the delete
/// marker's version ID, each optional text behind its flag and length.
pub const DELETED_BYTES: usize = (4 + MAX_KEY) + (1 + 4 + MAX_VERSION_ID) + 1 + (1 + 4 + MAX_TAG);

pub fn put_deleted(w: &mut Writer, results: &[Deleted]) -> Result<(), ObjectError> {
    if results.len() > MAX_OBJECTS {
        return Err(ObjectError::TooLong(results.len()));
    }
    put_count(w, results.len())?;
    for d in results {
        put_text(w, &d.key, MAX_KEY)?;
        put_optional_text(w, d.version.as_deref(), MAX_VERSION_ID)?;
        w.u8(d.status.code());
        put_optional_text(w, d.marker.as_deref(), MAX_TAG)?;
    }
    Ok(())
}

pub fn take_deleted(r: &mut Reader<'_>) -> Option<Vec<Deleted>> {
    // A result takes 7 bytes at least.
    let count = take_count(r, MAX_OBJECTS, 7)?;
    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        results.push(Deleted {
            key: take_text(r, MAX_KEY)?,
            version: take_optional_text(r, MAX_VERSION_ID)?,
            status: Status::of(r.u8()?)?,
            marker: take_optional_text(r, MAX_TAG)?,
        });
    }
    Some(results)
}

/// A count within `most`, read as a u32, and no more than the rest holds at `least` bytes each.
pub fn put_principals(w: &mut Writer, page: &Principals) -> Result<(), ObjectError> {
    if page.principals.len() > MAX_PAGE_PRINCIPALS {
        return Err(ObjectError::TooLong(page.principals.len()));
    }
    put_count(w, page.principals.len())?;
    for listed in &page.principals {
        put_text(w, &listed.name, mantle_s3::account::MAX_NAME)?;
        put_text(w, &listed.id, mantle_s3::account::KEY_ID_CHARS)?;
        w.u64(listed.created);
        put_optional_text(w, listed.policy.as_deref(), MAX_POLICY_BYTES)?;
        if listed.keys.len() > mantle_s3::account::MAX_KEYS {
            return Err(ObjectError::TooLong(listed.keys.len()));
        }
        put_count(w, listed.keys.len())?;
        for (id, created) in &listed.keys {
            put_text(w, id, mantle_s3::account::KEY_ID_CHARS)?;
            w.u64(*created);
        }
    }
    put_optional_text(w, page.next.as_deref(), mantle_s3::account::MAX_NAME)
}

pub fn take_principals(r: &mut Reader<'_>) -> Option<Principals> {
    // A principal takes at least its name's and id's lengths, its creation, the policy's flag
    // and its keys' count.
    let count = take_count(r, MAX_PAGE_PRINCIPALS, 4 + 4 + 8 + 1 + 4)?;
    let mut principals = Vec::with_capacity(count);
    for _ in 0..count {
        let name = take_text(r, mantle_s3::account::MAX_NAME)?;
        let id = take_text(r, mantle_s3::account::KEY_ID_CHARS)?;
        let created = r.u64()?;
        let policy = take_optional_text(r, MAX_POLICY_BYTES)?;
        let held = take_count(r, mantle_s3::account::MAX_KEYS, 4 + 8)?;
        let mut keys = Vec::with_capacity(held);
        for _ in 0..held {
            keys.push((take_text(r, mantle_s3::account::KEY_ID_CHARS)?, r.u64()?));
        }
        principals.push(PrincipalListed {
            name,
            id,
            created,
            policy,
            keys,
        });
    }
    Some(Principals {
        principals,
        next: take_optional_text(r, mantle_s3::account::MAX_NAME)?,
    })
}

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
            slowed: None,
            size: 0,
            version: String::new(),
            etag: String::new(),
            modified: 0,
            checksum: None,
            marker: false,
            range: None,
            versioning: None,
            upload: String::new(),
            listing: None,
            progress: None,
            metadata: Vec::new(),
            source_version: String::new(),
        }
    }

    /// A reply of `SlowDown` for `cause`.
    pub fn slowed(cause: Slowed) -> Self {
        Self {
            slowed: Some(cause),
            ..Self::status(Status::SlowDown)
        }
    }

    /// The reply's head and the fields that open its body: the fields counted first, then
    /// written into one buffer at their length (`mantle_codec::encode`'s rule), so it never grows.
    /// A cause beside a status other than `SlowDown` is refused: the head says one decision.
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), ObjectError> {
        let cause = match (self.status, self.slowed) {
            (_, None) => 0,
            (Status::SlowDown, Some(cause)) => cause.code(),
            (status, Some(_)) => return Err(ObjectError::Cause(status)),
        };
        let fields = mantle_codec::encode(0, |f| self.put_fields(f))?.into_vec();
        let mut w = Writer::with_capacity(REPLY_HEAD_BYTES);
        w.u8(self.status.code());
        w.u8(cause);
        w.u64(self.size);
        w.u32(u32::try_from(fields.len()).map_err(|_| ObjectError::TooLong(fields.len()))?);
        Ok((w.into_vec(), fields))
    }

    /// Writes the reply's fields into `f`.
    fn put_fields(&self, f: &mut Writer) -> Result<(), ObjectError> {
        put_text(f, &self.version, MAX_TAG)?;
        put_text(f, &self.etag, MAX_TAG)?;
        f.u64(self.modified);
        put_flag(f, self.checksum.is_some());
        if let Some(c) = &self.checksum {
            if c.value.len() > MAX_CHECKSUM {
                return Err(ObjectError::TooLong(c.value.len()));
            }
            f.u8(c.algorithm);
            f.u16(c.parts);
            f.u8(u8::try_from(c.value.len()).map_err(|_| ObjectError::TooLong(0))?);
            f.bytes(&c.value);
        }
        put_flag(f, self.marker);
        put_flag(f, self.range.is_some());
        if let Some((first, last)) = self.range {
            f.u64(first);
            f.u64(last);
        }
        f.u8(self.versioning.map_or(0, versioning_code));
        put_text(f, &self.upload, MAX_TAG)?;
        match &self.listing {
            None => f.u8(0),
            Some(Listing::Keys(page)) => {
                f.u8(1);
                put_page(f, page)?;
            }
            Some(Listing::Buckets(page)) => {
                f.u8(2);
                put_buckets(f, page)?;
            }
            Some(Listing::Parts(page)) => {
                f.u8(3);
                put_parts(f, page)?;
            }
            Some(Listing::Uploads(page)) => {
                f.u8(4);
                put_uploads(f, page)?;
            }
            Some(Listing::Versions(page)) => {
                f.u8(5);
                put_versions(f, page)?;
            }
            Some(Listing::Deleted(results)) => {
                f.u8(6);
                put_deleted(f, results)?;
            }
            Some(Listing::Principals(page)) => {
                f.u8(7);
                put_principals(f, page)?;
            }
            Some(Listing::Issued(issued)) => {
                f.u8(8);
                put_text(f, &issued.id, mantle_s3::account::KEY_ID_CHARS)?;
                put_text(f, &issued.secret, mantle_s3::account::SECRET_CHARS)?;
                put_optional_text(
                    f,
                    issued.replacing.as_deref(),
                    mantle_s3::account::KEY_ID_CHARS,
                )?;
            }
        }
        put_flag(f, self.progress.is_some());
        if let Some(p) = &self.progress {
            f.u64(p.offset);
            f.u16(p.runs);
            f.u64(p.run);
            put_flag(f, p.ended);
            put_flag(f, p.checksum.is_some());
            if let Some(value) = &p.checksum {
                if value.len() > MAX_CHECKSUM {
                    return Err(ObjectError::TooLong(value.len()));
                }
                f.u8(u8::try_from(value.len()).map_err(|_| ObjectError::TooLong(0))?);
                f.bytes(value);
            }
        }
        put_metadata(f, &self.metadata)?;
        put_text(f, &self.source_version, MAX_TAG)?;
        Ok(())
    }
}

/// A reply's head: its status, a `SlowDown`'s cause and its size, and how long the fields that
/// open its body are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplyHead {
    pub status: Status,
    pub slowed: Option<Slowed>,
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
            let status = Status::of(r.u8()?)?;
            // A cause stands beside `SlowDown` alone, and is one the protocol names.
            let slowed = match (status, r.u8()?) {
                (_, 0) => None,
                (Status::SlowDown, code) => Some(Slowed::of(code)?),
                _ => return None,
            };
            Some(Self {
                status,
                slowed,
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
                Some(Stored {
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
            let upload = take_text(&mut r, MAX_TAG)?;
            let listing = match r.u8()? {
                0 => None,
                1 => Some(Listing::Keys(take_page(&mut r)?)),
                2 => Some(Listing::Buckets(take_buckets(&mut r)?)),
                3 => Some(Listing::Parts(take_parts(&mut r)?)),
                4 => Some(Listing::Uploads(take_uploads(&mut r)?)),
                5 => Some(Listing::Versions(take_versions(&mut r)?)),
                6 => Some(Listing::Deleted(take_deleted(&mut r)?)),
                7 => Some(Listing::Principals(take_principals(&mut r)?)),
                8 => Some(Listing::Issued(Box::new(Issued {
                    id: take_text(&mut r, mantle_s3::account::KEY_ID_CHARS)?,
                    secret: mantle_s3::account::Secret::new(take_text(
                        &mut r,
                        mantle_s3::account::SECRET_CHARS,
                    )?),
                    replacing: take_optional_text(&mut r, mantle_s3::account::KEY_ID_CHARS)?,
                }))),
                _ => return None,
            };
            let progress = if take_flag(&mut r)? {
                let offset = r.u64()?;
                let runs = r.u16()?;
                let run = r.u64()?;
                let ended = take_flag(&mut r)?;
                let checksum = if take_flag(&mut r)? {
                    let len = usize::from(r.u8()?);
                    if len > MAX_CHECKSUM {
                        return None;
                    }
                    Some(r.take(len)?.to_vec())
                } else {
                    None
                };
                Some(Box::new(Progress {
                    offset,
                    runs,
                    run,
                    ended,
                    checksum,
                }))
            } else {
                None
            };
            let metadata = take_metadata(&mut r)?;
            let source_version = take_text(&mut r, MAX_TAG)?;
            Some(Reply {
                status: self.status,
                slowed: self.slowed,
                size: self.size,
                version,
                etag,
                modified,
                checksum,
                marker,
                range,
                versioning,
                upload,
                listing,
                progress,
                metadata,
                source_version,
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
    #[error("a SlowDown's cause beside the status {0:?}")]
    Cause(Status),
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
                trailing: None,
                metadata: Vec::new(),
            },
            Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: 1,
                length: 0,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: Some(Algorithm::Crc64Nvme),
                metadata: Vec::new(),
            },
            Op::Get {
                bucket: "b".into(),
                key: "k".repeat(MAX_KEY),
                headers: headers(),
                version: None,
            },
            Op::Head {
                bucket: "b".repeat(MAX_BUCKET),
                key: "k".into(),
                headers: Headers::default(),
                version: None,
            },
            Op::Delete {
                bucket: "b".into(),
                key: "k".into(),
                id: 3,
                headers: headers(),
                version: None,
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
            Op::CreateUpload {
                bucket: "b".into(),
                key: "k".into(),
                id: 6,
                checksum: Some((Algorithm::Crc64Nvme, true)),
                metadata: Vec::new(),
            },
            Op::UploadPart {
                bucket: "b".into(),
                key: "k".into(),
                upload: "0123456789ABC".into(),
                number: 10_000,
                id: 7,
                length: 5 << 30,
                checksum: Some(Checksum {
                    algorithm: Algorithm::Crc32,
                    bytes: vec![1; 4],
                }),
                md5: Some([2; 16]),
                trailing: Some(Algorithm::Sha512),
            },
            Op::Complete {
                bucket: "b".into(),
                key: "k".repeat(MAX_KEY),
                upload: "0123456789ABC".into(),
                id: 8,
                headers: headers(),
                parts: (1..=10_000u16).map(|n| (n, "e".repeat(MAX_TAG))).collect(),
            },
            Op::Abort {
                bucket: "b".into(),
                key: "k".into(),
                upload: "0123456789ABC".into(),
                id: 9,
            },
            Op::ListParts {
                bucket: "b".into(),
                key: "k".into(),
                upload: "0123456789ABC".into(),
                after: 3,
                max: 1000,
            },
            Op::ListUploads {
                bucket: "b".into(),
                prefix: "p".into(),
                delimiter: "/".into(),
                key_marker: Some("p/k".into()),
                id_marker: None,
                max: 1000,
            },
            Op::Open {
                bucket: "b".into(),
                key: "k".into(),
                id: 9,
                length: Some(12 << 20),
                buffer: None,
                checksum: Algorithm::Crc64Nvme,
                metadata: Vec::new(),
            },
            Op::Open {
                bucket: "b".into(),
                key: "k".into(),
                id: 10,
                length: None,
                buffer: Some(8 << 20),
                checksum: Algorithm::Crc32c,
                metadata: Vec::new(),
            },
            Op::Append {
                bucket: "b".into(),
                key: "k".into(),
                upload: "0123456789ABC".into(),
                id: 9,
                offset: 5 << 20,
                length: 5 << 20,
                last: true,
                trailing: Some(Algorithm::Crc64Nvme),
            },
            Op::Offset {
                bucket: "b".into(),
                key: "k".into(),
                upload: "0123456789ABC".into(),
            },
            Op::Finish {
                bucket: "b".into(),
                key: "k".into(),
                upload: "0123456789ABC".into(),
                id: 9,
                headers: Headers {
                    if_none_match: Some("*".into()),
                    ..Headers::default()
                },
                length: 10 << 20,
                checksum: Checksum {
                    algorithm: Algorithm::Crc64Nvme,
                    bytes: vec![7; 8],
                },
            },
            // The operations research/89 adds, after every one before them, so those keep
            // their golden vectors' places.
            Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: 11,
                length: 3,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: None,
                metadata: metadata(),
            },
            Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: Some("null".into()),
            },
            Op::Head {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: Some("0123456789ABC".into()),
            },
            Op::Delete {
                bucket: "b".into(),
                key: "k".into(),
                id: 12,
                headers: Headers::default(),
                version: Some("v".repeat(MAX_VERSION_ID)),
            },
            Op::CreateUpload {
                bucket: "b".into(),
                key: "k".into(),
                id: 13,
                checksum: None,
                metadata: metadata(),
            },
            Op::Open {
                bucket: "b".into(),
                key: "k".into(),
                id: 14,
                length: Some(1),
                buffer: None,
                checksum: Algorithm::Crc64Nvme,
                metadata: metadata(),
            },
            Op::Copy(Box::new(Copy {
                bucket: "b".into(),
                key: "copy".into(),
                id: 15,
                source: Source {
                    bucket: "src".into(),
                    key: "k/ü".into(),
                    version: Some("0123456789ABC".into()),
                },
                conditions: Headers {
                    if_match: Some("\"e\"".into()),
                    if_unmodified_since: Some("Tue, 24 Sep 2024 12:00:00 GMT".into()),
                    ..Headers::default()
                },
                headers: Headers {
                    if_none_match: Some("*".into()),
                    ..Headers::default()
                },
                directive: Directive::Replace,
                metadata: metadata(),
                checksum: Some(Algorithm::Sha256),
            })),
            Op::Copy(Box::new(Copy {
                bucket: "b".into(),
                key: "copy".into(),
                id: 16,
                source: Source {
                    bucket: "src".into(),
                    key: "k".into(),
                    version: None,
                },
                conditions: Headers::default(),
                headers: Headers::default(),
                directive: Directive::Copy,
                metadata: Vec::new(),
                checksum: None,
            })),
            Op::CopyPart(Box::new(CopyPart {
                bucket: "b".into(),
                key: "k".into(),
                upload: "0123456789ABC".into(),
                number: 10_000,
                id: 17,
                source: Source {
                    bucket: "src".into(),
                    key: "k".into(),
                    version: Some("null".into()),
                },
                conditions: Headers {
                    if_none_match: Some("\"e\"".into()),
                    range: Some("bytes=0-5242879".into()),
                    ..Headers::default()
                },
            })),
            Op::DeleteObjects {
                bucket: "b".into(),
                id: 18,
                objects: (0..MAX_OBJECTS)
                    .map(|n| Target {
                        key: format!("k{n}"),
                        version: n.is_multiple_of(2).then(|| "0123456789ABC".into()),
                        etag: n.is_multiple_of(3).then(|| "e".repeat(MAX_CONDITION_TAG)),
                    })
                    .collect(),
                quiet: true,
            },
            Op::ListVersions(ListVersions {
                bucket: "b".into(),
                prefix: "p/".into(),
                delimiter: "/".into(),
                key_marker: Some("p/k".into()),
                id_marker: Some("passed".into()),
                max: 1000,
            }),
            // The principals' operations (docs/design/principals.md), after research/89's,
            // so theirs keep their golden vectors' places.
            Op::CreatePrincipal {
                name: "runner-7".into(),
                policy: "{\"Version\":\"2012-10-17\"}".into(),
            },
            Op::PutPolicy {
                name: "runner-7".into(),
                policy: "{}".into(),
            },
            Op::DeletePrincipal {
                name: "runner-7".into(),
            },
            Op::ListPrincipals {
                after: Some("a".into()),
                max: 1000,
            },
            Op::GetPrincipal {
                name: "runner-7".into(),
            },
            Op::CreateKey {
                principal: "runner-7".into(),
                rotating: true,
            },
            Op::RevokeKey { id: "A".repeat(20) },
        ]
    }

    /// Metadata of each kind a version keeps: a content header and a user-defined entry past
    /// US-ASCII.
    fn metadata() -> Metadata {
        vec![
            ("content-type".into(), "text/plain".into()),
            ("x-amz-meta-a".into(), "é".into()),
        ]
    }

    /// Every request and reply is written into buffers made at their lengths, its head's and its
    /// fields', counted first (mantle_codec::encode's rule): two allocations, none grown, a
    /// completion of S3's 10,000 parts among them.
    #[test]
    fn every_request_and_reply_is_written_at_its_length() {
        use hyper_measure::alloc;
        let parts = (1..=u16::try_from(MAX_PARTS).unwrap())
            .map(|n| (n, format!("\"{}\"", "0".repeat(32))))
            .collect();
        let complete = Op::Complete {
            bucket: "b".into(),
            key: "k".into(),
            upload: "u".repeat(MAX_TAG),
            id: 9,
            headers: headers(),
            parts,
        };
        let ops = every_op().into_iter().chain([complete]);
        for op in ops {
            alloc::begin();
            let encoded = op.encode();
            let c = alloc::end();
            assert!(encoded.is_ok(), "{op:?}");
            assert_eq!((c.allocations, c.reallocations), (2, 0), "{op:?}");
        }
        for reply in every_reply() {
            alloc::begin();
            let encoded = reply.encode();
            let c = alloc::end();
            assert!(encoded.is_ok(), "{reply:?}");
            assert_eq!((c.allocations, c.reallocations), (2, 0), "{reply:?}");
        }
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
    fn a_requests_time_left_reads_back_as_set_at_each_attempt_and_none_as_none() {
        let put = every_op().swap_remove(1);
        let (mut head, fields) = put.encode().unwrap();
        let body = Some(put.body(&fields));
        assert_eq!(Head::decode(&head, body).unwrap().remaining(), None);
        within(&mut head, Some(Duration::from_millis(1500))).unwrap();
        let decoded = Head::decode(&head, body).unwrap();
        assert_eq!(decoded.remaining(), Some(Duration::from_millis(1500)));
        assert_eq!(decoded.op(&fields), Ok(put.clone()));
        // Set again for the next attempt, less; and none again.
        within(&mut head, Some(Duration::from_millis(400))).unwrap();
        assert_eq!(
            Head::decode(&head, body).unwrap().remaining(),
            Some(Duration::from_millis(400))
        );
        within(&mut head, None).unwrap();
        assert_eq!(Head::decode(&head, body).unwrap().remaining(), None);
        // A time past what nanoseconds count waits for as long as any; no head but a request's.
        within(&mut head, Some(Duration::MAX)).unwrap();
        assert_eq!(
            Head::decode(&head, body).unwrap().remaining(),
            Some(Duration::from_nanos(u64::MAX))
        );
        assert!(within(&mut head[1..], None).is_err());
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
        // A length or a trailer on any kind but a put's.
        let (get_head, get_fields) = every_op().swap_remove(3).encode().unwrap();
        let mut lengthy = get_head.clone();
        lengthy[HEAD_BYTES - 9..HEAD_BYTES - 1].copy_from_slice(&5u64.to_le_bytes());
        assert!(Head::decode(&lengthy, Some(get_fields.len() as u64 + 5)).is_err());
        let mut trailed = get_head.clone();
        trailed[HEAD_BYTES - 1] = Algorithm::Crc32.code() + 1;
        assert!(Head::decode(&trailed, Some(get_fields.len() as u64 + 4)).is_err());
        // A put's trailer: its value's width past the put's bytes, and no code past the
        // algorithms'.
        let trailing = every_op().swap_remove(2);
        let (trailing_head, trailing_fields) = trailing.encode().unwrap();
        let body = trailing.body(&trailing_fields);
        assert_eq!(body, trailing_fields.len() as u64 + 8);
        assert!(Head::decode(&trailing_head, Some(body - 8)).is_err());
        let mut unnamed = trailing_head.clone();
        unnamed[HEAD_BYTES - 1] = u8::try_from(Algorithm::ALL.len()).unwrap() + 1;
        assert!(Head::decode(&unnamed, Some(body)).is_err());
        // An unknown kind.
        let mut unknown = head.clone();
        unknown[0] = REVOKE_KEY + 1;
        assert!(Head::decode(&unknown, Some(put.body(&fields))).is_err());
        // A key past S3's bound, headers past S3's 8 KB, a checksum of the wrong width.
        let long = Op::Head {
            bucket: "b".into(),
            key: "k".repeat(MAX_KEY + 1),
            headers: Headers::default(),
            version: None,
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
            version: None,
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
            trailing: None,
            metadata: Vec::new(),
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

    /// Every kind of reply this suite writes: each status bare, and the replies that carry an
    /// object's facts, a native upload's progress and each listing.
    fn every_reply() -> Vec<Reply> {
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
        for cause in Slowed::ALL {
            replies.push(Reply::slowed(cause));
        }
        replies.push(Reply {
            status: Status::Done,
            slowed: None,
            size: 7,
            version: "0123456789ABC".into(),
            etag: "d41d8cd98f00b204e9800998ecf8427e-10000".into(),
            modified: u64::MAX,
            checksum: Some(Stored {
                algorithm: Algorithm::Sha512.code(),
                parts: 3,
                value: vec![1; 64],
            }),
            marker: true,
            range: Some((3, 6)),
            versioning: Some(Versioning::Suspended),
            upload: "0123456789ABC".into(),
            listing: None,
            progress: Some(Box::new(Progress {
                offset: 10 << 20,
                runs: 2,
                run: 5 << 20,
                ended: false,
                checksum: Some(vec![3; 8]),
            })),
            metadata: Vec::new(),
            source_version: String::new(),
        });
        replies.push(Reply {
            progress: Some(Box::new(Progress {
                offset: 0,
                runs: 0,
                run: 5 << 30,
                ended: true,
                checksum: None,
            })),
            ..Reply::status(Status::Offset)
        });
        replies.push(Reply {
            listing: Some(Listing::Parts(Parts {
                parts: (1..=1000u16)
                    .map(|number| PartListed {
                        number,
                        etag: "0".repeat(32),
                        size: 5 << 20,
                        modified: 1_727_179_200,
                        checksum: if number.is_multiple_of(2) {
                            Some(vec![9; 64])
                        } else {
                            None
                        },
                    })
                    .collect(),
                truncated: true,
                next: Some(1000),
            })),
            ..Reply::status(Status::Done)
        });
        replies.push(Reply {
            listing: Some(Listing::Uploads(Uploads {
                uploads: (0..999)
                    .map(|n| UploadListed {
                        key: format!("k{n}"),
                        id: "0123456789ABC".into(),
                        initiated: n,
                        owner: "o".repeat(MAX_OWNER),
                    })
                    .collect(),
                common_prefixes: vec!["p/".into()],
                truncated: true,
                next: Some(("p/".into(), String::new())),
            })),
            ..Reply::status(Status::Done)
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
        // The replies research/89 adds, after every one before them.
        replies.push(Reply {
            size: 3,
            version: "0123456789ABC".into(),
            etag: "0".repeat(32),
            modified: 1_727_179_200,
            metadata: metadata(),
            source_version: "null".into(),
            ..Reply::status(Status::Done)
        });
        replies.push(Reply {
            listing: Some(Listing::Versions(Versions {
                versions: (0..999u64)
                    .map(|n| VersionListed {
                        key: format!("k{}", n / 3),
                        id: if n % 3 == 2 {
                            "null".into()
                        } else {
                            "0123456789ABC".into()
                        },
                        latest: n % 3 == 0,
                        marker: n % 5 == 0,
                        etag: if n % 5 == 0 {
                            String::new()
                        } else {
                            "0".repeat(32)
                        },
                        size: n,
                        modified: 1_727_179_200,
                        checksum: if n.is_multiple_of(2) {
                            Some((Algorithm::Crc64Nvme, 0))
                        } else {
                            None
                        },
                        owner: "o".repeat(MAX_OWNER),
                    })
                    .collect(),
                common_prefixes: vec!["p/".into()],
                truncated: true,
                next: Some(("p/".into(), String::new())),
            })),
            ..Reply::status(Status::Done)
        });
        replies.push(Reply {
            listing: Some(Listing::Deleted(
                (0..MAX_OBJECTS)
                    .map(|n| Deleted {
                        key: format!("k{n}"),
                        version: n.is_multiple_of(2).then(|| "v".repeat(MAX_VERSION_ID)),
                        status: STATUSES[n % STATUSES.len()],
                        marker: n.is_multiple_of(3).then(|| "0123456789ABC".into()),
                    })
                    .collect(),
            )),
            ..Reply::status(Status::Done)
        });
        // The principals' replies (docs/design/principals.md), after research/89's.
        replies.push(Reply {
            listing: Some(Listing::Principals(Principals {
                principals: vec![
                    PrincipalListed {
                        name: "runner-7".into(),
                        id: "P".repeat(20),
                        created: 1_727_179_200,
                        policy: None,
                        keys: vec![
                            ("A".repeat(20), 1_727_179_201),
                            ("B".repeat(20), 1_727_179_202),
                        ],
                    },
                    PrincipalListed {
                        name: "svc".into(),
                        id: "Q".repeat(20),
                        created: 1,
                        policy: Some("{}".into()),
                        keys: Vec::new(),
                    },
                ],
                next: Some("svc".into()),
            })),
            ..Reply::status(Status::Done)
        });
        replies.push(Reply {
            listing: Some(Listing::Issued(Box::new(Issued {
                id: "A".repeat(20),
                secret: mantle_s3::account::Secret::new("s".repeat(40)),
                replacing: Some("B".repeat(20)),
            }))),
            ..Reply::status(Status::Done)
        });
        replies
    }

    /// Each request and reply's head and fields in hex, a line each.
    fn object_lines() -> Vec<String> {
        let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let mut lines = Vec::new();
        for op in every_op() {
            let (head, fields) = op.encode().unwrap();
            lines.push(format!("{} {}", hex(&head), hex(&fields)));
        }
        for reply in every_reply() {
            let (head, fields) = reply.encode().unwrap();
            lines.push(format!("{} {}", hex(&head), hex(&fields)));
        }
        lines
    }

    /// Every native message this suite writes is the bytes its golden vector holds, which the
    /// format wrote before its writers were sized (tests/golden/object.txt): none differs.
    #[test]
    fn every_request_and_reply_is_its_golden_vector() {
        let golden: Vec<&str> = include_str!("../tests/golden/object.txt").lines().collect();
        let lines = object_lines();
        assert_eq!(lines.len(), golden.len());
        let differ = lines.iter().zip(&golden).filter(|(l, g)| l != g).count();
        assert_eq!(differ, 0);
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
        let replies = every_reply();
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
        let unknown = u8::try_from(STATUSES.len()).unwrap();
        assert!(ReplyHead::decode(&[unknown, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
        let mut long = Reply::status(Status::Done).encode().unwrap().0;
        long[10..].copy_from_slice(&(u32::try_from(MAX_REPLY_FIELDS).unwrap() + 1).to_le_bytes());
        assert!(ReplyHead::decode(&long).is_err());
        let (head, fields) = Reply::status(Status::NotModified).encode().unwrap();
        let decoded = ReplyHead::decode(&head).unwrap();
        assert!(decoded.reply(&fields[1..]).is_err());
    }

    /// Each cause a node answers `SlowDown` for reads back beside that status, each its own code
    /// and words; a cause beside any other status, or one the protocol does not name, is refused
    /// both ways, so the head says one decision.
    #[test]
    fn each_slowdowns_cause_reads_back_beside_it_and_beside_nothing_else() {
        let mut words = std::collections::BTreeSet::new();
        let mut names = std::collections::BTreeSet::new();
        for (at, cause) in Slowed::ALL.into_iter().enumerate() {
            assert_eq!(cause.index(), at);
            assert!(
                words.insert(cause.words()),
                "{cause:?} says another's words"
            );
            assert!(names.insert(cause.name()), "{cause:?} has another's name");
            let reply = Reply::slowed(cause);
            let (head, fields) = reply.encode().unwrap();
            assert_eq!(head.len(), REPLY_HEAD_BYTES);
            let decoded = ReplyHead::decode(&head).unwrap();
            assert_eq!(
                (decoded.status, decoded.slowed),
                (Status::SlowDown, Some(cause))
            );
            assert_eq!(decoded.reply(&fields), Ok(reply));
        }
        // A cause beside another status is not written, nor read.
        let beside = Reply {
            slowed: Some(Slowed::Door),
            ..Reply::status(Status::Failed)
        };
        assert_eq!(beside.encode(), Err(ObjectError::Cause(Status::Failed)));
        let mut head = Reply::slowed(Slowed::Door).encode().unwrap().0;
        head[0] = Status::Failed.code();
        assert!(ReplyHead::decode(&head).is_err());
        // A code past the last cause names none.
        let mut head = Reply::slowed(Slowed::Door).encode().unwrap().0;
        head[1] = u8::try_from(Slowed::ALL.len()).unwrap() + 1;
        assert!(ReplyHead::decode(&head).is_err());
        // A `SlowDown` with no cause beside it reads back as one.
        let (head, _) = Reply::status(Status::SlowDown).encode().unwrap();
        assert_eq!(ReplyHead::decode(&head).unwrap().slowed, None);
    }
}
