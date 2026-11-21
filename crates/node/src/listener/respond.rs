//! The object host's reply as S3's response (node.md §4.4, step 8 of §4.2): a status and its
//! fields, and the document S3 answers the operation with, from `mantle_s3::response`. A refusal
//! is S3's error document, which names its code, its resource and the request; a reply whose
//! outcome is unknown is `500 InternalError`, never a success or a refusal with no effect.

use std::borrow::Cow;
use std::io::Write as _;

use mantle_http::write::{self, Length, WriteError};
use mantle_s3::checksum::{Algorithm, Checksum, ChecksumType};
use mantle_s3::list::{self as s3list, Paging};
use mantle_s3::response::{self as s3, Entry, Failure};
use mantle_s3::route::{Operation, Query, Route};

use super::request::Refused;
use mantle_protocol::object::{Listing, Reply, Status, Stored};

/// The region whose buckets S3's `LocationConstraint` names by its empty value (13 §9.5).
const US_EAST_1: &str = "us-east-1";

/// A response's head, and the document it carries.
pub struct Response {
    pub status: u16,
    pub fields: Fields,
    /// The body written with the head: a document, or nothing.
    pub document: Vec<u8>,
    /// What the head states of the body's length: the document's, or the object's that follows.
    pub length: Length,
    /// An object's bytes follow the head, as the host gives them.
    pub object: bool,
}

/// A response's field lines, written as they are named into a buffer its connection keeps across
/// its requests and lends to each response (`conn::Connection::lines`): each line checked as
/// `mantle_http::write::field` checks it, and the first refused kept, so the head is refused
/// whole, as a list of fields checked before any was written would be.
#[derive(Debug)]
pub struct Fields {
    lines: Vec<u8>,
    refused: Option<WriteError>,
}

/// The longest value written from numbers: `bytes <first>-<last>/<size>`, three of u64's 20
/// digits and its 8 other bytes.
const DISPLAYED: usize = 3 * 20 + 8;

impl Fields {
    /// Fields written into `lines`, emptied first.
    pub fn new(mut lines: Vec<u8>) -> Self {
        lines.clear();
        Self {
            lines,
            refused: None,
        }
    }

    fn put(&mut self, name: &str, value: &[&[u8]]) {
        if let Err(why) = write::field(&mut self.lines, name, value) {
            self.refused.get_or_insert(why);
        }
    }

    fn text(&mut self, name: &str, value: &[u8]) {
        self.put(name, &[value]);
    }

    /// An ETag, in the double quotes a field carries it in (05 §5.1).
    fn quoted(&mut self, name: &str, etag: &str) {
        self.put(name, &[b"\"", etag.as_bytes(), b"\""]);
    }

    /// A value as it displays: a range's numbers, at most `DISPLAYED` bytes, written on the
    /// stack and then as a field's.
    fn display(&mut self, name: &str, value: impl std::fmt::Display) {
        let mut text = [0u8; DISPLAYED];
        let mut room = text.as_mut_slice();
        if write!(room, "{value}").is_err() {
            self.refused.get_or_insert(WriteError::Value);
            return;
        }
        let len = DISPLAYED.saturating_sub(room.len());
        self.put(name, &[text.get(..len).unwrap_or_default()]);
    }

    /// `Connection: close`, for a response after which the connection ends.
    pub fn close(&mut self) {
        self.text("Connection", b"close");
    }

    /// The lines written, or why one was refused.
    pub fn lines(&self) -> Result<&[u8], WriteError> {
        match self.refused {
            Some(why) => Err(why),
            None => Ok(&self.lines),
        }
    }

    /// The buffer, for the connection's next response.
    pub fn into_buffer(self) -> Vec<u8> {
        self.lines
    }
}

/// The reason phrase of each status the listener answers (RFC 9110 §15).
pub fn reason(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        412 => "Precondition Failed",
        414 => "URI Too Long",
        416 => "Range Not Satisfiable",
        417 => "Expectation Failed",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        507 => "Insufficient Storage",
        _ => "",
    }
}

/// The answer of an S3 request slowed for `cause`: `503 SlowDown`, its message S3's words and then
/// the cause's, joined in one string made at their length.
pub fn slowed(cause: crate::door::Slowed) -> Refused {
    Refused::new(
        "SlowDown",
        503,
        ["Please reduce your request rate. ", cause.words()].concat(),
    )
}

/// The answer of an S3 request the node failed for `cause`: `500 InternalError`, its message S3's
/// words and then the cause's, joined in one string made at their length.
pub fn failed(cause: crate::door::Fault) -> Refused {
    Refused::new(
        "InternalError",
        500,
        [
            "We encountered an internal error. Please try again. ",
            cause.words(),
        ]
        .concat(),
    )
}

/// S3's code, status and message for a host's refusal (node.md §4.4; 05 §11.2).
pub fn refusal(status: Status) -> Refused {
    let (code, http, message) = match status {
        Status::Done | Status::NotModified => ("InternalError", 500, "not a refusal"),
        // A native upload's run at another offset, which no request of this listener makes.
        Status::Offset => ("InternalError", 500, "a native upload's refusal"),
        Status::NoSuchBucket => ("NoSuchBucket", 404, "The specified bucket does not exist"),
        Status::NoSuchKey => ("NoSuchKey", 404, "The specified key does not exist."),
        Status::BucketAlreadyExists => (
            "BucketAlreadyExists",
            409,
            "The requested bucket name is not available. The bucket namespace is shared by all \
             users of the system. Please select a different name and try again.",
        ),
        Status::BucketAlreadyOwnedByYou => (
            "BucketAlreadyOwnedByYou",
            409,
            "Your previous request to create the named bucket succeeded and you already own it.",
        ),
        Status::OperationAborted => (
            "OperationAborted",
            409,
            "A conflicting conditional operation is currently in progress against this \
             resource. Try again.",
        ),
        Status::TooManyBuckets => (
            "TooManyBuckets",
            400,
            "You have attempted to create more buckets than allowed",
        ),
        Status::InvalidRange => (
            "InvalidRange",
            416,
            "The requested range is not satisfiable",
        ),
        Status::EntityTooLarge => (
            "EntityTooLarge",
            400,
            "Your proposed upload exceeds the maximum allowed size",
        ),
        Status::IncompleteBody => (
            "IncompleteBody",
            400,
            "You did not provide the number of bytes specified by the Content-Length HTTP header",
        ),
        Status::SlowDown => ("SlowDown", 503, "Please reduce your request rate."),
        Status::Malformed => ("InvalidRequest", 400, "A bucket or key past S3's bounds"),
        Status::Failed => (
            "InternalError",
            500,
            "We encountered an internal error. Please try again.",
        ),
        Status::PreconditionFailed => (
            "PreconditionFailed",
            412,
            "At least one of the pre-conditions you specified did not hold",
        ),
        Status::BadDigest => (
            "BadDigest",
            400,
            "The Content-MD5 or checksum value that you specified did not match what the server \
             received.",
        ),
        Status::BucketNotEmpty => (
            "BucketNotEmpty",
            409,
            "The bucket you tried to delete is not empty",
        ),
        Status::AccessDenied => ("AccessDenied", 403, "Access Denied"),
        Status::InvalidBucketState => (
            "InvalidBucketState",
            409,
            "The request is not valid with the current state of the bucket.",
        ),
        Status::NoSuchUpload => (
            "NoSuchUpload",
            404,
            "The specified upload does not exist. The upload ID may be invalid, or the upload \
             may have been aborted or completed.",
        ),
        Status::InvalidPart => (
            "InvalidPart",
            400,
            "One or more of the specified parts could not be found. The part may not have been \
             uploaded, or the specified entity tag may not match the part's entity tag.",
        ),
        Status::InvalidPartOrder => (
            "InvalidPartOrder",
            400,
            "The list of parts was not in ascending order. Parts must be ordered by part number.",
        ),
        Status::EntityTooSmall => (
            "EntityTooSmall",
            400,
            "Your proposed upload is smaller than the minimum allowed object size.",
        ),
        Status::InvalidArgument => ("InvalidArgument", 400, "Invalid Argument"),
        Status::InvalidRequest => ("InvalidRequest", 400, "Invalid Request"),
        // S3 names no code for a store out of space; the status's own, which stock clients do
        // not retry (node.md §4.4, research/75).
        Status::InsufficientStorage => (
            "InsufficientStorage",
            507,
            "The cell has no room for the object: every volume offered its chunks is full.",
        ),
        Status::NoSuchVersion => (
            "NoSuchVersion",
            404,
            "The specified version does not exist.",
        ),
        Status::MethodNotAllowed => (
            "MethodNotAllowed",
            405,
            "The specified method is not allowed against this resource.",
        ),
        Status::MetadataTooLarge => (
            "MetadataTooLarge",
            400,
            "Your metadata headers exceed the maximum allowed metadata size",
        ),
        Status::InvalidVersion => ("InvalidArgument", 400, "Invalid version id specified"),
        Status::CopyToItself => (
            "InvalidRequest",
            400,
            "This copy request is illegal because it is trying to copy an object to itself \
             without changing the object's metadata, storage class, website redirect location \
             or encryption attributes.",
        ),
        Status::CopySourceTooLarge => (
            "InvalidRequest",
            400,
            "CopyObject request made on objects larger than 5GB in size.",
        ),
        Status::CopyRangeSource => (
            "InvalidRequest",
            400,
            "The specified copy source is not supported as a byte-range copy source.",
        ),
        Status::CopyMarker => (
            "InvalidRequest",
            400,
            "You are not allowed to specify a delete marker as a version for the \
             x-amz-copy-source.",
        ),
        Status::InvalidCopyRange => (
            "InvalidArgument",
            400,
            "The range value must use the form bytes=first-last, where the first and last are \
             the zero-based byte offsets to copy.",
        ),
        // An operator's refusals on the account's principals, which only the native door asks
        // (docs/design/principals.md): IAM's codes and statuses (research/91 §2).
        Status::EntityAlreadyExists => (
            "EntityAlreadyExists",
            409,
            "A principal of that name exists.",
        ),
        Status::NoSuchEntity => ("NoSuchEntity", 404, "No such principal or access key."),
        Status::LimitExceeded => (
            "LimitExceeded",
            409,
            "The account holds 5,000 principals, or the principal two keys.",
        ),
        Status::DeleteConflict => (
            "DeleteConflict",
            409,
            "The principal holds access keys, which are revoked first.",
        ),
        Status::MalformedPolicy => (
            "MalformedPolicyDocument",
            400,
            "The policy document is malformed.",
        ),
        Status::NothingToRotate => (
            "NoSuchEntity",
            404,
            "The principal holds no access key to rotate.",
        ),
    };
    Refused::new(code, http, message)
}

/// What every response names: the request, and the host that answered it (05 §11.1).
pub struct Ids<'h> {
    /// 16 hexadecimal digits, as S3's are.
    pub request: [u8; 16],
    pub host: &'h str,
}

impl Ids<'_> {
    /// The request's id as text.
    pub fn request(&self) -> &str {
        std::str::from_utf8(&self.request).unwrap_or_default()
    }

    fn fields(&self, fields: &mut Fields) {
        fields.text("x-amz-request-id", &self.request);
        fields.text("x-amz-id-2", self.host.as_bytes());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_secs()).ok())
            .and_then(mantle_s3::time::http_stamp);
        if let Some(date) = now {
            fields.text("Date", date.as_str().as_bytes());
        }
    }
}

/// The resource an error names, written only when an error names it.
#[derive(Clone, Copy)]
pub enum Resource<'a> {
    Text(&'a str),
    /// The bucket and key as the request's path names them.
    Route(&'a Route<'a>),
}

impl Resource<'_> {
    fn text(&self) -> Cow<'_, str> {
        match self {
            Self::Text(text) => Cow::Borrowed(text),
            Self::Route(route) => Cow::Owned(resource(route)),
        }
    }
}

/// An error response: S3's `Error` document naming `resource`, none for a HEAD, whose response
/// has no body (RFC 9110 §9.3.2).
pub fn error(
    refused: &Refused,
    resource: Resource<'_>,
    ids: &Ids<'_>,
    head: bool,
    mut fields: Fields,
) -> Response {
    ids.fields(&mut fields);
    let document = if head {
        Vec::new()
    } else {
        fields.text("Content-Type", b"application/xml");
        s3::error(&Failure {
            code: refused.code,
            message: &refused.message,
            resource: Some(&resource.text()),
            details: &[],
            request_id: ids.request(),
            host_id: ids.host,
        })
        .into_bytes()
    };
    Response {
        status: refused.status,
        fields,
        length: Length::Known(u64::try_from(document.len()).unwrap_or(u64::MAX)),
        document,
        object: false,
    }
}

/// What a response echoes of its request.
pub struct Echo<'a> {
    pub route: &'a Route<'a>,
    pub query: &'a Query,
    /// The account that asked: the owner a listing names.
    pub principal: &'a str,
    pub region: &'a str,
    /// The request was a HEAD.
    pub head: bool,
    /// `x-amz-checksum-mode: ENABLED`: a read's checksum is returned (05 §3.4).
    pub checksums: bool,
}

/// The response to the host's `reply`, its fields written into `fields`; a refusal of the reply
/// itself, and the buffer back.
pub fn reply(
    echo: &Echo<'_>,
    reply: Reply,
    ids: &Ids<'_>,
    mut fields: Fields,
) -> Result<Response, (Refused, Fields)> {
    let resource = Resource::Route(echo.route);
    match reply.status {
        Status::Done => done(echo, reply, ids, fields),
        Status::NotModified => {
            ids.fields(&mut fields);
            if let Err(why) = object_facts(&reply, &mut fields) {
                return Err((why, fields));
            }
            Ok(Response {
                status: 304,
                fields,
                document: Vec::new(),
                length: Length::None,
                object: false,
            })
        }
        // A key whose current version is a delete marker reads as none, saying so (05 §7.4).
        Status::NoSuchKey if reply.marker => {
            let refused = refusal(Status::NoSuchKey);
            let mut response = error(&refused, resource, ids, echo.head, fields);
            response.fields.text("x-amz-delete-marker", b"true");
            version(&reply, &mut response.fields);
            Ok(response)
        }
        // A get or head naming a delete marker's version: 405, "and the `Last-Modified:
        // timestamp` response header" (research/89 §2.1), the marker named.
        Status::MethodNotAllowed => {
            let refused = refusal(Status::MethodNotAllowed);
            let mut response = error(&refused, resource, ids, echo.head, fields);
            response.fields.text("x-amz-delete-marker", b"true");
            if let Err(why) = object_facts(&reply, &mut response.fields) {
                return Err((why, response.fields));
            }
            Ok(response)
        }
        // A read past S3's single GET: `EntityTooLarge` in its download form (05 §4.1, §11.2).
        Status::EntityTooLarge if matches!(echo.route.operation, Operation::GetObject) => {
            let refused = Refused::new(
                "EntityTooLarge",
                405,
                "Your proposed download exceeds the maximum allowed size",
            );
            Ok(error(&refused, resource, ids, echo.head, fields))
        }
        // A failure names its cause after S3's words (`door::Fault`), as a `SlowDown` does.
        Status::Failed if reply.fault.is_some() => {
            let refused = reply.fault.map_or_else(|| refusal(Status::Failed), failed);
            Ok(error(&refused, resource, ids, echo.head, fields))
        }
        status => Ok(error(&refusal(status), resource, ids, echo.head, fields)),
    }
}

/// The resource an error names: the bucket and key as the request's path names them, joined in one
/// string made at their length.
pub fn resource(route: &Route<'_>) -> String {
    match (&route.bucket, &route.key) {
        (Some(bucket), Some(key)) => ["/", bucket, "/", key].concat(),
        (Some(bucket), None) => ["/", bucket].concat(),
        _ => "/".into(),
    }
}

fn internal(why: impl std::fmt::Display) -> Refused {
    Refused::new("InternalError", 500, why.to_string())
}

/// A response of `status` with no body.
fn empty(status: u16, fields: Fields) -> Response {
    Response {
        status,
        fields,
        document: Vec::new(),
        length: if status == 204 {
            Length::None
        } else {
            Length::Known(0)
        },
        object: false,
    }
}

/// A response carrying `document`, S3's XML.
fn document(mut fields: Fields, document: String) -> Response {
    fields.text("Content-Type", b"application/xml");
    let document = document.into_bytes();
    Response {
        status: 200,
        fields,
        length: Length::Known(u64::try_from(document.len()).unwrap_or(u64::MAX)),
        document,
        object: false,
    }
}

fn done(
    echo: &Echo<'_>,
    reply: Reply,
    ids: &Ids<'_>,
    mut fields: Fields,
) -> Result<Response, (Refused, Fields)> {
    ids.fields(&mut fields);
    match answered(echo, reply, &mut fields) {
        Ok(Answered::Empty(status)) => Ok(empty(status, fields)),
        Ok(Answered::Document(text)) => Ok(document(fields, text)),
        Ok(Answered::Object(status, length)) => Ok(Response {
            status,
            fields,
            document: Vec::new(),
            length: Length::Known(length),
            object: echo.route.operation == Operation::GetObject,
        }),
        Err(why) => Err((why, fields)),
    }
}

/// What a done reply answers, its fields written.
enum Answered {
    Empty(u16),
    Document(String),
    /// An object's status and length, its bytes to follow on a GET.
    Object(u16, u64),
}

fn answered(echo: &Echo<'_>, reply: Reply, fields: &mut Fields) -> Result<Answered, Refused> {
    let route = echo.route;
    let bucket = route.bucket.as_deref().unwrap_or_default();
    let key = route.key.as_deref().unwrap_or_default();
    let document = |text| Ok(Answered::Document(text));
    let empty = |status| Ok(Answered::Empty(status));
    match route.operation {
        Operation::CreateBucket => {
            fields.put("Location", &[b"/", bucket.as_bytes()]);
            empty(200)
        }
        Operation::HeadBucket => {
            fields.text("x-amz-bucket-region", echo.region.as_bytes());
            empty(200)
        }
        // us-east-1 is S3's empty constraint (13 §9.5).
        Operation::GetBucketLocation => {
            let location = if echo.region == US_EAST_1 {
                ""
            } else {
                echo.region
            };
            document(s3::location_constraint(location))
        }
        Operation::DeleteBucket | Operation::AbortMultipartUpload => empty(204),
        Operation::PutBucketVersioning => empty(200),
        Operation::GetBucketVersioning => document(s3::versioning_configuration(reply.versioning)),
        Operation::ListBuckets => document(list_buckets(echo, &reply)?),
        Operation::ListObjectsV2 => document(list_objects(echo, reply)?),
        Operation::PutObject | Operation::UploadPart => {
            fields.quoted("ETag", &reply.etag);
            version(&reply, fields);
            checksum(&reply, fields);
            empty(200)
        }
        Operation::GetObject | Operation::HeadObject => {
            object_facts(&reply, fields)?;
            fields.text("Accept-Ranges", b"bytes");
            stored(echo.query, &reply.metadata, fields);
            if echo.checksums && reply.range.is_none() {
                checksum(&reply, fields);
            }
            let (status, length) = match reply.range {
                Some((first, last)) => {
                    fields.display(
                        "Content-Range",
                        format_args!("bytes {first}-{last}/{}", reply.size),
                    );
                    let length = last
                        .checked_sub(first)
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| internal("a range whose last byte is before its first"))?;
                    (206, length)
                }
                None => (200, reply.size),
            };
            Ok(Answered::Object(status, length))
        }
        // The version's tag set: mantle keeps none yet, so the empty one (research/89 §3.8).
        Operation::GetObjectTagging => {
            version(&reply, fields);
            document(s3::tagging(&[]))
        }
        Operation::DeleteObject => {
            if reply.marker {
                fields.text("x-amz-delete-marker", b"true");
            }
            // A version named is answered with its ID, `null` among them (research/89 §2.2).
            if echo.query.has("versionId") && !reply.version.is_empty() {
                fields.text("x-amz-version-id", reply.version.as_bytes());
            } else {
                version(&reply, fields);
            }
            empty(204)
        }
        Operation::CopyObject => {
            version(&reply, fields);
            source_version(&reply, fields);
            let value = reply.checksum.as_ref().and_then(checksum_value);
            let object_checksum = match (&reply.checksum, &value) {
                (Some(kept), Some(value)) => {
                    Algorithm::from_code(kept.algorithm).map(|algorithm| s3::ObjectChecksum {
                        algorithm,
                        kind: kind(kept.parts),
                        value,
                    })
                }
                _ => None,
            };
            document(
                s3::copy_object(&reply.etag, seconds(reply.modified)?, object_checksum)
                    .map_err(time)?,
            )
        }
        Operation::UploadPartCopy => {
            source_version(&reply, fields);
            let part = reply.checksum.as_ref().and_then(|kept| {
                Some(Checksum {
                    algorithm: Algorithm::from_code(kept.algorithm)?,
                    bytes: kept.value.clone(),
                })
            });
            document(
                s3::copy_part(&reply.etag, seconds(reply.modified)?, part.as_ref())
                    .map_err(time)?,
            )
        }
        Operation::DeleteObjects => document(delete_result(&reply)?),
        Operation::ListObjectVersions => document(list_versions(echo, reply)?),
        Operation::CreateMultipartUpload => {
            document(s3::initiate_multipart_upload(bucket, key, &reply.upload))
        }
        Operation::CompleteMultipartUpload => {
            version(&reply, fields);
            let value = reply.checksum.as_ref().and_then(checksum_value);
            let object_checksum = match (&reply.checksum, &value) {
                (Some(kept), Some(value)) => {
                    Algorithm::from_code(kept.algorithm).map(|algorithm| s3::ObjectChecksum {
                        algorithm,
                        kind: kind(kept.parts),
                        value,
                    })
                }
                _ => None,
            };
            // The key encoded at its length, then joined behind the bucket in one string at theirs.
            let encoded = mantle_s3::sigv4::uri_encode(key.as_bytes(), false);
            let location = ["/", bucket, "/", &encoded].concat();
            document(s3::complete_multipart_upload(&s3::Completed {
                location: &location,
                bucket,
                key,
                etag: &reply.etag,
                checksum: object_checksum,
            }))
        }
        Operation::ListParts => document(list_parts(echo, &reply)?),
        Operation::ListMultipartUploads => document(list_uploads(echo, &reply)?),
        other => Err(internal(format!("a reply to {other:?}"))),
    }
}

/// The version's id, where the bucket names versions: an unversioned bucket's are `null`, which
/// S3 leaves unsaid.
fn version(reply: &Reply, fields: &mut Fields) {
    if !reply.version.is_empty() && reply.version != "null" {
        fields.text("x-amz-version-id", reply.version.as_bytes());
    }
}

/// The version a copy read of its source, as `version` states its own (research/89 §3.6).
fn source_version(reply: &Reply, fields: &mut Fields) {
    if !reply.source_version.is_empty() && reply.source_version != "null" {
        fields.text(
            "x-amz-copy-source-version-id",
            reply.source_version.as_bytes(),
        );
    }
}

/// The content headers S3 returns, as their fields name them, beside the names a version keeps
/// them under (`mantle_s3::metadata::CONTENT`), and the query parameter that overrides each on a
/// read: "The response headers that you can override for the `GetObject` response are
/// `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Content-Type`,
/// and `Expires`" (research/89 §1.5).
const CONTENT: [(&str, &str, &str); 6] = [
    ("cache-control", "Cache-Control", "response-cache-control"),
    (
        "content-disposition",
        "Content-Disposition",
        "response-content-disposition",
    ),
    (
        "content-encoding",
        "Content-Encoding",
        "response-content-encoding",
    ),
    (
        "content-language",
        "Content-Language",
        "response-content-language",
    ),
    ("content-type", "Content-Type", "response-content-type"),
    ("expires", "Expires", "response-expires"),
];

/// A version's metadata as a read's response states it (research/89 §1): each content header it
/// keeps, or the query's override of it, `Content-Type` S3's `binary/octet-stream` where neither
/// names one; then each user-defined entry under its `x-amz-meta-` name, its value as S3 returns
/// it (`mantle_s3::metadata::shown`).
fn stored(query: &Query, kept: &[(String, String)], fields: &mut Fields) {
    for (name, field, parameter) in CONTENT {
        let default = if name == "content-type" {
            Some(mantle_s3::metadata::DEFAULT_TYPE)
        } else {
            None
        };
        let value = query
            .value(parameter)
            .or_else(|| mantle_s3::metadata::value(kept, name))
            .or(default);
        if let Some(value) = value {
            fields.text(field, mantle_s3::metadata::shown(value).as_bytes());
        }
    }
    for (name, value) in kept {
        if name.starts_with(mantle_s3::metadata::USER) {
            fields.text(name, mantle_s3::metadata::shown(value).as_bytes());
        }
    }
}

/// DeleteObjects' `DeleteResult` (research/89 §5.2): each object's result in the order named,
/// quiet's failures alone, as the host gave them.
fn delete_result(reply: &Reply) -> Result<String, Refused> {
    let Some(Listing::Deleted(results)) = &reply.listing else {
        return Err(internal("a delete's reply without its results"));
    };
    let refusals: Vec<Refused> = results.iter().map(|d| refusal(d.status)).collect();
    let deletions: Vec<s3::Deletion<'_>> = results
        .iter()
        .zip(&refusals)
        .map(|(d, refused)| {
            if d.status == Status::Done {
                s3::Deletion::Deleted {
                    key: &d.key,
                    version_id: d.version.as_deref(),
                    delete_marker: d.marker.as_deref(),
                }
            } else {
                s3::Deletion::Failed {
                    key: &d.key,
                    version_id: d.version.as_deref(),
                    code: refused.code,
                    message: &refused.message,
                }
            }
        })
        .collect();
    Ok(s3::delete_result(&deletions, false))
}

/// ListObjectVersions' `ListVersionsResult` (research/89 §2.3).
fn list_versions(echo: &Echo<'_>, reply: Reply) -> Result<String, Refused> {
    let Some(Listing::Versions(page)) = reply.listing else {
        return Err(internal("a listing reply without its page"));
    };
    let mut versions = Vec::with_capacity(page.versions.len());
    for listed in page.versions {
        versions.push(s3::Version {
            key: listed.key,
            version_id: listed.id,
            latest: listed.latest,
            marker: listed.marker,
            entry: Entry {
                modified: seconds(listed.modified)?,
                etag: listed.etag,
                size: listed.size,
                owner: listed.owner,
                checksum: listed
                    .checksum
                    .map(|(algorithm, parts)| (algorithm, kind(parts))),
            },
        });
    }
    let query = echo.query;
    let max_keys = s3list::max_keys(query.value("max-keys"))
        .unwrap_or(s3list::MAX_KEYS)
        .min(s3list::MAX_KEYS);
    let next = page.next.as_ref().map(|(key, id)| s3::Next { key, id });
    s3::list_object_versions(&s3::ListVersions {
        bucket: echo.route.bucket.as_deref().unwrap_or_default(),
        prefix: query.value("prefix").unwrap_or_default(),
        delimiter: query.value("delimiter"),
        max_keys,
        key_marker: query.value("key-marker"),
        version_id_marker: query.value("version-id-marker"),
        url: query.value("encoding-type") == Some("url"),
        versions: &versions,
        common_prefixes: &page.common_prefixes,
        next,
    })
    .map_err(time)
}

/// An object's ETag, Last-Modified and version, as a read's response states them.
fn object_facts(reply: &Reply, fields: &mut Fields) -> Result<(), Refused> {
    fields.quoted("ETag", &reply.etag);
    let seconds = i64::try_from(reply.modified).map_err(|_| internal("a time past i64"))?;
    let modified =
        mantle_s3::time::http_stamp(seconds).ok_or_else(|| internal("a time past 9999"))?;
    fields.text("Last-Modified", modified.as_str().as_bytes());
    version(reply, fields);
    Ok(())
}

/// How a kept value was formed: from its parts' values when it combines any.
fn kind(parts: u16) -> ChecksumType {
    if parts == 0 {
        ChecksumType::FullObject
    } else {
        ChecksumType::Composite
    }
}

/// The longest `-n` behind a composite's value: `-` and u16's 5 digits.
const PARTS_SUFFIX: usize = 6;

/// A kept checksum's value as its header writes it: base64, a composite's followed by `-n`, its
/// suffix written on the stack and joined behind the value in one string at their length.
fn checksum_value(kept: &Stored) -> Option<String> {
    let algorithm = Algorithm::from_code(kept.algorithm)?;
    let value = Checksum {
        algorithm,
        bytes: kept.value.clone(),
    }
    .to_base64();
    if kept.parts == 0 {
        return Some(value);
    }
    let mut suffix = [0u8; PARTS_SUFFIX];
    let mut room = suffix.as_mut_slice();
    write!(room, "-{}", kept.parts).ok()?;
    let len = PARTS_SUFFIX.saturating_sub(room.len());
    let suffix = std::str::from_utf8(suffix.get(..len)?).ok()?;
    Some([value.as_str(), suffix].concat())
}

fn checksum(reply: &Reply, fields: &mut Fields) {
    let Some(kept) = &reply.checksum else {
        return;
    };
    let (Some(algorithm), Some(value)) =
        (Algorithm::from_code(kept.algorithm), checksum_value(kept))
    else {
        return;
    };
    fields.text(algorithm.header(), value.as_bytes());
    fields.text("x-amz-checksum-type", kind(kept.parts).name().as_bytes());
}

fn seconds(value: u64) -> Result<i64, Refused> {
    i64::try_from(value).map_err(|_| internal("a time past i64"))
}

fn time(e: s3::TimeOutOfRange) -> Refused {
    Refused::new(e.code().0, e.code().1, e.to_string())
}

fn list_objects(echo: &Echo<'_>, reply: Reply) -> Result<String, Refused> {
    let Some(Listing::Keys(page)) = reply.listing else {
        return Err(internal("a listing reply without its page"));
    };
    // The page's keys, tags and owners move into the document's page, not copied.
    let mut contents = Vec::with_capacity(page.contents.len());
    for listed in page.contents {
        contents.push((
            listed.key,
            Entry {
                modified: seconds(listed.modified)?,
                etag: listed.etag,
                size: listed.size,
                owner: listed.owner.unwrap_or_default(),
                checksum: listed
                    .checksum
                    .map(|(algorithm, parts)| (algorithm, kind(parts))),
            },
        ));
    }
    let listed = s3list::Page {
        contents,
        common_prefixes: page.common_prefixes,
        truncated: page.truncated,
        next: page.next,
    };
    let query = echo.query;
    let max_keys = s3list::max_keys(query.value("max-keys"))
        .unwrap_or(s3list::MAX_KEYS)
        .min(s3list::MAX_KEYS);
    s3::list_objects_v2(
        &s3::ListV2 {
            bucket: echo.route.bucket.as_deref().unwrap_or_default(),
            request: s3list::Request {
                prefix: query.value("prefix").unwrap_or_default(),
                delimiter: query.value("delimiter"),
                start: None,
                max_keys,
                paging: Paging::Token,
            },
            continuation_token: query.value("continuation-token"),
            start_after: query.value("start-after"),
            url: query.value("encoding-type") == Some("url"),
            fetch_owner: query.value("fetch-owner") == Some("true"),
        },
        &listed,
    )
    .map_err(time)
}

fn list_buckets(echo: &Echo<'_>, reply: &Reply) -> Result<String, Refused> {
    let Some(Listing::Buckets(page)) = &reply.listing else {
        return Err(internal("a listing reply without its page"));
    };
    let mut buckets = Vec::with_capacity(page.buckets.len());
    for (name, created) in &page.buckets {
        buckets.push(s3::Bucket {
            name: name.clone(),
            created: seconds(*created)?,
            region: echo.region.to_owned(),
        });
    }
    let query = echo.query;
    s3::list_buckets(&s3::ListBuckets {
        owner: echo.principal,
        buckets: &buckets,
        regions: query.has("prefix")
            || query.has("continuation-token")
            || query.has("max-buckets")
            || query.has("bucket-region"),
        continuation_token: page.next.as_deref(),
        prefix: query.value("prefix"),
    })
    .map_err(time)
}

fn list_parts(echo: &Echo<'_>, reply: &Reply) -> Result<String, Refused> {
    let Some(Listing::Parts(page)) = &reply.listing else {
        return Err(internal("a listing reply without its page"));
    };
    let algorithm = reply
        .checksum
        .as_ref()
        .and_then(|kept| Algorithm::from_code(kept.algorithm));
    let mut parts = Vec::with_capacity(page.parts.len());
    for part in &page.parts {
        parts.push(s3::Part {
            number: part.number,
            modified: seconds(part.modified)?,
            etag: part.etag.clone(),
            size: part.size,
            checksum: match (algorithm, &part.checksum) {
                (Some(algorithm), Some(bytes)) => Some(Checksum {
                    algorithm,
                    bytes: bytes.clone(),
                }),
                _ => None,
            },
        });
    }
    let query = echo.query;
    let marker = query
        .value("part-number-marker")
        .and_then(|m| m.trim().parse().ok())
        .unwrap_or(0);
    let max_parts = query
        .value("max-parts")
        .and_then(|m| m.trim().parse().ok())
        .unwrap_or(mantle_protocol::object::MAX_PAGE_PARTS)
        .min(mantle_protocol::object::MAX_PAGE_PARTS);
    s3::list_parts(&s3::ListParts {
        bucket: echo.route.bucket.as_deref().unwrap_or_default(),
        key: echo.route.key.as_deref().unwrap_or_default(),
        upload_id: query.value("uploadId").unwrap_or_default(),
        part_number_marker: marker,
        next: if page.truncated { page.next } else { None },
        max_parts,
        parts: &parts,
        initiator: echo.principal,
        owner: echo.principal,
        checksum: reply
            .checksum
            .as_ref()
            .and_then(|kept| Some((Algorithm::from_code(kept.algorithm)?, kind(kept.parts)))),
    })
    .map_err(time)
}

fn list_uploads(echo: &Echo<'_>, reply: &Reply) -> Result<String, Refused> {
    let Some(Listing::Uploads(page)) = &reply.listing else {
        return Err(internal("a listing reply without its page"));
    };
    let mut uploads = Vec::with_capacity(page.uploads.len());
    for upload in &page.uploads {
        uploads.push(s3::Upload {
            key: upload.key.clone(),
            upload_id: upload.id.clone(),
            initiated: seconds(upload.initiated)?,
            initiator: upload.owner.clone(),
            owner: upload.owner.clone(),
            checksum: None,
        });
    }
    let query = echo.query;
    // S3 writes the markers on every page (13 §9.3): where the next starts, else the last listed.
    let (next_key, next_id) = match (&page.next, page.uploads.last()) {
        (Some((key, id)), _) => (key.as_str(), id.as_str()),
        (None, Some(last)) => (last.key.as_str(), last.id.as_str()),
        (None, None) => ("", ""),
    };
    let max_uploads = query
        .value("max-uploads")
        .and_then(|m| m.trim().parse().ok())
        .unwrap_or(s3list::MAX_KEYS)
        .min(s3list::MAX_KEYS);
    s3::list_multipart_uploads(&s3::ListUploads {
        bucket: echo.route.bucket.as_deref().unwrap_or_default(),
        prefix: query.value("prefix").unwrap_or_default(),
        delimiter: query.value("delimiter"),
        max_uploads,
        key_marker: query.value("key-marker"),
        upload_id_marker: query.value("upload-id-marker"),
        url: query.value("encoding-type") == Some("url"),
        uploads: &uploads,
        common_prefixes: &page.common_prefixes,
        truncated: page.truncated,
        next: s3::Next {
            key: next_key,
            id: next_id,
        },
    })
    .map_err(time)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answered(operation: Operation) -> Response {
        let route = Route {
            operation,
            bucket: Some("bucket".into()),
            key: Some("key".into()),
        };
        let query = Query::parse("");
        let echo = Echo {
            route: &route,
            query: &query,
            principal: "owner",
            region: "us-east-1",
            head: false,
            checksums: false,
        };
        let ids = Ids {
            request: *b"0123456789ABCDEF",
            host: "host",
        };
        reply(
            &echo,
            Reply::status(Status::EntityTooLarge),
            &ids,
            Fields::new(Vec::new()),
        )
        .unwrap()
    }

    /// A read past S3's single GET is refused in `EntityTooLarge`'s download form, 405, and an
    /// upload past a single PUT in its upload form, 400 (05 §4.1, §11.2).
    #[test]
    fn entity_too_large_answers_a_get_as_a_download_and_a_put_as_an_upload() {
        let get = answered(Operation::GetObject);
        assert_eq!(get.status, 405);
        let document = String::from_utf8(get.document).unwrap();
        assert!(
            document.contains("<Code>EntityTooLarge</Code>"),
            "{document}"
        );
        assert!(document.contains("proposed download exceeds"), "{document}");
        let put = answered(Operation::PutObject);
        assert_eq!(put.status, 400);
        let document = String::from_utf8(put.document).unwrap();
        assert!(document.contains("proposed upload exceeds"), "{document}");
    }

    /// Each path that answers an S3 request `503 SlowDown` says which it is: the code S3's API
    /// reference states for 503, the message S3's own words and then its cause, each cause's apart,
    /// so a client's failure names the path that answered it.
    #[test]
    fn each_slowdown_names_its_cause_under_s3s_code() {
        use crate::door::Slowed;
        let causes = [
            Slowed::Connections,
            Slowed::Door,
            Slowed::Lookups,
            Slowed::Stopping,
            Slowed::KeyLeaderless,
            Slowed::Leaderless,
            Slowed::Unreadable,
            Slowed::Unplaced,
            Slowed::Patience,
        ];
        let mut messages = std::collections::BTreeSet::new();
        for cause in causes {
            let refused = slowed(cause);
            assert_eq!(
                (refused.code, refused.status),
                ("SlowDown", 503),
                "{cause:?}"
            );
            assert!(
                refused
                    .message
                    .starts_with("Please reduce your request rate. "),
                "{cause:?}: {}",
                refused.message
            );
            assert!(
                messages.insert(refused.message.clone()),
                "{cause:?} says another's"
            );
            let ids = Ids {
                request: *b"0123456789ABCDEF",
                host: "host",
            };
            let response = error(
                &refused,
                Resource::Text("/b/o"),
                &ids,
                false,
                Fields::new(Vec::new()),
            );
            let document = String::from_utf8(response.document).unwrap();
            assert!(document.contains("<Code>SlowDown</Code>"), "{document}");
            assert!(document.contains(&refused.message), "{document}");
        }
    }

    /// The allocations and reallocations `f` makes on this thread, on the counting allocator the
    /// unit tests run on (lib.rs).
    fn counted<T>(f: impl FnOnce() -> T) -> (T, (u64, u64)) {
        hyper_measure::alloc::begin();
        let out = f();
        let counts = hyper_measure::alloc::end();
        (out, (counts.allocations, counts.reallocations))
    }

    /// `resource` as it was before Lean doors step 7, `format!`ed into a string that grows.
    fn reference_resource(route: &Route<'_>) -> String {
        match (&route.bucket, &route.key) {
            (Some(bucket), Some(key)) => format!("/{bucket}/{key}"),
            (Some(bucket), None) => format!("/{bucket}"),
            _ => "/".into(),
        }
    }

    /// `checksum_value` as it was before Lean doors step 7: a composite's `format!`ed.
    fn reference_checksum_value(kept: &Stored) -> Option<String> {
        let algorithm = Algorithm::from_code(kept.algorithm)?;
        let value = Checksum {
            algorithm,
            bytes: kept.value.clone(),
        }
        .to_base64();
        Some(if kept.parts == 0 {
            value
        } else {
            format!("{value}-{}", kept.parts)
        })
    }

    /// A key that `encoding-type=url` and a completion's `Location` escape: spaces, `+` and UTF-8.
    const KEY: &str = "photos/2026/a b+c \u{e9}.jpg";
    const MODIFIED: u64 = 1_727_179_200;
    const ETAG: &str = "6805f2cfc46c0f04559748bb039d69ae";

    fn stored(parts: u16) -> Stored {
        Stored {
            algorithm: Algorithm::Crc32c.code(),
            parts,
            value: vec![0xab; 4],
        }
    }

    fn done() -> Reply {
        Reply::status(Status::Done)
    }

    /// An object's reply: a get's, a head's, a put's or a copy's, its checksum a composite's.
    fn object() -> Reply {
        Reply {
            size: 100,
            version: "3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY".into(),
            etag: format!("{ETAG}-3"),
            modified: MODIFIED,
            checksum: Some(stored(3)),
            metadata: vec![
                ("content-type".into(), "text/plain".into()),
                ("x-amz-meta-a".into(), "b".into()),
            ],
            source_version: "3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHZ".into(),
            ..done()
        }
    }

    fn listing(listing: Listing) -> Reply {
        Reply {
            listing: Some(listing),
            ..done()
        }
    }

    fn keys() -> Listing {
        use mantle_protocol::object::{Listed, Page};
        Listing::Keys(Page {
            contents: (0..3)
                .map(|i| Listed {
                    key: format!("{KEY}-{i}"),
                    etag: ETAG.into(),
                    size: 5,
                    modified: MODIFIED,
                    checksum: Some((Algorithm::Crc32c, 3)),
                    owner: Some("owner".into()),
                })
                .collect(),
            common_prefixes: vec!["photos/2027 a/".into()],
            truncated: true,
            next: Some(s3list::Resume::After(format!("{KEY}-2"))),
        })
    }

    fn buckets() -> Listing {
        Listing::Buckets(mantle_protocol::object::Buckets {
            buckets: (0..3).map(|i| (format!("bucket-{i}"), MODIFIED)).collect(),
            next: Some("token".into()),
        })
    }

    fn parts() -> Reply {
        use mantle_protocol::object::{PartListed, Parts};
        Reply {
            checksum: Some(stored(3)),
            ..listing(Listing::Parts(Parts {
                parts: (1..=3)
                    .map(|number| PartListed {
                        number,
                        etag: ETAG.into(),
                        size: 5 << 20,
                        modified: MODIFIED,
                        checksum: Some(vec![0xab; 4]),
                    })
                    .collect(),
                truncated: true,
                next: Some(3),
            }))
        }
    }

    fn uploads() -> Listing {
        use mantle_protocol::object::{UploadListed, Uploads};
        Listing::Uploads(Uploads {
            uploads: (0..3)
                .map(|i| UploadListed {
                    key: format!("{KEY}-{i}"),
                    id: format!("upload-{i}"),
                    initiated: MODIFIED,
                    owner: "owner".into(),
                })
                .collect(),
            common_prefixes: vec!["photos/2027 a/".into()],
            truncated: true,
            next: Some((format!("{KEY}-2"), "upload-2".into())),
        })
    }

    fn versions() -> Listing {
        use mantle_protocol::object::{VersionListed, Versions};
        Listing::Versions(Versions {
            versions: (0..3)
                .map(|i| VersionListed {
                    key: format!("{KEY}-{i}"),
                    id: format!("version-{i}"),
                    latest: i == 0,
                    marker: i == 2,
                    etag: ETAG.into(),
                    size: 5,
                    modified: MODIFIED,
                    checksum: Some((Algorithm::Crc32c, 0)),
                    owner: "owner".into(),
                })
                .collect(),
            common_prefixes: vec!["photos/2027 a/".into()],
            truncated: true,
            next: Some((format!("{KEY}-2"), "version-2".into())),
        })
    }

    fn deleted() -> Listing {
        use mantle_protocol::object::Deleted;
        Listing::Deleted(vec![
            Deleted {
                key: KEY.into(),
                version: Some("version-0".into()),
                status: Status::Done,
                marker: Some("version-1".into()),
            },
            Deleted {
                key: format!("{KEY}-1"),
                version: None,
                status: Status::AccessDenied,
                marker: None,
            },
        ])
    }

    /// A response asked: its operation, query, whether a HEAD asked and with checksums, its key,
    /// and the host's reply.
    type Asked = (
        Operation,
        &'static str,
        bool,
        bool,
        Option<&'static str>,
        Reply,
    );

    /// Every response the listener writes: each operation's done reply, the documents with their
    /// keys escaped where `encoding-type=url` asks, the refusals `reply` answers itself, and a
    /// refusal of each kind of resource.
    fn responses() -> Vec<Asked> {
        use Operation as O;
        let url = "encoding-type=url&fetch-owner=true&max-keys=3";
        vec![
            (O::CreateBucket, "", false, false, None, done()),
            (O::HeadBucket, "", true, false, None, done()),
            (O::GetBucketLocation, "location", false, false, None, done()),
            (O::DeleteBucket, "", false, false, None, done()),
            (
                O::AbortMultipartUpload,
                "uploadId=u",
                false,
                false,
                Some(KEY),
                done(),
            ),
            (
                O::PutBucketVersioning,
                "versioning",
                false,
                false,
                None,
                done(),
            ),
            (
                O::GetBucketVersioning,
                "versioning",
                false,
                false,
                None,
                done(),
            ),
            (O::ListBuckets, "", false, false, None, listing(buckets())),
            (
                O::ListBuckets,
                "prefix=b",
                false,
                false,
                None,
                listing(buckets()),
            ),
            (
                O::ListObjectsV2,
                "list-type=2",
                false,
                false,
                None,
                listing(keys()),
            ),
            (O::ListObjectsV2, url, false, false, None, listing(keys())),
            (O::PutObject, "", false, false, Some(KEY), object()),
            (
                O::UploadPart,
                "partNumber=1&uploadId=u",
                false,
                false,
                Some(KEY),
                object(),
            ),
            (O::GetObject, "", false, false, Some(KEY), object()),
            (
                O::GetObject,
                "",
                false,
                false,
                Some(KEY),
                Reply {
                    range: Some((10, 19)),
                    ..object()
                },
            ),
            (O::HeadObject, "", true, true, Some(KEY), object()),
            (
                O::GetObjectTagging,
                "tagging",
                false,
                false,
                Some(KEY),
                object(),
            ),
            (
                O::DeleteObject,
                "versionId=v",
                false,
                false,
                Some(KEY),
                Reply {
                    marker: true,
                    ..object()
                },
            ),
            (O::CopyObject, "", false, false, Some(KEY), object()),
            (
                O::UploadPartCopy,
                "partNumber=1&uploadId=u",
                false,
                false,
                Some(KEY),
                Reply {
                    checksum: Some(stored(0)),
                    ..object()
                },
            ),
            (
                O::DeleteObjects,
                "delete",
                false,
                false,
                None,
                listing(deleted()),
            ),
            (
                O::ListObjectVersions,
                "versions",
                false,
                false,
                None,
                listing(versions()),
            ),
            (
                O::ListObjectVersions,
                url,
                false,
                false,
                None,
                listing(versions()),
            ),
            (
                O::CreateMultipartUpload,
                "uploads",
                false,
                false,
                Some(KEY),
                Reply {
                    upload: "upload-0".into(),
                    ..done()
                },
            ),
            (
                O::CompleteMultipartUpload,
                "uploadId=u",
                false,
                false,
                Some(KEY),
                object(),
            ),
            (
                O::ListParts,
                "uploadId=u&max-parts=3",
                false,
                false,
                Some(KEY),
                parts(),
            ),
            (
                O::ListMultipartUploads,
                "uploads",
                false,
                false,
                None,
                listing(uploads()),
            ),
            (
                O::ListMultipartUploads,
                url,
                false,
                false,
                None,
                listing(uploads()),
            ),
            (
                O::GetObject,
                "",
                false,
                false,
                Some(KEY),
                Reply {
                    status: Status::NotModified,
                    ..object()
                },
            ),
            (
                O::GetObject,
                "",
                false,
                false,
                Some(KEY),
                Reply {
                    status: Status::NoSuchKey,
                    marker: true,
                    ..object()
                },
            ),
            (
                O::HeadObject,
                "versionId=v",
                true,
                false,
                Some(KEY),
                Reply {
                    status: Status::MethodNotAllowed,
                    ..object()
                },
            ),
            (
                O::GetObject,
                "",
                false,
                false,
                Some(KEY),
                Reply::status(Status::EntityTooLarge),
            ),
            (
                O::ListObjectsV2,
                "list-type=2",
                false,
                false,
                None,
                Reply::status(Status::NoSuchBucket),
            ),
            (
                O::PutObject,
                "",
                false,
                false,
                Some(KEY),
                Reply::status(Status::AccessDenied),
            ),
            (
                O::CompleteMultipartUpload,
                "uploadId=u",
                false,
                false,
                Some(KEY),
                Reply::status(Status::InvalidPart),
            ),
            (
                O::PutObject,
                "",
                false,
                false,
                Some(KEY),
                Reply::status(Status::InsufficientStorage),
            ),
        ]
    }

    /// Every response the listener writes to a done reply or a refusal, its document and its fields
    /// in a buffer its connection's earlier requests warmed, reallocates nothing: each document is
    /// made at its length, and each string it is made from (the resource an error names, a
    /// completion's `Location`, a composite's checksum, a key escaped for `encoding-type=url`) at
    /// its own. `resource`, `checksum_value`, `slowed`'s message and a completion's `Location` are
    /// the strings `format!` made before Lean doors step 7 (kept here), and the documents built
    /// from them those it built.
    #[test]
    fn every_response_is_written_with_no_reallocation() {
        let ids = Ids {
            request: *b"0123456789ABCDEF",
            host: "host",
        };
        let mut wrong = Vec::new();
        for (operation, asked, head, checksums, key, r) in responses() {
            let route = Route {
                operation,
                bucket: Some("bucket".into()),
                key: key.map(Into::into),
            };
            let query = Query::parse(asked);
            let echo = Echo {
                route: &route,
                query: &query,
                principal: "owner",
                region: "eu-west-1",
                head,
                checksums,
            };
            // The field lines' buffer as a connection's earlier requests left it.
            let fields = Fields::new(Vec::with_capacity(1 << 14));
            let status = r.status;
            let (response, made) = counted(|| reply(&echo, r, &ids, fields));
            let response = match response {
                Ok(response) => response,
                Err((why, _)) => panic!("{operation:?} {status:?}: {why:?}"),
            };
            let document = &response.document;
            println!(
                "{operation:?} {status:?} {asked:?}: {made:?}, a document of {} bytes",
                document.len()
            );
            if made.1 != 0 || document.capacity() != document.len() {
                wrong.push(format!(
                    "{operation:?} {status:?}: {made:?}, capacity {} for {}",
                    document.capacity(),
                    document.len()
                ));
            }
            if status != Status::Done && !head {
                let expected = s3::error(&Failure {
                    code: refusal_code(operation, status),
                    message: &message(operation, status),
                    resource: Some(&reference_resource(&route)),
                    details: &[],
                    request_id: ids.request(),
                    host_id: ids.host,
                });
                if status != Status::NotModified {
                    assert_eq!(*document, expected.into_bytes(), "{operation:?} {status:?}");
                }
            }
            if operation == Operation::CompleteMultipartUpload && status == Status::Done {
                let value = reference_checksum_value(&stored(3)).unwrap();
                let location = format!(
                    "/bucket/{}",
                    mantle_s3::sigv4::uri_encode(KEY.as_bytes(), false)
                );
                let expected = s3::complete_multipart_upload(&s3::Completed {
                    location: &location,
                    bucket: "bucket",
                    key: KEY,
                    etag: &format!("{ETAG}-3"),
                    checksum: Some(s3::ObjectChecksum {
                        algorithm: Algorithm::Crc32c,
                        kind: ChecksumType::Composite,
                        value: &value,
                    }),
                });
                assert_eq!(*document, expected.into_bytes());
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
        // The strings themselves, each as `format!` made it before.
        for route in [
            Route {
                operation: Operation::GetObject,
                bucket: Some("bucket".into()),
                key: Some(KEY.into()),
            },
            Route {
                operation: Operation::ListObjectsV2,
                bucket: Some("bucket".into()),
                key: None,
            },
            Route {
                operation: Operation::ListBuckets,
                bucket: None,
                key: None,
            },
        ] {
            let (text, made) = counted(|| resource(&route));
            assert_eq!((made, text.capacity()), ((1, 0), text.len()));
            assert_eq!(text, reference_resource(&route));
        }
        for parts in [0, 1, 3, 10_000, u16::MAX] {
            let kept = stored(parts);
            let (value, made) = counted(|| checksum_value(&kept).unwrap());
            assert_eq!(
                (made, value.capacity()),
                ((2 + u64::from(parts != 0), 0), value.len()),
                "{parts} parts"
            );
            assert_eq!(Some(value), reference_checksum_value(&kept));
        }
        for cause in crate::door::Slowed::ALL {
            let (refused, made) = counted(|| slowed(cause));
            assert_eq!(
                (made, refused.message.capacity()),
                ((1, 0), refused.message.len()),
                "{cause:?}"
            );
            assert_eq!(
                refused.message,
                format!("Please reduce your request rate. {}", cause.words())
            );
        }
    }

    /// The code a refused reply's error document names.
    fn refusal_code(operation: Operation, status: Status) -> &'static str {
        match (operation, status) {
            (Operation::GetObject, Status::EntityTooLarge) => "EntityTooLarge",
            _ => refusal(status).code,
        }
    }

    /// The message a refused reply's error document names.
    fn message(operation: Operation, status: Status) -> String {
        match (operation, status) {
            (Operation::GetObject, Status::EntityTooLarge) => {
                "Your proposed download exceeds the maximum allowed size".into()
            }
            _ => refusal(status).message,
        }
    }
}
