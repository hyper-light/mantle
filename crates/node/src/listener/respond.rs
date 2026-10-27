//! The object host's reply as S3's response (node.md §4.4, step 8 of §4.2): a status and its
//! fields, and the document S3 answers the operation with, from `mantle_s3::response`. A refusal
//! is S3's error document, which names its code, its resource and the request; a reply whose
//! outcome is unknown is `500 InternalError`, never a success or a refusal with no effect.

use mantle_http::write::Length;
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
    pub fields: Vec<(&'static str, Vec<u8>)>,
    /// The body written with the head: a document, or nothing.
    pub document: Vec<u8>,
    /// What the head states of the body's length: the document's, or the object's that follows.
    pub length: Length,
    /// An object's bytes follow the head, as the host gives them.
    pub object: bool,
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
        _ => "",
    }
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
    };
    Refused::new(code, http, message)
}

/// What every response names: the request, and the host that answered it (05 §11.1).
pub struct Ids {
    pub request: String,
    pub host: String,
}

impl Ids {
    fn fields(&self, fields: &mut Vec<(&'static str, Vec<u8>)>) {
        fields.push(("x-amz-request-id", self.request.clone().into_bytes()));
        fields.push(("x-amz-id-2", self.host.clone().into_bytes()));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_secs()).ok())
            .and_then(mantle_s3::time::http_date);
        if let Some(date) = now {
            fields.push(("Date", date.into_bytes()));
        }
    }
}

/// An error response: S3's `Error` document naming `resource`, none for a HEAD, whose response
/// has no body (RFC 9110 §9.3.2).
pub fn error(refused: &Refused, resource: &str, ids: &Ids, head: bool) -> Response {
    let mut fields = Vec::new();
    ids.fields(&mut fields);
    let document = if head {
        Vec::new()
    } else {
        fields.push(("Content-Type", b"application/xml".to_vec()));
        s3::error(&Failure {
            code: refused.code,
            message: &refused.message,
            resource: Some(resource),
            details: &[],
            request_id: &ids.request,
            host_id: &ids.host,
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
    pub route: &'a Route,
    pub query: &'a Query,
    /// The account that asked: the owner a listing names.
    pub principal: &'a str,
    pub region: &'a str,
    /// The request was a HEAD.
    pub head: bool,
    /// `x-amz-checksum-mode: ENABLED`: a read's checksum is returned (05 §3.4).
    pub checksums: bool,
}

/// The response to the host's `reply`.
pub fn reply(echo: &Echo<'_>, reply: &Reply, ids: &Ids) -> Result<Response, Refused> {
    let resource = resource(echo.route);
    match reply.status {
        Status::Done => done(echo, reply, ids),
        Status::NotModified => {
            let mut fields = Vec::new();
            ids.fields(&mut fields);
            object_facts(reply, &mut fields)?;
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
            let mut response = error(&refusal(Status::NoSuchKey), &resource, ids, echo.head);
            response
                .fields
                .push(("x-amz-delete-marker", b"true".to_vec()));
            version(reply, &mut response.fields);
            Ok(response)
        }
        status => Ok(error(&refusal(status), &resource, ids, echo.head)),
    }
}

/// The resource an error names: the bucket and key as the request's path names them.
pub fn resource(route: &Route) -> String {
    match (&route.bucket, &route.key) {
        (Some(bucket), Some(key)) => format!("/{bucket}/{key}"),
        (Some(bucket), None) => format!("/{bucket}"),
        _ => "/".into(),
    }
}

fn internal(why: impl std::fmt::Display) -> Refused {
    Refused::new("InternalError", 500, why.to_string())
}

/// A response of `status` with no body.
fn empty(status: u16, fields: Vec<(&'static str, Vec<u8>)>) -> Response {
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
fn document(mut fields: Vec<(&'static str, Vec<u8>)>, document: String) -> Response {
    fields.push(("Content-Type", b"application/xml".to_vec()));
    let document = document.into_bytes();
    Response {
        status: 200,
        fields,
        length: Length::Known(u64::try_from(document.len()).unwrap_or(u64::MAX)),
        document,
        object: false,
    }
}

fn done(echo: &Echo<'_>, reply: &Reply, ids: &Ids) -> Result<Response, Refused> {
    let mut fields = Vec::new();
    ids.fields(&mut fields);
    let route = echo.route;
    let bucket = route.bucket.as_deref().unwrap_or_default();
    let key = route.key.as_deref().unwrap_or_default();
    Ok(match route.operation {
        Operation::CreateBucket => {
            fields.push(("Location", format!("/{bucket}").into_bytes()));
            empty(200, fields)
        }
        Operation::HeadBucket => {
            fields.push(("x-amz-bucket-region", echo.region.as_bytes().to_vec()));
            empty(200, fields)
        }
        // us-east-1 is S3's empty constraint (13 §9.5).
        Operation::GetBucketLocation => {
            let location = if echo.region == US_EAST_1 {
                ""
            } else {
                echo.region
            };
            document(fields, s3::location_constraint(location))
        }
        Operation::DeleteBucket | Operation::AbortMultipartUpload => empty(204, fields),
        Operation::PutBucketVersioning => empty(200, fields),
        Operation::GetBucketVersioning => {
            document(fields, s3::versioning_configuration(reply.versioning))
        }
        Operation::ListBuckets => document(fields, list_buckets(echo, reply)?),
        Operation::ListObjectsV2 => document(fields, list_objects(echo, reply)?),
        Operation::PutObject | Operation::UploadPart => {
            fields.push(("ETag", quoted(&reply.etag)));
            version(reply, &mut fields);
            checksum(reply, &mut fields);
            empty(200, fields)
        }
        Operation::GetObject | Operation::HeadObject => {
            object_facts(reply, &mut fields)?;
            fields.push(("Accept-Ranges", b"bytes".to_vec()));
            fields.push(("Content-Type", b"binary/octet-stream".to_vec()));
            if echo.checksums && reply.range.is_none() {
                checksum(reply, &mut fields);
            }
            let (status, length) = match reply.range {
                Some((first, last)) => {
                    fields.push((
                        "Content-Range",
                        format!("bytes {first}-{last}/{}", reply.size).into_bytes(),
                    ));
                    let length = last
                        .checked_sub(first)
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| internal("a range whose last byte is before its first"))?;
                    (206, length)
                }
                None => (200, reply.size),
            };
            Response {
                status,
                fields,
                document: Vec::new(),
                length: Length::Known(length),
                object: route.operation == Operation::GetObject,
            }
        }
        Operation::DeleteObject => {
            if reply.marker {
                fields.push(("x-amz-delete-marker", b"true".to_vec()));
            }
            version(reply, &mut fields);
            empty(204, fields)
        }
        Operation::CreateMultipartUpload => document(
            fields,
            s3::initiate_multipart_upload(bucket, key, &reply.upload),
        ),
        Operation::CompleteMultipartUpload => {
            version(reply, &mut fields);
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
            let location = format!(
                "/{bucket}/{}",
                mantle_s3::sigv4::uri_encode(key.as_bytes(), false)
            );
            document(
                fields,
                s3::complete_multipart_upload(&s3::Completed {
                    location: &location,
                    bucket,
                    key,
                    etag: &reply.etag,
                    checksum: object_checksum,
                }),
            )
        }
        Operation::ListParts => document(fields, list_parts(echo, reply)?),
        Operation::ListMultipartUploads => document(fields, list_uploads(echo, reply)?),
        other => return Err(internal(format!("a reply to {other:?}"))),
    })
}

fn quoted(etag: &str) -> Vec<u8> {
    format!("\"{etag}\"").into_bytes()
}

/// The version's id, where the bucket names versions: an unversioned bucket's are `null`, which
/// S3 leaves unsaid.
fn version(reply: &Reply, fields: &mut Vec<(&'static str, Vec<u8>)>) {
    if !reply.version.is_empty() && reply.version != "null" {
        fields.push(("x-amz-version-id", reply.version.clone().into_bytes()));
    }
}

/// An object's ETag, Last-Modified and version, as a read's response states them.
fn object_facts(reply: &Reply, fields: &mut Vec<(&'static str, Vec<u8>)>) -> Result<(), Refused> {
    fields.push(("ETag", quoted(&reply.etag)));
    let seconds = i64::try_from(reply.modified).map_err(|_| internal("a time past i64"))?;
    let modified =
        mantle_s3::time::http_date(seconds).ok_or_else(|| internal("a time past 9999"))?;
    fields.push(("Last-Modified", modified.into_bytes()));
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

/// A kept checksum's value as its header writes it: base64, a composite's followed by `-n`.
fn checksum_value(kept: &Stored) -> Option<String> {
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

fn checksum(reply: &Reply, fields: &mut Vec<(&'static str, Vec<u8>)>) {
    let Some(kept) = &reply.checksum else {
        return;
    };
    let (Some(algorithm), Some(value)) =
        (Algorithm::from_code(kept.algorithm), checksum_value(kept))
    else {
        return;
    };
    fields.push((algorithm.header(), value.into_bytes()));
    fields.push((
        "x-amz-checksum-type",
        kind(kept.parts).name().as_bytes().to_vec(),
    ));
}

fn seconds(value: u64) -> Result<i64, Refused> {
    i64::try_from(value).map_err(|_| internal("a time past i64"))
}

fn time(e: s3::TimeOutOfRange) -> Refused {
    Refused::new(e.code().0, e.code().1, e.to_string())
}

fn list_objects(echo: &Echo<'_>, reply: &Reply) -> Result<String, Refused> {
    let Some(Listing::Keys(page)) = &reply.listing else {
        return Err(internal("a listing reply without its page"));
    };
    let mut contents = Vec::with_capacity(page.contents.len());
    for listed in &page.contents {
        contents.push((
            listed.key.clone(),
            Entry {
                modified: seconds(listed.modified)?,
                etag: listed.etag.clone(),
                size: listed.size,
                owner: listed.owner.clone().unwrap_or_default(),
                checksum: listed
                    .checksum
                    .map(|(algorithm, parts)| (algorithm, kind(parts))),
            },
        ));
    }
    let listed = s3list::Page {
        contents,
        common_prefixes: page.common_prefixes.clone(),
        truncated: page.truncated,
        next: page.next.clone(),
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
