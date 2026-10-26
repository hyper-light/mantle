//! What an HTTP request asks (node.md §4.2, steps 2 and 3): its route by `mantle_s3::route`, its
//! signature by `mantle_s3::sigv4` against the cell's access keys, and the object host's operation
//! it makes, with how its body is read. Who asks is the account the signing key acts for, never a
//! field the request supplies (audit §9); a mutation's identity is the SDK's invocation id under
//! that account, so its retries are one operation (gateway.md §2.1) and no principal can name
//! another's.

use mantle_http::{Framing, Head};
use mantle_s3::checksum::{self, Algorithm, Checksum};
use mantle_s3::chunked::Decoder;
use mantle_s3::list::{self as s3list, Resume};
use mantle_s3::route::{self, Operation, Query, Route};
use mantle_s3::sigv4::{self, Body, Verified};

use super::Endpoint;
use mantle_protocol::object::{Headers, List, Op};

/// The largest object a single PUT or part carries: 5 GiB (05 §4.3).
const MAX_OBJECT: u64 = mantle_s3::body::MAX_UPLOAD;

/// The most octets a request's body may run to: the largest object, framed as aws-chunked in
/// S3's least chunks, each behind its longest size line and its line ends (05 §1.8), and its
/// trailer section within a head's bound.
pub const MAX_BODY: u64 = MAX_OBJECT
    + (MAX_OBJECT / mantle_s3::chunked::MIN_CHUNK + 1)
        * (mantle_s3::chunked::MAX_LINE as u64 + "\r\n\r\n".len() as u64)
    + mantle_http::MAX_HEAD as u64;

/// A refusal the listener answers itself: S3's code, the HTTP status, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub code: &'static str,
    pub status: u16,
    pub message: String,
}

impl Refused {
    pub fn new(code: &'static str, status: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            status,
            message: message.into(),
        }
    }

    fn of((code, status): (&'static str, u16), message: impl std::fmt::Display) -> Self {
        Self::new(code, status, message.to_string())
    }
}

/// A request's parts as its head holds them, each borrowed from the head's octets.
pub struct Parts<'h> {
    pub method: &'h str,
    /// The path as sent, still percent-encoded.
    pub path: &'h str,
    /// The query as sent, without its `?`.
    pub query: &'h str,
    pub host: Option<&'h str>,
    pub headers: Vec<(&'h str, &'h str)>,
}

impl<'h> Parts<'h> {
    /// The parts of `head`, parsed from `buf`. A request whose method, target or a field is not
    /// UTF-8 names nothing S3 serves: S3's names and values are ASCII and its keys UTF-8.
    pub fn of(head: &'h Head, buf: &'h [u8]) -> Result<Self, Refused> {
        let text = |octets: &'h [u8]| {
            std::str::from_utf8(octets)
                .map_err(|_| Refused::new("InvalidArgument", 400, "a request that is not UTF-8"))
        };
        let origin = text(head.origin(buf))?;
        let (path, query) = origin.split_once('?').unwrap_or((origin, ""));
        let mut headers = Vec::with_capacity(head.fields(buf).size_hint().0);
        for (name, value) in head.fields(buf) {
            headers.push((text(name)?, text(value)?));
        }
        Ok(Self {
            method: text(head.method(buf))?,
            path,
            query,
            host: head.host(buf).map(text).transpose()?,
            headers,
        })
    }

    /// The first field named `name`, case-insensitively, as SigV4 and the router read it.
    pub fn header(&self, name: &str) -> Option<&'h str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| *v)
    }
}

/// How an object's body is read.
pub enum Upload {
    /// The octets as framed; their SHA-256, where the signature covers it, checked at their end.
    Plain(Option<[u8; 32]>),
    /// aws-chunked within the framing (05 §1.8–§1.9): each chunk's signature checked as it comes,
    /// and the trailing checksum the decoder reads.
    Chunked(Box<Decoder>),
}

/// The operation a document read whole makes.
pub enum Make {
    CreateBucket {
        bucket: String,
        id: u128,
    },
    Versioning {
        bucket: String,
        id: u128,
    },
    Complete {
        bucket: String,
        key: String,
        upload: String,
        id: u128,
        headers: Headers,
    },
}

/// An object's body: how it is read, its object's length, and the algorithm of the checksum its
/// trailer carries, if it has one.
pub type Streamed = (Upload, u64);

/// What the request asks of the host.
pub enum Plan {
    /// `op` handed over now; an object's body, if it has one, read as the host's driver wants it.
    Host { op: Op, body: Option<Streamed> },
    /// A document of at most `limit` octets read whole, then the operation it makes.
    Document {
        limit: usize,
        upload: Upload,
        make: Make,
    },
}

/// A request the listener will hand over: what it routes to, who asks, and its plan.
pub struct Asked {
    pub route: Route,
    pub principal: String,
    pub plan: Plan,
}

/// Reads what `parts` asks, its signature checked at `now` (Unix seconds). `framing` is how its
/// body is framed, which says whether it has one.
pub fn ask(
    parts: &Parts<'_>,
    endpoint: &Endpoint,
    now: i64,
    framing: Framing,
) -> Result<Asked, Refused> {
    let route = route::route(
        &endpoint.addressing,
        parts.method,
        parts.host,
        parts.path,
        parts.query,
        &parts.headers,
    )
    .map_err(|e| Refused::of(e.code(), &e))?;
    let request = sigv4::Request {
        method: parts.method,
        path: parts.path,
        query: parts.query,
        headers: &parts.headers,
    };
    let verified = sigv4::verify(&request, &endpoint.region, now, |id| {
        endpoint.key(id).map(|key| key.secret.clone())
    })
    .map_err(|e| Refused::of(e.code(), &e))?;
    let principal = endpoint
        .key(&verified.access_key)
        .map(|key| key.account.clone())
        .ok_or_else(|| Refused::new("InvalidAccessKeyId", 403, "the access key is not known"))?;
    let id = identity(parts, &principal)?;
    let query = Query::parse(parts.query);
    let plan = plan(parts, &route, &query, verified, id, framing)?;
    Ok(Asked {
        route,
        principal,
        plan,
    })
}

/// A mutation's identity: the SDK's invocation id, which every attempt of one operation carries,
/// under the account that asks, so two accounts' ids never meet; a request without one is an
/// operation of its own, its identity drawn.
fn identity(parts: &Parts<'_>, principal: &str) -> Result<u128, Refused> {
    let internal = || Refused::new("InternalError", 500, "the operation's identity");
    let digest = match parts.header("amz-sdk-invocation-id") {
        Some(invocation) => {
            let mut named = Vec::with_capacity(principal.len().saturating_add(invocation.len()));
            named.extend_from_slice(principal.as_bytes());
            named.push(0);
            named.extend_from_slice(invocation.as_bytes());
            mantle_s3::crypto::sha256(&named).map_err(|_| internal())?
        }
        None => {
            let mut drawn = [0u8; 32];
            getrandom::fill(&mut drawn).map_err(|_| internal())?;
            drawn
        }
    };
    let mut octets = [0u8; 16];
    for (to, from) in octets.iter_mut().zip(digest.iter()) {
        *to = *from;
    }
    Ok(u128::from_le_bytes(octets))
}

/// The bucket a route names, which every operation but ListBuckets has.
fn bucket(route: &Route) -> Result<String, Refused> {
    route
        .bucket
        .clone()
        .ok_or_else(|| Refused::new("InvalidRequest", 400, "no bucket named"))
}

fn key(route: &Route) -> Result<String, Refused> {
    route
        .key
        .clone()
        .ok_or_else(|| Refused::new("InvalidRequest", 400, "no key named"))
}

/// Whether the framing carries a body.
fn has_body(framing: Framing) -> bool {
    !matches!(framing, Framing::None | Framing::Length(0))
}

/// The preconditions and range a request sends (05 §2, §12), as the host judges them.
fn headers(parts: &Parts<'_>, range: bool) -> Headers {
    let take = |name: &str| parts.header(name).map(str::to_owned);
    Headers {
        if_match: take("if-match"),
        if_none_match: take("if-none-match"),
        if_modified_since: take("if-modified-since"),
        if_unmodified_since: take("if-unmodified-since"),
        range: if range { take("range") } else { None },
    }
}

fn plan(
    parts: &Parts<'_>,
    route: &Route,
    query: &Query,
    verified: Verified,
    id: u128,
    framing: Framing,
) -> Result<Plan, Refused> {
    let host = |op| Ok(Plan::Host { op, body: None });
    match route.operation {
        Operation::ListBuckets => host(Op::ListBuckets {
            prefix: query.value("prefix").unwrap_or_default().to_owned(),
            after: query
                .value("continuation-token")
                .filter(|t| !t.is_empty())
                .map(str::to_owned),
            max: count(
                query.value("max-buckets"),
                mantle_protocol::object::MAX_BUCKETS,
            )?,
        }),
        Operation::CreateBucket if has_body(framing) => Ok(Plan::Document {
            limit: mantle_s3::body::CREATE_BUCKET_LIMIT,
            upload: upload(parts, verified, framing)?.0.0,
            make: Make::CreateBucket {
                bucket: bucket(route)?,
                id,
            },
        }),
        Operation::CreateBucket => host(Op::Create {
            bucket: bucket(route)?,
            id,
        }),
        Operation::HeadBucket | Operation::GetBucketLocation => host(Op::HeadBucket {
            bucket: bucket(route)?,
        }),
        Operation::DeleteBucket => host(Op::DeleteBucket {
            bucket: bucket(route)?,
            id,
        }),
        Operation::PutBucketVersioning => Ok(Plan::Document {
            limit: mantle_s3::body::VERSIONING_LIMIT,
            upload: upload(parts, verified, framing)?.0.0,
            make: Make::Versioning {
                bucket: bucket(route)?,
                id,
            },
        }),
        Operation::GetBucketVersioning => host(Op::GetVersioning {
            bucket: bucket(route)?,
        }),
        Operation::ListObjectsV2 => host(Op::List(list(route, query)?)),
        Operation::PutObject => {
            let (body, trailing) = upload(parts, verified, framing)?;
            let (checksum, md5) = digests(parts)?;
            single(checksum.as_ref(), trailing)?;
            Ok(Plan::Host {
                op: Op::Put {
                    bucket: bucket(route)?,
                    key: key(route)?,
                    id,
                    length: body.1,
                    headers: headers(parts, false),
                    checksum,
                    md5,
                    trailing,
                },
                body: Some(body),
            })
        }
        Operation::GetObject => host(Op::Get {
            bucket: bucket(route)?,
            key: key(route)?,
            headers: headers(parts, true),
        }),
        Operation::HeadObject => host(Op::Head {
            bucket: bucket(route)?,
            key: key(route)?,
            headers: headers(parts, true),
        }),
        Operation::DeleteObject if query.has("versionId") => Err(not_implemented(
            "DeleteObject of a version by its ID on the S3 listener",
        )),
        Operation::DeleteObject => host(Op::Delete {
            bucket: bucket(route)?,
            key: key(route)?,
            id,
            headers: headers(parts, false),
        }),
        Operation::CreateMultipartUpload => host(Op::CreateUpload {
            bucket: bucket(route)?,
            key: key(route)?,
            id,
            checksum: upload_checksum(parts)?,
        }),
        Operation::UploadPart => {
            let (body, trailing) = upload(parts, verified, framing)?;
            let (checksum, md5) = digests(parts)?;
            single(checksum.as_ref(), trailing)?;
            Ok(Plan::Host {
                op: Op::UploadPart {
                    bucket: bucket(route)?,
                    key: key(route)?,
                    upload: upload_id(query)?,
                    number: part_number(query)?,
                    id,
                    length: body.1,
                    checksum,
                    md5,
                    trailing,
                },
                body: Some(body),
            })
        }
        Operation::CompleteMultipartUpload => Ok(Plan::Document {
            limit: mantle_s3::body::COMPLETE_LIMIT,
            upload: upload(parts, verified, framing)?.0.0,
            make: Make::Complete {
                bucket: bucket(route)?,
                key: key(route)?,
                upload: upload_id(query)?,
                id,
                headers: headers(parts, false),
            },
        }),
        Operation::AbortMultipartUpload => host(Op::Abort {
            bucket: bucket(route)?,
            key: key(route)?,
            upload: upload_id(query)?,
            id,
        }),
        Operation::ListParts => host(Op::ListParts {
            bucket: bucket(route)?,
            key: key(route)?,
            upload: upload_id(query)?,
            after: match query.value("part-number-marker") {
                None => 0,
                Some(marker) => marker.trim().parse().map_err(|_| {
                    Refused::new("InvalidArgument", 400, "part-number-marker is not a number")
                })?,
            },
            max: count(
                query.value("max-parts"),
                mantle_protocol::object::MAX_PAGE_PARTS,
            )?,
        }),
        Operation::ListMultipartUploads => host(Op::ListUploads {
            bucket: bucket(route)?,
            prefix: query.value("prefix").unwrap_or_default().to_owned(),
            delimiter: query.value("delimiter").unwrap_or_default().to_owned(),
            key_marker: query
                .value("key-marker")
                .filter(|m| !m.is_empty())
                .map(str::to_owned),
            id_marker: query
                .value("upload-id-marker")
                .filter(|m| !m.is_empty())
                .map(str::to_owned),
            max: count(query.value("max-uploads"), s3list::MAX_KEYS)?,
        }),
        other => Err(not_implemented(&format!("{other:?} on the S3 listener"))),
    }
}

fn not_implemented(what: &str) -> Refused {
    Refused::new("NotImplemented", 501, format!("{what} is not implemented"))
}

/// A page's size as sent, at most `most`; absent is `most`.
fn count(value: Option<&str>, most: usize) -> Result<u32, Refused> {
    let asked = match value {
        None => most,
        Some(v) => v.trim().parse::<usize>().map_err(|_| {
            Refused::new("InvalidArgument", 400, "a page size that is not a number")
        })?,
    };
    u32::try_from(asked.min(most))
        .map_err(|_| Refused::new("InvalidArgument", 400, "a page size past u32"))
}

/// ListObjectsV2's request (05 §6.2): a continuation token mantle issued, else `start-after`.
fn list(route: &Route, query: &Query) -> Result<List, Refused> {
    let start = match query.value("continuation-token") {
        Some(token) => Some(s3list::continuation_start(token).ok_or_else(|| {
            Refused::new(
                "InvalidArgument",
                400,
                "The continuation token provided is incorrect",
            )
        })?),
        None => query
            .value("start-after")
            .filter(|after| !after.is_empty())
            .map(|after| Resume::After(after.to_owned())),
    };
    let max_keys = s3list::max_keys(query.value("max-keys"))
        .map_err(|e| Refused::of(e.code(), e))?
        .min(s3list::MAX_KEYS);
    Ok(List {
        bucket: bucket(route)?,
        prefix: query.value("prefix").unwrap_or_default().to_owned(),
        delimiter: query.value("delimiter").unwrap_or_default().to_owned(),
        start,
        max_keys: u32::try_from(max_keys)
            .map_err(|_| Refused::new("InvalidArgument", 400, "max-keys past u32"))?,
        fetch_owner: query.value("fetch-owner") == Some("true"),
    })
}

fn upload_id(query: &Query) -> Result<String, Refused> {
    query
        .value("uploadId")
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Refused::new("InvalidRequest", 400, "no uploadId"))
}

/// A part's number, 1 to 10,000 (05 §4.3).
fn part_number(query: &Query) -> Result<u16, Refused> {
    let refused = || {
        Refused::new(
            "InvalidArgument",
            400,
            "Part number must be an integer between 1 and 10000, inclusive",
        )
    };
    let number: u16 = query
        .value("partNumber")
        .ok_or_else(refused)?
        .trim()
        .parse()
        .map_err(|_| refused())?;
    if (1..=mantle_s3::body::MAX_PARTS).contains(&number) {
        Ok(number)
    } else {
        Err(refused())
    }
}

/// A body states one checksum at most, in a header or in its trailer (05 §3.2).
fn single(header: Option<&Checksum>, trailing: Option<Algorithm>) -> Result<(), Refused> {
    if header.is_some() && trailing.is_some() {
        return Err(Refused::new(
            "InvalidRequest",
            400,
            "Expecting a single x-amz-checksum- header",
        ));
    }
    Ok(())
}

/// The checksum a body's headers state, and its `Content-MD5` (05 §3.2): at most one checksum
/// header, each value its algorithm's width.
fn digests(parts: &Parts<'_>) -> Result<(Option<Checksum>, Option<[u8; 16]>), Refused> {
    let mut checksum = None;
    for (name, value) in &parts.headers {
        let Some(algorithm) = Algorithm::from_header(name) else {
            continue;
        };
        if checksum.is_some() {
            return Err(Refused::new(
                "InvalidRequest",
                400,
                "Expecting a single x-amz-checksum- header",
            ));
        }
        checksum = Some(Checksum::from_base64(algorithm, value).ok_or_else(|| {
            Refused::new(
                "InvalidRequest",
                400,
                format!("Value for {} header is invalid.", algorithm.header()),
            )
        })?);
    }
    let md5 = match parts.header("content-md5") {
        None => None,
        Some(text) => Some(checksum::content_md5(text).ok_or_else(|| {
            Refused::new(
                "InvalidDigest",
                400,
                "The Content-MD5 you specified was invalid.",
            )
        })?),
    };
    Ok((checksum, md5))
}

/// The checksum CreateMultipartUpload names its parts with, and whether their values combine as
/// one full-object value (05 §3.3): `x-amz-checksum-type` where sent, else CRC-64/NVME's full
/// object, the others' composite.
fn upload_checksum(parts: &Parts<'_>) -> Result<Option<(Algorithm, bool)>, Refused> {
    let Some(name) = parts.header("x-amz-checksum-algorithm") else {
        return Ok(None);
    };
    let algorithm = Algorithm::from_name(name).ok_or_else(|| {
        Refused::new(
            "InvalidRequest",
            400,
            "Checksum algorithm provided is unsupported.",
        )
    })?;
    let full = match parts.header("x-amz-checksum-type") {
        None => algorithm == Algorithm::Crc64Nvme,
        Some(kind) if kind.eq_ignore_ascii_case("FULL_OBJECT") => true,
        Some(kind) if kind.eq_ignore_ascii_case("COMPOSITE") => false,
        Some(_) => {
            return Err(Refused::new(
                "InvalidRequest",
                400,
                "The x-amz-checksum-type header value is invalid.",
            ));
        }
    };
    Ok(Some((algorithm, full)))
}

/// How a body is read, and its object's length: aws-chunked's decoded length as
/// `x-amz-decoded-content-length` states it, else the framing's. A body whose length is not
/// stated before it comes is refused, as S3 refuses a PUT without one (`411
/// MissingContentLength`), and one past S3's largest object is refused before any of it is read.
fn upload(
    parts: &Parts<'_>,
    verified: Verified,
    framing: Framing,
) -> Result<(Streamed, Option<Algorithm>), Refused> {
    let missing = || {
        Refused::new(
            "MissingContentLength",
            411,
            "You must provide the Content-Length HTTP header.",
        )
    };
    let Verified { body, chain, .. } = verified;
    // The checksum a trailer carries is the one `x-amz-trailer` names, which the decoder holds the
    // trailer to.
    let algorithm = |named: &str| {
        Algorithm::from_header(named).ok_or_else(|| {
            Refused::new(
                "InvalidRequest",
                400,
                format!("The trailer {named} names no checksum S3 takes."),
            )
        })
    };
    let (upload, length, trailing) = match body {
        Body::Sha256(sha256) => (
            Upload::Plain(Some(sha256)),
            framed(framing).ok_or_else(missing)?,
            None,
        ),
        Body::Unsigned => (
            Upload::Plain(None),
            framed(framing).ok_or_else(missing)?,
            None,
        ),
        Body::SignedChunks { trailer } => {
            let length = decoded(parts).ok_or_else(missing)??;
            let chain = chain
                .ok_or_else(|| Refused::new("InternalError", 500, "a signed body with no chain"))?;
            let named = trailer.then(|| parts.header("x-amz-trailer")).flatten();
            let trailing = named.map(algorithm).transpose()?;
            (
                Upload::Chunked(Box::new(Decoder::signed(chain, named, length))),
                length,
                trailing,
            )
        }
        Body::UnsignedChunks => {
            let length = decoded(parts).ok_or_else(missing)??;
            let named = parts.header("x-amz-trailer").ok_or_else(|| {
                Refused::new("InvalidRequest", 400, "an unsigned body names no trailer")
            })?;
            (
                Upload::Chunked(Box::new(Decoder::unsigned(named, length))),
                length,
                Some(algorithm(named)?),
            )
        }
    };
    if length > MAX_OBJECT {
        return Err(Refused::new(
            "EntityTooLarge",
            400,
            "Your proposed upload exceeds the maximum allowed size",
        ));
    }
    Ok(((upload, length), trailing))
}

/// The octets the framing states before the body comes.
fn framed(framing: Framing) -> Option<u64> {
    match framing {
        Framing::None => Some(0),
        Framing::Length(n) => Some(n),
        Framing::Chunked => None,
    }
}

/// `x-amz-decoded-content-length`, where sent.
fn decoded(parts: &Parts<'_>) -> Option<Result<u64, Refused>> {
    parts.header("x-amz-decoded-content-length").map(|text| {
        text.trim().parse().map_err(|_| {
            Refused::new(
                "InvalidArgument",
                400,
                "x-amz-decoded-content-length is not a number",
            )
        })
    })
}

/// The operation a document makes, once read whole.
pub fn made(make: Make, document: &[u8], region: &str) -> Result<Op, Refused> {
    let malformed = |e: mantle_s3::body::BodyError| Refused::of(e.code(), &e);
    match make {
        Make::CreateBucket { bucket, id } => {
            let configuration = mantle_s3::body::create_bucket(document).map_err(malformed)?;
            // A bucket is made in the cell's region: a constraint naming another is S3's
            // `IllegalLocationConstraintException`; us-east-1's is S3's empty one (13 §6.5).
            match configuration.location.as_deref() {
                None | Some("") => {}
                Some(location) if location == region => {}
                Some(_) => {
                    return Err(Refused::new(
                        "IllegalLocationConstraintException",
                        400,
                        format!("The specified location-constraint is not valid for {region}."),
                    ));
                }
            }
            if !configuration.tags.is_empty() {
                return Err(not_implemented("A new bucket's tags"));
            }
            Ok(Op::Create { bucket, id })
        }
        Make::Versioning { bucket, id } => {
            let configuration = mantle_s3::body::versioning(document).map_err(malformed)?;
            if configuration.mfa_delete == Some(true) {
                return Err(not_implemented("MFA delete"));
            }
            let versioning = match configuration.status {
                Some(versioning) => versioning,
                None => {
                    return Err(Refused::new(
                        "MalformedXML",
                        400,
                        "The XML you provided was not well-formed or did not validate against our published schema",
                    ));
                }
            };
            Ok(Op::PutVersioning {
                bucket,
                id,
                versioning,
            })
        }
        Make::Complete {
            bucket,
            key,
            upload,
            id,
            headers,
        } => {
            let listed = mantle_s3::body::complete(document).map_err(malformed)?;
            let parts = listed
                .into_iter()
                .map(|part| {
                    let etag = part.etag.trim().trim_matches('"').to_owned();
                    (part.number, etag)
                })
                .collect();
            Ok(Op::Complete {
                bucket,
                key,
                upload,
                id,
                headers,
                parts,
            })
        }
    }
}
