//! What an HTTP request asks (node.md §4.2, steps 2 to 4): its route by `mantle_s3::route`, its
//! signature by `mantle_s3::sigv4` against the key it claims, and the object host's operation it
//! makes, with how its body is read. The key is the account's root's, as `mantle init` wrote it
//! into the node's configuration, or a principal's, as the cell's root range holds it
//! (docs/design/principals.md); a principal's request goes ahead only as its identity policy
//! allows (`mantle_s3::policy::permits`). Who asks is the account the signing key acts for, never
//! a field the request supplies (audit §9); a mutation's identity is the SDK's invocation id under
//! that account, so its retries are one operation (gateway.md §2.1) and no principal can name
//! another's.

use mantle_http::{Framing, Head};
use mantle_s3::checksum::{self, Algorithm, Checksum};
use mantle_s3::chunked::Decoder;
use mantle_s3::list::{self as s3list, Resume};
use mantle_s3::policy::{self, Requester};
use mantle_s3::route::{self, Operation, Query, Route};
use mantle_s3::sigv4::{self, Body, Verified};

use super::Endpoint;
use crate::door::Signer;
use mantle_protocol::object::{
    self as native, Copy, CopyPart, Headers, List, ListVersions, MAX_CONDITION_TAG, Op, Target,
};
use mantle_s3::copy::{self, Directive};
use mantle_s3::metadata;

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
    /// DeleteObjects (research/89 §5): its document held to the `Content-MD5` or checksum its
    /// headers state, one of which it must carry; a principal's objects each judged by its policy.
    Deletes {
        bucket: String,
        id: u128,
        md5: Option<[u8; 16]>,
        checksum: Option<Checksum>,
        judge: Option<Box<Judge>>,
    },
}

/// The operation a document makes, and the places of a DeleteObjects' objects its principal's
/// policy does not let it delete, each answered `AccessDenied` in its place (research/91 §6).
pub type Made = (Op, Vec<usize>);

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
        upload: Streamed,
        make: Make,
    },
}

/// A request the listener will hand over: what it routes to, who asks, and its plan.
pub struct Asked {
    pub route: Route,
    pub principal: String,
    pub plan: Plan,
}

/// The key a request is signed with, as the cell holds it.
pub enum Key<'k> {
    /// One of the account's root's keys, from the node's configuration (`mantle init`).
    Root(&'k crate::process::AccessKey),
    /// A principal's, from the root range, under the account the cell's keys act for.
    Principal {
        signer: &'k Signer,
        account: &'k str,
    },
}

impl Key<'_> {
    fn secret(&self) -> &str {
        match self {
            Self::Root(key) => &key.secret,
            Self::Principal { signer, .. } => &signer.secret,
        }
    }

    /// The account a request signed with the key acts for: the root's, or the principal's own
    /// account's, which owns what its principals make, as an AWS account owns what its users
    /// make (research/91 §1).
    fn account(&self) -> &str {
        match self {
            Self::Root(key) => &key.account,
            Self::Principal { account, .. } => account,
        }
    }
}

/// Where a request came from and how, as a policy's global keys describe it: whether it came over
/// TLS, and its peer's address (research/91 §3).
#[derive(Clone, Copy)]
pub struct Source {
    pub secure: bool,
    pub address: Option<std::net::IpAddr>,
}

/// What `parts` routes to (node.md §4.2, step 2).
pub fn routed(parts: &Parts<'_>, endpoint: &Endpoint) -> Result<Route, Refused> {
    route::route(
        &endpoint.addressing,
        parts.method,
        parts.host,
        parts.path,
        parts.query,
        &parts.headers,
    )
    .map_err(|e| Refused::of(e.code(), &e))
}

/// The access key `parts`'s signature claims, its form checked before any key is looked up.
pub fn claimed(parts: &Parts<'_>) -> Result<String, Refused> {
    sigv4::access_key(&request_of(parts)).map_err(|e| Refused::of(e.code(), &e))
}

fn request_of<'p>(parts: &'p Parts<'p>) -> sigv4::Request<'p> {
    sigv4::Request {
        method: parts.method,
        path: parts.path,
        query: parts.query,
        headers: &parts.headers,
    }
}

/// Reads what `parts` asks of `route`, its signature checked at `now` (Unix seconds) against
/// `key`, the one it claimed, and a principal's request judged by its identity policy, from
/// `source`. `framing` is how its body is framed, which says whether it has one.
pub fn ask(
    parts: &Parts<'_>,
    (endpoint, route, key, source): (&Endpoint, Route, &Key<'_>, Source),
    now: i64,
    framing: Framing,
) -> Result<Asked, Refused> {
    let verified = sigv4::verify(&request_of(parts), &endpoint.region, now, |_| {
        Some(key.secret())
    })
    .map_err(|e| Refused::of(e.code(), &e))?;
    let query = Query::parse(parts.query);
    let judge = match key {
        Key::Principal { signer, account } => Some(Box::new(judge(
            signer,
            account,
            (parts, &route, &query),
            source,
            now,
        )?)),
        Key::Root(_) => None,
    };
    let principal = key.account().to_owned();
    let id = identity(parts, &principal)?;
    let plan = plan(parts, (&route, &query), verified, id, framing, judge)?;
    Ok(Asked {
        route,
        principal,
        plan,
    })
}

/// A principal's request as its identity policy judges it (research/91 §3): the policy, the
/// principal it is attached to, and the keys each of the request's actions is judged with, the
/// principal's and the request's: `aws:SecureTransport`, `aws:SourceIp`, `aws:CurrentTime` and
/// `aws:EpochTime`.
pub struct Judge {
    policy: policy::Policy,
    account: String,
    name: String,
    id: String,
    context: Vec<(String, Vec<String>)>,
}

impl Judge {
    fn requester(&self) -> Requester<'_> {
        Requester::User {
            account: &self.account,
            name: &self.name,
            id: &self.id,
        }
    }

    /// Whether the policy lets the principal take `action` on `resource`, judged with `context`.
    fn lets(&self, action: &str, resource: &str, context: &[(String, Vec<String>)]) -> bool {
        let request = policy::Request {
            requester: self.requester(),
            action,
            resource,
            context,
        };
        policy::permits(&self.policy, &request)
    }

    /// Whether the principal may delete `target` of `bucket`, one of a DeleteObjects' objects,
    /// each judged as a delete of its own ([`deleting`]): `s3:DeleteObject` always, and
    /// `s3:DeleteObjectVersion` too where it names a version (research/91 §6).
    fn deletes(&self, bucket: &str, target: &Target) -> bool {
        let resource = policy::resource(bucket, Some(&target.key));
        deleting(target.version.is_some())
            .iter()
            .all(|action| self.lets(action, &resource, &self.context))
    }
}

/// A principal's request judged by its identity policy (research/91 §3): each action its
/// operation asks ([`asks`]), with the principal's keys and the request's, and a listing's
/// `s3:prefix`, `s3:delimiter` and `s3:max-keys` where its action carries them. Anything its policy
/// does not allow is S3's `403 AccessDenied`, as is an operation S3 names no action for. The
/// judge, for a DeleteObjects' objects, which are judged once its document is read.
fn judge(
    signer: &Signer,
    account: &str,
    (parts, route, query): (&Parts<'_>, &Route, &Query),
    source: Source,
    now: i64,
) -> Result<Judge, Refused> {
    let denied = || Refused::new("AccessDenied", 403, "Access Denied");
    let kept = policy::kept(&signer.policy)
        .map_err(|_| Refused::new("InternalError", 500, "a principal's policy does not read"))?;
    let mut judge = Judge {
        policy: kept,
        account: account.to_owned(),
        name: signer.name.clone(),
        id: signer.id.clone(),
        context: Vec::new(),
    };
    let mut context = policy::principal_keys(judge.requester());
    let key = |name: &str, value: String| (name.to_owned(), vec![value]);
    context.push(key("aws:securetransport", source.secure.to_string()));
    if let Some(address) = source.address {
        context.push(key("aws:sourceip", address.to_string()));
    }
    context.push(key("aws:epochtime", now.to_string()));
    // ISO 8601, as IAM gives it, from the request's Unix seconds in milliseconds.
    if let Some(at) = now.checked_mul(1000).and_then(mantle_s3::time::iso8601) {
        context.push(key("aws:currenttime", at));
    }
    judge.context = context;
    let every = judge.context.len();
    for (action, resource) in asks(parts, route, query)? {
        // A listing's own keys, where its action carries them: ListBucket's and
        // ListBucketVersions', not ListBucketMultipartUploads' (`policy::carries`); judged with
        // its action alone.
        judge.context.extend(
            ["prefix", "delimiter", "max-keys"]
                .into_iter()
                .filter_map(|name| {
                    let named = format!("s3:{name}");
                    let value = query.value(name)?;
                    policy::carries(action, &named).then(|| key(&named, value.to_owned()))
                }),
        );
        let allowed = judge.lets(action, &resource, &judge.context);
        judge.context.truncate(every);
        if !allowed {
            return Err(denied());
        }
    }
    Ok(judge)
}

/// What a principal's request asks, each action on its resource, as S3 authorizes its operation
/// (research/91 §6): ListBuckets's `s3:ListAllMyBuckets` on S3's whole namespace; a CopyObject's
/// or an UploadPartCopy's `s3:PutObject` on its destination and `s3:GetObject` on its source,
/// `s3:GetObjectVersion` where it names the source's version; a DeleteObject's [`deleting`] on
/// its object; none for a DeleteObjects, whose objects are each judged as a delete once its
/// document is read ([`Judge::deletes`]); and any other operation's action on its bucket's or
/// object's ARN. An operation S3 names no action for asks what no policy grants: `AccessDenied`.
fn asks(
    parts: &Parts<'_>,
    route: &Route,
    query: &Query,
) -> Result<Vec<(&'static str, String)>, Refused> {
    let source = match route.operation {
        Operation::CopyObject | Operation::UploadPartCopy => Some(source(parts)?),
        _ => None,
    };
    asked(route, query.has("versionId"), source.as_ref())
}

/// The actions a delete asks, a DeleteObject's or one of a DeleteObjects' objects': S3's pages for
/// both say "you must always have the `s3:DeleteObject` permission" and "To delete a specific
/// version ... you must have the `s3:DeleteObjectVersion` permission", the permissions table either
/// one by whether a version is named; both where a version is named satisfies all three
/// (research/91 §6).
fn deleting(versioned: bool) -> &'static [&'static str] {
    if versioned {
        &["s3:DeleteObject", "s3:DeleteObjectVersion"]
    } else {
        &["s3:DeleteObject"]
    }
}

/// [`asks`] of `route`, `versioned` where its query names a version, and a copy's `source`; a
/// copy with no source asks what no policy grants.
fn asked(
    route: &Route,
    versioned: bool,
    source: Option<&copy::Source>,
) -> Result<Vec<(&'static str, String)>, Refused> {
    let denied = || Refused::new("AccessDenied", 403, "Access Denied");
    match (route.operation, source) {
        (Operation::DeleteObjects, _) => Ok(Vec::new()),
        (Operation::DeleteObject, _) => {
            let resource = policy::resource(&bucket(route)?, Some(&key(route)?));
            Ok(deleting(versioned)
                .iter()
                .map(|action| (*action, resource.clone()))
                .collect())
        }
        (Operation::CopyObject | Operation::UploadPartCopy, None) => Err(denied()),
        (Operation::CopyObject | Operation::UploadPartCopy, Some(source)) => {
            let read = if source.version.is_some() {
                "s3:GetObjectVersion"
            } else {
                "s3:GetObject"
            };
            Ok(vec![
                (
                    "s3:PutObject",
                    policy::resource(&bucket(route)?, Some(&key(route)?)),
                ),
                (read, policy::resource(&source.bucket, Some(&source.key))),
            ])
        }
        (operation, _) => {
            let action = policy::identity_action(operation, versioned).ok_or_else(denied)?;
            let resource = match &route.bucket {
                _ if operation == Operation::ListBuckets => policy::ACCOUNT_RESOURCE.to_owned(),
                Some(bucket) => policy::resource(bucket, route.key.as_deref()),
                None => return Err(denied()),
            };
            Ok(vec![(action, resource)])
        }
    }
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

/// A copy's `x-amz-copy-source-if-*`, in the preconditions' slots, and an UploadPartCopy's
/// `x-amz-copy-source-range` in the range's (research/89 §3.3, §4.1).
fn copy_conditions(parts: &Parts<'_>, range: bool) -> Headers {
    let take = |name: &str| parts.header(name).map(str::to_owned);
    Headers {
        if_match: take("x-amz-copy-source-if-match"),
        if_none_match: take("x-amz-copy-source-if-none-match"),
        if_modified_since: take("x-amz-copy-source-if-modified-since"),
        if_unmodified_since: take("x-amz-copy-source-if-unmodified-since"),
        range: if range {
            take("x-amz-copy-source-range")
        } else {
            None
        },
    }
}

/// The content headers and user-defined metadata a request's headers name, as a version keeps
/// them (research/89 §1).
fn stored(parts: &Parts<'_>) -> Result<native::Metadata, Refused> {
    metadata::of_request(parts.headers.iter().copied()).map_err(|e| Refused::of(e.code(), e))
}

/// The source `x-amz-copy-source` names (research/89 §3.1).
fn source(parts: &Parts<'_>) -> Result<copy::Source, Refused> {
    copy::source(parts.header("x-amz-copy-source").unwrap_or_default())
        .map_err(|e| Refused::of(e.code(), e))
}

/// The version `versionId` names, as sent.
fn version_id(query: &Query) -> Option<String> {
    query.value("versionId").map(str::to_owned)
}

/// A write's tag set, of which mantle keeps none yet (research/89 §3.8): one named, in the
/// `x-amz-tagging` header or in a presigned URL's query where SDK presigners move it
/// (s3-protocol.md §5), is refused rather than dropped; the empty one names none, and is taken.
fn untagged(parts: &Parts<'_>, query: &Query) -> Result<(), Refused> {
    match parts
        .header("x-amz-tagging")
        .or_else(|| query.value("x-amz-tagging"))
    {
        Some(tags) if !tags.trim().is_empty() => Err(not_implemented("Object tagging")),
        _ => Ok(()),
    }
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
    (route, query): (&Route, &Query),
    verified: Verified,
    id: u128,
    framing: Framing,
    judge: Option<Box<Judge>>,
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
            upload: upload(parts, verified, framing)?.0,
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
            upload: upload(parts, verified, framing)?.0,
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
            untagged(parts, query)?;
            let metadata = stored(parts)?;
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
                    metadata,
                },
                body: Some(body),
            })
        }
        Operation::GetObject => host(Op::Get {
            bucket: bucket(route)?,
            key: key(route)?,
            headers: headers(parts, true),
            version: version_id(query),
        }),
        Operation::HeadObject => host(Op::Head {
            bucket: bucket(route)?,
            key: key(route)?,
            headers: headers(parts, true),
            version: version_id(query),
        }),
        // A version's tag set, the empty one while mantle keeps none (research/89 §3.8): the
        // version a head reads, current or named, its absence answered as a head's is. Stock
        // clients ask it of every source they copy in parts.
        Operation::GetObjectTagging => host(Op::Head {
            bucket: bucket(route)?,
            key: key(route)?,
            headers: Headers::default(),
            version: version_id(query),
        }),
        Operation::DeleteObject => host(Op::Delete {
            bucket: bucket(route)?,
            key: key(route)?,
            id,
            headers: headers(parts, false),
            version: version_id(query),
        }),
        Operation::CreateMultipartUpload => {
            untagged(parts, query)?;
            host(Op::CreateUpload {
                bucket: bucket(route)?,
                key: key(route)?,
                id,
                checksum: upload_checksum(parts)?,
                metadata: stored(parts)?,
            })
        }
        Operation::CopyObject => {
            let directive =
                copy::directive(parts.header("x-amz-metadata-directive")).ok_or_else(|| {
                    Refused::new("InvalidArgument", 400, "Unknown metadata directive.")
                })?;
            // Under `x-amz-tagging-directive: COPY`, the default, the copy takes its source's tag
            // set, which is empty; a set replacing it is refused as a PUT's is (research/89 §3.8).
            if parts
                .header("x-amz-tagging-directive")
                .is_some_and(|d| d.trim() == "REPLACE")
            {
                untagged(parts, query)?;
            }
            let checksum = parts
                .header("x-amz-checksum-algorithm")
                .map(|name| {
                    Algorithm::from_name(name).ok_or_else(|| {
                        Refused::new(
                            "InvalidRequest",
                            400,
                            "Checksum algorithm provided is unsupported.",
                        )
                    })
                })
                .transpose()?;
            host(Op::Copy(Box::new(Copy {
                bucket: bucket(route)?,
                key: key(route)?,
                id,
                source: source(parts)?,
                conditions: copy_conditions(parts, false),
                headers: headers(parts, false),
                directive,
                metadata: match directive {
                    Directive::Copy => Vec::new(),
                    Directive::Replace => stored(parts)?,
                },
                checksum,
            })))
        }
        Operation::UploadPartCopy => host(Op::CopyPart(Box::new(CopyPart {
            bucket: bucket(route)?,
            key: key(route)?,
            upload: upload_id(query)?,
            number: part_number(query)?,
            id,
            source: source(parts)?,
            conditions: copy_conditions(parts, true),
        }))),
        Operation::DeleteObjects => {
            let (checksum, md5) = digests(parts)?;
            Ok(Plan::Document {
                limit: mantle_s3::body::DELETE_LIMIT,
                upload: upload(parts, verified, framing)?.0,
                make: Make::Deletes {
                    bucket: bucket(route)?,
                    id,
                    md5,
                    checksum,
                    judge,
                },
            })
        }
        Operation::ListObjectVersions => host(Op::ListVersions(list_versions(route, query)?)),
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
            upload: upload(parts, verified, framing)?.0,
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

/// ListObjectVersions' request (research/89 §2.3): its markers, the version ID marker only with
/// a key marker, and its page's size.
fn list_versions(route: &Route, query: &Query) -> Result<ListVersions, Refused> {
    let key_marker = query
        .value("key-marker")
        .filter(|m| !m.is_empty())
        .map(str::to_owned);
    let id_marker = query
        .value("version-id-marker")
        .filter(|m| !m.is_empty())
        .map(str::to_owned);
    if id_marker.is_some() && key_marker.is_none() {
        return Err(Refused::new(
            "InvalidArgument",
            400,
            "A version-id marker cannot be specified without a key marker.",
        ));
    }
    let max = s3list::max_keys(query.value("max-keys"))
        .map_err(|e| Refused::of(e.code(), e))?
        .min(s3list::MAX_KEYS);
    Ok(ListVersions {
        bucket: bucket(route)?,
        prefix: query.value("prefix").unwrap_or_default().to_owned(),
        delimiter: query.value("delimiter").unwrap_or_default().to_owned(),
        key_marker,
        id_marker,
        max: u32::try_from(max)
            .map_err(|_| Refused::new("InvalidArgument", 400, "max-keys past u32"))?,
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

/// The operation a document makes, once read whole, the checksum its trailer carried with it;
/// and for a principal's DeleteObjects, the places of the objects its policy refuses.
pub fn made(
    make: Make,
    document: &[u8],
    trailer: Option<Checksum>,
    region: &str,
) -> Result<Made, Refused> {
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
            Ok((Op::Create { bucket, id }, Vec::new()))
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
            Ok((
                Op::PutVersioning {
                    bucket,
                    id,
                    versioning,
                },
                Vec::new(),
            ))
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
            Ok((
                Op::Complete {
                    bucket,
                    key,
                    upload,
                    id,
                    headers,
                    parts,
                },
                Vec::new(),
            ))
        }
        Make::Deletes {
            bucket,
            id,
            md5,
            checksum,
            judge,
        } => {
            held(document, md5, checksum.or(trailer))?;
            let delete = mantle_s3::body::delete(document).map_err(malformed)?;
            let objects = delete
                .objects
                .into_iter()
                .map(|object| Target {
                    key: object.key,
                    version: object.version_id,
                    // An ETag longer than any mantle makes names no version, as the empty one
                    // names none (`object::MAX_CONDITION_TAG`).
                    etag: object.etag.map(|etag| {
                        if etag.len() > MAX_CONDITION_TAG {
                            String::new()
                        } else {
                            etag
                        }
                    }),
                })
                .collect::<Vec<Target>>();
            // A principal's objects each judged as a delete of its own; the host answers those
            // its policy refuses `AccessDenied` in their places, and deletes the rest
            // (research/91 §6).
            let denied = judge.map_or_else(Vec::new, |judge| {
                objects
                    .iter()
                    .enumerate()
                    .filter(|(_, target)| !judge.deletes(&bucket, target))
                    .map(|(at, _)| at)
                    .collect()
            });
            Ok((
                Op::DeleteObjects {
                    bucket,
                    id,
                    objects,
                    quiet: delete.quiet,
                },
                denied,
            ))
        }
    }
}

/// A DeleteObjects document held to its integrity header (research/89 §5.1): its `Content-MD5`,
/// and a checksum its headers or trailer state; one of them it must carry, "Missing required
/// header for this request: Content-MD5 OR x-amz-checksum-*" otherwise.
fn held(document: &[u8], md5: Option<[u8; 16]>, checksum: Option<Checksum>) -> Result<(), Refused> {
    if md5.is_none() && checksum.is_none() {
        return Err(Refused::new(
            "InvalidRequest",
            400,
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        ));
    }
    let digest = |algorithm| {
        checksum::checksum(algorithm, document)
            .map_err(|_| Refused::new("InternalError", 500, "a document's digest"))
    };
    if let Some(md5) = md5
        && digest(Algorithm::Md5)?.bytes.as_slice() != md5.as_slice()
    {
        return Err(Refused::new(
            "BadDigest",
            400,
            "The Content-MD5 you specified did not match what we received.",
        ));
    }
    if let Some(stated) = checksum
        && digest(stated.algorithm)? != stated
    {
        return Err(Refused::new(
            "BadDigest",
            400,
            format!(
                "The {} you specified did not match the calculated checksum.",
                stated.algorithm.name()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(operation: Operation, key: Option<&str>) -> Route {
        Route {
            operation,
            bucket: Some("dst".into()),
            key: key.map(str::to_owned),
        }
    }

    /// A copy asks for both its objects as S3 authorizes one (research/91 §6): `s3:PutObject` on
    /// its destination and `s3:GetObject` on its source, `s3:GetObjectVersion` where it names the
    /// source's version, and nothing with no source; a batch delete asks for nothing until its
    /// objects are read; any other operation for its own action on its own resource.
    #[test]
    fn a_copy_asks_for_its_source_and_its_destination() {
        let source = |version: Option<&str>| copy::Source {
            bucket: "src".into(),
            key: "s/k".into(),
            version: version.map(str::to_owned),
        };
        for operation in [Operation::CopyObject, Operation::UploadPartCopy] {
            let copy = route(operation, Some("d/k"));
            assert_eq!(
                asked(&copy, false, Some(&source(None))).unwrap(),
                [
                    ("s3:PutObject", "arn:aws:s3:::dst/d/k".to_owned()),
                    ("s3:GetObject", "arn:aws:s3:::src/s/k".to_owned()),
                ]
            );
            assert_eq!(
                asked(&copy, false, Some(&source(Some("null")))).unwrap(),
                [
                    ("s3:PutObject", "arn:aws:s3:::dst/d/k".to_owned()),
                    ("s3:GetObjectVersion", "arn:aws:s3:::src/s/k".to_owned()),
                ]
            );
            assert_eq!(asked(&copy, false, None).unwrap_err().code, "AccessDenied");
        }
        assert!(
            asked(&route(Operation::DeleteObjects, None), false, None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            asked(&route(Operation::GetObject, Some("k")), true, None).unwrap(),
            [("s3:GetObjectVersion", "arn:aws:s3:::dst/k".to_owned())]
        );
        // A delete asks `s3:DeleteObject` always, and `s3:DeleteObjectVersion` too where it names a
        // version.
        let delete = route(Operation::DeleteObject, Some("k"));
        assert_eq!(
            asked(&delete, false, None).unwrap(),
            [("s3:DeleteObject", "arn:aws:s3:::dst/k".to_owned())]
        );
        assert_eq!(
            asked(&delete, true, None).unwrap(),
            [
                ("s3:DeleteObject", "arn:aws:s3:::dst/k".to_owned()),
                ("s3:DeleteObjectVersion", "arn:aws:s3:::dst/k".to_owned()),
            ]
        );
        let buckets = Route {
            operation: Operation::ListBuckets,
            bucket: None,
            key: None,
        };
        assert_eq!(
            asked(&buckets, false, None).unwrap(),
            [("s3:ListAllMyBuckets", "arn:aws:s3:::*".to_owned())]
        );
    }
}
