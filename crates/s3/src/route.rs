//! What an S3 request asks for: its bucket and key, from the `Host` header or the path, and its
//! operation, from the method, the query's subresources and a few headers (docs/research/05
//! §10, §14).
//!
//! S3 reads the bucket from the host in virtual-hosted style (`bucket.s3.example.com/key`)
//! and from the first path segment in path style (`s3.example.com/bucket/key`), and a host it
//! does not recognize is taken as a CNAME naming the bucket (05 §14). mantle serves the domains
//! it is configured with, treats an IP address or `localhost` as path style, and takes the
//! CNAME rule only when configured to: otherwise a request to an address the deployment did
//! not expect would silently name a bucket.

use crate::sigv4::percent_decode;

/// The endpoint's addressing.
#[derive(Debug, Clone, Default)]
pub struct Endpoint {
    /// Domains whose subdomains name buckets, lowercase, e.g. `s3.example.com`. A request to
    /// one of these domains itself is path style.
    pub domains: Vec<String>,
    /// Take any other host as the bucket's name (S3's CNAME rule).
    pub cname: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    ListBuckets,
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    GetBucketLocation,
    GetBucketVersioning,
    PutBucketVersioning,
    GetBucketTagging,
    PutBucketTagging,
    DeleteBucketTagging,
    GetBucketAcl,
    PutBucketAcl,
    GetBucketOwnershipControls,
    PutBucketOwnershipControls,
    DeleteBucketOwnershipControls,
    GetBucketLifecycleConfiguration,
    PutBucketLifecycleConfiguration,
    DeleteBucketLifecycle,
    GetBucketCors,
    PutBucketCors,
    DeleteBucketCors,
    GetBucketPolicy,
    PutBucketPolicy,
    DeleteBucketPolicy,
    /// A CORS preflight, `OPTIONS` on a bucket or an object.
    Preflight,
    ListObjects,
    ListObjectsV2,
    ListObjectVersions,
    ListMultipartUploads,
    DeleteObjects,
    PutObject,
    CopyObject,
    GetObject,
    HeadObject,
    DeleteObject,
    GetObjectAttributes,
    GetObjectTagging,
    PutObjectTagging,
    DeleteObjectTagging,
    GetObjectAcl,
    PutObjectAcl,
    CreateMultipartUpload,
    UploadPart,
    UploadPartCopy,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
}

/// A request's operation and what it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub operation: Operation,
    pub bucket: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    #[error("the URI could not be parsed")]
    InvalidUri,
    #[error("the key is longer than 1024 bytes")]
    KeyTooLong,
    #[error("the method is not allowed against this resource")]
    MethodNotAllowed,
    #[error("{0} is not implemented")]
    NotImplemented(&'static str),
}

impl RouteError {
    /// The S3 error code and HTTP status (05 §11.2).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::InvalidUri => ("InvalidURI", 400),
            Self::KeyTooLong => ("KeyTooLongError", 400),
            Self::MethodNotAllowed => ("MethodNotAllowed", 405),
            Self::NotImplemented(_) => ("NotImplemented", 501),
        }
    }
}

/// "The object key name consists of a sequence of Unicode characters encoded in UTF-8, with a
/// maximum length of 1,024 bytes" (05 §10.1).
pub const MAX_KEY: usize = 1024;

/// Bucket subresources S3 defines that mantle does not serve: answered `501 NotImplemented`
/// rather than taken for another operation.
const UNSUPPORTED: [&str; 17] = [
    "accelerate",
    "analytics",
    "encryption",
    "intelligent-tiering",
    "inventory",
    "logging",
    "metrics",
    "notification",
    "object-lock",
    "policyStatus",
    "publicAccessBlock",
    "replication",
    "requestPayment",
    "website",
    "legal-hold",
    "retention",
    "restore",
];

/// Finds a request's route. `host` is the `Host` header, `path` the request target's path as
/// sent, still percent-encoded, and `query` its query string.
pub fn route(
    endpoint: &Endpoint,
    method: &str,
    host: Option<&str>,
    path: &str,
    query: &str,
    headers: &[(&str, &str)],
) -> Result<Route, RouteError> {
    let (bucket, key) = target(endpoint, host, path)?;
    // A preflight asks about the request it precedes, whose parameters it may carry: it is
    // answered from the bucket's CORS rules whatever its query (16 §3).
    if method == "OPTIONS" {
        return match bucket {
            Some(_) => Ok(Route {
                operation: Operation::Preflight,
                bucket,
                key,
            }),
            None => Err(RouteError::MethodNotAllowed),
        };
    }
    let q = Query::parse(query);
    if let Some(name) = UNSUPPORTED.iter().find(|n| q.has(n)) {
        return Err(RouteError::NotImplemented(name));
    }
    let copy = headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("x-amz-copy-source"));
    let operation = match (&bucket, &key) {
        (None, _) if method == "GET" => Operation::ListBuckets,
        (None, _) => return Err(RouteError::MethodNotAllowed),
        (Some(_), None) => bucket_operation(method, &q)?,
        (Some(_), Some(_)) => object_operation(method, &q, copy)?,
    };
    Ok(Route {
        operation,
        bucket,
        key,
    })
}

/// A subresource and the operation each method names on it.
type Subresource = (&'static str, &'static [(&'static str, Operation)]);

/// The operation `method` names on the first of `subresources` the query carries, or `None`
/// if it carries none. A method the subresource does not define is `MethodNotAllowed`, never
/// the operation on the bucket or object itself: `DELETE /bucket?versioning` deletes no
/// bucket, and `PUT /bucket/key?attributes` writes no object.
fn subresource(
    subresources: &[Subresource],
    method: &str,
    q: &Query,
) -> Option<Result<Operation, RouteError>> {
    let (_, methods) = subresources.iter().find(|(name, _)| q.has(name))?;
    Some(
        methods
            .iter()
            .find(|(m, _)| *m == method)
            .map(|&(_, operation)| operation)
            .ok_or(RouteError::MethodNotAllowed),
    )
}

/// The subresources of a bucket.
const BUCKET_SUBRESOURCES: [Subresource; 11] = {
    use Operation as O;
    [
        ("location", &[("GET", O::GetBucketLocation)]),
        (
            "versioning",
            &[
                ("GET", O::GetBucketVersioning),
                ("PUT", O::PutBucketVersioning),
            ],
        ),
        (
            "tagging",
            &[
                ("GET", O::GetBucketTagging),
                ("PUT", O::PutBucketTagging),
                ("DELETE", O::DeleteBucketTagging),
            ],
        ),
        ("acl", &[("GET", O::GetBucketAcl), ("PUT", O::PutBucketAcl)]),
        (
            "ownershipControls",
            &[
                ("GET", O::GetBucketOwnershipControls),
                ("PUT", O::PutBucketOwnershipControls),
                ("DELETE", O::DeleteBucketOwnershipControls),
            ],
        ),
        (
            "lifecycle",
            &[
                ("GET", O::GetBucketLifecycleConfiguration),
                ("PUT", O::PutBucketLifecycleConfiguration),
                ("DELETE", O::DeleteBucketLifecycle),
            ],
        ),
        (
            "cors",
            &[
                ("GET", O::GetBucketCors),
                ("PUT", O::PutBucketCors),
                ("DELETE", O::DeleteBucketCors),
            ],
        ),
        (
            "policy",
            &[
                ("GET", O::GetBucketPolicy),
                ("PUT", O::PutBucketPolicy),
                ("DELETE", O::DeleteBucketPolicy),
            ],
        ),
        ("versions", &[("GET", O::ListObjectVersions)]),
        ("uploads", &[("GET", O::ListMultipartUploads)]),
        ("delete", &[("POST", O::DeleteObjects)]),
    ]
};

/// The subresources of an object.
const OBJECT_SUBRESOURCES: [Subresource; 5] = {
    use Operation as O;
    [
        (
            "tagging",
            &[
                ("GET", O::GetObjectTagging),
                ("PUT", O::PutObjectTagging),
                ("DELETE", O::DeleteObjectTagging),
            ],
        ),
        ("acl", &[("GET", O::GetObjectAcl), ("PUT", O::PutObjectAcl)]),
        ("attributes", &[("GET", O::GetObjectAttributes)]),
        ("uploads", &[("POST", O::CreateMultipartUpload)]),
        (
            "uploadId",
            &[
                ("GET", O::ListParts),
                ("PUT", O::UploadPart),
                ("POST", O::CompleteMultipartUpload),
                ("DELETE", O::AbortMultipartUpload),
            ],
        ),
    ]
};

/// Whether the query carries a subresource of `others` but none of `own`: one S3 defines on
/// the other kind of resource. Such a request is refused, never taken for the bucket's or the
/// object's own operation: `PUT /bucket/key?lifecycle` writes no object, and
/// `DELETE /bucket?uploadId=u` deletes no bucket.
fn misdirected(own: &[Subresource], others: &[Subresource], q: &Query) -> bool {
    let carries = |list: &[Subresource]| list.iter().any(|(name, _)| q.has(name));
    carries(others) && !carries(own)
}

fn bucket_operation(method: &str, q: &Query) -> Result<Operation, RouteError> {
    use Operation as O;
    if let Some(operation) = subresource(&BUCKET_SUBRESOURCES, method, q) {
        return operation;
    }
    if misdirected(&BUCKET_SUBRESOURCES, &OBJECT_SUBRESOURCES, q) {
        return Err(RouteError::MethodNotAllowed);
    }
    match method {
        "GET" if q.value("list-type") == Some("2") => Ok(O::ListObjectsV2),
        "GET" => Ok(O::ListObjects),
        "PUT" => Ok(O::CreateBucket),
        "DELETE" => Ok(O::DeleteBucket),
        "HEAD" => Ok(O::HeadBucket),
        _ => Err(RouteError::MethodNotAllowed),
    }
}

fn object_operation(method: &str, q: &Query, copy: bool) -> Result<Operation, RouteError> {
    use Operation as O;
    match subresource(&OBJECT_SUBRESOURCES, method, q) {
        // A part is named by its number; a PUT to an upload without one names nothing, and
        // is never taken for a PutObject that would replace the object with the part.
        Some(Ok(O::UploadPart)) if !q.has("partNumber") => Err(RouteError::MethodNotAllowed),
        Some(Ok(O::UploadPart)) if copy => Ok(O::UploadPartCopy),
        Some(operation) => operation,
        None if misdirected(&OBJECT_SUBRESOURCES, &BUCKET_SUBRESOURCES, q) => {
            Err(RouteError::MethodNotAllowed)
        }
        None => match method {
            "PUT" if copy => Ok(O::CopyObject),
            "PUT" => Ok(O::PutObject),
            "GET" => Ok(O::GetObject),
            "HEAD" => Ok(O::HeadObject),
            "DELETE" => Ok(O::DeleteObject),
            _ => Err(RouteError::MethodNotAllowed),
        },
    }
}

/// The bucket and key a request names (05 §14).
fn target(
    endpoint: &Endpoint,
    host: Option<&str>,
    path: &str,
) -> Result<(Option<String>, Option<String>), RouteError> {
    let path = path.strip_prefix('/').ok_or(RouteError::InvalidUri)?;
    let decoded = |s: &str| -> Result<String, RouteError> {
        String::from_utf8(percent_decode(s.as_bytes()).ok_or(RouteError::InvalidUri)?)
            .map_err(|_| RouteError::InvalidUri)
    };
    let host = host.map(|h| strip_port(h).to_ascii_lowercase());
    let bucket_from_host = match host.as_deref() {
        None => None,
        Some(h) if is_path_style_host(endpoint, h) => None,
        Some(h) => match endpoint
            .domains
            .iter()
            .find_map(|d| h.strip_suffix(d.as_str()).and_then(|b| b.strip_suffix('.')))
        {
            Some(bucket) => Some(bucket.to_owned()),
            None if endpoint.cname => Some(h.to_owned()),
            None => return Err(RouteError::InvalidUri),
        },
    };
    let (bucket, key) = match bucket_from_host {
        Some(bucket) => (
            Some(bucket),
            (!path.is_empty()).then(|| decoded(path)).transpose()?,
        ),
        None => match path.split_once('/') {
            None if path.is_empty() => (None, None),
            None => (Some(decoded(path)?), None),
            Some((bucket, "")) => (Some(decoded(bucket)?), None),
            Some((bucket, key)) => (Some(decoded(bucket)?), Some(decoded(key)?)),
        },
    };
    if key.as_ref().is_some_and(|k| k.len() > MAX_KEY) {
        return Err(RouteError::KeyTooLong);
    }
    if bucket.as_ref().is_some_and(String::is_empty) {
        return Err(RouteError::InvalidUri);
    }
    Ok((bucket, key))
}

fn is_path_style_host(endpoint: &Endpoint, host: &str) -> bool {
    host == "localhost"
        || host.parse::<std::net::IpAddr>().is_ok()
        || host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .is_some_and(|h| h.parse::<std::net::Ipv6Addr>().is_ok())
        || endpoint.domains.iter().any(|d| d == host)
}

/// A host without its port; an IPv6 literal keeps its brackets.
fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return host.find(']').and_then(|i| host.get(..=i)).unwrap_or(host);
    }
    host.rsplit_once(':')
        .filter(|(_, port)| !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()))
        .map_or(host, |(h, _)| h)
}

/// S3's general-purpose bucket naming rules (05 §10.3), with each dot-separated label also
/// beginning and ending in a letter or digit, which ceph s3-tests expects of S3.
pub fn bucket_name_valid(name: &str) -> bool {
    let bytes = name.as_bytes();
    (3..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'.' || *b == b'-')
        && name.split('.').all(|label| {
            let l = label.as_bytes();
            l.first().is_some_and(u8::is_ascii_alphanumeric)
                && l.last().is_some_and(u8::is_ascii_alphanumeric)
        })
        && name.parse::<std::net::Ipv4Addr>().is_err()
        && !["xn--", "sthree-", "amzn-s3-demo-"]
            .iter()
            .any(|p| name.starts_with(p))
        && !["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]
            .iter()
            .any(|s| name.ends_with(s))
}

/// A request's query parameters, decoded.
pub struct Query {
    pairs: Vec<(String, String)>,
}

impl Query {
    pub fn parse(query: &str) -> Self {
        let pairs = query
            .split('&')
            .filter(|p| !p.is_empty())
            .filter_map(|p| {
                let (n, v) = p.split_once('=').unwrap_or((p, ""));
                let n = String::from_utf8(percent_decode(n.as_bytes())?).ok()?;
                let v = String::from_utf8(percent_decode(v.as_bytes())?).ok()?;
                Some((n, v))
            })
            .collect();
        Self { pairs }
    }

    pub fn has(&self, name: &str) -> bool {
        self.pairs.iter().any(|(n, _)| n == name)
    }

    pub fn value(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> Endpoint {
        Endpoint {
            domains: vec!["s3.example.com".into()],
            cname: false,
        }
    }

    fn r(method: &str, host: &str, path: &str, query: &str) -> Result<Route, RouteError> {
        route(&endpoint(), method, Some(host), path, query, &[])
    }

    fn op(method: &str, path: &str, query: &str) -> Operation {
        r(method, "s3.example.com", path, query).unwrap().operation
    }

    #[test]
    fn both_addressing_styles_name_the_same_object() {
        let path = r("GET", "s3.example.com:9000", "/photos/2024/a%20b.jpg", "").unwrap();
        let host = r("GET", "photos.s3.example.com", "/2024/a%20b.jpg", "").unwrap();
        for route in [path, host] {
            assert_eq!(route.bucket.as_deref(), Some("photos"));
            assert_eq!(route.key.as_deref(), Some("2024/a b.jpg"));
            assert_eq!(route.operation, Operation::GetObject);
        }
        let local = r("PUT", "127.0.0.1:9000", "/b/k", "").unwrap();
        assert_eq!(
            (local.bucket.as_deref(), local.key.as_deref()),
            (Some("b"), Some("k"))
        );
        let v6 = r("PUT", "[::1]:9000", "/b/k", "").unwrap();
        assert_eq!(v6.bucket.as_deref(), Some("b"));
        // Keys are never normalized (05 §1.2.2).
        let odd = r("GET", "s3.example.com", "/b/a//b/./c/../d", "").unwrap();
        assert_eq!(odd.key.as_deref(), Some("a//b/./c/../d"));
    }

    #[test]
    fn an_unknown_host_names_a_bucket_only_with_the_cname_rule() {
        assert_eq!(
            r("GET", "images.other.org", "/k", ""),
            Err(RouteError::InvalidUri)
        );
        let cname = Endpoint {
            cname: true,
            ..endpoint()
        };
        let route = route(&cname, "GET", Some("Images.Other.org"), "/k", "", &[]).unwrap();
        assert_eq!(route.bucket.as_deref(), Some("images.other.org"));
    }

    #[test]
    fn operations_follow_method_and_subresource() {
        use Operation as O;
        assert_eq!(op("GET", "/", ""), O::ListBuckets);
        assert_eq!(op("PUT", "/b", ""), O::CreateBucket);
        assert_eq!(op("GET", "/b", "list-type=2&prefix=x"), O::ListObjectsV2);
        assert_eq!(op("GET", "/b", "prefix=x"), O::ListObjects);
        assert_eq!(op("GET", "/b", "versions"), O::ListObjectVersions);
        assert_eq!(op("GET", "/b/", "uploads"), O::ListMultipartUploads);
        assert_eq!(op("POST", "/b", "delete"), O::DeleteObjects);
        assert_eq!(op("PUT", "/b", "versioning"), O::PutBucketVersioning);
        assert_eq!(op("POST", "/b/k", "uploads"), O::CreateMultipartUpload);
        assert_eq!(op("PUT", "/b/k", "partNumber=1&uploadId=u"), O::UploadPart);
        assert_eq!(op("POST", "/b/k", "uploadId=u"), O::CompleteMultipartUpload);
        assert_eq!(op("GET", "/b/k", "uploadId=u"), O::ListParts);
        assert_eq!(op("DELETE", "/b/k", "uploadId=u"), O::AbortMultipartUpload);
        assert_eq!(op("DELETE", "/b/k", "versionId=v"), O::DeleteObject);
        assert_eq!(op("GET", "/b/k", "attributes"), O::GetObjectAttributes);
        assert_eq!(op("HEAD", "/b/k", ""), O::HeadObject);
        let copy = [("x-amz-copy-source", "/src/key")];
        let e = endpoint();
        let host = Some("s3.example.com");
        assert_eq!(
            route(&e, "PUT", host, "/b/k", "", &copy).unwrap().operation,
            O::CopyObject
        );
        assert_eq!(
            route(&e, "PUT", host, "/b/k", "partNumber=2&uploadId=u", &copy)
                .unwrap()
                .operation,
            O::UploadPartCopy
        );
    }

    #[test]
    fn tags_and_acls_are_subresources() {
        use Operation as O;
        assert_eq!(op("GET", "/b", "tagging"), O::GetBucketTagging);
        assert_eq!(op("DELETE", "/b", "tagging"), O::DeleteBucketTagging);
        assert_eq!(op("GET", "/b", "acl"), O::GetBucketAcl);
        assert_eq!(op("PUT", "/b", "acl"), O::PutBucketAcl);
        assert_eq!(
            op("PUT", "/b/k", "tagging&versionId=v"),
            O::PutObjectTagging
        );
        assert_eq!(op("GET", "/b/k", "acl&versionId=v"), O::GetObjectAcl);
        assert_eq!(op("PUT", "/b/k", "acl"), O::PutObjectAcl);
        assert_eq!(
            op("GET", "/b", "ownershipControls"),
            O::GetBucketOwnershipControls
        );
        assert_eq!(
            op("DELETE", "/b", "ownershipControls"),
            O::DeleteBucketOwnershipControls
        );
    }

    /// A subresource's undefined methods are refused, never taken for the operation on the
    /// bucket or object itself.
    #[test]
    fn a_subresource_is_never_its_bucket_or_object() {
        let refused = |method: &str, path: &str, query: &str| {
            assert_eq!(
                r(method, "s3.example.com", path, query),
                Err(RouteError::MethodNotAllowed),
                "{method} {path}?{query}"
            );
        };
        for query in [
            "versioning",
            "location",
            "acl",
            "versions",
            "uploads",
            "delete",
        ] {
            refused("DELETE", "/b", query);
        }
        for query in ["location", "versions", "uploads", "delete"] {
            refused("PUT", "/b", query);
        }
        refused("DELETE", "/b/k", "acl");
        refused("DELETE", "/b/k", "attributes");
        refused("PUT", "/b/k", "attributes");
        refused("PUT", "/b/k", "uploads");
        refused("GET", "/b/k", "uploads");
        refused("PUT", "/b/k", "uploadId=u");
        refused("POST", "/b/k", "tagging");
        refused("HEAD", "/b", "acl");
        refused("POST", "/b", "lifecycle");
        // A subresource S3 defines only on the other kind of resource.
        for query in [
            "lifecycle",
            "versioning",
            "location",
            "ownershipControls",
            "versions",
            "delete",
        ] {
            for method in ["GET", "PUT", "DELETE", "HEAD"] {
                refused(method, "/b/k", query);
            }
        }
        for query in ["uploadId=u", "attributes", "uploadId=u&partNumber=1"] {
            for method in ["GET", "PUT", "DELETE", "HEAD"] {
                refused(method, "/b", query);
            }
        }
    }

    /// `?cors` is the bucket's CORS rules; `OPTIONS` on a bucket or an object is a preflight
    /// whatever its query, and needs a bucket (16 §3).
    #[test]
    fn cors_and_preflights_route() {
        use Operation as O;
        assert_eq!(op("GET", "/b", "cors"), O::GetBucketCors);
        assert_eq!(op("PUT", "/b", "cors"), O::PutBucketCors);
        assert_eq!(op("DELETE", "/b", "cors"), O::DeleteBucketCors);
        let preflight = r(
            "OPTIONS",
            "s3.example.com",
            "/b/k",
            "uploadId=u&partNumber=1",
        )
        .unwrap();
        assert_eq!(preflight.operation, O::Preflight);
        assert_eq!(preflight.key.as_deref(), Some("k"));
        assert_eq!(op("OPTIONS", "/b", "policy"), O::Preflight);
        assert_eq!(
            r("OPTIONS", "s3.example.com", "/", ""),
            Err(RouteError::MethodNotAllowed)
        );
        assert_eq!(
            r("PUT", "s3.example.com", "/b/k", "cors"),
            Err(RouteError::MethodNotAllowed)
        );
    }

    #[test]
    fn policy_is_a_bucket_subresource() {
        use Operation as O;
        assert_eq!(op("GET", "/b", "policy"), O::GetBucketPolicy);
        assert_eq!(op("PUT", "/b", "policy"), O::PutBucketPolicy);
        assert_eq!(op("DELETE", "/b", "policy"), O::DeleteBucketPolicy);
        assert_eq!(
            r("PUT", "s3.example.com", "/b/k", "policy"),
            Err(RouteError::MethodNotAllowed)
        );
        assert_eq!(
            r("GET", "s3.example.com", "/b", "policyStatus"),
            Err(RouteError::NotImplemented("policyStatus"))
        );
    }

    #[test]
    fn lifecycle_is_a_bucket_subresource() {
        use Operation as O;
        assert_eq!(
            op("GET", "/b", "lifecycle"),
            O::GetBucketLifecycleConfiguration
        );
        assert_eq!(
            op("PUT", "/b", "lifecycle="),
            O::PutBucketLifecycleConfiguration
        );
        assert_eq!(op("DELETE", "/b", "lifecycle"), O::DeleteBucketLifecycle);
    }

    #[test]
    fn what_is_not_served_is_refused_plainly() {
        assert_eq!(
            r("GET", "s3.example.com", "/b", "replication"),
            Err(RouteError::NotImplemented("replication"))
        );
        assert_eq!(
            r("DELETE", "s3.example.com", "/b", "website"),
            Err(RouteError::NotImplemented("website"))
        );
        assert_eq!(
            r("POST", "s3.example.com", "/", ""),
            Err(RouteError::MethodNotAllowed)
        );
        assert_eq!(
            r("PATCH", "s3.example.com", "/b/k", ""),
            Err(RouteError::MethodNotAllowed)
        );
        let long = format!("/b/{}", "k".repeat(MAX_KEY + 1));
        assert_eq!(
            r("GET", "s3.example.com", &long, ""),
            Err(RouteError::KeyTooLong)
        );
        assert_eq!(
            r("GET", "s3.example.com", "/b/%zz", ""),
            Err(RouteError::InvalidUri)
        );
    }

    #[test]
    fn bucket_names_follow_s3() {
        for good in ["abc", "my-bucket", "my.bucket.1", "a1-2.b3"] {
            assert!(bucket_name_valid(good), "{good}");
        }
        for bad in [
            "a",
            "aa",
            "foo_bar",
            "foo-",
            "foo..bar",
            "foo.-bar",
            "foo-.bar",
            "Foo",
            "192.168.5.123",
            "xn--abc",
            "bucket-s3alias",
            &"a".repeat(64),
        ] {
            assert!(!bucket_name_valid(bad), "{bad}");
        }
    }
}
