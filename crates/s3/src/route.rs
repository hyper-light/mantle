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
const UNSUPPORTED: [&str; 22] = [
    "accelerate",
    "acl",
    "analytics",
    "cors",
    "encryption",
    "intelligent-tiering",
    "inventory",
    "lifecycle",
    "logging",
    "metrics",
    "notification",
    "object-lock",
    "ownershipControls",
    "policy",
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
    let q = Query::parse(query);
    if let Some(name) = UNSUPPORTED.iter().find(|n| q.has(n)) {
        return Err(RouteError::NotImplemented(name));
    }
    let copy = headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("x-amz-copy-source"));
    use Operation as O;
    let operation = match (bucket.is_some(), key.is_some(), method) {
        (false, _, "GET") => O::ListBuckets,
        (false, _, _) => return Err(RouteError::MethodNotAllowed),
        (true, false, "GET") if q.has("location") => O::GetBucketLocation,
        (true, false, "GET") if q.has("versioning") => O::GetBucketVersioning,
        (true, false, "PUT") if q.has("versioning") => O::PutBucketVersioning,
        (true, false, "GET") if q.has("tagging") => O::GetBucketTagging,
        (true, false, "PUT") if q.has("tagging") => O::PutBucketTagging,
        (true, false, "DELETE") if q.has("tagging") => O::DeleteBucketTagging,
        (true, false, "GET") if q.has("versions") => O::ListObjectVersions,
        (true, false, "GET") if q.has("uploads") => O::ListMultipartUploads,
        (true, false, "GET") if q.value("list-type") == Some("2") => O::ListObjectsV2,
        (true, false, "GET") => O::ListObjects,
        (true, false, "POST") if q.has("delete") => O::DeleteObjects,
        (true, false, "PUT") => O::CreateBucket,
        (true, false, "DELETE") => O::DeleteBucket,
        (true, false, "HEAD") => O::HeadBucket,
        (true, false, _) => return Err(RouteError::MethodNotAllowed),
        (true, true, "PUT") if q.has("tagging") => O::PutObjectTagging,
        (true, true, "GET") if q.has("tagging") => O::GetObjectTagging,
        (true, true, "DELETE") if q.has("tagging") => O::DeleteObjectTagging,
        (true, true, "PUT") if q.has("partNumber") && q.has("uploadId") => {
            if copy {
                O::UploadPartCopy
            } else {
                O::UploadPart
            }
        }
        (true, true, "PUT") if copy => O::CopyObject,
        (true, true, "PUT") => O::PutObject,
        (true, true, "GET") if q.has("uploadId") => O::ListParts,
        (true, true, "GET") if q.has("attributes") => O::GetObjectAttributes,
        (true, true, "GET") => O::GetObject,
        (true, true, "HEAD") => O::HeadObject,
        (true, true, "DELETE") if q.has("uploadId") => O::AbortMultipartUpload,
        (true, true, "DELETE") => O::DeleteObject,
        (true, true, "POST") if q.has("uploads") => O::CreateMultipartUpload,
        (true, true, "POST") if q.has("uploadId") => O::CompleteMultipartUpload,
        (true, true, _) => return Err(RouteError::MethodNotAllowed),
    };
    Ok(Route {
        operation,
        bucket,
        key,
    })
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
    fn what_is_not_served_is_refused_plainly() {
        assert_eq!(
            r("GET", "s3.example.com", "/b", "acl"),
            Err(RouteError::NotImplemented("acl"))
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
