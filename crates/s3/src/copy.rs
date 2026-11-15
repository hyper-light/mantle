//! CopyObject's and UploadPartCopy's source (docs/research/89 §3): the bucket, key and version
//! `x-amz-copy-source` names, and how a copy takes its metadata.
//!
//! The header is "the name of the source bucket and the key of the source object, separated by
//! a slash (/)", which "must be URL-encoded", a version named by appending `?versionId=`; AWS's
//! examples write it with a leading slash and without (89 §3.1). The version is split off before
//! the rest is decoded, and the rest decoded exactly once, so a key named `?versionId` arrives as
//! `%3FversionId` and a key holding a `%` that was not encoded is refused, as ceph s3-tests holds
//! S3 to (research/05 §4.3, §9.2).

use crate::body::MAX_VERSION_ID;
use crate::route::MAX_KEY;
use crate::sigv4::percent_decode;

/// What a copy names as its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub bucket: String,
    pub key: String,
    /// The version `?versionId=` names, as sent; none for the key's current version.
    pub version: Option<String>,
}

/// Why `x-amz-copy-source` names no source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    #[error("Copy Source must mention the source bucket and key: sourcebucket/sourcekey")]
    Malformed,
    #[error("Your key is too long")]
    KeyTooLong,
    /// An access point's or an Outpost's ARN: mantle has neither.
    #[error("a copy source through an access point")]
    AccessPoint,
}

impl SourceError {
    /// The S3 error code and status (89 §3.1; research/05 §10.1, §11.2).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Malformed => ("InvalidArgument", 400),
            Self::KeyTooLong => ("KeyTooLongError", 400),
            Self::AccessPoint => ("NotImplemented", 501),
        }
    }
}

/// The source `header` names.
pub fn source(header: &str) -> Result<Source, SourceError> {
    let text = header.trim();
    let text = text.strip_prefix('/').unwrap_or(text);
    if text.starts_with("arn:") {
        return Err(SourceError::AccessPoint);
    }
    let (path, query) = match text.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (text, None),
    };
    let version = match query {
        None => None,
        Some(query) => {
            let raw = query
                .strip_prefix("versionId=")
                .ok_or(SourceError::Malformed)?;
            let id = decoded(raw)?;
            if id.is_empty() || id.len() > MAX_VERSION_ID {
                return Err(SourceError::Malformed);
            }
            Some(id)
        }
    };
    let named = decoded(path)?;
    let (bucket, key) = named.split_once('/').ok_or(SourceError::Malformed)?;
    if bucket.is_empty() || key.is_empty() {
        return Err(SourceError::Malformed);
    }
    if key.len() > MAX_KEY {
        return Err(SourceError::KeyTooLong);
    }
    Ok(Source {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        version,
    })
}

/// `text` percent-decoded once, as UTF-8.
fn decoded(text: &str) -> Result<String, SourceError> {
    percent_decode(text.as_bytes())
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or(SourceError::Malformed)
}

/// Where a copy's metadata comes from (89 §3.2): "If this header isn't specified, `COPY` is the
/// default behavior."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Directive {
    /// The source's content headers and user-defined metadata.
    Copy,
    /// The request's.
    Replace,
}

/// `x-amz-metadata-directive`, `COPY` where absent; `None` for any value but its two, "Valid
/// Values: `COPY | REPLACE`", which S3 refuses `400 InvalidArgument` (89 §3.2).
pub fn directive(header: Option<&str>) -> Option<Directive> {
    match header.map(str::trim) {
        None | Some("COPY") => Some(Directive::Copy),
        Some("REPLACE") => Some(Directive::Replace),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(bucket: &str, key: &str, version: Option<&str>) -> Source {
        Source {
            bucket: bucket.into(),
            key: key.into(),
            version: version.map(str::to_owned),
        }
    }

    /// AWS's forms, with and without the leading slash and a version (89 §3.1), and s3-tests'
    /// special keys, each decoded once (research/05 §4.3, §9.2).
    #[test]
    fn a_source_is_its_bucket_key_and_version_decoded_once() {
        assert_eq!(
            source("awsexamplebucket/reports/january.pdf"),
            Ok(named("awsexamplebucket", "reports/january.pdf", None))
        );
        assert_eq!(
            source(
                "/awsexamplebucket/reports/january.pdf?versionId=QUpfdndhfd8438MNFDN93jdnJFkdmqnh893"
            ),
            Ok(named(
                "awsexamplebucket",
                "reports/january.pdf",
                Some("QUpfdndhfd8438MNFDN93jdnJFkdmqnh893")
            ))
        );
        assert_eq!(source("b/%3FversionId"), Ok(named("b", "?versionId", None)));
        assert_eq!(source("b/%20"), Ok(named("b", " ", None)));
        assert_eq!(source("b/foo%3Fbar"), Ok(named("b", "foo?bar", None)));
        assert_eq!(
            source("b/bar%26foo?versionId=0000000000001"),
            Ok(named("b", "bar&foo", Some("0000000000001")))
        );
        assert_eq!(
            source("b/anyfilename%25.txt"),
            Ok(named("b", "anyfilename%.txt", None))
        );
        // A `%` the client did not encode, which s3-tests requires to fail.
        assert_eq!(source("b/anyfilename%.txt"), Err(SourceError::Malformed));
    }

    #[test]
    fn a_source_naming_no_bucket_and_key_is_refused() {
        for bad in [
            "",
            "/",
            "bucket",
            "bucket/",
            "/key-only",
            "b/k?other=1",
            "b/k?versionId=",
        ] {
            assert_eq!(source(bad), Err(SourceError::Malformed), "{bad}");
        }
        let long = format!("b/{}", "k".repeat(MAX_KEY + 1));
        assert_eq!(source(&long), Err(SourceError::KeyTooLong));
        assert_eq!(
            source("arn:aws:s3:us-west-2:123456789012:accesspoint/ap/object/k"),
            Err(SourceError::AccessPoint)
        );
        assert_eq!(SourceError::Malformed.code(), ("InvalidArgument", 400));
    }

    #[test]
    fn the_directive_is_copy_unless_replace_is_named() {
        assert_eq!(directive(None), Some(Directive::Copy));
        assert_eq!(directive(Some("COPY")), Some(Directive::Copy));
        assert_eq!(directive(Some("REPLACE")), Some(Directive::Replace));
        assert_eq!(directive(Some("replace")), None);
        assert_eq!(directive(Some("MOVE")), None);
    }
}
