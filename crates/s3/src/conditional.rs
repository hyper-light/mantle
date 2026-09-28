//! Conditional requests (docs/research/05 §2): reads with `If-Match`, `If-None-Match`,
//! `If-Modified-Since` and `If-Unmodified-Since`; writes with `If-None-Match` and `If-Match`;
//! deletes with `If-Match`.
//!
//! Reads follow RFC 9110 §13.2.2's order, which gives exactly the two combinations AWS
//! documents. Writes and deletes are judged against the object's current version at the moment
//! they commit, not when the request arrives (05 §2.2), so the caller evaluates them inside the
//! metadata layer's compare-and-set; this module only states the rule. ETags compare without
//! their quotes, since clients send them either way (05 §2.2).

/// The object's current version, as a precondition sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Current<'a> {
    /// No version exists.
    Missing,
    /// The current version is a delete marker.
    DeleteMarker,
    Present {
        etag: &'a str,
        /// Last modified, Unix seconds (HTTP dates have one-second resolution).
        modified: i64,
    },
}

/// What a precondition decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Proceed,
    /// `304 Not Modified`, which carries the ETag.
    NotModified,
    /// `412 PreconditionFailed`.
    Failed,
    /// `404 NoSuchKey`.
    NotFound,
}

/// A request's precondition headers.
#[derive(Debug, Clone, Copy, Default)]
pub struct Conditions<'a> {
    pub if_match: Option<&'a str>,
    pub if_none_match: Option<&'a str>,
    pub if_modified_since: Option<&'a str>,
    pub if_unmodified_since: Option<&'a str>,
}

/// A GET or HEAD (RFC 9110 §13.2.2): `If-Match`, else `If-Unmodified-Since`; then
/// `If-None-Match`, else `If-Modified-Since`. A date that does not parse is ignored, as RFC
/// 9110 §13.1.3–§13.1.4 require.
pub fn read(c: &Conditions<'_>, current: &Current<'_>) -> Outcome {
    let Current::Present { etag, modified } = current else {
        return Outcome::NotFound;
    };
    match c.if_match {
        Some(list) if !matches_any(list, etag) => return Outcome::Failed,
        Some(_) => {}
        None => {
            if let Some(since) = c.if_unmodified_since.and_then(crate::time::parse_http_date)
                && *modified > since
            {
                return Outcome::Failed;
            }
        }
    }
    match c.if_none_match {
        Some(list) if matches_any(list, etag) => return Outcome::NotModified,
        Some(_) => {}
        None => {
            if let Some(since) = c.if_modified_since.and_then(crate::time::parse_http_date)
                && *modified <= since
            {
                return Outcome::NotModified;
            }
        }
    }
    Outcome::Proceed
}

/// A PutObject, CompleteMultipartUpload or CopyObject destination, at commit (05 §2.2).
/// `If-None-Match: *` requires that no current version exist; an ETag there fails if it
/// matches the current one. `If-Match` requires a current version, else 404, whose ETag
/// matches, else 412.
pub fn write(c: &Conditions<'_>, current: &Current<'_>) -> Outcome {
    let etag = match current {
        Current::Present { etag, .. } => Some(*etag),
        Current::Missing | Current::DeleteMarker => None,
    };
    if let Some(list) = c.if_none_match
        && let Some(etag) = etag
        && matches_any(list, etag)
    {
        return Outcome::Failed;
    }
    match (c.if_match, etag) {
        (Some(_), None) => Outcome::NotFound,
        (Some(list), Some(etag)) if !matches_any(list, etag) => Outcome::Failed,
        _ => Outcome::Proceed,
    }
}

/// A DeleteObject with `If-Match` (05 §2.3): a missing object is not found; a current delete
/// marker fails the precondition, as AWS documents for `If-Match: *`.
pub fn delete(c: &Conditions<'_>, current: &Current<'_>) -> Outcome {
    let Some(list) = c.if_match else {
        return Outcome::Proceed;
    };
    match current {
        Current::Missing => Outcome::NotFound,
        Current::DeleteMarker => Outcome::Failed,
        Current::Present { etag, .. } if matches_any(list, etag) => Outcome::Proceed,
        Current::Present { .. } => Outcome::Failed,
    }
}

/// Whether a header's list of entity tags (or `*`) names `etag`.
fn matches_any(list: &str, etag: &str) -> bool {
    let etag = bare(etag);
    list.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || bare(candidate.strip_prefix("W/").unwrap_or(candidate)) == etag
    })
}

fn bare(etag: &str) -> &str {
    let etag = etag.trim();
    etag.strip_prefix('"')
        .and_then(|e| e.strip_suffix('"'))
        .unwrap_or(etag)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETAG: &str = "\"6805f2cfc46c0f04559748bb039d69ae\"";

    fn present() -> Current<'static> {
        Current::Present {
            etag: ETAG,
            modified: crate::time::parse_http_date("Tue, 24 Sep 2024 12:00:00 GMT").unwrap(),
        }
    }

    /// The s3-tests read cases and AWS's two documented combinations (05 §2.4).
    #[test]
    fn reads_follow_rfc_9110_order() {
        let c = |im, inm, ims, ius| Conditions {
            if_match: im,
            if_none_match: inm,
            if_modified_since: ims,
            if_unmodified_since: ius,
        };
        let p = present();
        assert_eq!(
            read(&c(Some("\"bogus\""), None, None, None), &p),
            Outcome::Failed
        );
        assert_eq!(
            read(&c(None, Some(ETAG), None, None), &p),
            Outcome::NotModified
        );
        // Unquoted, as the AWS CLI sends it.
        assert_eq!(
            read(
                &c(None, Some("6805f2cfc46c0f04559748bb039d69ae"), None, None),
                &p
            ),
            Outcome::NotModified
        );
        let later = "Tue, 24 Sep 2024 12:00:01 GMT";
        assert_eq!(
            read(&c(None, None, Some(later), None), &p),
            Outcome::NotModified
        );
        assert_eq!(
            read(
                &c(None, None, None, Some("Sat, 01 Jan 1994 00:00:00 GMT")),
                &p
            ),
            Outcome::Failed
        );
        assert_eq!(
            read(
                &c(None, None, None, Some("Fri, 01 Jan 2100 00:00:00 GMT")),
                &p
            ),
            Outcome::Proceed
        );
        // If-Match true with If-Unmodified-Since false: 200 (AWS).
        assert_eq!(
            read(
                &c(
                    Some(ETAG),
                    None,
                    None,
                    Some("Sat, 01 Jan 1994 00:00:00 GMT")
                ),
                &p
            ),
            Outcome::Proceed
        );
        // If-None-Match false (the ETag matches) with If-Modified-Since true: 304 (AWS).
        assert_eq!(
            read(
                &c(
                    None,
                    Some(ETAG),
                    Some("Sat, 01 Jan 1994 00:00:00 GMT"),
                    None
                ),
                &p
            ),
            Outcome::NotModified
        );
        // A different ETag with the object unmodified since: If-Modified-Since is ignored
        // once If-None-Match is present (RFC 9110 §13.1.3).
        assert_eq!(
            read(&c(None, Some("\"other\""), Some(later), None), &p),
            Outcome::Proceed
        );
        assert_eq!(
            read(&c(None, None, Some("not a date"), None), &p),
            Outcome::Proceed
        );
    }

    /// s3-tests' conditional-write matrix (05 §2.2).
    #[test]
    fn writes_follow_the_s3_tests_matrix() {
        let none_match = |v| Conditions {
            if_none_match: Some(v),
            ..Conditions::default()
        };
        let if_match = |v| Conditions {
            if_match: Some(v),
            ..Conditions::default()
        };
        let p = present();
        assert_eq!(write(&none_match("*"), &p), Outcome::Failed);
        assert_eq!(write(&none_match(ETAG), &p), Outcome::Failed);
        assert_eq!(write(&none_match("badetag"), &p), Outcome::Proceed);
        assert_eq!(write(&none_match("*"), &Current::Missing), Outcome::Proceed);
        assert_eq!(
            write(&none_match("*"), &Current::DeleteMarker),
            Outcome::Proceed
        );
        assert_eq!(write(&if_match("*"), &p), Outcome::Proceed);
        assert_eq!(write(&if_match("badetag"), &p), Outcome::Failed);
        assert_eq!(write(&if_match("*"), &Current::Missing), Outcome::NotFound);
        assert_eq!(
            write(&if_match("badetag"), &Current::DeleteMarker),
            Outcome::NotFound
        );
        assert_eq!(write(&Conditions::default(), &p), Outcome::Proceed);
    }

    #[test]
    fn deletes_follow_aws() {
        let if_match = |v| Conditions {
            if_match: Some(v),
            ..Conditions::default()
        };
        let p = present();
        assert_eq!(delete(&if_match("*"), &p), Outcome::Proceed);
        assert_eq!(delete(&if_match(ETAG), &p), Outcome::Proceed);
        assert_eq!(delete(&if_match("\"other\""), &p), Outcome::Failed);
        assert_eq!(
            delete(&if_match("*"), &Current::DeleteMarker),
            Outcome::Failed
        );
        assert_eq!(delete(&if_match("*"), &Current::Missing), Outcome::NotFound);
        assert_eq!(
            delete(&Conditions::default(), &Current::Missing),
            Outcome::Proceed
        );
    }
}
