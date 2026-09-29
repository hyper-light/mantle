//! Byte ranges (docs/research/05 §12): the `Range` header of GetObject and HeadObject, and the
//! `x-amz-copy-source-range` of UploadPartCopy.
//!
//! S3 serves one range per request (RFC 9110 §14 byte ranges, without multiple parts). A range
//! that starts past the object's end, or any range of an empty object, is `416 InvalidRange`
//! (ceph s3-tests). A `Range` header that does not parse or names several ranges is ignored and
//! the whole object served, as RFC 9110 §14.2 permits; the copy-source range, which s3-tests
//! holds to `400 InvalidArgument`, is parsed strictly instead.

/// The bytes a request asks for, resolved against the object's length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Span {
    Whole,
    /// Bytes `first..=last`.
    Part {
        first: u64,
        last: u64,
    },
    /// `416 InvalidRange`.
    Unsatisfiable,
}

impl Span {
    /// The `Content-Range` header of a `206` response, or of a `416` (`bytes */len`, RFC 9110
    /// §15.5.17).
    pub fn content_range(&self, len: u64) -> Option<String> {
        match self {
            Self::Whole => None,
            Self::Part { first, last } => Some(format!("bytes {first}-{last}/{len}")),
            Self::Unsatisfiable => Some(format!("bytes */{len}")),
        }
    }
}

/// One range spec: `a-b`, `a-` or `-n`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Spec {
    FromTo(u64, u64),
    From(u64),
    Suffix(u64),
}

fn parse(header: &str) -> Option<Spec> {
    let spec = header.trim().strip_prefix("bytes=")?.trim();
    if spec.contains(',') {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    let num = |s: &str| -> Option<u64> {
        let s = s.trim();
        if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        s.parse().ok()
    };
    match (a.trim().is_empty(), b.trim().is_empty()) {
        (true, false) => Some(Spec::Suffix(num(b)?)),
        (false, true) => Some(Spec::From(num(a)?)),
        (false, false) => {
            let (first, last) = (num(a)?, num(b)?);
            if first <= last {
                Some(Spec::FromTo(first, last))
            } else {
                None
            }
        }
        (true, true) => None,
    }
}

/// A GET's or HEAD's `Range` against an object of `len` bytes.
pub fn span(header: Option<&str>, len: u64) -> Span {
    let Some(spec) = header.and_then(parse) else {
        return Span::Whole;
    };
    resolve(spec, len)
}

fn resolve(spec: Spec, len: u64) -> Span {
    let Some(end) = len.checked_sub(1) else {
        return Span::Unsatisfiable;
    };
    match spec {
        Spec::FromTo(first, last) if first <= end => Span::Part {
            first,
            last: last.min(end),
        },
        Spec::From(first) if first <= end => Span::Part { first, last: end },
        Spec::Suffix(n) if n > 0 => Span::Part {
            first: len.saturating_sub(n),
            last: end,
        },
        _ => Span::Unsatisfiable,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CopyRangeError {
    #[error("x-amz-copy-source-range must be bytes=first-last")]
    Malformed,
    #[error("x-amz-copy-source-range lies outside the source object")]
    Outside,
}

impl CopyRangeError {
    /// The S3 error code and HTTP status: s3-tests expects `400 InvalidArgument` for a
    /// malformed range and `416 InvalidRange` for one past the source (05 §4.3).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Malformed => ("InvalidArgument", 400),
            Self::Outside => ("InvalidRange", 416),
        }
    }
}

/// UploadPartCopy's `x-amz-copy-source-range`, which must be `bytes=first-last` within the
/// source; returns the inclusive bounds.
pub fn copy_range(header: &str, source_len: u64) -> Result<(u64, u64), CopyRangeError> {
    match parse(header) {
        Some(Spec::FromTo(first, last)) if last < source_len => Ok((first, last)),
        Some(Spec::FromTo(..)) => Err(CopyRangeError::Outside),
        _ => Err(CopyRangeError::Malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// s3-tests' ranged requests on an 11-byte object, an 8 MiB one and an empty one
    /// (05 §12.1).
    #[test]
    fn s3_tests_ranges() {
        let part = |first, last| Span::Part { first, last };
        assert_eq!(span(Some("bytes=4-7"), 11), part(4, 7));
        assert_eq!(span(Some("bytes=4-"), 11), part(4, 10));
        assert_eq!(span(Some("bytes=-7"), 11), part(4, 10));
        assert_eq!(
            span(Some("bytes=3145728-5242880"), 8 << 20),
            part(3_145_728, 5_242_880)
        );
        assert_eq!(
            part(3_145_728, 5_242_880).content_range(8 << 20).as_deref(),
            Some("bytes 3145728-5242880/8388608")
        );
        assert_eq!(span(Some("bytes=40-50"), 11), Span::Unsatisfiable);
        assert_eq!(span(Some("bytes=40-50"), 0), Span::Unsatisfiable);
        assert_eq!(span(Some("bytes=-0"), 11), Span::Unsatisfiable);
        assert_eq!(span(Some("bytes=4-100"), 11), part(4, 10));
        assert_eq!(span(Some("bytes=-100"), 11), part(0, 10));
    }

    #[test]
    fn what_does_not_parse_is_ignored() {
        for header in [
            "bytes=0-1,3-4",
            "bytes=7-4",
            "items=0-1",
            "bytes=a-b",
            "bytes=-",
        ] {
            assert_eq!(span(Some(header), 11), Span::Whole, "{header}");
        }
        assert_eq!(span(None, 11), Span::Whole);
    }

    #[test]
    fn copy_ranges_are_strict() {
        assert_eq!(copy_range("bytes=0-4", 11), Ok((0, 4)));
        assert_eq!(copy_range("bytes=0-11", 11), Err(CopyRangeError::Outside));
        assert_eq!(copy_range("bytes=0-", 11), Err(CopyRangeError::Malformed));
        assert_eq!(
            copy_range("bytes=0-1,3-4", 11),
            Err(CopyRangeError::Malformed)
        );
    }
}
