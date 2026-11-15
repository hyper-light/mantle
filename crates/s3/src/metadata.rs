//! An object's stored metadata (docs/research/89 §1): the content headers S3 keeps with an object
//! as a PutObject, a CreateMultipartUpload or a CopyObject's `REPLACE` names them, and its
//! user-defined metadata, `x-amz-meta-*`; kept with the version and returned on GetObject and
//! HeadObject.
//!
//! A version keeps them as pairs of a lowercase field name and its value, sorted by name, each
//! name once (`mantle_meta::record::Version::headers`): the content headers by their names, and
//! each user-defined entry under its full `x-amz-meta-` name, "this prefix is returned" (89 §1.2).
//! A name a request repeats is kept once, its values in the order sent, joined by commas, as S3
//! "combines headers that have the same name (ignoring case) into a comma-delimited list" (89
//! §1.2). Each half is held to S3's 2 KB (89 §1.4): the user-defined metadata measured as "the sum
//! of the number of bytes in the UTF-8 encoding of each key and value", a key its name past the
//! prefix, and the content headers as the system-defined metadata is, each name and value.
//!
//! A value is kept as its request's field carried it. One that is all US-ASCII is returned as it
//! is; any other is returned as S3 returns it, its octets read as ISO-8859-1 characters and
//! RFC 2047's B encoding of their UTF-8, which reproduces S3's worked example byte for byte
//! (89 §1.3; [`shown`]).

use std::borrow::Cow;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

/// The system-defined metadata a request sets and S3 keeps with the object: the content headers
/// of PutObject's, CreateMultipartUpload's and CopyObject's requests, which GetObject and
/// HeadObject return (89 §1.1), in the order a version keeps them.
pub const CONTENT: [&str; 6] = [
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-type",
    "expires",
];

/// User-defined metadata's prefix: "the optional user-defined metadata names must begin with
/// `x-amz-meta-`" (89 §1.2).
pub const USER: &str = "x-amz-meta-";

/// The most bytes of user-defined metadata: "the user-defined metadata is limited to 2 KB in
/// size" (89 §1.4), a kilobyte 1024 octets as S3's 8 KB of headers is (`route::MAX_HEADERS`).
pub const MAX_USER: usize = 2 << 10;

/// The most bytes of the content headers kept: "the system-defined metadata is limited to 2 KB
/// in size" (89 §1.4).
pub const MAX_SYSTEM: usize = 2 << 10;

/// The type a version is served with when its request named none: S3's `binary/octet-stream`
/// (89 §1.1).
pub const DEFAULT_TYPE: &str = "binary/octet-stream";

/// Why a request's metadata is not kept.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    /// Past S3's 2 KB of either half (89 §1.4).
    #[error("Your metadata headers exceed the maximum allowed metadata size")]
    TooLarge,
    /// A name or a value no HTTP field carries, or names out of order or repeated: what only a
    /// request of mantle's own protocol can send, the HTTP/1.1 codec refusing it first.
    #[error("a metadata header that is not an HTTP field: {0}")]
    Invalid(String),
}

impl MetadataError {
    /// The S3 error code and status (89 §1.4; research/05 §11.2).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::TooLarge => ("MetadataTooLarge", 400),
            Self::Invalid(_) => ("InvalidArgument", 400),
        }
    }
}

/// Whether `name`, lowercase, is a field name a version keeps: a content header's, or a
/// user-defined entry's.
fn kept(name: &str) -> bool {
    CONTENT.contains(&name) || name.starts_with(USER)
}

/// RFC 9110 §5.6.2's `tchar`: the octets of a token, which a field name is.
fn tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Whether `value` is a field value as RFC 9110 §5.5 writes one: visible octets, `obs-text` and
/// spaces and tabs between them, none leading or trailing.
fn field_value(value: &str) -> bool {
    let bytes = value.as_bytes();
    let inner = |b: &u8| *b == b' ' || *b == b'\t' || *b >= 0x21 && *b != 0x7f;
    let edge = |b: Option<&u8>| b.is_none_or(|b| *b != b' ' && *b != b'\t');
    bytes.iter().all(inner) && edge(bytes.first()) && edge(bytes.last())
}

/// The metadata `headers` name, as a version keeps it: each content header and each user-defined
/// entry, its name lowercase, a name repeated kept once with its values joined by commas in the
/// order sent, sorted by name; refused past S3's 2 KB of either half. `aws-chunked`, the framing
/// of a streamed body rather than a coding of the object, is not kept in `Content-Encoding`:
/// "Amazon S3 stores the resulting object without the `aws-chunked` value", and one left empty
/// is no header (research/05 §1.8).
pub fn of_request<'a>(
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Vec<(String, String)>, MetadataError> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        if !kept(&lower) {
            continue;
        }
        match pairs.iter_mut().find(|(n, _)| *n == lower) {
            Some((_, joined)) => {
                joined.push(',');
                joined.push_str(value);
            }
            None => pairs.push((lower, value.to_owned())),
        }
    }
    if let Some(at) = pairs.iter().position(|(n, _)| n == "content-encoding")
        && let Some((_, value)) = pairs.get_mut(at)
    {
        let codings: Vec<&str> = value
            .split(',')
            .map(str::trim)
            .filter(|c| !c.is_empty() && !c.eq_ignore_ascii_case(AWS_CHUNKED))
            .collect();
        let kept = codings.join(", ");
        if kept.is_empty() {
            pairs.remove(at);
        } else {
            *value = kept;
        }
    }
    pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    check(&pairs)?;
    Ok(pairs)
}

/// The content coding that names a streamed body's framing (research/05 §1.8).
const AWS_CHUNKED: &str = "aws-chunked";

/// Whether `pairs` is metadata a version keeps: each name a content header's or a user-defined
/// entry's, lowercase and a token, in ascending order and each once; each value a field value;
/// each half within S3's 2 KB.
pub fn check(pairs: &[(String, String)]) -> Result<(), MetadataError> {
    let (mut user, mut system) = (0usize, 0usize);
    let mut last: Option<&str> = None;
    for (name, value) in pairs {
        let valid = kept(name)
            && name.bytes().all(|b| tchar(b) && !b.is_ascii_uppercase())
            && last.is_none_or(|before| before < name.as_str())
            && field_value(value);
        if !valid {
            return Err(MetadataError::Invalid(name.clone()));
        }
        last = Some(name);
        match name.strip_prefix(USER) {
            Some(key) => {
                user = user.saturating_add(key.len()).saturating_add(value.len());
            }
            None => {
                system = system
                    .saturating_add(name.len())
                    .saturating_add(value.len());
            }
        }
    }
    if user > MAX_USER || system > MAX_SYSTEM {
        return Err(MetadataError::TooLarge);
    }
    Ok(())
}

/// The value kept under `name` in `pairs`, a version's metadata.
pub fn value<'a>(pairs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    pairs
        .binary_search_by(|(n, _)| n.as_str().cmp(name))
        .ok()
        .and_then(|at| pairs.get(at))
        .map(|(_, v)| v.as_str())
}

/// A kept value as a response writes it (89 §1.3): as it is where it is all US-ASCII; otherwise
/// `=?UTF-8?B?…?=` around the base64 of the UTF-8 of its octets each read as an ISO-8859-1
/// character, which is S3's answer to its own example (`x-amz-meta-nonascii: ÄMÄZÕÑ S3` returned
/// as `=?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?=`).
pub fn shown(value: &str) -> Cow<'_, str> {
    if value.is_ascii() {
        return Cow::Borrowed(value);
    }
    let latin: String = value.bytes().map(char::from).collect();
    Cow::Owned(format!("=?UTF-8?B?{}?=", BASE64.encode(latin.as_bytes())))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The user guide's example (89 §1.3): a value with characters past US-ASCII comes back as
    /// S3 returns it, an all-ASCII one as it went.
    #[test]
    fn a_value_past_ascii_is_returned_as_s3_returns_its_example() {
        assert_eq!(
            shown("ÄMÄZÕÑ S3"),
            "=?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?="
        );
        assert_eq!(shown("AMAZONS3"), "AMAZONS3");
        assert_eq!(shown("=?UTF-8?B?QQ==?="), "=?UTF-8?B?QQ==?=");
    }

    #[test]
    fn a_requests_metadata_is_kept_lowercase_sorted_and_joined() {
        let headers = [
            ("Content-Type", "text/plain"),
            ("X-Amz-Meta-Author", "Janet"),
            ("x-amz-meta-tag", "a"),
            ("Host", "example.com"),
            ("x-amz-meta-TAG", "b"),
            ("Cache-Control", "no-cache"),
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ("Expires", "Thu, 01 Dec 1994 16:00:00 GMT"),
            ("x-amz-meta-empty", ""),
        ];
        let kept = of_request(headers).unwrap();
        assert_eq!(
            kept,
            [
                ("cache-control", "no-cache"),
                ("content-type", "text/plain"),
                ("expires", "Thu, 01 Dec 1994 16:00:00 GMT"),
                ("x-amz-meta-author", "Janet"),
                ("x-amz-meta-empty", ""),
                ("x-amz-meta-tag", "a,b"),
            ]
            .map(|(n, v)| (n.to_owned(), v.to_owned()))
        );
        assert_eq!(value(&kept, "content-type"), Some("text/plain"));
        assert_eq!(value(&kept, "content-language"), None);
    }

    /// s3-tests' cases of a streamed body's `Content-Encoding` (research/05 §1.8): `aws-chunked`
    /// is left out, and an encoding of it alone is no header.
    #[test]
    fn aws_chunked_is_no_coding_of_the_object() {
        let encoding = |sent: &str| {
            let kept = of_request([("Content-Encoding", sent)]).unwrap();
            value(&kept, "content-encoding").map(str::to_owned)
        };
        assert_eq!(encoding("gzip, aws-chunked").as_deref(), Some("gzip"));
        assert_eq!(encoding("aws-chunked, gzip").as_deref(), Some("gzip"));
        assert_eq!(encoding("aws-chunked"), None);
        assert_eq!(encoding("aws-chunked, aws-chunked"), None);
        assert_eq!(encoding("gzip").as_deref(), Some("gzip"));
    }

    /// S3's 2 KB of user-defined metadata, a key counted past its prefix, and of the content
    /// headers kept, each name and value (89 §1.4): at the bound kept, a byte past it refused.
    #[test]
    fn each_half_is_held_to_s3s_two_kilobytes() {
        let name = "x-amz-meta-k";
        let at = "v".repeat(MAX_USER - 1);
        assert!(of_request([(name, at.as_str())]).is_ok());
        let past = "v".repeat(MAX_USER);
        assert_eq!(
            of_request([(name, past.as_str())]),
            Err(MetadataError::TooLarge)
        );
        let fits = "t".repeat(MAX_SYSTEM - "content-type".len());
        assert!(of_request([("content-type", fits.as_str())]).is_ok());
        let over = "t".repeat(MAX_SYSTEM - "content-type".len() + 1);
        assert_eq!(
            of_request([("content-type", over.as_str())]),
            Err(MetadataError::TooLarge)
        );
        // Both halves at their bounds at once.
        assert!(of_request([(name, at.as_str()), ("content-type", fits.as_str())]).is_ok());
        assert_eq!(MetadataError::TooLarge.code(), ("MetadataTooLarge", 400));
    }

    /// What a native request could name and an HTTP field could not is refused: a control
    /// character, a name out of order or repeated, an uppercase name, a header no version keeps.
    #[test]
    fn metadata_no_field_carries_is_refused() {
        let pairs = |list: &[(&str, &str)]| -> Vec<(String, String)> {
            list.iter()
                .map(|(n, v)| ((*n).to_owned(), (*v).to_owned()))
                .collect()
        };
        assert!(check(&pairs(&[("content-type", "a/b"), ("x-amz-meta-a", "é")])).is_ok());
        for bad in [
            pairs(&[("x-amz-meta-a", "line\nbreak")]),
            pairs(&[("x-amz-meta-a", " lead")]),
            pairs(&[("x-amz-meta-b", "1"), ("x-amz-meta-a", "2")]),
            pairs(&[("x-amz-meta-a", "1"), ("x-amz-meta-a", "2")]),
            pairs(&[("x-amz-meta-A", "1")]),
            pairs(&[("x-amz-meta-a b", "1")]),
            pairs(&[("host", "example.com")]),
        ] {
            assert!(
                matches!(check(&bad), Err(MetadataError::Invalid(_))),
                "{bad:?}"
            );
        }
    }
}
