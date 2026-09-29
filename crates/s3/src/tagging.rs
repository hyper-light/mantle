//! Tags on objects and buckets (docs/research/13 §6.7): the rules a tag set follows, whether
//! it arrives in a `Tagging` document ([`crate::body::tagging`]), in CreateBucket's
//! configuration, or, for an object, in the `x-amz-tagging` header of PutObject,
//! CreateMultipartUpload and CopyObject.

use std::panic::{AssertUnwindSafe, catch_unwind};

use unicode_general_category::{GeneralCategory as Category, get_general_category};

use crate::sigv4::percent_decode;

/// A tag. Keys and values are case-sensitive (13 §6.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// What a tag set is on, which bounds how many tags it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tagged {
    Object,
    Bucket,
}

impl Tagged {
    /// "You can associate up to 10 tags with an object"; a bucket's "tag set can contain as
    /// many as 50 tags" (13 §6.7).
    pub const fn limit(self) -> usize {
        match self {
            Self::Object => 10,
            Self::Bucket => 50,
        }
    }
}

/// A key's longest, in UTF-16 code units: "A tag key can be up to 128 Unicode characters in
/// length ... Amazon S3 object tags are internally represented in UTF-16. Note that in UTF-16,
/// characters consume either 1 or 2 character positions" (13 §6.7).
pub const MAX_KEY: usize = 128;

/// A value's longest, in UTF-16 code units (13 §6.7).
pub const MAX_VALUE: usize = 256;

/// The prefix of the tags AWS applies itself, which a request may not set (13 §6.7).
const RESERVED: &str = "aws:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TagError {
    #[error("an object holds at most 10 tags, and a bucket 50")]
    TooMany,
    #[error("a tag key must hold 1 to 128 letters, numbers, spaces or _ . : / = + @ -")]
    Key,
    #[error("a tag value must hold at most 256 letters, numbers, spaces or _ . : / = + @ -")]
    Value,
    #[error("tag keys starting aws: are AWS's own")]
    Reserved,
    #[error("two tags with one key")]
    Duplicate,
    #[error(
        "the header 'x-amz-tagging' must be UTF-8, URL-encoded as query parameters, without a tag key given twice"
    )]
    Header,
    #[error("the Unicode tables failed")]
    Internal,
}

impl TagError {
    /// The S3 error code and status (13 §6.7).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            // What S3 answered both PutObject's header and PutObjectTagging.
            Self::TooMany => ("BadRequest", 400),
            Self::Header => ("InvalidArgument", 400),
            Self::Key | Self::Value | Self::Reserved | Self::Duplicate => ("InvalidTag", 400),
            Self::Internal => ("InternalError", 500),
        }
    }
}

/// `tags` checked against S3's rules and put in key order, the order S3 gives them back in
/// (13 §6.7).
pub fn check(mut tags: Vec<Tag>, tagged: Tagged) -> Result<Vec<Tag>, TagError> {
    if tags.len() > tagged.limit() {
        return Err(TagError::TooMany);
    }
    for tag in &tags {
        if tag.key.is_empty() || tag.key.encode_utf16().count() > MAX_KEY || !allowed(&tag.key)? {
            return Err(TagError::Key);
        }
        if tag.value.encode_utf16().count() > MAX_VALUE || !allowed(&tag.value)? {
            return Err(TagError::Value);
        }
        if tag.key.starts_with(RESERVED) {
            return Err(TagError::Reserved);
        }
    }
    tags.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    if tags
        .windows(2)
        .any(|pair| matches!(pair, [a, b] if a.key == b.key))
    {
        return Err(TagError::Duplicate);
    }
    Ok(tags)
}

/// The tags an `x-amz-tagging` header names, checked: URL query parameters,
/// `Key1=Value1&Key2=Value2`, each side UTF-8 URL-encoded as a form encodes it, and a key
/// without `=` holding the empty value (13 §6.7). A key given twice is refused with the rest
/// of what breaks that form.
pub fn header(value: &str) -> Result<Vec<Tag>, TagError> {
    let mut tags = Vec::new();
    for pair in value.split('&').filter(|p| !p.is_empty()) {
        if tags.len() >= Tagged::Object.limit() {
            return Err(TagError::TooMany);
        }
        let mut sides = pair.split('=');
        let key = sides.next().unwrap_or_default();
        let value = sides.next().unwrap_or_default();
        if sides.next().is_some() {
            return Err(TagError::Header);
        }
        tags.push(Tag {
            key: decoded(key)?,
            value: decoded(value)?,
        });
    }
    check(tags, Tagged::Object).map_err(|error| match error {
        TagError::Duplicate => TagError::Header,
        error => error,
    })
}

/// One side of a query parameter, decoded as the form it is written in: `+` is a space, and
/// `%XX` a byte (13 §6.7).
fn decoded(text: &str) -> Result<String, TagError> {
    let spaced: Vec<u8> = text
        .bytes()
        .map(|b| if b == b'+' { b' ' } else { b })
        .collect();
    let bytes = percent_decode(&spaced).ok_or(TagError::Header)?;
    String::from_utf8(bytes).map_err(|_| TagError::Header)
}

/// Whether `text` holds only what a tag may: "Unicode letters or numbers, white space, and
/// the following symbols: _ . : / = + @ -", AWS's pattern `[\p{L}\p{Z}\p{N}_.:/=+\-@]`
/// (13 §6.7). The category tables are a dependency's, looked up behind an unwind boundary
/// (CLAUDE.md §1).
fn allowed(text: &str) -> Result<bool, TagError> {
    catch_unwind(AssertUnwindSafe(|| {
        text.chars().all(|c| {
            matches!(c, '_' | '.' | ':' | '/' | '=' | '+' | '@' | '-')
                || matches!(
                    get_general_category(c),
                    Category::UppercaseLetter
                        | Category::LowercaseLetter
                        | Category::TitlecaseLetter
                        | Category::ModifierLetter
                        | Category::OtherLetter
                        | Category::DecimalNumber
                        | Category::LetterNumber
                        | Category::OtherNumber
                        | Category::SpaceSeparator
                        | Category::LineSeparator
                        | Category::ParagraphSeparator
                )
        })
    }))
    .map_err(|_| TagError::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: key.into(),
            value: value.into(),
        }
    }

    fn tags(pairs: &[(&str, &str)]) -> Vec<Tag> {
        pairs.iter().map(|&(k, v)| tag(k, v)).collect()
    }

    /// s3-tests' limits: 10 tags of a 128-character key and a 256-character value are
    /// accepted, and one more tag, or one more character, is not (13 §6.7).
    #[test]
    fn limits_are_s3s() {
        let (key, value) = ("k".repeat(MAX_KEY), "v".repeat(MAX_VALUE));
        let most: Vec<Tag> = (0..10)
            .map(|i| tag(&format!("{i}{}", "k".repeat(MAX_KEY - 1)), &value))
            .collect();
        assert_eq!(check(most.clone(), Tagged::Object).unwrap().len(), 10);
        let mut eleven = most.clone();
        eleven.push(tag("x", ""));
        assert_eq!(
            check(eleven.clone(), Tagged::Object),
            Err(TagError::TooMany)
        );
        assert_eq!(check(eleven, Tagged::Bucket).unwrap().len(), 11);
        let fifty_one: Vec<Tag> = (0..51).map(|i| tag(&i.to_string(), "")).collect();
        assert_eq!(check(fifty_one, Tagged::Bucket), Err(TagError::TooMany));
        let long_key = tags(&[(&format!("{key}k"), "")]);
        assert_eq!(check(long_key, Tagged::Object), Err(TagError::Key));
        let long_value = tags(&[("k", &format!("{value}v"))]);
        assert_eq!(check(long_value, Tagged::Object), Err(TagError::Value));
        assert_eq!(
            check(tags(&[("", "v")]), Tagged::Object),
            Err(TagError::Key)
        );
        assert_eq!(check(Vec::new(), Tagged::Bucket), Ok(Vec::new()));
    }

    /// Lengths count UTF-16 code units: a character beyond the Basic Multilingual Plane
    /// takes two.
    #[test]
    fn lengths_count_utf16_units() {
        let bmp = "é".repeat(MAX_KEY);
        assert!(check(tags(&[(&bmp, "")]), Tagged::Object).is_ok());
        // U+10400 DESERET CAPITAL LETTER LONG I, a letter taking two units.
        let astral = "\u{10400}".repeat(MAX_KEY / 2);
        assert!(check(tags(&[(&astral, "")]), Tagged::Object).is_ok());
        let over = format!("{astral}a");
        assert_eq!(
            check(tags(&[(&over, "")]), Tagged::Object),
            Err(TagError::Key)
        );
    }

    #[test]
    fn characters_are_letters_numbers_spaces_and_eight_symbols() {
        let fine = [
            "Cost Center",
            "team:storage/east=1+2@x_y.z-w",
            "Übergröße",
            "東京",
            "٣٤",
            "Ⅻ",
            "a\u{3000}b",
        ];
        for text in fine {
            assert!(
                check(tags(&[(text, text)]), Tagged::Object).is_ok(),
                "{text}"
            );
        }
        // aws-cli issue 2841: S3 refused this key, decoded from its header.
        let refused = [
            "TagSet=[{Key=string,Value=string}]",
            "a,b",
            "a\tb",
            "a#b",
            "e\u{301}",
            "a\u{0}b",
        ];
        for text in refused {
            assert_eq!(
                check(tags(&[(text, "v")]), Tagged::Object),
                Err(TagError::Key)
            );
            assert_eq!(
                check(tags(&[("k", text)]), Tagged::Object),
                Err(TagError::Value)
            );
        }
    }

    #[test]
    fn keys_are_unique_unreserved_and_sorted() {
        assert_eq!(
            check(tags(&[("b", "1"), ("a", "2")]), Tagged::Object),
            Ok(tags(&[("a", "2"), ("b", "1")]))
        );
        assert_eq!(
            check(tags(&[("a", "1"), ("b", ""), ("a", "2")]), Tagged::Object),
            Err(TagError::Duplicate)
        );
        assert_eq!(
            check(
                tags(&[("aws:cloudformation:stack-name", "s")]),
                Tagged::Bucket
            ),
            Err(TagError::Reserved)
        );
        // Keys are case-sensitive, and so is the prefix.
        assert!(check(tags(&[("AWS:x", ""), ("aws", "")]), Tagged::Bucket).is_ok());
        assert!(check(tags(&[("a", ""), ("A", "")]), Tagged::Object).is_ok());
    }

    /// s3-tests' `foo=bar&bar` reads as `bar` with the empty value and `foo=bar`, in key
    /// order (13 §6.7).
    #[test]
    fn header_is_query_parameters() {
        assert_eq!(
            header("foo=bar&bar"),
            Ok(tags(&[("bar", ""), ("foo", "bar")]))
        );
        assert_eq!(header("tag1=value1&tag2=value2").unwrap().len(), 2);
        assert_eq!(header(""), Ok(Vec::new()));
        assert_eq!(header("a=1&&b=2&").unwrap().len(), 2);
        assert_eq!(
            header("Cost+Center=a%2Bb%20c&%C3%9Cber=gr%C3%B6%C3%9Fe"),
            Ok(tags(&[("Cost Center", "a+b c"), ("Über", "größe")]))
        );
    }

    /// aws-cli issue 2841: S3 answered a pair with a second `=` InvalidArgument, and the
    /// key decoded from `TagSet%3D%5B...` InvalidTag.
    #[test]
    fn header_refusals_are_s3s() {
        let code = |value: &str| header(value).map_err(|e| e.code().0);
        assert_eq!(
            code("TagSet=[{Key=string,Value=string}]"),
            Err("InvalidArgument")
        );
        assert_eq!(
            code("TagSet%3D%5B%7BKey%3Dstring%2CValue%3Dstring%7D%5D"),
            Err("InvalidTag")
        );
        assert_eq!(code("a=1&a=2"), Err("InvalidArgument"));
        assert_eq!(code("a=%ZZ"), Err("InvalidArgument"));
        assert_eq!(code("a=%FF"), Err("InvalidArgument"));
        assert_eq!(code("a=%"), Err("InvalidArgument"));
        let eleven: Vec<String> = (0..11).map(|i| format!("k{i}=v")).collect();
        assert_eq!(code(&eleven.join("&")), Err("BadRequest"));
        assert_eq!(code("aws:x=1"), Err("InvalidTag"));
        assert_eq!(code("=v"), Err("InvalidTag"));
    }
}
