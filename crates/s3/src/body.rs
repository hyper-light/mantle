//! The XML bodies S3 requests carry (docs/research/13 §6), each read strictly against its
//! schema: an element the schema does not define, or one given twice, is `MalformedXML`.
//!
//! Each body has a size limit computed from S3's own limits (docs/design/s3-protocol.md §2):
//! the most items S3 allows, every field at its longest and written with the most escaping a
//! serializer uses, and as much white space again. The gateway refuses a longer body before
//! reading it.

use crate::checksum::Algorithm;
use crate::route::MAX_KEY;
use crate::xml::{NAMESPACE, Reader, XmlError};

/// A field of arbitrary text at its longest: every byte written as `&quot;`, `&apos;` or
/// `&#x0D;`, six bytes, the longest escape of one byte that XML 1.0 §2.4 or S3's key rules
/// call for (05 §10.1). Digits, hex and base64 need no escape and count once.
const ESCAPED: usize = 6;

/// White space between elements carries nothing and has no length of its own to bound, so
/// a body may hold as much of it as of everything else.
const SPACE: usize = 2;

/// The XML declaration and the root's namespace declaration.
const PROLOG: usize =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?> xmlns=\"\"".len() + NAMESPACE.len();

/// The longest ETag mantle writes, a multipart upload's, with its quotes escaped.
const ETAG: usize = "&quot;d41d8cd98f00b204e9800998ecf8427e-10000&quot;".len();

/// Parts in a multipart upload (05 §4.1).
pub const MAX_PARTS: u16 = 10_000;

/// "The request can contain a list of up to 1,000 keys" (05 §8.1).
pub const MAX_OBJECTS: usize = 1000;

/// "Version IDs are ... URL-ready, opaque strings that are no more than 1,024 bytes long"
/// (13 §6.4); URL-ready text needs no XML escape.
const MAX_VERSION_ID: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BodyError {
    #[error(transparent)]
    Xml(#[from] XmlError),
    #[error("a part number outside 1 to 10,000")]
    InvalidPart,
    #[error("parts not listed in ascending order of part number")]
    InvalidPartOrder,
    #[error("{0}, which mantle does not implement")]
    NotImplemented(&'static str),
}

impl BodyError {
    /// The S3 error code and status (05 §4.4, §11.2).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Xml(error) => error.code(),
            Self::InvalidPart => ("InvalidPart", 400),
            Self::InvalidPartOrder => ("InvalidPartOrder", 400),
            Self::NotImplemented(_) => ("NotImplemented", 501),
        }
    }
}

/// A part CompleteMultipartUpload lists (05 §4.4; 13 §6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedPart {
    pub number: u16,
    /// As sent, quoted or not.
    pub etag: String,
    /// As sent, for comparison with the part's own.
    pub checksums: Vec<(Algorithm, String)>,
}

/// Every checksum element a part may carry, each holding its base64 value.
const CHECKSUMS: usize = {
    let mut total = 0;
    let mut rest: &[Algorithm] = &Algorithm::ALL;
    while let [algorithm, tail @ ..] = rest {
        total += 2 * algorithm.element().len() + "<></>".len() + 4 * algorithm.width().div_ceil(3);
        rest = tail;
    }
    total
};

/// The largest CompleteMultipartUpload body: 10,000 parts, each with a five-digit number, the
/// longest ETag and all ten checksums.
pub const COMPLETE_LIMIT: usize = SPACE
    * (PROLOG
        + "<CompleteMultipartUpload></CompleteMultipartUpload>".len()
        + MAX_PARTS as usize
            * ("<Part></Part><PartNumber>10000</PartNumber><ETag></ETag>".len()
                + ETAG
                + CHECKSUMS));

/// CompleteMultipartUpload's parts. Each part is checked as it is read, so a list never holds
/// more than 10,000: numbers from 1 to 10,000 (`InvalidPart`) in ascending order
/// (`InvalidPartOrder`), and at least one part (05 §4.4).
pub fn complete(body: &[u8]) -> Result<Vec<CompletedPart>, BodyError> {
    let mut reader = Reader::open(body, COMPLETE_LIMIT, "CompleteMultipartUpload")?;
    let mut parts: Vec<CompletedPart> = Vec::new();
    while let Some(name) = reader.child()? {
        if name != "Part" {
            return Err(schema("an element CompleteMultipartUpload does not have"));
        }
        let part = part(&mut reader)?;
        if parts.last().is_some_and(|last| last.number >= part.number) {
            return Err(BodyError::InvalidPartOrder);
        }
        parts.push(part);
    }
    reader.finish()?;
    if parts.is_empty() {
        return Err(schema("no parts"));
    }
    Ok(parts)
}

fn part(reader: &mut Reader<'_>) -> Result<CompletedPart, BodyError> {
    let (mut number, mut etag) = (None, None);
    let mut checksums: Vec<(Algorithm, String)> = Vec::new();
    while let Some(name) = reader.child()? {
        match name {
            "PartNumber" => once(&mut number, int(&reader.text()?)?)?,
            "ETag" => once(&mut etag, reader.text()?.into_owned())?,
            _ => {
                let algorithm = Algorithm::ALL
                    .into_iter()
                    .find(|a| a.element() == name)
                    .ok_or(schema("an element Part does not have"))?;
                if checksums.iter().any(|(seen, _)| *seen == algorithm) {
                    return Err(schema("an element given twice"));
                }
                checksums.push((algorithm, reader.text()?.into_owned()));
            }
        }
    }
    let number = number.ok_or(schema("a part without a PartNumber"))?;
    let etag = etag.ok_or(schema("a part without an ETag"))?;
    let number = u16::try_from(number)
        .ok()
        .filter(|n| (1..=MAX_PARTS).contains(n))
        .ok_or(BodyError::InvalidPart)?;
    Ok(CompletedPart {
        number,
        etag,
        checksums,
    })
}

/// DeleteObjects' request (05 §8.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub objects: Vec<ObjectIdentifier>,
    /// Report only the objects that could not be deleted.
    pub quiet: bool,
}

/// An object DeleteObjects names (13 §6.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIdentifier {
    pub key: String,
    pub version_id: Option<String>,
    /// Delete only if the current version's ETag matches (05 §2.3).
    pub etag: Option<String>,
}

/// The largest DeleteObjects body: 1,000 objects, each with the longest key, version ID and
/// ETag, and the directory-bucket conditions at their longest: an ISO 8601 time with
/// nanoseconds and an offset, and the most negative 64-bit size.
pub const DELETE_LIMIT: usize = SPACE
    * (PROLOG
        + "<Delete></Delete><Quiet>false</Quiet>".len()
        + MAX_OBJECTS
            * ("<Object></Object><Key></Key><VersionId></VersionId><ETag></ETag>".len()
                + ESCAPED * MAX_KEY
                + MAX_VERSION_ID
                + ETAG
                + "<LastModifiedTime>1970-01-01T00:00:00.000000000+00:00</LastModifiedTime>"
                    .len()
                + "<Size>-9223372036854775808</Size>".len()));

/// DeleteObjects' objects, from 1 to 1,000, each with a key of at least one byte (13 §6.3).
pub fn delete(body: &[u8]) -> Result<Delete, BodyError> {
    let mut reader = Reader::open(body, DELETE_LIMIT, "Delete")?;
    let mut objects = Vec::new();
    let mut quiet = None;
    while let Some(name) = reader.child()? {
        match name {
            "Object" => {
                if objects.len() >= MAX_OBJECTS {
                    return Err(schema("more than 1,000 objects"));
                }
                objects.push(object(&mut reader)?);
            }
            "Quiet" => once(&mut quiet, boolean(&reader.text()?)?)?,
            _ => return Err(schema("an element Delete does not have")),
        }
    }
    reader.finish()?;
    if objects.is_empty() {
        return Err(schema("no objects"));
    }
    Ok(Delete {
        objects,
        quiet: quiet.unwrap_or(false),
    })
}

fn object(reader: &mut Reader<'_>) -> Result<ObjectIdentifier, BodyError> {
    let (mut key, mut version_id, mut etag) = (None, None, None);
    while let Some(name) = reader.child()? {
        let field = match name {
            "Key" => &mut key,
            "VersionId" => &mut version_id,
            "ETag" => &mut etag,
            "LastModifiedTime" | "Size" => {
                return Err(BodyError::NotImplemented(
                    "a delete conditioned on time or size, which S3 honors for directory buckets",
                ));
            }
            _ => return Err(schema("an element Object does not have")),
        };
        once(field, reader.text()?.into_owned())?;
    }
    let key = key
        .filter(|k: &String| !k.is_empty())
        .ok_or(schema("an object without a key"))?;
    Ok(ObjectIdentifier {
        key,
        version_id,
        etag,
    })
}

/// A region name at its longest: a DNS label, since it names S3's endpoints, at most 63
/// octets (RFC 1035 §2.3.4) of letters, digits and hyphens, which need no escape.
const MAX_REGION: usize = 63;

/// The largest CreateBucketConfiguration body mantle reads: a location constraint.
pub const CREATE_BUCKET_LIMIT: usize = SPACE
    * (PROLOG
        + "<CreateBucketConfiguration></CreateBucketConfiguration>".len()
        + "<LocationConstraint></LocationConstraint>".len()
        + MAX_REGION);

/// CreateBucket's `LocationConstraint`, as sent (13 §6.5). A CreateBucket without a body has
/// none; this reads a body that is there.
pub fn create_bucket(body: &[u8]) -> Result<Option<String>, BodyError> {
    let mut reader = Reader::open(body, CREATE_BUCKET_LIMIT, "CreateBucketConfiguration")?;
    let mut location = None;
    while let Some(name) = reader.child()? {
        match name {
            "LocationConstraint" => once(&mut location, reader.text()?.into_owned())?,
            "Location" | "Bucket" => return Err(BodyError::NotImplemented("directory buckets")),
            "Tags" => return Err(BodyError::NotImplemented("tags on a new bucket")),
            _ => return Err(schema("an element CreateBucketConfiguration does not have")),
        }
    }
    reader.finish()?;
    Ok(location)
}

/// PutBucketVersioning's request (13 §6.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VersioningConfiguration {
    pub status: Option<Versioning>,
    /// `Enabled` or `Disabled`.
    pub mfa_delete: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Versioning {
    Enabled,
    Suspended,
}

/// The largest VersioningConfiguration body.
pub const VERSIONING_LIMIT: usize = SPACE
    * (PROLOG
        + "<VersioningConfiguration></VersioningConfiguration>".len()
        + "<Status>Suspended</Status><MfaDelete>Disabled</MfaDelete>".len());

/// PutBucketVersioning's configuration. Its values are exact: an enumeration of `xs:string`
/// keeps its white space (XML Schema Part 2 §4.3.6; 13 §5).
pub fn versioning(body: &[u8]) -> Result<VersioningConfiguration, BodyError> {
    let mut reader = Reader::open(body, VERSIONING_LIMIT, "VersioningConfiguration")?;
    let mut configuration = VersioningConfiguration::default();
    let (mut status, mut mfa_delete) = (None, None);
    while let Some(name) = reader.child()? {
        match name {
            "Status" => once(&mut status, reader.text()?.into_owned())?,
            "MfaDelete" => once(&mut mfa_delete, reader.text()?.into_owned())?,
            _ => return Err(schema("an element VersioningConfiguration does not have")),
        }
    }
    reader.finish()?;
    configuration.status = match status.as_deref() {
        None => None,
        Some("Enabled") => Some(Versioning::Enabled),
        Some("Suspended") => Some(Versioning::Suspended),
        Some(_) => {
            return Err(schema(
                "a versioning status other than Enabled or Suspended",
            ));
        }
    };
    configuration.mfa_delete = match mfa_delete.as_deref() {
        None => None,
        Some("Enabled") => Some(true),
        Some("Disabled") => Some(false),
        Some(_) => {
            return Err(schema(
                "an MFA delete setting other than Enabled or Disabled",
            ));
        }
    };
    Ok(configuration)
}

const fn schema(reason: &'static str) -> BodyError {
    BodyError::Xml(XmlError::Schema(reason))
}

/// Sets a field; an element given twice is refused.
fn once<T>(field: &mut Option<T>, value: T) -> Result<(), BodyError> {
    match field.replace(value) {
        Some(_) => Err(schema("an element given twice")),
        None => Ok(()),
    }
}

/// XML Schema's white space collapse, which every atomic type but `string` applies (Part 2
/// §4.3.6); at the ends of a single token it trims.
fn collapsed(text: &str) -> &str {
    text.trim_matches(|c| matches!(c, ' ' | '\t' | '\r' | '\n'))
}

/// An `xs:int`: "a finite-length sequence of decimal digits (#x30-#x39) with an optional
/// leading sign", from -2147483648 to 2147483647 (XML Schema Part 2 §3.3.13, §3.3.17).
fn int(text: &str) -> Result<i32, BodyError> {
    let text = collapsed(text);
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(schema("not an integer"));
    }
    text.parse().map_err(|_| schema("an integer out of range"))
}

/// An `xs:boolean`: "true, false, 1, 0" (XML Schema Part 2 §3.2.2).
fn boolean(text: &str) -> Result<bool, BodyError> {
    match collapsed(text) {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(schema("not a boolean")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(number: u16, etag: &str) -> CompletedPart {
        CompletedPart {
            number,
            etag: etag.into(),
            checksums: Vec::new(),
        }
    }

    /// AWS's sample request, whose root carries no namespace (13 §6.1).
    #[test]
    fn complete_reads_aws_sample() {
        let body = br#"
            <CompleteMultipartUpload>
             <Part>
                <PartNumber>1</PartNumber>
               <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
             </Part>
             <Part>
                <PartNumber>2</PartNumber>
               <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
             </Part>
             <Part>
               <PartNumber>3</PartNumber>
               <ETag>"acbd18db4cc2f85cedef654fccc4a4d8"</ETag>
             </Part>
            </CompleteMultipartUpload>"#;
        assert_eq!(
            complete(body),
            Ok(vec![
                part(1, "\"a54357aff0632cce46d942af68356b38\""),
                part(2, "\"0c78aef83f66abc1fa1e8477f296d394\""),
                part(3, "\"acbd18db4cc2f85cedef654fccc4a4d8\""),
            ])
        );
    }

    #[test]
    fn complete_checks_parts_and_their_order() {
        let body =
            |parts: &str| format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>");
        let p = |n: &str| format!("<Part><PartNumber>{n}</PartNumber><ETag>e</ETag></Part>");
        let with_checksums = "<Part><ChecksumCRC32>AAAAAA==</ChecksumCRC32>\
            <PartNumber> +0001 </PartNumber><ChecksumSHA256>x</ChecksumSHA256><ETag>e</ETag></Part>";
        let read = complete(body(with_checksums).as_bytes()).unwrap();
        assert_eq!(read[0].number, 1);
        assert_eq!(
            read[0].checksums,
            [
                (Algorithm::Crc32, "AAAAAA==".to_string()),
                (Algorithm::Sha256, "x".to_string())
            ]
        );
        let code = |parts: String| complete(body(&parts).as_bytes()).map_err(|e| e.code().0);
        assert_eq!(code(p("2") + &p("1")), Err("InvalidPartOrder"));
        assert_eq!(code(p("2") + &p("2")), Err("InvalidPartOrder"));
        assert_eq!(code(p("0")), Err("InvalidPart"));
        assert_eq!(code(p("10001")), Err("InvalidPart"));
        assert_eq!(code(p("-1")), Err("InvalidPart"));
        assert_eq!(code(p("99999999999")), Err("MalformedXML"));
        assert_eq!(code(p("1.0")), Err("MalformedXML"));
        assert_eq!(code(String::new()), Err("MalformedXML"));
        assert_eq!(
            code("<Part><PartNumber>1</PartNumber></Part>".into()),
            Err("MalformedXML")
        );
        assert_eq!(
            code(
                "<Part><PartNumber>1</PartNumber><PartNumber>2</PartNumber><ETag>e</ETag></Part>"
                    .into()
            ),
            Err("MalformedXML")
        );
        assert_eq!(
            code("<Part><PartNumber>1</PartNumber><ETag>e</ETag><Size>1</Size></Part>".into()),
            Err("MalformedXML")
        );
        // s3-tests: CompleteMultipartUpload with no body is MalformedXML (05 §4.4).
        assert_eq!(complete(b"").map_err(|e| e.code().0), Err("MalformedXML"));
        let all: String = (1..=MAX_PARTS).map(|n| p(&n.to_string())).collect();
        assert_eq!(complete(body(&all).as_bytes()).unwrap().len(), 10_000);
    }

    /// The limit admits the longest body S3's limits allow: 10,000 parts with every field
    /// at its longest.
    #[test]
    fn complete_limit_admits_the_longest_body() {
        let checksums: String = Algorithm::ALL
            .into_iter()
            .map(|a| {
                let value = "A".repeat(4 * a.width().div_ceil(3));
                format!("<{0}>{value}</{0}>", a.element())
            })
            .collect();
        let parts: String = (1..=MAX_PARTS)
            .map(|n| {
                format!(
                    "<Part><PartNumber>{n:05}</PartNumber>\
                     <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e-10000&quot;</ETag>{checksums}</Part>"
                )
            })
            .collect();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"{NAMESPACE}\">\
             {parts}</CompleteMultipartUpload>"
        );
        assert!(
            body.len() * 2 <= COMPLETE_LIMIT + 1,
            "{} > {COMPLETE_LIMIT}",
            body.len()
        );
        assert_eq!(complete(body.as_bytes()).unwrap().len(), 10_000);
        assert_eq!(
            complete(&vec![b' '; COMPLETE_LIMIT + 1]),
            Err(BodyError::Xml(XmlError::TooLarge {
                limit: COMPLETE_LIMIT
            }))
        );
    }

    /// AWS's DeleteObjects shape and s3-tests' limits (05 §8.1).
    #[test]
    fn delete_reads_objects_and_quiet() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
            <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Object><Key>a&#13;b</Key></Object>
               <Object><Key>c</Key><VersionId>v1</VersionId><ETag>"e"</ETag></Object>
               <Quiet>true</Quiet>
            </Delete>"#;
        assert_eq!(
            delete(body),
            Ok(Delete {
                objects: vec![
                    ObjectIdentifier {
                        key: "a\rb".into(),
                        version_id: None,
                        etag: None,
                    },
                    ObjectIdentifier {
                        key: "c".into(),
                        version_id: Some("v1".into()),
                        etag: Some("\"e\"".into()),
                    },
                ],
                quiet: true,
            })
        );
        let objects = |n: usize| "<Object><Key>k</Key></Object>".repeat(n);
        let code = |inner: String| {
            delete(format!("<Delete>{inner}</Delete>").as_bytes()).map_err(|e| e.code().0)
        };
        assert!(code(objects(1000)).is_ok());
        assert_eq!(code(objects(1001)), Err("MalformedXML"));
        assert_eq!(code(String::new()), Err("MalformedXML"));
        assert_eq!(
            code("<Object><Key></Key></Object>".into()),
            Err("MalformedXML")
        );
        assert_eq!(
            code("<Object><VersionId>v</VersionId></Object>".into()),
            Err("MalformedXML")
        );
        assert_eq!(
            code(objects(1) + "<Quiet>maybe</Quiet>"),
            Err("MalformedXML")
        );
        assert_eq!(
            code(objects(1) + "<Quiet>false</Quiet><Quiet>true</Quiet>"),
            Err("MalformedXML")
        );
        assert_eq!(
            code("<Object><Key>k</Key><Size>1</Size></Object>".into()),
            Err("NotImplemented")
        );
        assert_eq!(
            delete(format!("<Delete>{}<Quiet> 0 </Quiet></Delete>", objects(1)).as_bytes())
                .map(|d| d.quiet),
            Ok(false)
        );
    }

    #[test]
    fn delete_limit_admits_the_longest_body() {
        let object = format!(
            "<Object><Key>{}</Key><VersionId>{}</VersionId>\
             <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e-10000&quot;</ETag></Object>",
            "&quot;".repeat(MAX_KEY),
            "v".repeat(MAX_VERSION_ID)
        );
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete xmlns=\"{NAMESPACE}\">{}\
             <Quiet>false</Quiet></Delete>",
            object.repeat(MAX_OBJECTS)
        );
        assert!(
            body.len() * 2 <= DELETE_LIMIT,
            "{} > {DELETE_LIMIT}",
            body.len()
        );
        let read = delete(body.as_bytes()).unwrap();
        assert_eq!(read.objects.len(), MAX_OBJECTS);
        assert_eq!(read.objects[0].key, "\"".repeat(MAX_KEY));
    }

    /// AWS's CreateBucket sample, with its end tag's trailing space (13 §6.1).
    #[test]
    fn create_bucket_reads_the_location_constraint() {
        let body =
            b"<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"> \n\
             <LocationConstraint>EU</LocationConstraint> \n</CreateBucketConfiguration >";
        assert_eq!(create_bucket(body), Ok(Some("EU".into())));
        assert_eq!(create_bucket(b"<CreateBucketConfiguration/>"), Ok(None));
        let tagged =
            b"<CreateBucketConfiguration><Tags><Tag><Key>a</Key><Value>b</Value></Tag></Tags>\
             </CreateBucketConfiguration>";
        assert_eq!(
            create_bucket(tagged).map_err(|e| e.code()),
            Err(("NotImplemented", 501))
        );
        assert_eq!(
            create_bucket(b"<CreateBucketConfiguration><Location/></CreateBucketConfiguration>")
                .map_err(|e| e.code().0),
            Err("NotImplemented")
        );
    }

    /// AWS's PutBucketVersioning samples (13 §6.6).
    #[test]
    fn versioning_reads_status_and_mfa_delete() {
        let body =
            b"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"> \n\
             <Status>Enabled</Status> \n<MfaDelete>Enabled</MfaDelete>\n</VersioningConfiguration>";
        assert_eq!(
            versioning(body),
            Ok(VersioningConfiguration {
                status: Some(Versioning::Enabled),
                mfa_delete: Some(true),
            })
        );
        assert_eq!(
            versioning(
                b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"
            ),
            Ok(VersioningConfiguration {
                status: Some(Versioning::Suspended),
                mfa_delete: None,
            })
        );
        assert_eq!(
            versioning(b"<VersioningConfiguration/>"),
            Ok(VersioningConfiguration::default())
        );
        for bad in [
            &b"<VersioningConfiguration><Status>enabled</Status></VersioningConfiguration>"[..],
            b"<VersioningConfiguration><Status> Enabled</Status></VersioningConfiguration>",
            b"<VersioningConfiguration><MFADelete>Enabled</MFADelete></VersioningConfiguration>",
        ] {
            assert_eq!(versioning(bad).map_err(|e| e.code().0), Err("MalformedXML"));
        }
    }
}
