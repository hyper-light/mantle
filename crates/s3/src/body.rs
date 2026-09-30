//! The XML bodies S3 requests carry (docs/research/13 §6), each read strictly against its
//! schema: an element the schema does not define, or one given twice, is `MalformedXML`.
//!
//! Each body has a size limit computed from S3's own limits (docs/design/s3-protocol.md §2):
//! the most items S3 allows, every field at its longest and written with the most escaping a
//! serializer uses. White space between elements, which carries nothing, is dropped as the
//! body is read (`xml::Compact`), so the limit bounds what is kept and a body is refused once
//! what is kept passes it; each reader here takes the body as kept.

use crate::acl::{Grant, Grantee, MAX_GRANTS, Ownership, Permission, Policy};
use crate::checksum::Algorithm;
use crate::cors::{self, CorsError};
use crate::lifecycle::{
    self, And, Filter, Given, GivenExpiration, GivenNoncurrent, GivenTransition, LifecycleError,
    Rule,
};
use crate::route::MAX_KEY;
use crate::tagging::{self, Tag, TagError, Tagged};
use crate::xml::{NAMESPACE, Reader, XmlError};

/// A field of arbitrary text at its longest: every byte written as `&quot;`, `&apos;` or
/// `&#x0D;`, six bytes, the longest escape of one byte that XML 1.0 §2.4 or S3's key rules
/// call for (05 §10.1). Digits, hex and base64 need no escape and count once.
const ESCAPED: usize = 6;

/// The XML declaration and the root's namespace declaration.
const PROLOG: usize =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?> xmlns=\"\"".len() + NAMESPACE.len();

/// The longest ETag mantle writes, a multipart upload's, with its quotes escaped.
const ETAG: usize = "&quot;d41d8cd98f00b204e9800998ecf8427e-10000&quot;".len();

/// Parts in a multipart upload (05 §4.1).
pub const MAX_PARTS: u16 = 10_000;

/// Bytes one PutObject or UploadPart carries at most (05 §4.1): a part runs "5 MiB to 5 GiB",
/// and a single PUT to "5 GB" as the upload guide writes it. The binary figure never refuses
/// an upload S3 takes.
pub const MAX_UPLOAD: u64 = 5 << 30;

/// "The request can contain a list of up to 1,000 keys" (05 §8.1).
pub const MAX_OBJECTS: usize = 1000;

/// "Version IDs are ... URL-ready, opaque strings that are no more than 1,024 bytes long"
/// (13 §6.4); URL-ready text needs no XML escape.
const MAX_VERSION_ID: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BodyError {
    #[error(transparent)]
    Xml(#[from] XmlError),
    #[error("a part number outside 1 to 10,000")]
    InvalidPart,
    #[error("parts not listed in ascending order of part number")]
    InvalidPartOrder,
    #[error("{0}, which mantle does not implement")]
    NotImplemented(&'static str),
    #[error(transparent)]
    Tag(#[from] TagError),
    #[error("the ACL is not well-formed or does not validate against S3's schema: {0}")]
    MalformedAcl(XmlError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Cors(#[from] CorsError),
    #[error(transparent)]
    Lock(#[from] crate::lock::LockError),
    #[error(transparent)]
    Sse(#[from] crate::sse::SseError),
}

impl BodyError {
    /// The S3 error code and status (05 §4.4, §11.2; 13 §6.7, §6.8).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Xml(error) => error.code(),
            Self::InvalidPart => ("InvalidPart", 400),
            Self::InvalidPartOrder => ("InvalidPartOrder", 400),
            Self::NotImplemented(_) => ("NotImplemented", 501),
            Self::Tag(error) => error.code(),
            Self::MalformedAcl(_) => ("MalformedACLError", 400),
            Self::Lifecycle(error) => error.code(),
            Self::Cors(error) => error.code(),
            Self::Lock(error) => error.code(),
            Self::Sse(error) => error.code(),
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
        // Base64's four-character groups: one per three bytes and one for a rest.
        let width = algorithm.width();
        let base64_groups = width / 3 + if width.is_multiple_of(3) { 0 } else { 1 };
        total += 2 * algorithm.element().len() + "<></>".len() + 4 * base64_groups;
        rest = tail;
    }
    total
};

/// The largest CompleteMultipartUpload body: 10,000 parts, each with a five-digit number, the
/// longest ETag and all ten checksums.
pub const COMPLETE_LIMIT: usize = PROLOG
    + "<CompleteMultipartUpload></CompleteMultipartUpload>".len()
    + MAX_PARTS as usize
        * ("<Part></Part><PartNumber>10000</PartNumber><ETag></ETag>".len() + ETAG + CHECKSUMS);

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
pub const DELETE_LIMIT: usize = PROLOG
    + "<Delete></Delete><Quiet>false</Quiet>".len()
    + MAX_OBJECTS
        * ("<Object></Object><Key></Key><VersionId></VersionId><ETag></ETag>".len()
            + ESCAPED * MAX_KEY
            + MAX_VERSION_ID
            + ETAG
            + "<LastModifiedTime>1970-01-01T00:00:00.000000000+00:00</LastModifiedTime>".len()
            + "<Size>-9223372036854775808</Size>".len());

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

/// UTF-8 bytes in one UTF-16 code unit at most: a character in the Basic Multilingual Plane
/// is one unit and at most three bytes, and one beyond it two units and four bytes
/// (RFC 3629 §3; RFC 2781 §2.1).
const UTF8_PER_UTF16: usize = 3;

/// One tag at its longest: a 128-unit key and a 256-unit value (13 §6.7), every unit three
/// bytes, written escaped.
const TAG: usize = "<Tag><Key></Key><Value></Value></Tag>".len()
    + ESCAPED * UTF8_PER_UTF16 * (tagging::MAX_KEY + tagging::MAX_VALUE);

/// CreateBucket's configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateBucketConfiguration {
    /// The location constraint, as sent.
    pub location: Option<String>,
    /// The new bucket's tags, checked, in key order.
    pub tags: Vec<Tag>,
}

/// The largest CreateBucketConfiguration body mantle reads: a location constraint and a
/// bucket's 50 tags.
pub const CREATE_BUCKET_LIMIT: usize = PROLOG
        + "<CreateBucketConfiguration></CreateBucketConfiguration>".len()
        + "<LocationConstraint></LocationConstraint>".len()
        + MAX_REGION
        // A directory bucket's zone and redundancy (13 §6.5), which are refused as not
        // implemented, and so read: a zone's name is held to a DNS label, as a region's.
        + "<Location><Name></Name><Type>AvailabilityZone</Type></Location><Bucket>\
           <DataRedundancy>SingleAvailabilityZone</DataRedundancy><Type>Directory</Type></Bucket>"
            .len()
        + MAX_REGION
        + "<Tags></Tags>".len()
        + Tagged::Bucket.limit() * TAG;

/// CreateBucket's configuration (13 §6.5): its `LocationConstraint`, as sent, and the tags
/// S3 applies to a new general purpose bucket, checked as a bucket's (13 §6.7). A
/// CreateBucket without a body has neither; this reads a body that is there.
pub fn create_bucket(body: &[u8]) -> Result<CreateBucketConfiguration, BodyError> {
    let mut reader = Reader::open(body, CREATE_BUCKET_LIMIT, "CreateBucketConfiguration")?;
    let (mut location, mut tags) = (None, None);
    while let Some(name) = reader.child()? {
        match name {
            "LocationConstraint" => once(&mut location, reader.text()?.into_owned())?,
            "Tags" => once(&mut tags, tag_list(&mut reader, Tagged::Bucket)?)?,
            "Location" | "Bucket" => return Err(BodyError::NotImplemented("directory buckets")),
            _ => return Err(schema("an element CreateBucketConfiguration does not have")),
        }
    }
    reader.finish()?;
    Ok(CreateBucketConfiguration {
        location,
        tags: tagging::check(tags.unwrap_or_default(), Tagged::Bucket)?,
    })
}

/// The root and set of a `Tagging` document.
const TAGGING: usize = PROLOG + "<Tagging><TagSet></TagSet></Tagging>".len();

/// The largest PutObjectTagging body: an object's 10 tags, each at its longest.
pub const OBJECT_TAGGING_LIMIT: usize = TAGGING + Tagged::Object.limit() * TAG;

/// The largest PutBucketTagging body: a bucket's 50 tags, each at its longest.
pub const BUCKET_TAGGING_LIMIT: usize = TAGGING + Tagged::Bucket.limit() * TAG;

/// PutObjectTagging's or PutBucketTagging's tags, checked, in key order (13 §6.7). The set
/// may be empty: "If you send this request with an empty tag set, Amazon S3 deletes the
/// existing tag set on the object."
pub fn tagging(body: &[u8], tagged: Tagged) -> Result<Vec<Tag>, BodyError> {
    let limit = match tagged {
        Tagged::Object => OBJECT_TAGGING_LIMIT,
        Tagged::Bucket => BUCKET_TAGGING_LIMIT,
    };
    let mut reader = Reader::open(body, limit, "Tagging")?;
    let mut set = None;
    while let Some(name) = reader.child()? {
        match name {
            "TagSet" => once(&mut set, tag_list(&mut reader, tagged)?)?,
            _ => return Err(schema("an element Tagging does not have")),
        }
    }
    reader.finish()?;
    let tags = set.ok_or(schema("a Tagging without a TagSet"))?;
    Ok(tagging::check(tags, tagged)?)
}

/// The tags a `TagSet`, or CreateBucket's `Tags`, holds: each is counted as it is read, so
/// the list never holds more than `tagged` may.
fn tag_list(reader: &mut Reader<'_>, tagged: Tagged) -> Result<Vec<Tag>, BodyError> {
    let mut tags = Vec::new();
    while let Some(name) = reader.child()? {
        if name != "Tag" {
            return Err(schema("an element other than Tag in a list of tags"));
        }
        if tags.len() >= tagged.limit() {
            return Err(TagError::TooMany.into());
        }
        tags.push(tag(reader)?);
    }
    Ok(tags)
}

/// A `Tag`: its `Key` and its `Value`, both required, the value possibly empty (13 §6.7).
fn tag(reader: &mut Reader<'_>) -> Result<Tag, BodyError> {
    let (mut key, mut value) = (None, None);
    while let Some(name) = reader.child()? {
        let field = match name {
            "Key" => &mut key,
            "Value" => &mut value,
            _ => return Err(schema("an element Tag does not have")),
        };
        once(field, reader.text()?.into_owned())?;
    }
    Ok(Tag {
        key: key.ok_or(schema("a tag without a key"))?,
        value: value.ok_or(schema("a tag without a value"))?,
    })
}

/// An email address at its longest, 254 octets: SMTP's 256-octet path less its angle
/// brackets (RFC 5321 §4.5.3.1.3).
const MAX_EMAIL: usize = 254;

/// A canonical user ID: 64 hexadecimal digits in every AWS sample (13 §6.8), counted as
/// arbitrary text, since an owner mantle names need not be hex.
const MAX_ID: usize = 64;

/// A `DisplayName`, which S3 ignores in a request, at the longest an email address is: the
/// display names in S3's samples are email addresses (13 §6.8).
const MAX_DISPLAY_NAME: usize = MAX_EMAIL;

/// One grant at its longest: a grantee named by email, the longest of the three ways to
/// name one, with a display name; the `xsi` declaration and type AWS's samples write; and
/// the `xmlns=""` AWS's PutBucketAcl sample puts on its leaves (13 §6.8).
const GRANT: usize = "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
    xsi:type=\"AmazonCustomerByEmail\"><EmailAddress xmlns=\"\"></EmailAddress>\
    <DisplayName xmlns=\"\"></DisplayName></Grantee>\
    <Permission xmlns=\"\">FULL_CONTROL</Permission></Grant>"
    .len()
    + ESCAPED * (MAX_EMAIL + MAX_DISPLAY_NAME);

/// The largest AccessControlPolicy body: an owner and 100 grants, each at its longest.
pub const ACL_LIMIT: usize = PROLOG
    + "<AccessControlPolicy><AccessControlList></AccessControlList>\
           <Owner><ID></ID><DisplayName></DisplayName></Owner></AccessControlPolicy>"
        .len()
    + ESCAPED * (MAX_ID + MAX_DISPLAY_NAME)
    + MAX_GRANTS * GRANT;

/// PutBucketAcl's or PutObjectAcl's `AccessControlPolicy` (13 §6.8). A grantee is read by
/// its `xsi:type` and named by the one element that type takes; a `DisplayName` is read and
/// dropped, as S3 ignores it. A document S3's schema refuses is `MalformedACLError`.
pub fn access_control_policy(body: &[u8]) -> Result<Policy, BodyError> {
    policy(body).map_err(|error| match error {
        BodyError::Xml(error @ (XmlError::NotWellFormed(_) | XmlError::Schema(_))) => {
            BodyError::MalformedAcl(error)
        }
        error => error,
    })
}

fn policy(body: &[u8]) -> Result<Policy, BodyError> {
    let mut reader = Reader::open(body, ACL_LIMIT, "AccessControlPolicy")?.admit_types("Grantee");
    let (mut owner, mut grants) = (None, None);
    while let Some(name) = reader.child()? {
        match name {
            "Owner" => once(&mut owner, acl_owner(&mut reader)?)?,
            "AccessControlList" => once(&mut grants, grant_list(&mut reader)?)?,
            _ => return Err(schema("an element AccessControlPolicy does not have")),
        }
    }
    reader.finish()?;
    Ok(Policy {
        owner,
        grants: grants.unwrap_or_default(),
    })
}

/// An `Owner`'s ID; its display name is dropped.
fn acl_owner(reader: &mut Reader<'_>) -> Result<String, BodyError> {
    let (mut id, mut display_name) = (None, None);
    while let Some(name) = reader.child()? {
        let field = match name {
            "ID" => &mut id,
            "DisplayName" => &mut display_name,
            _ => return Err(schema("an element Owner does not have")),
        };
        once(field, reader.text()?.into_owned())?;
    }
    id.ok_or(schema("an owner without an ID"))
}

/// An `AccessControlList`'s grants, counted as they are read.
fn grant_list(reader: &mut Reader<'_>) -> Result<Vec<Grant>, BodyError> {
    let mut grants = Vec::new();
    while let Some(name) = reader.child()? {
        if name != "Grant" {
            return Err(schema("an element AccessControlList does not have"));
        }
        if grants.len() >= MAX_GRANTS {
            return Err(schema("more than 100 grants"));
        }
        grants.push(grant(reader)?);
    }
    Ok(grants)
}

/// A grantee's kind, from its `xsi:type` (13 §6.8).
#[derive(Clone, Copy)]
enum Kind {
    User,
    Email,
    Group,
}

fn grant(reader: &mut Reader<'_>) -> Result<Grant, BodyError> {
    let (mut grantee, mut permission) = (None, None);
    while let Some(name) = reader.child()? {
        match name {
            "Grantee" => {
                let kind = match reader.xsi_type() {
                    Some("CanonicalUser") => Kind::User,
                    Some("AmazonCustomerByEmail") => Kind::Email,
                    Some("Group") => Kind::Group,
                    Some(_) => return Err(schema("a grantee type S3 does not define")),
                    None => return Err(schema("a grantee without an xsi:type")),
                };
                once(&mut grantee, acl_grantee(reader, kind)?)?;
            }
            "Permission" => once(&mut permission, reader.text()?.into_owned())?,
            _ => return Err(schema("an element Grant does not have")),
        }
    }
    let grantee = grantee.ok_or(schema("a grant without a grantee"))?;
    // An enumeration of `xs:string` keeps its white space (13 §5).
    let permission = permission
        .as_deref()
        .and_then(Permission::from_name)
        .ok_or(schema("a grant without a permission S3 defines"))?;
    Ok(Grant {
        grantee,
        permission,
    })
}

/// A grantee, named by the one element its kind takes; a display name is dropped.
fn acl_grantee(reader: &mut Reader<'_>, kind: Kind) -> Result<Grantee, BodyError> {
    let (mut id, mut email, mut uri, mut display_name) = (None, None, None, None);
    while let Some(name) = reader.child()? {
        let field = match name {
            "ID" => &mut id,
            "EmailAddress" => &mut email,
            "URI" => &mut uri,
            "DisplayName" => &mut display_name,
            _ => return Err(schema("an element Grantee does not have")),
        };
        once(field, reader.text()?.into_owned())?;
    }
    match (kind, id, email, uri) {
        (Kind::User, Some(id), None, None) => Ok(Grantee::User(id)),
        (Kind::Email, None, Some(email), None) => Ok(Grantee::Email(email)),
        (Kind::Group, None, None, Some(uri)) => Ok(Grantee::Group(uri)),
        _ => Err(schema(
            "a grantee not named by the one element its type takes",
        )),
    }
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
pub const VERSIONING_LIMIT: usize = PROLOG
    + "<VersioningConfiguration></VersioningConfiguration>".len()
    + "<Status>Suspended</Status><MfaDelete>Disabled</MfaDelete>".len();

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

/// The largest OwnershipControls body: its rule with the longest setting.
pub const OWNERSHIP_CONTROLS_LIMIT: usize = PROLOG
    + "<OwnershipControls></OwnershipControls><Rule></Rule>".len()
    + "<ObjectOwnership>BucketOwnerPreferred</ObjectOwnership>".len();

/// PutBucketOwnershipControls' setting (13 §6.8): the one `Rule` a bucket has, holding its
/// `ObjectOwnership`, exact, as an enumeration of `xs:string` keeps its white space (13 §5).
pub fn ownership_controls(body: &[u8]) -> Result<Ownership, BodyError> {
    let mut reader = Reader::open(body, OWNERSHIP_CONTROLS_LIMIT, "OwnershipControls")?;
    let mut rule = None;
    while let Some(name) = reader.child()? {
        if name != "Rule" {
            return Err(schema("an element OwnershipControls does not have"));
        }
        let mut setting = None;
        while let Some(name) = reader.child()? {
            if name != "ObjectOwnership" {
                return Err(schema("an element Rule does not have"));
            }
            once(&mut setting, reader.text()?.into_owned())?;
        }
        let ownership = setting
            .as_deref()
            .and_then(Ownership::from_name)
            .ok_or(schema("a rule without an ObjectOwnership S3 defines"))?;
        once(&mut rule, ownership)?;
    }
    reader.finish()?;
    rule.ok_or(schema("OwnershipControls without a rule"))
}

/// The largest ObjectLockConfiguration body: its rule with the longest mode and period.
pub const OBJECT_LOCK_LIMIT: usize = PROLOG
    + "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule>\
           <DefaultRetention><Mode>GOVERNANCE</Mode><Days></Days><Years></Years>\
           <DefaultEventHold><Days></Days><Years></Years></DefaultEventHold>\
           </DefaultRetention></Rule></ObjectLockConfiguration>"
        .len()
    + 4 * INT;

/// PutObjectLockConfiguration's configuration (18 §1.1): `ObjectLockEnabled`, which must be
/// `Enabled`, and a rule, which must hold a default retention of a mode and one period. Every
/// other shape is `MalformedXML`, as S3 answered each LocalStack sent (18 §5); a period out of
/// range is S3's `InvalidArgument`.
pub fn object_lock_configuration(body: &[u8]) -> Result<crate::lock::Configuration, BodyError> {
    use crate::lock::{Mode, PeriodFault, period};
    let mut reader = Reader::open(body, OBJECT_LOCK_LIMIT, "ObjectLockConfiguration")?;
    let (mut enabled, mut rule) = (None, None);
    while let Some(name) = reader.child()? {
        match name {
            "ObjectLockEnabled" => once(&mut enabled, reader.text()?.into_owned())?,
            "Rule" => {
                let mut retention = None;
                while let Some(name) = reader.child()? {
                    if name != "DefaultRetention" {
                        return Err(schema("an element Rule does not have"));
                    }
                    let (mut mode, mut days, mut years) = (None, None, None);
                    while let Some(name) = reader.child()? {
                        match name {
                            "Mode" => once(&mut mode, reader.text()?.into_owned())?,
                            "Days" => once(&mut days, int(&reader.text()?)?)?,
                            "Years" => once(&mut years, int(&reader.text()?)?)?,
                            "DefaultEventHold" => {
                                return Err(crate::lock::LockError::EventHold.into());
                            }
                            _ => return Err(schema("an element DefaultRetention does not have")),
                        }
                    }
                    let mode = mode
                        .as_deref()
                        .and_then(Mode::from_name)
                        .ok_or(schema("a default retention without a mode S3 defines"))?;
                    let period = match period(days, years) {
                        Ok(Some(period)) => period,
                        Ok(None) | Err(PeriodFault::Both) => {
                            return Err(schema("a default retention without one period"));
                        }
                        Err(PeriodFault::Error(error)) => return Err(error.into()),
                    };
                    once(&mut retention, (mode, period))?;
                }
                let retention = retention.ok_or(schema("a rule without a default retention"))?;
                once(&mut rule, retention)?;
            }
            _ => return Err(schema("an element ObjectLockConfiguration does not have")),
        }
    }
    reader.finish()?;
    if enabled.as_deref() != Some("Enabled") {
        return Err(schema("ObjectLockEnabled other than Enabled"));
    }
    Ok(crate::lock::Configuration { default: rule })
}

/// The largest Retention body: a mode and a date at their longest.
pub const RETENTION_LIMIT: usize = PROLOG
        + "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate></RetainUntilDate></Retention>"
            .len()
        + DATE_TIME
        // Event holds (18 §2.6), refused as not implemented, and so read: their duration of
        // days or years, as `DefaultEventHold` holds it (18 §1.1).
        + "<EventHold>false</EventHold><EventHoldDuration><Days></Days><Years></Years>\
           </EventHoldDuration>"
            .len()
        + 2 * INT;

/// PutObjectRetention's request (18 §1.2): a mode and a date together, or neither, which asks
/// to remove the retention. S3 answered a mode alone, and a mode it does not define,
/// `MalformedXML` (18 §5).
pub fn retention(body: &[u8]) -> Result<crate::lock::RetentionRequest, BodyError> {
    use crate::lock::{Mode, Retention, RetentionRequest, instant};
    let mut reader = Reader::open(body, RETENTION_LIMIT, "Retention")?;
    let (mut mode, mut until) = (None, None);
    while let Some(name) = reader.child()? {
        match name {
            "Mode" => once(&mut mode, reader.text()?.into_owned())?,
            "RetainUntilDate" => once(&mut until, reader.text()?.into_owned())?,
            "EventHold" | "EventHoldDuration" => {
                return Err(crate::lock::LockError::EventHold.into());
            }
            _ => return Err(schema("an element Retention does not have")),
        }
    }
    reader.finish()?;
    match (mode, until) {
        (None, None) => Ok(RetentionRequest::Remove),
        (Some(mode), Some(until)) => Ok(RetentionRequest::Set(Retention {
            mode: Mode::from_name(&mode).ok_or(schema("a mode S3 does not define"))?,
            until: instant(collapsed(&until)).ok_or(schema("a date that is not ISO 8601"))?,
        })),
        _ => Err(schema("a retention without both a mode and a date")),
    }
}

/// The largest LegalHold body.
pub const LEGAL_HOLD_LIMIT: usize = PROLOG + "<LegalHold><Status>OFF</Status></LegalHold>".len();

/// PutObjectLegalHold's status (18 §1.2): `ON` or `OFF`, exactly; s3-tests expects `abc`
/// `MalformedXML`.
pub fn legal_hold(body: &[u8]) -> Result<bool, BodyError> {
    let mut reader = Reader::open(body, LEGAL_HOLD_LIMIT, "LegalHold")?;
    let mut status = None;
    while let Some(name) = reader.child()? {
        if name != "Status" {
            return Err(schema("an element LegalHold does not have"));
        }
        once(&mut status, reader.text()?.into_owned())?;
    }
    reader.finish()?;
    match status.as_deref() {
        Some("ON") => Ok(true),
        Some("OFF") => Ok(false),
        _ => Err(schema("a legal hold without a status of ON or OFF")),
    }
}

/// The largest PublicAccessBlockConfiguration body: its four settings, each at its longest.
pub const PUBLIC_ACCESS_BLOCK_LIMIT: usize = PROLOG
    + "<PublicAccessBlockConfiguration></PublicAccessBlockConfiguration>".len()
    + "<BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls>\
           <BlockPublicPolicy>false</BlockPublicPolicy>\
           <RestrictPublicBuckets>false</RestrictPublicBuckets>"
        .len();

/// PutPublicAccessBlock's settings (17 §7): each an `xs:boolean`, and one the document leaves
/// out off, as the settings are replaced whole.
pub fn public_access_block(body: &[u8]) -> Result<crate::policy::PublicAccessBlock, BodyError> {
    let mut reader = Reader::open(
        body,
        PUBLIC_ACCESS_BLOCK_LIMIT,
        "PublicAccessBlockConfiguration",
    )?;
    let (mut acls, mut ignore, mut policy, mut restrict) = (None, None, None, None);
    while let Some(name) = reader.child()? {
        let field = match name {
            "BlockPublicAcls" => &mut acls,
            "IgnorePublicAcls" => &mut ignore,
            "BlockPublicPolicy" => &mut policy,
            "RestrictPublicBuckets" => &mut restrict,
            _ => {
                return Err(schema(
                    "an element PublicAccessBlockConfiguration does not have",
                ));
            }
        };
        once(field, boolean(&reader.text()?)?)?;
    }
    reader.finish()?;
    Ok(crate::policy::PublicAccessBlock {
        block_public_acls: acls.unwrap_or(false),
        ignore_public_acls: ignore.unwrap_or(false),
        block_public_policy: policy.unwrap_or(false),
        restrict_public_buckets: restrict.unwrap_or(false),
    })
}

/// The largest ServerSideEncryptionConfiguration body: one rule with its default, the longest KMS
/// key ID botocore's model allows (2048), its Bucket Key flag, and both blocked types (20 §4.1).
pub const SSE_LIMIT: usize = PROLOG
    + "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
           <SSEAlgorithm>aws:kms:dsse</SSEAlgorithm><KMSMasterKeyID></KMSMasterKeyID>\
           </ApplyServerSideEncryptionByDefault><BucketKeyEnabled>false</BucketKeyEnabled>\
           <BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType>\
           <EncryptionType>NONE</EncryptionType></BlockedEncryptionTypes></Rule>\
           </ServerSideEncryptionConfiguration>"
        .len()
    + 2048;

/// PutBucketEncryption's rule (20 §4.1): exactly one `Rule`, S3 answering none or two
/// `MalformedXML` (20 §4.3); a default of `AES256`, SSE-KMS and DSSE-KMS not being implemented,
/// and a KMS key only with those; the Bucket Key flag; and the blocked types, `SSE-C` or `NONE`.
pub fn server_side_encryption_configuration(body: &[u8]) -> Result<crate::sse::Rule, BodyError> {
    let mut reader = Reader::open(body, SSE_LIMIT, "ServerSideEncryptionConfiguration")?;
    let mut rule = None;
    while let Some(name) = reader.child()? {
        if name != "Rule" {
            return Err(schema(
                "an element ServerSideEncryptionConfiguration does not have",
            ));
        }
        let (mut default, mut bucket_key, mut blocked) = (None, None, None);
        while let Some(name) = reader.child()? {
            match name {
                "ApplyServerSideEncryptionByDefault" => {
                    let (mut algorithm, mut kms_key) = (None, None);
                    while let Some(name) = reader.child()? {
                        match name {
                            "SSEAlgorithm" => once(&mut algorithm, reader.text()?.into_owned())?,
                            "KMSMasterKeyID" => once(&mut kms_key, reader.text()?.into_owned())?,
                            _ => {
                                return Err(schema(
                                    "an element ApplyServerSideEncryptionByDefault does not have",
                                ));
                            }
                        }
                    }
                    let algorithm = algorithm.ok_or(schema("a default without an SSEAlgorithm"))?;
                    once(&mut default, (algorithm, kms_key.is_some()))?;
                }
                "BucketKeyEnabled" => once(&mut bucket_key, boolean(&reader.text()?)?)?,
                "BlockedEncryptionTypes" => {
                    let mut types = Vec::new();
                    while let Some(name) = reader.child()? {
                        if name != "EncryptionType" {
                            return Err(schema("an element BlockedEncryptionTypes does not have"));
                        }
                        types.push(reader.text()?.into_owned());
                    }
                    let customer = match types.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                        ["SSE-C"] => true,
                        ["NONE"] => false,
                        _ => return Err(schema("blocked types other than one of SSE-C or NONE")),
                    };
                    once(&mut blocked, customer)?;
                }
                _ => return Err(schema("an element Rule does not have")),
            }
        }
        if let Some((algorithm, kms_key)) = &default {
            crate::sse::default_algorithm(algorithm, *kms_key)
                .ok_or(schema("an SSEAlgorithm S3 does not define"))??;
        }
        once(
            &mut rule,
            crate::sse::Rule {
                bucket_key: bucket_key.unwrap_or(false),
                customer_blocked: blocked,
            },
        )?;
    }
    reader.finish()?;
    rule.ok_or(schema("a configuration without a rule"))
}

/// The largest CORSConfiguration body: "The document is limited to 64 KB in size"
/// (16 §1.1).
pub const CORS_LIMIT: usize = cors::LIMIT;

/// PutBucketCors' rules (16 §1.3): read against S3's schema, then checked by
/// [`cors::check`]. Rules are counted as they are read, so a document never holds more than
/// 100.
pub fn cors(body: &[u8]) -> Result<Vec<cors::Rule>, BodyError> {
    // S3's limit is on the document as sent, white space and all (16 §1.1).
    if body.len() > CORS_LIMIT {
        return Err(XmlError::TooLarge { limit: CORS_LIMIT }.into());
    }
    let mut reader = Reader::open(body, CORS_LIMIT, "CORSConfiguration")?;
    let mut rules = Vec::new();
    while let Some(name) = reader.child()? {
        if name != "CORSRule" {
            return Err(schema("an element CORSConfiguration does not have"));
        }
        if rules.len() >= cors::MAX_RULES {
            return Err(CorsError::TooManyRules.into());
        }
        rules.push(cors_rule(&mut reader)?);
    }
    reader.finish()?;
    Ok(cors::check(rules)?)
}

/// A `CORSRule`: its lists, each repeated without a wrapper, and at most one `ID` and one
/// `MaxAgeSeconds` (16 §1.3). Text is kept exactly, white space and all, as `xs:string` keeps
/// it (13 §5).
fn cors_rule(reader: &mut Reader<'_>) -> Result<cors::Given, BodyError> {
    let mut rule = cors::Given::default();
    while let Some(name) = reader.child()? {
        let list = match name {
            "AllowedHeader" => &mut rule.headers,
            "AllowedMethod" => &mut rule.methods,
            "AllowedOrigin" => &mut rule.origins,
            "ExposeHeader" => &mut rule.expose,
            "ID" => {
                once(&mut rule.id, reader.text()?.into_owned())?;
                continue;
            }
            "MaxAgeSeconds" => {
                once(&mut rule.max_age, int(&reader.text()?)?)?;
                continue;
            }
            _ => return Err(schema("an element CORSRule does not have")),
        };
        list.push(reader.text()?.into_owned());
    }
    Ok(rule)
}

/// The longest `xs:int` a serializer writes.
const INT: usize = "-2147483648".len();

/// The longest `xs:long` a serializer writes.
const LONG: usize = "-9223372036854775808".len();

/// The longest time a serializer writes: nanoseconds and a zone offset.
const DATE_TIME: usize = "2017-09-27T00:00:00.000000000+00:00".len();

/// The longest storage class a transition names.
const CLASS: usize = "INTELLIGENT_TIERING".len();

/// A lifecycle filter at its longest: an `And` of the longest prefix, both sizes and the most
/// tags an object holds, each at its longest.
const LIFECYCLE_FILTER: usize = "<Filter><And><Prefix></Prefix><ObjectSizeGreaterThan>\
    </ObjectSizeGreaterThan><ObjectSizeLessThan></ObjectSizeLessThan></And></Filter>"
    .len()
    + ESCAPED * MAX_KEY
    + 2 * LONG
    + lifecycle::MAX_FILTER_TAGS * TAG;

/// A lifecycle rule at its longest: the longest ID and filter, each action once at its
/// longest, and a transition and a noncurrent transition to each class.
const LIFECYCLE_RULE: usize = "<Rule><ID></ID><Status>Disabled</Status><Expiration><Date>\
    </Date><Days></Days><ExpiredObjectDeleteMarker>false</ExpiredObjectDeleteMarker>\
    </Expiration><NoncurrentVersionExpiration><NoncurrentDays></NoncurrentDays>\
    <NewerNoncurrentVersions></NewerNoncurrentVersions></NoncurrentVersionExpiration>\
    <AbortIncompleteMultipartUpload><DaysAfterInitiation></DaysAfterInitiation>\
    </AbortIncompleteMultipartUpload></Rule>"
    .len()
    + ESCAPED * UTF8_PER_UTF16 * lifecycle::MAX_ID
    + LIFECYCLE_FILTER
    + DATE_TIME
    + 4 * INT
    + lifecycle::CLASSES.len()
        * ("<Transition><Date></Date><Days></Days><StorageClass></StorageClass></Transition>"
            .len()
            + DATE_TIME
            + INT
            + CLASS
            + "<NoncurrentVersionTransition><NoncurrentDays></NoncurrentDays>\
               <NewerNoncurrentVersions></NewerNoncurrentVersions><StorageClass></StorageClass>\
               </NoncurrentVersionTransition>"
                .len()
            + 2 * INT
            + CLASS);

/// The largest LifecycleConfiguration body: 1,000 rules, each at its longest.
pub const LIFECYCLE_LIMIT: usize = PROLOG
    + "<LifecycleConfiguration></LifecycleConfiguration>".len()
    + lifecycle::MAX_RULES * LIFECYCLE_RULE;

/// PutBucketLifecycleConfiguration's rules (13 §6.9): read against S3's schema, then checked
/// by [`lifecycle::check`]. The deprecated PutBucketLifecycle sends the same request with each
/// rule's own `Prefix` in place of a `Filter`, and is read by the same schema. Rules are
/// counted as they are read, so a document never holds more than 1,000.
pub fn lifecycle(body: &[u8]) -> Result<Vec<Rule>, BodyError> {
    let mut reader = Reader::open(body, LIFECYCLE_LIMIT, "LifecycleConfiguration")?;
    let mut rules = Vec::new();
    while let Some(name) = reader.child()? {
        if name != "Rule" {
            return Err(schema("an element LifecycleConfiguration does not have"));
        }
        if rules.len() >= lifecycle::MAX_RULES {
            return Err(LifecycleError::TooManyRules.into());
        }
        rules.push(lifecycle_rule(&mut reader)?);
    }
    reader.finish()?;
    Ok(lifecycle::check(rules)?)
}

/// A `Rule`. Its `Status` is exact, as an enumeration of `xs:string` keeps its white space
/// (13 §5), and required.
fn lifecycle_rule(reader: &mut Reader<'_>) -> Result<Given, BodyError> {
    let mut rule = Given::default();
    let mut status = None;
    while let Some(name) = reader.child()? {
        match name {
            "ID" => once(&mut rule.id, reader.text()?.into_owned())?,
            "Prefix" => once(&mut rule.prefix, reader.text()?.into_owned())?,
            "Filter" => once(&mut rule.filter, lifecycle_filter(reader)?)?,
            "Status" => once(&mut status, reader.text()?.into_owned())?,
            "Expiration" => once(&mut rule.expiration, lifecycle_expiration(reader)?)?,
            "NoncurrentVersionExpiration" => {
                let mut given = GivenNoncurrent::default();
                while let Some(name) = reader.child()? {
                    match name {
                        "NoncurrentDays" => once(&mut given.days, int(&reader.text()?)?)?,
                        "NewerNoncurrentVersions" => {
                            once(&mut given.newer, int(&reader.text()?)?)?;
                        }
                        _ => {
                            return Err(schema(
                                "an element NoncurrentVersionExpiration does not have",
                            ));
                        }
                    }
                }
                once(&mut rule.noncurrent, given)?;
            }
            "AbortIncompleteMultipartUpload" => {
                let mut days = None;
                while let Some(name) = reader.child()? {
                    if name != "DaysAfterInitiation" {
                        return Err(schema(
                            "an element AbortIncompleteMultipartUpload does not have",
                        ));
                    }
                    once(&mut days, int(&reader.text()?)?)?;
                }
                once(&mut rule.abort, days)?;
            }
            "Transition" => rule.transitions.push(transition(reader, false)?),
            "NoncurrentVersionTransition" => {
                rule.noncurrent_transitions.push(transition(reader, true)?);
            }
            _ => return Err(schema("an element Rule does not have")),
        }
    }
    rule.enabled = match status.as_deref() {
        Some("Enabled") => true,
        Some("Disabled") => false,
        Some(_) => return Err(schema("a rule status other than Enabled or Disabled")),
        None => return Err(schema("a rule without a Status")),
    };
    Ok(rule)
}

/// A `Filter`: empty, which applies to every object, or exactly one predicate, as its type
/// says (13 §6.9).
fn lifecycle_filter(reader: &mut Reader<'_>) -> Result<Filter, BodyError> {
    let mut filter = None;
    while let Some(name) = reader.child()? {
        let predicate = match name {
            "Prefix" => Filter::Prefix(reader.text()?.into_owned()),
            "Tag" => Filter::Tag(filter_tag(reader)?),
            "ObjectSizeGreaterThan" => Filter::Larger(larger(&reader.text()?)?),
            "ObjectSizeLessThan" => Filter::Smaller(smaller(&reader.text()?)?),
            "And" => Filter::And(and(reader)?),
            _ => return Err(schema("an element Filter does not have")),
        };
        if filter.replace(predicate).is_some() {
            return Err(schema("a filter holding two predicates outside And"));
        }
    }
    Ok(filter.unwrap_or(Filter::All))
}

/// An `And`: "two or more predicates" (13 §6.9).
fn and(reader: &mut Reader<'_>) -> Result<And, BodyError> {
    let mut and = And::default();
    while let Some(name) = reader.child()? {
        match name {
            "Prefix" => once(&mut and.prefix, reader.text()?.into_owned())?,
            "Tag" => and.tags.push(filter_tag(reader)?),
            "ObjectSizeGreaterThan" => once(&mut and.larger, larger(&reader.text()?)?)?,
            "ObjectSizeLessThan" => once(&mut and.smaller, smaller(&reader.text()?)?)?,
            _ => return Err(schema("an element And does not have")),
        }
    }
    let predicates = and
        .tags
        .len()
        .saturating_add(usize::from(and.prefix.is_some()))
        .saturating_add(usize::from(and.larger.is_some()))
        .saturating_add(usize::from(and.smaller.is_some()));
    if predicates < 2 {
        return Err(schema("an And holding fewer than two predicates"));
    }
    Ok(and)
}

/// A filter's `Tag`. Its `Value` may be left out, and is then empty: "If you specify only a
/// `<Key>` element and no `<Value>` element, the rule will apply only to objects that match the
/// tag key and that do not have a value specified" (13 §6.9).
fn filter_tag(reader: &mut Reader<'_>) -> Result<Tag, BodyError> {
    let (mut key, mut value) = (None, None);
    while let Some(name) = reader.child()? {
        let field = match name {
            "Key" => &mut key,
            "Value" => &mut value,
            _ => return Err(schema("an element Tag does not have")),
        };
        once(field, reader.text()?.into_owned())?;
    }
    Ok(Tag {
        key: key.ok_or(schema("a tag without a key"))?,
        value: value.unwrap_or_default(),
    })
}

/// An `Expiration`: each of its elements, read as its type; which of them may be given
/// together is [`lifecycle::check`]'s.
fn lifecycle_expiration(reader: &mut Reader<'_>) -> Result<GivenExpiration, BodyError> {
    let mut given = GivenExpiration::default();
    while let Some(name) = reader.child()? {
        match name {
            "Date" => once(&mut given.date, date(&reader.text()?)?)?,
            "Days" => once(&mut given.days, int(&reader.text()?)?)?,
            "ExpiredObjectDeleteMarker" => {
                once(&mut given.marker, boolean(&reader.text()?)?)?;
            }
            _ => return Err(schema("an element Expiration does not have")),
        }
    }
    Ok(given)
}

/// A `Transition`, or with `noncurrent` a `NoncurrentVersionTransition`.
fn transition(reader: &mut Reader<'_>, noncurrent: bool) -> Result<GivenTransition, BodyError> {
    let mut given = GivenTransition::default();
    while let Some(name) = reader.child()? {
        match (name, noncurrent) {
            ("Date", false) => once(&mut given.date, date(&reader.text()?)?)?,
            ("Days", false) | ("NoncurrentDays", true) => {
                once(&mut given.days, int(&reader.text()?)?)?;
            }
            ("NewerNoncurrentVersions", true) => once(&mut given.newer, int(&reader.text()?)?)?,
            ("StorageClass", _) => once(&mut given.class, reader.text()?.into_owned())?,
            _ => return Err(schema("an element a transition does not have")),
        }
    }
    Ok(given)
}

/// A `Date`: an `xs:dateTime` with its zone, collapsed as the type's white space facet says
/// (XML Schema Part 2 §3.2.7).
fn date(text: &str) -> Result<(i64, u32), BodyError> {
    crate::time::parse_iso8601(collapsed(text)).ok_or(schema("a Date that is not an ISO 8601 time"))
}

/// An object size in a filter: an `xs:long`, in bytes, from `least` to
/// [`lifecycle::MAX_FILTER_SIZE`], or else the fault `outside` (13 §6.9).
fn size(text: &str, least: u64, outside: LifecycleError) -> Result<u64, BodyError> {
    let text = collapsed(text);
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(schema("not an integer"));
    }
    let size: i64 = text
        .parse()
        .map_err(|_| schema("an integer out of range"))?;
    u64::try_from(size)
        .ok()
        .filter(|size| (least..=lifecycle::MAX_FILTER_SIZE).contains(size))
        .ok_or(BodyError::Lifecycle(outside))
}

/// `ObjectSizeGreaterThan`, which may be 0.
fn larger(text: &str) -> Result<u64, BodyError> {
    size(text, 0, LifecycleError::LargerRange)
}

/// `ObjectSizeLessThan`, which is at least 1.
fn smaller(text: &str) -> Result<u64, BodyError> {
    size(text, 1, LifecycleError::SmallerRange)
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
            body.len() <= COMPLETE_LIMIT,
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
            body.len() <= DELETE_LIMIT,
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
        assert_eq!(
            create_bucket(body).map(|c| c.location),
            Ok(Some("EU".into()))
        );
        assert_eq!(
            create_bucket(b"<CreateBucketConfiguration/>"),
            Ok(CreateBucketConfiguration::default())
        );
        assert_eq!(
            create_bucket(b"<CreateBucketConfiguration><Location/></CreateBucketConfiguration>")
                .map_err(|e| e.code().0),
            Err("NotImplemented")
        );
    }

    /// A new general purpose bucket's tags are read and checked as a bucket's (13 §6.5,
    /// §6.7).
    #[test]
    fn create_bucket_reads_tags() {
        let body = |tags: &str| {
            format!(
                "<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint>\
                 <Tags>{tags}</Tags></CreateBucketConfiguration>"
            )
        };
        let tag = |k: &str, v: &str| format!("<Tag><Key>{k}</Key><Value>{v}</Value></Tag>");
        assert_eq!(
            create_bucket(
                body(&(tag("User", "jsmith") + &tag("Project", "Project One"))).as_bytes()
            ),
            Ok(CreateBucketConfiguration {
                location: Some("eu-west-1".into()),
                tags: vec![
                    Tag {
                        key: "Project".into(),
                        value: "Project One".into()
                    },
                    Tag {
                        key: "User".into(),
                        value: "jsmith".into()
                    },
                ],
            })
        );
        let code = |tags: String| create_bucket(body(&tags).as_bytes()).map_err(|e| e.code().0);
        let many = |n: usize| (0..n).map(|i| tag(&i.to_string(), "")).collect::<String>();
        assert!(code(many(50)).is_ok());
        assert_eq!(code(many(51)), Err("BadRequest"));
        assert_eq!(code(tag("aws:createdBy", "x")), Err("InvalidTag"));
        assert_eq!(code(tag("a", "1") + &tag("a", "2")), Err("InvalidTag"));
    }

    /// AWS's PutObjectTagging and PutBucketTagging samples, whose roots carry no namespace
    /// (13 §6.7).
    #[test]
    fn tagging_reads_aws_samples() {
        let object = b"<Tagging>
   <TagSet>
      <Tag>
         <Key>tag1</Key>
         <Value>val1</Value>
      </Tag>
      <Tag>
         <Key>tag2</Key>
         <Value>val2</Value>
      </Tag>
   </TagSet>
</Tagging>
         ";
        let tag = |key: &str, value: &str| Tag {
            key: key.into(),
            value: value.into(),
        };
        assert_eq!(
            tagging(object, Tagged::Object),
            Ok(vec![tag("tag1", "val1"), tag("tag2", "val2")])
        );
        let bucket = b"<Tagging>
  <TagSet>
    <Tag>
      <Key>User</Key>
      <Value>jsmith</Value>
    </Tag>
    <Tag>
      <Key>Project</Key>
      <Value>Project One</Value>
    </Tag>
  </TagSet>
</Tagging>";
        assert_eq!(
            tagging(bucket, Tagged::Bucket),
            Ok(vec![tag("Project", "Project One"), tag("User", "jsmith")])
        );
        // An empty set removes an object's tags (13 §6.7).
        assert_eq!(
            tagging(b"<Tagging><TagSet/></Tagging>", Tagged::Object),
            Ok(Vec::new())
        );
    }

    #[test]
    fn tagging_refusals_are_s3s() {
        let code = |set: &str, tagged| {
            tagging(format!("<Tagging>{set}</Tagging>").as_bytes(), tagged).map_err(|e| e.code().0)
        };
        let tags = |n: usize| {
            let tags: String = (0..n)
                .map(|i| format!("<Tag><Key>{i}</Key><Value>{i}</Value></Tag>"))
                .collect();
            format!("<TagSet>{tags}</TagSet>")
        };
        assert!(code(&tags(10), Tagged::Object).is_ok());
        // What S3 answered PutObjectTagging with more than 10 tags (13 §6.7).
        assert_eq!(code(&tags(11), Tagged::Object), Err("BadRequest"));
        assert!(code(&tags(50), Tagged::Bucket).is_ok());
        assert_eq!(code(&tags(51), Tagged::Bucket), Err("BadRequest"));
        for (set, expected) in [
            ("", "MalformedXML"),
            ("<TagSet/><TagSet/>", "MalformedXML"),
            ("<TagSet><Tag><Key>k</Key></Tag></TagSet>", "MalformedXML"),
            (
                "<TagSet><Tag><Value>v</Value></Tag></TagSet>",
                "MalformedXML",
            ),
            (
                "<TagSet><Tag><Key>k</Key><Key>j</Key><Value/></Tag></TagSet>",
                "MalformedXML",
            ),
            (
                "<TagSet><Tag><Key>k</Key><Value/><Other/></Tag></TagSet>",
                "MalformedXML",
            ),
            (
                "<TagSet><Tag><Key></Key><Value/></Tag></TagSet>",
                "InvalidTag",
            ),
            (
                "<TagSet><Tag><Key>a,b</Key><Value/></Tag></TagSet>",
                "InvalidTag",
            ),
            (
                "<TagSet><Tag><Key>k</Key><Value>&lt;</Value></Tag></TagSet>",
                "InvalidTag",
            ),
            (
                "<TagSet><Tag><Key>k</Key><Value/></Tag><Tag><Key>k</Key><Value/></Tag></TagSet>",
                "InvalidTag",
            ),
        ] {
            assert_eq!(code(set, Tagged::Object), Err(expected), "{set}");
        }
    }

    /// The limits admit the longest bodies S3's limits allow: every tag with a key and value
    /// of characters that each take three bytes and are written as character references.
    #[test]
    fn tagging_limits_admit_the_longest_bodies() {
        for (tagged, limit) in [
            (Tagged::Object, OBJECT_TAGGING_LIMIT),
            (Tagged::Bucket, BUCKET_TAGGING_LIMIT),
        ] {
            let tags: String = (0..tagged.limit())
                .map(|i| {
                    // U+4E00 and on: letters of three bytes, one UTF-16 unit each.
                    let key: String = (0..tagging::MAX_KEY)
                        .map(|j| format!("&#{};", 0x4E00 + i * tagging::MAX_KEY + j))
                        .collect();
                    let value = "&#x9FA5;".repeat(tagging::MAX_VALUE);
                    format!("<Tag><Key>{key}</Key><Value>{value}</Value></Tag>")
                })
                .collect();
            let body = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Tagging xmlns=\"{NAMESPACE}\">\
                 <TagSet>{tags}</TagSet></Tagging>"
            );
            assert!(body.len() <= limit, "{} > {limit}", body.len());
            assert_eq!(
                tagging(body.as_bytes(), tagged).unwrap().len(),
                tagged.limit()
            );
        }
        let longest_create = format!(
            "<CreateBucketConfiguration><LocationConstraint>{}</LocationConstraint><Tags>{}</Tags>\
             </CreateBucketConfiguration>",
            "a".repeat(MAX_REGION),
            (0..50)
                .map(|i| format!(
                    "<Tag><Key>{i:0>128}</Key><Value>{}</Value></Tag>",
                    "&#x9FA5;".repeat(tagging::MAX_VALUE)
                ))
                .collect::<String>()
        );
        assert!(longest_create.len() <= CREATE_BUCKET_LIMIT);
        assert_eq!(
            create_bucket(longest_create.as_bytes()).unwrap().tags.len(),
            50
        );
    }

    const OWNER: &str = "852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID";

    /// AWS's PutBucketAcl sample: a namespaced root whose leaves undeclare the default
    /// namespace, and every kind of grantee (13 §6.8).
    #[test]
    fn access_control_policy_reads_aws_samples() {
        let bucket = br#"<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>
    <DisplayName>OwnerDisplayName</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>
        <DisplayName>OwnerDisplayName</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI xmlns="">http://acs.amazonaws.com/groups/global/AllUsers</URI>
      </Grantee>
      <Permission xmlns="">READ</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI xmlns="">http://acs.amazonaws.com/groups/s3/LogDelivery</URI>
      </Grantee>
      <Permission xmlns="">WRITE</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail">
        <EmailAddress xmlns="">xyz@amazon.com</EmailAddress>
      </Grantee>
      <Permission xmlns="">WRITE_ACP</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID xmlns="">f30716ab7115dcb44a5ef76e9d74b8e20567f63TestAccountCanonicalUserID</ID>
      </Grantee>
      <Permission xmlns="">READ_ACP</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
         "#;
        let grant = |grantee, permission| Grant {
            grantee,
            permission,
        };
        assert_eq!(
            access_control_policy(bucket),
            Ok(Policy {
                owner: Some(OWNER.into()),
                grants: vec![
                    grant(Grantee::User(OWNER.into()), Permission::FullControl),
                    grant(
                        Grantee::Group("http://acs.amazonaws.com/groups/global/AllUsers".into()),
                        Permission::Read
                    ),
                    grant(
                        Grantee::Group("http://acs.amazonaws.com/groups/s3/LogDelivery".into()),
                        Permission::Write
                    ),
                    grant(
                        Grantee::Email("xyz@amazon.com".into()),
                        Permission::WriteAcp
                    ),
                    grant(
                        Grantee::User(
                            "f30716ab7115dcb44a5ef76e9d74b8e20567f63TestAccountCanonicalUserID"
                                .into()
                        ),
                        Permission::ReadAcp
                    ),
                ],
            })
        );
        // PutObjectAcl's sample, whose root carries no namespace.
        let object = br#"<AccessControlPolicy>
  <Owner>
    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
    <DisplayName>mtd@amazon.com</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
        <DisplayName>mtd@amazon.com</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>"#;
        let read = access_control_policy(object).unwrap();
        assert_eq!(read.grants.len(), 1);
        assert_eq!(
            read.owner.as_deref(),
            Some("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
        );
        // Neither grants nor owner: an empty list.
        assert_eq!(
            access_control_policy(b"<AccessControlPolicy/>"),
            Ok(Policy::default())
        );
    }

    #[test]
    fn access_control_policy_refusals_are_malformed_acls() {
        let xsi = "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
        let code = |list: &str| {
            access_control_policy(
                format!(
                    "<AccessControlPolicy><AccessControlList>{list}</AccessControlList>\
                     </AccessControlPolicy>"
                )
                .as_bytes(),
            )
            .map_err(|e| e.code().0)
        };
        let user = |attributes: &str, inner: &str, permission: &str| {
            format!(
                "<Grant><Grantee {attributes}>{inner}</Grantee>\
                 <Permission>{permission}</Permission></Grant>"
            )
        };
        let typed = |kind: &str| format!("{xsi} xsi:type=\"{kind}\"");
        assert!(code(&user(&typed("CanonicalUser"), "<ID>a</ID>", "READ")).is_ok());
        for refused in [
            // No type, an unknown one, and GetObjectAcl's sample's `Type` element.
            user(xsi, "<ID>a</ID>", "READ"),
            user(&typed("Canonical User"), "<ID>a</ID>", "READ"),
            user(&typed("Owner"), "<ID>a</ID>", "READ"),
            user(xsi, "<ID>a</ID><Type>CanonicalUser</Type>", "READ"),
            // The element the type does not take, none, or two.
            user(&typed("CanonicalUser"), "<URI>u</URI>", "READ"),
            user(&typed("Group"), "<ID>a</ID>", "READ"),
            user(&typed("CanonicalUser"), "", "READ"),
            user(
                &typed("CanonicalUser"),
                "<ID>a</ID><EmailAddress>e</EmailAddress>",
                "READ",
            ),
            // A permission S3 does not define, or written otherwise.
            user(&typed("CanonicalUser"), "<ID>a</ID>", "read"),
            user(&typed("CanonicalUser"), "<ID>a</ID>", " READ"),
            user(&typed("CanonicalUser"), "<ID>a</ID>", "FULL-CONTROL"),
            // A grant without a grantee or permission, or a type on another element.
            "<Grant><Permission>READ</Permission></Grant>".into(),
            format!(
                "<Grant><Grantee {}><ID>a</ID></Grantee></Grant>",
                typed("CanonicalUser")
            ),
            format!(
                "<Grant {}><Grantee {}><ID>a</ID></Grantee><Permission>READ</Permission></Grant>",
                typed("CanonicalUser"),
                typed("CanonicalUser")
            ),
            "<Other/>".into(),
        ] {
            assert_eq!(code(&refused), Err("MalformedACLError"), "{refused}");
        }
        let grants = |n: usize| user(&typed("CanonicalUser"), "<ID>a</ID>", "READ").repeat(n);
        assert_eq!(
            code(&grants(MAX_GRANTS)).map(|p| p.grants.len()),
            Ok(MAX_GRANTS)
        );
        assert_eq!(code(&grants(MAX_GRANTS + 1)), Err("MalformedACLError"));
        assert_eq!(
            access_control_policy(b"<AccessControlPolicy><Owner/></AccessControlPolicy>")
                .map_err(|e| e.code().0),
            Err("MalformedACLError")
        );
        assert_eq!(
            access_control_policy(b"not xml").map_err(|e| e.code().0),
            Err("MalformedACLError")
        );
        assert_eq!(
            access_control_policy(&vec![b' '; ACL_LIMIT + 1]).map_err(|e| e.code().0),
            Err("MaxMessageLengthExceeded")
        );
    }

    /// PutBucketOwnershipControls' sample (13 §6.8).
    #[test]
    fn ownership_controls_reads_the_one_rule() {
        let sample = br#"<?xml version="1.0" encoding="UTF-8"?>
          <OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Rule>
              <ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>
            </Rule>
          </OwnershipControls>"#;
        assert_eq!(
            ownership_controls(sample),
            Ok(Ownership::BucketOwnerEnforced)
        );
        let rule =
            |setting: &str| format!("<Rule><ObjectOwnership>{setting}</ObjectOwnership></Rule>");
        let code = |rules: String| {
            ownership_controls(format!("<OwnershipControls>{rules}</OwnershipControls>").as_bytes())
                .map_err(|e| e.code().0)
        };
        assert_eq!(
            code(rule("ObjectWriter")).map(Ownership::name),
            Ok("ObjectWriter")
        );
        for refused in [
            String::new(),
            rule("BucketOwnerEnforced").repeat(2),
            rule(" BucketOwnerEnforced"),
            rule("bucketownerenforced"),
            "<Rule/>".into(),
            "<Rule><ObjectOwnership>ObjectWriter</ObjectOwnership><Other/></Rule>".into(),
        ] {
            assert_eq!(code(refused.clone()), Err("MalformedXML"), "{refused}");
        }
        let longest = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><OwnershipControls xmlns=\"{NAMESPACE}\">{}\
             </OwnershipControls>",
            rule("BucketOwnerPreferred")
        );
        assert!(longest.len() <= OWNERSHIP_CONTROLS_LIMIT);
        assert!(ownership_controls(longest.as_bytes()).is_ok());
    }

    /// The limit admits 100 grants each naming an email address of 254 octets, every one
    /// written as `&apos;`, with display names as long.
    #[test]
    fn acl_limit_admits_the_longest_body() {
        let long = "&apos;".repeat(MAX_EMAIL);
        let grant = format!(
            "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
             xsi:type=\"AmazonCustomerByEmail\"><EmailAddress xmlns=\"\">{long}</EmailAddress>\
             <DisplayName xmlns=\"\">{long}</DisplayName></Grantee>\
             <Permission xmlns=\"\">FULL_CONTROL</Permission></Grant>"
        );
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><AccessControlPolicy xmlns=\"{NAMESPACE}\">\
             <AccessControlList>{}</AccessControlList><Owner><ID>{}</ID>\
             <DisplayName>{long}</DisplayName></Owner></AccessControlPolicy>",
            grant.repeat(MAX_GRANTS),
            "&quot;".repeat(MAX_ID)
        );
        assert!(body.len() <= ACL_LIMIT, "{} > {ACL_LIMIT}", body.len());
        let read = access_control_policy(body.as_bytes()).unwrap();
        assert_eq!(read.grants.len(), MAX_GRANTS);
        assert_eq!(
            read.grants[0].grantee,
            Grantee::Email("'".repeat(MAX_EMAIL))
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
    /// A LifecycleConfiguration as botocore writes one: S3's namespace, no declaration, and
    /// each rule's elements in the order the caller gave them (13 §6.9).
    fn configuration(rules: &str) -> String {
        format!("<LifecycleConfiguration xmlns=\"{NAMESPACE}\">{rules}</LifecycleConfiguration>")
    }

    fn lifecycle_code(rules: &str) -> Result<usize, &'static str> {
        lifecycle(configuration(rules).as_bytes())
            .map(|rules| rules.len())
            .map_err(|e| e.code().0)
    }

    /// The configurations s3-tests sets and expects S3 to accept, as botocore sends them
    /// (13 §6.9).
    #[test]
    fn lifecycle_reads_what_s3_tests_sets() {
        let rules = lifecycle(
            configuration(
                "<Rule><ID>rule1</ID><Expiration><Days>1</Days></Expiration>\
                 <Prefix>test1/</Prefix><Status>Enabled</Status></Rule>\
                 <Rule><ID>rule2</ID><Expiration><Days>2</Days></Expiration>\
                 <Prefix>test2/</Prefix><Status>Disabled</Status></Rule>",
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(
            rules[0],
            Rule {
                id: "rule1".into(),
                scope: lifecycle::Scope::Prefix("test1/".into()),
                enabled: true,
                expiration: Some(lifecycle::Expiration::Days(1)),
                noncurrent: None,
                abort: None,
            }
        );
        assert!(!rules[1].enabled);
        let accepted = [
            // test_lifecycle_set_date: '2017-09-27' as botocore writes it.
            "<Rule><ID>rule1</ID><Expiration><Date>2017-09-27T00:00:00Z</Date></Expiration>\
             <Prefix>test1/</Prefix><Status>Enabled</Status></Rule>",
            // test_lifecycle_set_noncurrent.
            "<Rule><ID>rule1</ID><NoncurrentVersionExpiration><NoncurrentDays>2\
             </NoncurrentDays></NoncurrentVersionExpiration><Prefix>past/</Prefix>\
             <Status>Enabled</Status></Rule>",
            // test_lifecycle_set_deletemarker.
            "<Rule><ID>rule1</ID><Expiration><ExpiredObjectDeleteMarker>true\
             </ExpiredObjectDeleteMarker></Expiration><Prefix>test1/</Prefix>\
             <Status>Enabled</Status></Rule>",
            // test_lifecycle_set_filter and test_lifecycle_set_empty_filter.
            "<Rule><ID>rule1</ID><Expiration><ExpiredObjectDeleteMarker>true\
             </ExpiredObjectDeleteMarker></Expiration><Filter><Prefix>foo</Prefix></Filter>\
             <Status>Enabled</Status></Rule>",
            "<Rule><ID>rule1</ID><Expiration><ExpiredObjectDeleteMarker>true\
             </ExpiredObjectDeleteMarker></Expiration><Filter /><Status>Enabled</Status></Rule>",
            // test_lifecycle_set_multipart.
            "<Rule><ID>rule1</ID><Prefix>test1/</Prefix><Status>Enabled</Status>\
             <AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation>\
             </AbortIncompleteMultipartUpload></Rule>",
            // test_delete_marker_expiration: an empty rule-level prefix.
            "<Rule><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>\
             </Expiration><ID>dm-1-days</ID><Prefix></Prefix><Status>Enabled</Status></Rule>",
        ];
        for rules in accepted {
            assert_eq!(lifecycle_code(rules), Ok(1), "{rules}");
        }
        // test_lifecycle_get_no_id: every rule comes back with an ID.
        let unnamed = lifecycle(
            configuration(
                "<Rule><Expiration><Days>31</Days></Expiration><Prefix>test1/</Prefix>\
                 <Status>Enabled</Status></Rule>",
            )
            .as_bytes(),
        )
        .unwrap();
        assert!(!unnamed[0].id.is_empty());
    }

    /// What s3-tests expects S3 to refuse, and the codes it asserts (13 §6.9).
    #[test]
    fn lifecycle_refusals_are_s3_tests() {
        let rule = |id: &str, expiration: &str, status: &str| {
            format!(
                "<Rule><ID>{id}</ID><Expiration>{expiration}</Expiration>\
                 <Prefix>test1/</Prefix><Status>{status}</Status></Rule>"
            )
        };
        let days1 = "<Days>1</Days>";
        for status in ["enabled", "disabled", "invalid", " Enabled"] {
            assert_eq!(
                lifecycle_code(&rule("r", days1, status)),
                Err("MalformedXML")
            );
        }
        assert_eq!(
            lifecycle_code(&rule(&"a".repeat(256), days1, "Enabled")),
            Err("InvalidArgument")
        );
        let same = rule("rule1", days1, "Enabled") + &rule("rule1", "<Days>2</Days>", "Enabled");
        assert_eq!(lifecycle_code(&same), Err("InvalidArgument"));
        assert_eq!(
            lifecycle_code(&rule("r", "<Days>0</Days>", "Enabled")),
            Err("InvalidArgument")
        );
        // test_lifecycle_set_invalid_date: '20200101' as botocore writes it, not a midnight.
        let invalid_date = rule("r", "<Date>1970-08-22T19:08:21Z</Date>", "Enabled");
        assert_eq!(
            lifecycle(configuration(&invalid_date).as_bytes())
                .unwrap_err()
                .code()
                .1,
            400
        );
        // test_lifecycle_transition_set_invalid_date: a transition dated at no midnight is a
        // 400, although transitions are otherwise not implemented.
        let transition = "<Rule><ID>rule1</ID><Expiration><Date>2023-09-27T00:00:00Z</Date>\
            </Expiration><Transition><Date>1970-08-23T00:55:27Z</Date>\
            <StorageClass>GLACIER</StorageClass></Transition><Prefix>test1/</Prefix>\
            <Status>Enabled</Status></Rule>";
        assert_eq!(
            lifecycle(configuration(transition).as_bytes())
                .unwrap_err()
                .code()
                .1,
            400
        );
        assert_eq!(lifecycle_code(""), Err("InvalidRequest"));
        assert_eq!(lifecycle(b"").map_err(|e| e.code().0), Err("MalformedXML"));
    }

    /// AWS's examples, which transition objects, are refused as not implemented once read;
    /// its expiring rule alone is read. The examples that write the root
    /// `<LifeCycleConfiguration>` are not the document botocore sends (13 §6.9).
    #[test]
    fn lifecycle_reads_aws_samples() {
        let example_1 = b"<LifecycleConfiguration>
              <Rule>
                <ID>id1</ID>
                <Filter>
                   <Prefix>documents/</Prefix>
                </Filter>
                <Status>Enabled</Status>
                <Transition>
                  <Days>30</Days>
                  <StorageClass>GLACIER</StorageClass>
                </Transition>
              </Rule>
              <Rule>
                <ID>id2</ID>
                <Filter>
                   <Prefix>logs/</Prefix>
                </Filter>
                <Status>Enabled</Status>
                <Expiration>
                  <Days>365</Days>
                </Expiration>
              </Rule>
            </LifecycleConfiguration>";
        assert_eq!(
            lifecycle(example_1).map_err(|e| e.code()),
            Err(("NotImplemented", 501))
        );
        let get_sample = br#"<?xml version="1.0" encoding="UTF-8"?>
            <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Rule>
                  <ID>Archive and then delete rule</ID>
                  <Prefix>projectdocs/</Prefix>
                  <Status>Enabled</Status>
                  <Transition>
                     <Days>30</Days>
                     <StorageClass>STANDARD_IA</StorageClass>
                  </Transition>
                  <Transition>
                     <Days>365</Days>
                     <StorageClass>GLACIER</StorageClass>
                  </Transition>
                  <Expiration>
                     <Days>3650</Days>
                  </Expiration>
               </Rule>
            </LifecycleConfiguration>"#;
        assert_eq!(
            lifecycle(get_sample).map_err(|e| e.code().0),
            Err("NotImplemented")
        );
        let expiring = b"<LifecycleConfiguration>
              <Rule>
                <ID>id2</ID>
                <Filter>
                   <Prefix>logs/</Prefix>
                </Filter>
                <Status>Enabled</Status>
                <Expiration>
                  <Days>365</Days>
                </Expiration>
              </Rule>
            </LifecycleConfiguration>";
        let rules = lifecycle(expiring).unwrap();
        assert_eq!(
            rules[0].scope,
            lifecycle::Scope::Filter(Filter::Prefix("logs/".into()))
        );
        assert_eq!(rules[0].expiration, Some(lifecycle::Expiration::Days(365)));
        let example_3 = b"<LifeCycleConfiguration><Rule><ID>DeleteAfterBecomingNonCurrent</ID>\
            <Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status>\
            <NoncurrentVersionExpiration><NoncurrentDays>100</NoncurrentDays>\
            </NoncurrentVersionExpiration></Rule></LifeCycleConfiguration>";
        assert_eq!(
            lifecycle(example_3).map_err(|e| e.code().0),
            Err("MalformedXML")
        );
    }

    /// A filter holds one predicate, or an `And` of two or more; a tag's value may be left
    /// out (13 §6.9).
    #[test]
    fn lifecycle_filters_follow_their_schema() {
        let filtered = |filter: &str| {
            format!(
                "<Rule><ID>r</ID><Filter>{filter}</Filter><Status>Enabled</Status>\
                 <Expiration><Days>1</Days></Expiration></Rule>"
            )
        };
        let read = |filter: &str| {
            lifecycle(configuration(&filtered(filter)).as_bytes())
                .map(|mut rules| rules.remove(0).scope)
                .map_err(|e| e.code().0)
        };
        let tag = |key: &str, value: &str| Tag {
            key: key.into(),
            value: value.into(),
        };
        assert_eq!(
            read(
                "<And><Prefix>docs/</Prefix><Tag><Key>a</Key><Value>1</Value></Tag>\
                  <Tag><Key>b</Key></Tag><ObjectSizeGreaterThan> 500 </ObjectSizeGreaterThan>\
                  <ObjectSizeLessThan>64000</ObjectSizeLessThan></And>"
            ),
            Ok(lifecycle::Scope::Filter(Filter::And(And {
                prefix: Some("docs/".into()),
                tags: vec![tag("a", "1"), tag("b", "")],
                larger: Some(500),
                smaller: Some(64000),
            })))
        );
        assert_eq!(
            read("<Tag><Key>k</Key><Value>v</Value></Tag>"),
            Ok(lifecycle::Scope::Filter(Filter::Tag(tag("k", "v"))))
        );
        // S3's range for a size: 0 or 1 up to 1000 × 2^40 bytes (13 §6.9).
        assert_eq!(
            read("<ObjectSizeGreaterThan>0</ObjectSizeGreaterThan>"),
            Ok(lifecycle::Scope::Filter(Filter::Larger(0)))
        );
        assert_eq!(
            read("<ObjectSizeLessThan>1099511627776000</ObjectSizeLessThan>"),
            Ok(lifecycle::Scope::Filter(Filter::Smaller(
                lifecycle::MAX_FILTER_SIZE
            )))
        );
        for outside in [
            "<ObjectSizeLessThan>0</ObjectSizeLessThan>",
            "<ObjectSizeLessThan>1099511627776001</ObjectSizeLessThan>",
            "<ObjectSizeGreaterThan>-1</ObjectSizeGreaterThan>",
            "<And><Prefix/><ObjectSizeLessThan>-5</ObjectSizeLessThan></And>",
        ] {
            assert_eq!(read(outside), Err("InvalidRequest"), "{outside}");
        }
        for malformed in [
            "<Prefix>a</Prefix><Tag><Key>k</Key><Value>v</Value></Tag>",
            "<Prefix>a</Prefix><Prefix>b</Prefix>",
            "<And><Prefix>a</Prefix></And>",
            "<And><Tag><Key>k</Key><Value>v</Value></Tag></And>",
            "<And></And>",
            "<Tag><Value>v</Value></Tag>",
            "<ObjectSizeGreaterThan>1.5</ObjectSizeGreaterThan>",
            "<ObjectSizeGreaterThan>9223372036854775808</ObjectSizeGreaterThan>",
            "<Size>1</Size>",
        ] {
            assert_eq!(read(malformed), Err("MalformedXML"), "{malformed}");
        }
        // An And with an empty prefix and one other predicate holds two (13 §6.9).
        assert!(
            read("<And><Prefix></Prefix><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan></And>")
                .is_ok()
        );
        let two_tags = "<And><Tag><Key>k</Key><Value>1</Value></Tag>\
                        <Tag><Key>k</Key><Value>2</Value></Tag></And>";
        assert_eq!(read(two_tags), Err("InvalidRequest"));
        let range = "<And><ObjectSizeGreaterThan>10</ObjectSizeGreaterThan>\
                     <ObjectSizeLessThan>10</ObjectSizeLessThan></And>";
        assert_eq!(read(range), Err("InvalidRequest"));
        let both = "<Rule><ID>r</ID><Prefix>a</Prefix><Filter><Prefix>a</Prefix></Filter>\
                    <Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>";
        assert_eq!(lifecycle_code(both), Err("InvalidRequest"));
        let neither = "<Rule><ID>r</ID><Status>Enabled</Status>\
                       <Expiration><Days>1</Days></Expiration></Rule>";
        assert_eq!(lifecycle_code(neither), Err("MalformedXML"));
    }

    /// The limit admits the longest configuration S3's limits allow: 1,000 rules, each with
    /// the longest ID, an `And` of the longest prefix, both sizes and ten of the longest tags,
    /// an expiration, a noncurrent expiration and an abort. Its text is written as character
    /// references, as the tagging limits' test writes it.
    #[test]
    fn lifecycle_limit_admits_the_longest_body() {
        let tags: String = (0..lifecycle::MAX_FILTER_TAGS)
            .map(|i| {
                let key: String = (0..tagging::MAX_KEY)
                    .map(|j| format!("&#{};", 0x4E00 + i * tagging::MAX_KEY + j))
                    .collect();
                let value = "&#x9FA5;".repeat(tagging::MAX_VALUE);
                format!("<Tag><Key>{key}</Key><Value>{value}</Value></Tag>")
            })
            .collect();
        let prefix = "&quot;".repeat(MAX_KEY);
        let rules: String = (0..lifecycle::MAX_RULES)
            .map(|i| {
                let id = format!("{i:0>255}");
                format!(
                    "<Rule><ID>{id}</ID><Filter><And><Prefix>{prefix}</Prefix>{tags}\
                     <ObjectSizeGreaterThan>1099511627775999</ObjectSizeGreaterThan>\
                     <ObjectSizeLessThan>1099511627776000</ObjectSizeLessThan></And>\
                     </Filter><Status>Disabled</Status><Expiration><Days>2147483647</Days>\
                     </Expiration><NoncurrentVersionExpiration><NewerNoncurrentVersions>100\
                     </NewerNoncurrentVersions><NoncurrentDays>2147483647</NoncurrentDays>\
                     </NoncurrentVersionExpiration></Rule>"
                )
            })
            .collect();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>{}",
            configuration(&rules)
        );
        assert!(
            body.len() <= LIFECYCLE_LIMIT,
            "{} > {LIFECYCLE_LIMIT}",
            body.len()
        );
        assert_eq!(
            lifecycle(body.as_bytes()).unwrap().len(),
            lifecycle::MAX_RULES
        );
        assert_eq!(
            lifecycle(&vec![b' '; LIFECYCLE_LIMIT + 1]),
            Err(BodyError::Xml(XmlError::TooLarge {
                limit: LIFECYCLE_LIMIT
            }))
        );
        let one = "<Rule><Filter/><Status>Enabled</Status><Expiration><Days>1</Days>\
                   </Expiration></Rule>";
        assert_eq!(
            lifecycle_code(&one.repeat(lifecycle::MAX_RULES + 1)),
            Err("InvalidRequest")
        );
        assert_eq!(
            lifecycle_code(&one.repeat(lifecycle::MAX_RULES)),
            Ok(lifecycle::MAX_RULES)
        );
    }

    /// AWS's PutBucketCors examples, which write the root without a namespace, `AllowedOrigin`
    /// first and blank lines between elements (16 §1.1).
    #[test]
    fn cors_reads_aws_samples() {
        let example_1 = b"<CORSConfiguration>
 <CORSRule>
   <AllowedOrigin>http://www.example.com</AllowedOrigin>

   <AllowedMethod>PUT</AllowedMethod>
   <AllowedMethod>POST</AllowedMethod>
   <AllowedMethod>DELETE</AllowedMethod>

   <AllowedHeader>*</AllowedHeader>
 </CORSRule>
 <CORSRule>
   <AllowedOrigin>*</AllowedOrigin>
   <AllowedMethod>GET</AllowedMethod>
 </CORSRule>
</CORSConfiguration>";
        let rules = cors(example_1).unwrap();
        assert_eq!(
            rules[0],
            cors::Rule {
                id: None,
                headers: vec!["*".into()],
                methods: vec![cors::Method::Put, cors::Method::Post, cors::Method::Delete],
                origins: vec!["http://www.example.com".into()],
                expose: Vec::new(),
                max_age: None,
            }
        );
        assert_eq!(rules[1].methods, [cors::Method::Get]);
        let example_2 = b"<CORSConfiguration>
 <CORSRule>
   <AllowedOrigin>http://www.example.com</AllowedOrigin>
   <AllowedMethod>PUT</AllowedMethod>
   <AllowedMethod>POST</AllowedMethod>
   <AllowedMethod>DELETE</AllowedMethod>
   <AllowedHeader>*</AllowedHeader>
   <MaxAgeSeconds>3000</MaxAgeSeconds>
   <ExposeHeader>x-amz-server-side-encryption</ExposeHeader>
 </CORSRule>
</CORSConfiguration>";
        let rules = cors(example_2).unwrap();
        assert_eq!(rules[0].max_age, Some(3000));
        assert_eq!(rules[0].expose, ["x-amz-server-side-encryption"]);
    }

    /// The schema, and S3's recorded answers: no rules or a rule without an origin or a method
    /// is `MalformedXML`, an empty method `InvalidRequest`; one `MaxAgeSeconds` and one `ID`
    /// at most; a document over 64 KB refused before it is read (16 §1, §7).
    #[test]
    fn cors_refusals_are_s3s() {
        let code = |rules: &str| {
            cors(format!("<CORSConfiguration>{rules}</CORSConfiguration>").as_bytes())
                .map(|rules| rules.len())
                .map_err(|e| e.code().0)
        };
        let rule = |inner: &str| format!("<CORSRule>{inner}</CORSRule>");
        let get_any = "<AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin>";
        assert_eq!(code(&rule(get_any)), Ok(1));
        assert_eq!(code(""), Err("MalformedXML"));
        assert_eq!(code("<CORSRule></CORSRule>"), Err("MalformedXML"));
        assert_eq!(
            code(&rule("<AllowedMethod>GET</AllowedMethod>")),
            Err("MalformedXML")
        );
        assert_eq!(
            code(&rule(
                "<AllowedMethod></AllowedMethod><AllowedOrigin>*</AllowedOrigin>"
            )),
            Err("InvalidRequest")
        );
        assert_eq!(
            code(&rule(&format!(
                "{get_any}<MaxAgeSeconds>1</MaxAgeSeconds><MaxAgeSeconds>2</MaxAgeSeconds>"
            ))),
            Err("MalformedXML")
        );
        assert_eq!(
            code(&rule(&format!("{get_any}<MaxAgeSeconds>a</MaxAgeSeconds>"))),
            Err("MalformedXML")
        );
        assert_eq!(
            code(&rule(&format!("{get_any}<Allowed>x</Allowed>"))),
            Err("MalformedXML")
        );
        assert_eq!(
            code(&rule(get_any).repeat(cors::MAX_RULES)),
            Ok(cors::MAX_RULES)
        );
        assert_eq!(
            code(&rule(get_any).repeat(cors::MAX_RULES + 1)),
            Err("MalformedXML")
        );
        assert_eq!(
            cors(&vec![b' '; CORS_LIMIT + 1]).map_err(|e| e.code().0),
            Err("MaxMessageLengthExceeded")
        );
        let namespaced = format!(
            "<CORSConfiguration xmlns=\"{NAMESPACE}\">{}</CORSConfiguration>",
            rule(get_any)
        );
        assert!(cors(namespaced.as_bytes()).is_ok());
    }

    /// PutPublicAccessBlock's settings as botocore writes them; one left out is off (17 §7).
    /// AWS's PutBucketEncryption sample shape, and S3's recorded refusals (20 §4.1, §4.3).
    #[test]
    fn encryption_configuration_is_read_as_s3_reads_it() {
        use crate::sse::{Rule, SseError};
        let read = |rules: &str| {
            server_side_encryption_configuration(
                format!(
                    "<ServerSideEncryptionConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{rules}</ServerSideEncryptionConfiguration>"
                )
                .as_bytes(),
            )
        };
        assert_eq!(
            read(
                "<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled></Rule>"
            ),
            Ok(Rule {
                bucket_key: true,
                customer_blocked: None
            })
        );
        assert_eq!(
            read(
                "<Rule><BlockedEncryptionTypes><EncryptionType>NONE</EncryptionType></BlockedEncryptionTypes></Rule>"
            ),
            Ok(Rule {
                bucket_key: false,
                customer_blocked: Some(false)
            })
        );
        assert_eq!(
            read(
                "<Rule><BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes></Rule>"
            ),
            Ok(Rule {
                bucket_key: false,
                customer_blocked: Some(true)
            })
        );
        let malformed = |rules: &str| {
            assert_eq!(
                read(rules).map_err(|e| e.code()),
                Err(("MalformedXML", 400)),
                "{rules}"
            );
        };
        malformed("");
        malformed("<Rule/><Rule/>");
        malformed("<Rule><ApplyServerSideEncryptionByDefault/></Rule>");
        malformed(
            "<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:fsx</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule>",
        );
        malformed(
            "<Rule><BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType><EncryptionType>NONE</EncryptionType></BlockedEncryptionTypes></Rule>",
        );
        malformed("<Rule><BlockedEncryptionTypes/></Rule>");
        assert_eq!(
            read(
                "<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><KMSMasterKeyID>k</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule>"
            ),
            Err(BodyError::Sse(SseError::KmsKeyNotApplicable))
        );
        assert_eq!(
            read("<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule>")
                .map_err(|e| e.code()),
            Err(("NotImplemented", 501))
        );
    }

    #[test]
    fn public_access_block_reads_its_settings() {
        let all = format!(
            "<PublicAccessBlockConfiguration xmlns=\"{NAMESPACE}\"><BlockPublicAcls>true\
             </BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true\
             </BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets>\
             </PublicAccessBlockConfiguration>"
        );
        assert_eq!(
            public_access_block(all.as_bytes()),
            Ok(crate::policy::PublicAccessBlock::NEW_BUCKET)
        );
        let one =
            b"<PublicAccessBlockConfiguration><RestrictPublicBuckets>1</RestrictPublicBuckets>\
                    </PublicAccessBlockConfiguration>";
        let read = public_access_block(one).unwrap();
        assert!(read.restrict_public_buckets);
        assert!(!read.block_public_policy && !read.block_public_acls && !read.ignore_public_acls);
        for bad in [
            &b"<PublicAccessBlockConfiguration><BlockPublicPolicy>yes</BlockPublicPolicy></PublicAccessBlockConfiguration>"[..],
            b"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>",
            b"<PublicAccessBlockConfiguration><BlockAll>true</BlockAll></PublicAccessBlockConfiguration>",
        ] {
            assert_eq!(public_access_block(bad).map_err(|e| e.code().0), Err("MalformedXML"));
        }
    }

    /// PutObjectLockConfiguration's body as botocore writes it, and the shapes S3 was recorded
    /// refusing (18 §3, §5).
    #[test]
    fn object_lock_configurations_read_as_s3_reads_them() {
        use crate::lock::{Configuration, LockError, Mode, Period};
        let configured = |inner: &str| {
            object_lock_configuration(
                format!("<ObjectLockConfiguration xmlns=\"{NAMESPACE}\">{inner}</ObjectLockConfiguration>")
                    .as_bytes(),
            )
        };
        assert_eq!(
            configured(
                "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention>\
                 <Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule>"
            ),
            Ok(Configuration {
                default: Some((Mode::Governance, Period::Days(1)))
            })
        );
        assert_eq!(
            configured("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"),
            Ok(Configuration::default())
        );
        for malformed in [
            "",
            "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule>",
            "<ObjectLockEnabled>Disabled</ObjectLockEnabled>",
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule></Rule>",
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention></DefaultRetention></Rule>",
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode></DefaultRetention></Rule>",
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>BAD-VALUE</Mode><Days>1</Days></DefaultRetention></Rule>",
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>governance</Mode><Days>1</Days></DefaultRetention></Rule>",
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days><Years>1</Years></DefaultRetention></Rule>",
        ] {
            assert_eq!(
                configured(malformed).map_err(|e| e.code().0),
                Err("MalformedXML"),
                "{malformed}"
            );
        }
        let zero = configured(
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention>\
             <Mode>GOVERNANCE</Mode><Days>0</Days></DefaultRetention></Rule>",
        );
        assert_eq!(
            zero,
            Err(BodyError::Lock(LockError::PeriodNotPositive("Days")))
        );
        let huge = configured(
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention>\
             <Mode>COMPLIANCE</Mode><Days>999999999</Days></DefaultRetention></Rule>",
        );
        assert_eq!(huge.map_err(|e| e.code().0), Err("InvalidArgument"));
        let held = configured(
            "<ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>\
             COMPLIANCE</Mode><Days>365</Days><DefaultEventHold><Days>90</Days></DefaultEventHold>\
             </DefaultRetention></Rule>",
        );
        assert_eq!(held.map_err(|e| e.code()), Err(("NotImplemented", 501)));
    }

    /// PutObjectRetention's and PutObjectLegalHold's bodies as botocore writes them, and what S3
    /// and s3-tests refuse (18 §3, §4, §5).
    #[test]
    fn retentions_and_legal_holds_read_as_s3_reads_them() {
        use crate::lock::{Mode, Retention, RetentionRequest, instant};
        let set = retention(
            format!(
                "<Retention xmlns=\"{NAMESPACE}\"><Mode>GOVERNANCE</Mode><RetainUntilDate>\
                 2030-01-01T12:30:45.123456Z</RetainUntilDate></Retention>"
            )
            .as_bytes(),
        );
        assert_eq!(
            set,
            Ok(RetentionRequest::Set(Retention {
                mode: Mode::Governance,
                until: instant("2030-01-01T12:30:45.123Z").unwrap(),
            }))
        );
        let removal = format!("<Retention xmlns=\"{NAMESPACE}\" />");
        assert_eq!(retention(removal.as_bytes()), Ok(RetentionRequest::Remove));
        for malformed in [
            &b"<Retention><Mode>GOVERNANCE</Mode></Retention>"[..],
            b"<Retention><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>",
            b"<Retention><Mode>governance</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>",
            b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>next year</RetainUntilDate></Retention>",
        ] {
            assert_eq!(retention(malformed).map_err(|e| e.code().0), Err("MalformedXML"));
        }
        let held = b"<Retention><Mode>COMPLIANCE</Mode><EventHold>ON</EventHold></Retention>";
        assert_eq!(retention(held).map_err(|e| e.code().1), Err(501));
        let on = format!("<LegalHold xmlns=\"{NAMESPACE}\"><Status>ON</Status></LegalHold>");
        assert_eq!(legal_hold(on.as_bytes()), Ok(true));
        assert_eq!(
            legal_hold(b"<LegalHold><Status>OFF</Status></LegalHold>"),
            Ok(false)
        );
        for malformed in [
            &b"<LegalHold><Status>abc</Status></LegalHold>"[..],
            b"<LegalHold/>",
            b"",
        ] {
            assert_eq!(
                legal_hold(malformed).map_err(|e| e.code().0),
                Err("MalformedXML")
            );
        }
    }
}

#[cfg(test)]
mod compact_tests {
    use super::*;
    use crate::xml::Compact;
    use proptest::prelude::*;

    fn compact(doc: &[u8], limit: usize) -> Result<Vec<u8>, XmlError> {
        let mut c = Compact::new(limit);
        c.push(doc)?;
        c.finish()
    }

    /// A document written with white space between its elements reads, compacted, as written;
    /// the white space in an element's text is kept; read a byte at a time or all at once, the
    /// same bytes are kept; and a limit counts only what is kept, so a megabyte of white space
    /// between elements is read within a limit of the document's own size, while white space in
    /// an element's text past the limit is refused (docs/design/s3-protocol.md §2).
    #[test]
    fn white_space_between_elements_is_dropped_as_it_is_read() {
        let pretty = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete xmlns=\"{NAMESPACE}\">\n  \
             <Object>\n    <Key> spaced key </Key>\n    <VersionId>v1</VersionId>\n  </Object>\n  \
             <!-- a comment --> <Object ><Key> </Key></Object>\n  <Quiet>true</Quiet>\n</Delete>\n"
        );
        let kept = compact(pretty.as_bytes(), usize::MAX).unwrap();
        let mut one = Compact::new(usize::MAX);
        for b in pretty.as_bytes() {
            one.push(&[*b]).unwrap();
        }
        assert_eq!(one.finish().unwrap(), kept);
        let read = delete(&kept).unwrap();
        assert_eq!(read, delete(pretty.as_bytes()).unwrap());
        let keys: Vec<&str> = read.objects.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(keys, [" spaced key ", " "]);
        assert!(!String::from_utf8_lossy(&kept).contains("\n  "));
        let padded = pretty.replace('\n', &format!("\n{}", " ".repeat(1 << 20)));
        assert_eq!(compact(padded.as_bytes(), kept.len()).unwrap(), kept);
        // Read as sent, the limit counts the document as kept.
        assert_eq!(delete(padded.as_bytes()).unwrap(), read);
        let long = format!(
            "<Delete><Object><Key>{}</Key></Object></Delete>",
            " ".repeat(100)
        );
        assert_eq!(
            compact(long.as_bytes(), 60),
            Err(XmlError::TooLarge { limit: 60 })
        );
    }

    /// A comment's `-->`, a CDATA section's `]]>` and a processing instruction's `?>` are
    /// sought only after the whole of their opening, as XML 1.0 [15], [16] and [18] read them:
    /// `<!--->` opens a comment whose content starts `->`, and `<?>` closes nothing. Markup that
    /// only looks like an end tag inside them leaves the white space after them in the key, and
    /// the counting reader behind `Reader::open` counts that white space against the limit.
    #[test]
    fn markup_ends_only_after_its_whole_opening() {
        let docs = [
            (
                "<Delete><Object><Key><!---> </x -->  a</Key></Object></Delete>",
                "  a",
            ),
            (
                "<Delete><Object><Key><!----> a</Key></Object></Delete>",
                " a",
            ),
            (
                "<Delete><Object><Key><![CDATA[]> </x>]]>  a</Key></Object></Delete>",
                "]> </x>  a",
            ),
            (
                "<Delete><Object><Key><?p </x ?>  a</Key></Object></Delete>",
                "  a",
            ),
        ];
        for (doc, key) in docs {
            let sent = delete(doc.as_bytes()).unwrap();
            assert_eq!(sent.objects[0].key, key, "{doc}");
            let kept = compact(doc.as_bytes(), DELETE_LIMIT).unwrap();
            assert_eq!(delete(&kept).unwrap(), sent, "{doc}");
        }
        // `<?>` is no processing instruction, so the reader refuses it; kept, it is refused alike.
        let doc = "<Delete><Object><Key><?> </x ?>  a</Key></Object></Delete>";
        let kept = compact(doc.as_bytes(), DELETE_LIMIT).unwrap();
        assert_eq!(kept, doc.as_bytes());
        assert!(delete(doc.as_bytes()).is_err());
        // Counted, the key's white space after the comment is part of the document.
        let padded = format!(
            "<Delete><Object><Key><!---> </x -->{}a</Key></Object></Delete>",
            " ".repeat(64)
        );
        let limit = padded.len() - 1;
        assert_eq!(
            crate::xml::Reader::open(padded.as_bytes(), limit, "Delete").err(),
            Some(XmlError::TooLarge { limit })
        );
    }

    fn space() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop_oneof![Just(' '), Just('\t'), Just('\n'), Just('\r')],
            0..4,
        )
        .prop_map(|c| c.into_iter().collect())
    }

    proptest! {
        /// Any Tagging document, its keys and values holding white space of their own, with
        /// white space put anywhere between its elements, reads the same compacted as sent.
        #[test]
        fn a_compacted_document_reads_as_sent(
            tags in prop::collection::vec(("[ a-z]{0,6}", "[ a-z]{0,6}"), 0..6),
            gaps in prop::collection::vec(space(), 64),
        ) {
            let mut gap = gaps.iter().cycle();
            let mut g = || gap.next().cloned().unwrap_or_default();
            let mut doc = format!("<?xml version=\"1.0\"?>{}<Tagging xmlns=\"{NAMESPACE}\">{}<TagSet>", g(), g());
            for (k, v) in &tags {
                doc.push_str(&format!(
                    "{}<Tag>{}<Key>{k}</Key>{}<Value>{v}</Value>{}</Tag>{}",
                    g(), g(), g(), g(), g()
                ));
            }
            doc.push_str(&format!("</TagSet>{}</Tagging>{}", g(), g()));
            let kept = compact(doc.as_bytes(), usize::MAX).unwrap();
            prop_assert_eq!(
                tagging(&kept, Tagged::Object),
                tagging(doc.as_bytes(), Tagged::Object)
            );
        }
    }
}

#[cfg(test)]
mod cors_limit_tests {
    use super::*;

    /// A CORS configuration is held to S3's 64 KB as sent, white space included (16 §1.1).
    #[test]
    fn a_cors_configuration_is_held_to_its_size_as_sent() {
        let rule = "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin>\
                    </CORSRule>";
        let body = |space: usize| {
            format!(
                "<CORSConfiguration xmlns=\"{NAMESPACE}\">{}{rule}</CORSConfiguration>",
                " ".repeat(space)
            )
        };
        let base = body(0).len();
        assert!(cors(body(CORS_LIMIT - base).as_bytes()).is_ok());
        assert_eq!(
            cors(body(CORS_LIMIT - base + 1).as_bytes()).map_err(|e| e.code().0),
            Err("MaxMessageLengthExceeded")
        );
    }
}
