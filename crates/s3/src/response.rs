//! The XML documents S3's responses carry (docs/research/13 §9): listings, multipart results,
//! copy results, batch deletes, errors, bucket settings, tags and ACLs.
//!
//! Each document writes its elements in the order of its operation's Response Syntax, and
//! every result is in S3's namespace; the error document is in none. SDKs read elements by
//! name, and AWS's own samples order them differently from one another (13 §9.1). Times are
//! whole seconds, written as every sample writes them, `2009-10-12T17:50:30.000Z`, so a
//! listing's `LastModified` names the instant a HEAD's `Last-Modified` does (13 §9.2). ETags
//! are given without their quotes, as the metadata layer keeps them, and written quoted
//! (05 §5.1). An owner is its ID: S3 stopped returning `DisplayName` in November 2025
//! (13 §9.4).

use std::borrow::Cow;

use crate::acl::{Ownership, Permission};
use crate::body::Versioning;
use crate::checksum::{Algorithm, Checksum, ChecksumType};
use crate::cors;
use crate::lifecycle::{And, Expiration, Filter, Rule, Scope};
use crate::list::{Page, Request, Start, url_encode};
use crate::tagging::Tag;
use crate::time::iso8601;
use crate::xml::Writer;

/// Every object's storage class: mantle stores objects one way, as S3's default class.
const STORAGE_CLASS: &str = "STANDARD";

/// A time outside the years 0000–9999 that S3's time format holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{0} seconds from the Unix epoch is outside the years an S3 time holds")]
pub struct TimeOutOfRange(pub i64);

impl TimeOutOfRange {
    /// `500 InternalError`: every time a response carries is mantle's own.
    pub fn code(&self) -> (&'static str, u16) {
        ("InternalError", 500)
    }
}

/// An object as a listing shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Last modified, Unix seconds.
    pub modified: i64,
    /// Without its quotes.
    pub etag: String,
    pub size: u64,
    /// The owner's ID.
    pub owner: String,
    pub checksum: Option<(Algorithm, ChecksumType)>,
}

/// What a ListObjectsV2 response echoes of its request (05 §6.2).
#[derive(Debug, Clone, Copy)]
pub struct ListV2<'a> {
    pub bucket: &'a str,
    /// The prefix, delimiter and `max-keys` the page was listed with.
    pub request: Request<'a>,
    /// `continuation-token` as sent, echoed even when empty.
    pub continuation_token: Option<&'a str>,
    pub start_after: Option<&'a str>,
    /// `encoding-type=url`.
    pub url: bool,
    /// `fetch-owner=true`: V2 leaves `Owner` out unless asked.
    pub fetch_owner: bool,
}

/// ListObjectsV2's `ListBucketResult`. `KeyCount` counts common prefixes with keys, and an
/// empty delimiter is no delimiter and is not echoed (05 §6.2).
pub fn list_objects_v2(list: &ListV2<'_>, page: &Page<Entry>) -> Result<String, TimeOutOfRange> {
    let modified = times(page.contents.iter().map(|(_, entry)| entry.modified))?;
    let (request, url) = (&list.request, list.url);
    Ok(Writer::document("ListBucketResult", true, |w| {
        w.text("IsTruncated", flag(page.truncated));
        contents(w, page, &modified, list.fetch_owner, url);
        w.text("Name", list.bucket);
        w.text("Prefix", &encoded(request.prefix, url));
        delimiter(w, request.delimiter, url);
        w.text("MaxKeys", &request.max_keys.to_string());
        common_prefixes(w, &page.common_prefixes, url);
        encoding_type(w, url);
        w.text("KeyCount", &page.key_count().to_string());
        if let Some(token) = list.continuation_token {
            w.text("ContinuationToken", token);
        }
        if let Some(token) = page.continuation() {
            w.text("NextContinuationToken", &token);
        }
        if let Some(after) = list.start_after {
            w.text("StartAfter", &encoded(after, url));
        }
    }))
}

/// What a ListObjects (V1) response echoes of its request (05 §6.3).
#[derive(Debug, Clone, Copy)]
pub struct ListV1<'a> {
    pub bucket: &'a str,
    /// The prefix, delimiter, `max-keys` and `marker` the page was listed with.
    pub request: Request<'a>,
    /// `encoding-type=url`, which encodes the markers too (05 §6.3).
    pub url: bool,
}

/// ListObjects' `ListBucketResult`. `Marker` is written even when none was sent, as s3-tests
/// expects, and `NextMarker` as [`Page::next_marker`] gives it. V1 lists owners unasked.
pub fn list_objects(list: &ListV1<'_>, page: &Page<Entry>) -> Result<String, TimeOutOfRange> {
    let modified = times(page.contents.iter().map(|(_, entry)| entry.modified))?;
    let (request, url) = (&list.request, list.url);
    Ok(Writer::document("ListBucketResult", true, |w| {
        w.text("IsTruncated", flag(page.truncated));
        let marker = match request.start {
            Some(Start::After(marker) | Start::Past(marker)) => marker,
            None => "",
        };
        w.text("Marker", &encoded(marker, url));
        if let Some(next) = page.next_marker(delimited(request.delimiter).is_some()) {
            w.text("NextMarker", &encoded(next, url));
        }
        contents(w, page, &modified, true, url);
        w.text("Name", list.bucket);
        w.text("Prefix", &encoded(request.prefix, url));
        delimiter(w, request.delimiter, url);
        w.text("MaxKeys", &request.max_keys.to_string());
        common_prefixes(w, &page.common_prefixes, url);
        encoding_type(w, url);
    }))
}

/// A version as ListObjectVersions shows it: an object's, or a delete marker's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub key: String,
    pub version_id: String,
    /// The key's current version.
    pub latest: bool,
    /// A delete marker, which shows no ETag, size or checksum.
    pub marker: bool,
    pub entry: Entry,
}

/// Where a listing of versions or uploads resumes: a key, and the version or upload ID
/// within it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Next<'a> {
    pub key: &'a str,
    pub id: &'a str,
}

/// A page of ListObjectVersions, and what its response echoes of the request (05 §6.4).
#[derive(Debug, Clone, Copy)]
pub struct ListVersions<'a> {
    pub bucket: &'a str,
    pub prefix: &'a str,
    pub delimiter: Option<&'a str>,
    pub max_keys: usize,
    pub key_marker: Option<&'a str>,
    pub version_id_marker: Option<&'a str>,
    /// `encoding-type=url`, which encodes the key markers too.
    pub url: bool,
    /// In key order, and newest first within a key.
    pub versions: &'a [Version],
    pub common_prefixes: &'a [String],
    /// Where the next page starts; `None` on the last page.
    pub next: Option<Next<'a>>,
}

/// ListObjectVersions' `ListVersionsResult`. The markers are written even when none was
/// sent, and the next page's only when there is one, as AWS's samples do (13 §9.3).
pub fn list_object_versions(list: &ListVersions<'_>) -> Result<String, TimeOutOfRange> {
    let modified = times(list.versions.iter().map(|v| v.entry.modified))?;
    let url = list.url;
    Ok(Writer::document("ListVersionsResult", true, |w| {
        w.text("IsTruncated", flag(list.next.is_some()));
        w.text("KeyMarker", &encoded(list.key_marker.unwrap_or(""), url));
        w.text("VersionIdMarker", list.version_id_marker.unwrap_or(""));
        if let Some(next) = list.next {
            w.text("NextKeyMarker", &encoded(next.key, url));
            w.text("NextVersionIdMarker", next.id);
        }
        for (version, modified) in list.versions.iter().zip(&modified) {
            let entry = &version.entry;
            if version.marker {
                w.element("DeleteMarker", |w| {
                    w.text("IsLatest", flag(version.latest));
                    w.text("Key", &encoded(&version.key, url));
                    w.text("LastModified", modified);
                    owner(w, "Owner", &entry.owner);
                    w.text("VersionId", &version.version_id);
                });
            } else {
                w.element("Version", |w| {
                    checksum_type(w, entry.checksum);
                    w.text("ETag", &quoted(&entry.etag));
                    w.text("IsLatest", flag(version.latest));
                    w.text("Key", &encoded(&version.key, url));
                    w.text("LastModified", modified);
                    owner(w, "Owner", &entry.owner);
                    w.text("Size", &entry.size.to_string());
                    w.text("StorageClass", STORAGE_CLASS);
                    w.text("VersionId", &version.version_id);
                });
            }
        }
        w.text("Name", list.bucket);
        w.text("Prefix", &encoded(list.prefix, url));
        delimiter(w, list.delimiter, url);
        w.text("MaxKeys", &list.max_keys.to_string());
        common_prefixes(w, list.common_prefixes, url);
        encoding_type(w, url);
    }))
}

/// A bucket as ListBuckets shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    pub name: String,
    /// Unix seconds.
    pub created: i64,
    pub region: String,
}

/// A page of ListBuckets (05 §10.4).
#[derive(Debug, Clone, Copy)]
pub struct ListBuckets<'a> {
    /// The owner's ID.
    pub owner: &'a str,
    pub buckets: &'a [Bucket],
    /// Show each bucket's region, which S3 does "If the request contains at least one valid
    /// parameter" (13 §9.3).
    pub regions: bool,
    /// Where the next page starts, when more buckets follow.
    pub continuation_token: Option<&'a str>,
    /// `prefix` as sent.
    pub prefix: Option<&'a str>,
}

/// ListBuckets' `ListAllMyBucketsResult`.
pub fn list_buckets(list: &ListBuckets<'_>) -> Result<String, TimeOutOfRange> {
    let created = times(list.buckets.iter().map(|b| b.created))?;
    Ok(Writer::document("ListAllMyBucketsResult", true, |w| {
        w.element("Buckets", |w| {
            for (bucket, created) in list.buckets.iter().zip(&created) {
                w.element("Bucket", |w| {
                    if list.regions {
                        w.text("BucketRegion", &bucket.region);
                    }
                    w.text("CreationDate", created);
                    w.text("Name", &bucket.name);
                });
            }
        });
        owner(w, "Owner", list.owner);
        if let Some(token) = list.continuation_token {
            w.text("ContinuationToken", token);
        }
        if let Some(prefix) = list.prefix {
            w.text("Prefix", prefix);
        }
    }))
}

/// A multipart upload as ListMultipartUploads shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    pub key: String,
    pub upload_id: String,
    /// When it was created, Unix seconds.
    pub initiated: i64,
    /// The ID of whoever created it.
    pub initiator: String,
    pub owner: String,
    pub checksum: Option<(Algorithm, ChecksumType)>,
}

/// A page of ListMultipartUploads, and what its response echoes of the request (05 §4.7).
#[derive(Debug, Clone, Copy)]
pub struct ListUploads<'a> {
    pub bucket: &'a str,
    pub prefix: &'a str,
    pub delimiter: Option<&'a str>,
    pub max_uploads: usize,
    pub key_marker: Option<&'a str>,
    pub upload_id_marker: Option<&'a str>,
    /// `encoding-type=url`, which encodes the key markers too.
    pub url: bool,
    /// In key order, and in order of creation within a key.
    pub uploads: &'a [Upload],
    pub common_prefixes: &'a [String],
    pub truncated: bool,
    /// `NextKeyMarker` and `NextUploadIdMarker`, which S3 writes on every page, truncated or
    /// not ([`crate::list::EntryPage::upload_markers`]).
    pub next: Next<'a>,
}

/// ListMultipartUploads' `ListMultipartUploadsResult`.
pub fn list_multipart_uploads(list: &ListUploads<'_>) -> Result<String, TimeOutOfRange> {
    let initiated = times(list.uploads.iter().map(|u| u.initiated))?;
    let url = list.url;
    Ok(Writer::document("ListMultipartUploadsResult", true, |w| {
        w.text("Bucket", list.bucket);
        w.text("KeyMarker", &encoded(list.key_marker.unwrap_or(""), url));
        w.text("UploadIdMarker", list.upload_id_marker.unwrap_or(""));
        w.text("NextKeyMarker", &encoded(list.next.key, url));
        w.text("Prefix", &encoded(list.prefix, url));
        delimiter(w, list.delimiter, url);
        w.text("NextUploadIdMarker", list.next.id);
        w.text("MaxUploads", &list.max_uploads.to_string());
        w.text("IsTruncated", flag(list.truncated));
        for (upload, initiated) in list.uploads.iter().zip(&initiated) {
            w.element("Upload", |w| {
                checksum_type(w, upload.checksum);
                w.text("Initiated", initiated);
                owner(w, "Initiator", &upload.initiator);
                w.text("Key", &encoded(&upload.key, url));
                owner(w, "Owner", &upload.owner);
                w.text("StorageClass", STORAGE_CLASS);
                w.text("UploadId", &upload.upload_id);
            });
        }
        common_prefixes(w, list.common_prefixes, url);
        encoding_type(w, url);
    }))
}

/// A part as ListParts shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub number: u16,
    /// Unix seconds.
    pub modified: i64,
    /// Without its quotes.
    pub etag: String,
    pub size: u64,
    /// The part's value, under the upload's algorithm.
    pub checksum: Option<Checksum>,
}

/// A page of ListParts (05 §4.6).
#[derive(Debug, Clone, Copy)]
pub struct ListParts<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    /// `part-number-marker` as sent; 0, before every part, when none was.
    pub part_number_marker: u16,
    /// The last part listed, when more follow: "When a list is truncated, this element
    /// specifies the last part in the list" (13 §9.3).
    pub next: Option<u16>,
    pub max_parts: usize,
    pub parts: &'a [Part],
    pub initiator: &'a str,
    pub owner: &'a str,
    /// The upload's algorithm, and how its object's value will be formed.
    pub checksum: Option<(Algorithm, ChecksumType)>,
}

/// ListParts' `ListPartsResult`.
pub fn list_parts(list: &ListParts<'_>) -> Result<String, TimeOutOfRange> {
    let modified = times(list.parts.iter().map(|p| p.modified))?;
    Ok(Writer::document("ListPartsResult", true, |w| {
        w.text("Bucket", list.bucket);
        w.text("Key", list.key);
        w.text("UploadId", list.upload_id);
        w.text("PartNumberMarker", &list.part_number_marker.to_string());
        if let Some(next) = list.next {
            w.text("NextPartNumberMarker", &next.to_string());
        }
        w.text("MaxParts", &list.max_parts.to_string());
        w.text("IsTruncated", flag(list.next.is_some()));
        for (part, modified) in list.parts.iter().zip(&modified) {
            w.element("Part", |w| {
                if let Some(checksum) = &part.checksum {
                    w.text(checksum.algorithm.element(), &checksum.to_base64());
                }
                w.text("ETag", &quoted(&part.etag));
                w.text("LastModified", modified);
                w.text("PartNumber", &part.number.to_string());
                w.text("Size", &part.size.to_string());
            });
        }
        owner(w, "Initiator", list.initiator);
        owner(w, "Owner", list.owner);
        w.text("StorageClass", STORAGE_CLASS);
        checksum_type(w, list.checksum);
    }))
}

/// CreateMultipartUpload's `InitiateMultipartUploadResult`.
pub fn initiate_multipart_upload(bucket: &str, key: &str, upload_id: &str) -> String {
    Writer::document("InitiateMultipartUploadResult", true, |w| {
        w.text("Bucket", bucket);
        w.text("Key", key);
        w.text("UploadId", upload_id);
    })
}

/// An object's checksum as a result carries it (05 §3.3–§3.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectChecksum<'a> {
    pub algorithm: Algorithm,
    pub kind: ChecksumType,
    /// Base64, followed by `-n` for a composite of n parts' values.
    pub value: &'a str,
}

/// A completed multipart upload (05 §4.4).
#[derive(Debug, Clone, Copy)]
pub struct Completed<'a> {
    /// "The URI that identifies the newly created object."
    pub location: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    /// Without its quotes.
    pub etag: &'a str,
    pub checksum: Option<ObjectChecksum<'a>>,
}

/// CompleteMultipartUpload's `CompleteMultipartUploadResult`.
pub fn complete_multipart_upload(completed: &Completed<'_>) -> String {
    Writer::document("CompleteMultipartUploadResult", true, |w| {
        w.text("Location", completed.location);
        w.text("Bucket", completed.bucket);
        w.text("Key", completed.key);
        w.text("ETag", &quoted(completed.etag));
        if let Some(checksum) = completed.checksum {
            w.text(checksum.algorithm.element(), checksum.value);
            w.text("ChecksumType", checksum.kind.name());
        }
    })
}

/// CopyObject's `CopyObjectResult`, for the new object's ETag (without its quotes), time and
/// checksum.
pub fn copy_object(
    etag: &str,
    modified: i64,
    checksum: Option<ObjectChecksum<'_>>,
) -> Result<String, TimeOutOfRange> {
    let modified = timestamp(modified)?;
    Ok(Writer::document("CopyObjectResult", true, |w| {
        w.text("ETag", &quoted(etag));
        w.text("LastModified", &modified);
        if let Some(checksum) = checksum {
            w.text("ChecksumType", checksum.kind.name());
            w.text(checksum.algorithm.element(), checksum.value);
        }
    }))
}

/// UploadPartCopy's `CopyPartResult`, for the new part's ETag (without its quotes), time and
/// value under the upload's algorithm.
pub fn copy_part(
    etag: &str,
    modified: i64,
    checksum: Option<&Checksum>,
) -> Result<String, TimeOutOfRange> {
    let modified = timestamp(modified)?;
    Ok(Writer::document("CopyPartResult", true, |w| {
        w.text("ETag", &quoted(etag));
        w.text("LastModified", &modified);
        if let Some(checksum) = checksum {
            w.text(checksum.algorithm.element(), &checksum.to_base64());
        }
    }))
}

/// What became of one object a DeleteObjects request named (05 §8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deletion<'a> {
    Deleted {
        key: &'a str,
        /// The version the request named.
        version_id: Option<&'a str>,
        /// The version ID of the delete marker the delete made, or of the one it removed.
        delete_marker: Option<&'a str>,
    },
    Failed {
        key: &'a str,
        version_id: Option<&'a str>,
        code: &'a str,
        message: &'a str,
    },
}

/// DeleteObjects' `DeleteResult`: the objects deleted, then those that failed, each in the
/// request's order. Quiet mode leaves the deleted out: "In quiet mode the response includes
/// only keys where the delete operation encountered an error" (05 §8.1).
pub fn delete_result(results: &[Deletion<'_>], quiet: bool) -> String {
    Writer::document("DeleteResult", true, |w| {
        let deleted = if quiet { &[][..] } else { results };
        for result in deleted {
            if let Deletion::Deleted {
                key,
                version_id,
                delete_marker,
            } = *result
            {
                w.element("Deleted", |w| {
                    if let Some(marker) = delete_marker {
                        w.text("DeleteMarker", "true");
                        w.text("DeleteMarkerVersionId", marker);
                    }
                    w.text("Key", key);
                    if let Some(id) = version_id {
                        w.text("VersionId", id);
                    }
                });
            }
        }
        for result in results {
            if let Deletion::Failed {
                key,
                version_id,
                code,
                message,
            } = *result
            {
                w.element("Error", |w| {
                    w.text("Code", code);
                    w.text("Key", key);
                    w.text("Message", message);
                    if let Some(id) = version_id {
                        w.text("VersionId", id);
                    }
                });
            }
        }
    })
}

/// An error response's body (05 §11.1).
#[derive(Debug, Clone, Copy)]
pub struct Failure<'a> {
    /// The S3 error code, which clients act on.
    pub code: &'a str,
    pub message: &'a str,
    /// The bucket or object the error concerns.
    pub resource: Option<&'a str>,
    /// Elements S3 adds for some errors, after the message: `BucketName` for a missing
    /// bucket setting, `Method` and `ResourceType` for a refused preflight (16 §7).
    pub details: &'a [(&'a str, &'a str)],
    pub request_id: &'a str,
    /// The response's `x-amz-id-2`.
    pub host_id: &'a str,
}

/// The `Error` document, in no namespace, as AWS's samples write it (05 §11.1).
pub fn error(failure: &Failure<'_>) -> String {
    Writer::document("Error", false, |w| {
        w.text("Code", failure.code);
        w.text("Message", failure.message);
        if let Some(resource) = failure.resource {
            w.text("Resource", resource);
        }
        for (name, value) in failure.details {
            w.text(name, value);
        }
        w.text("RequestId", failure.request_id);
        w.text("HostId", failure.host_id);
    })
}

/// GetBucketLocation's `LocationConstraint`: the region as the root's text, empty for S3's
/// null, the default region (13 §9.5).
pub fn location_constraint(location: &str) -> String {
    Writer::document("LocationConstraint", true, |w| w.content(location))
}

/// GetBucketVersioning's `VersioningConfiguration`, empty for a bucket never versioned
/// (05 §7.1). `MfaDelete` is written only for a bucket configured with MFA delete, which
/// mantle keeps no setting for (13 §9.5).
pub fn versioning_configuration(status: Option<Versioning>) -> String {
    Writer::document("VersioningConfiguration", true, |w| {
        if let Some(status) = status {
            w.text(
                "Status",
                match status {
                    Versioning::Enabled => "Enabled",
                    Versioning::Suspended => "Suspended",
                },
            );
        }
    })
}

/// GetObjectTagging's and GetBucketTagging's `Tagging`, with the tags in the order given,
/// the key order S3 gives them back in (13 §6.7, §9.6).
pub fn tagging(tags: &[Tag]) -> String {
    Writer::document("Tagging", true, |w| {
        w.element("TagSet", |w| {
            for tag in tags {
                w.element("Tag", |w| {
                    w.text("Key", &tag.key);
                    w.text("Value", &tag.value);
                });
            }
        });
    })
}

/// GetBucketAcl's and GetObjectAcl's `AccessControlPolicy`. mantle's buckets have ACLs
/// disabled, and S3 answers such a bucket, and every object in it, with its owner's full
/// control: "Requests to read ACLs always return a response that shows full control for the
/// bucket owner" (13 §6.8, §9.6).
pub fn access_control_policy(bucket_owner: &str) -> String {
    Writer::document("AccessControlPolicy", true, |w| {
        owner(w, "Owner", bucket_owner);
        w.element("AccessControlList", |w| {
            w.element("Grant", |w| {
                w.typed("Grantee", "CanonicalUser", |w| w.text("ID", bucket_owner));
                w.text("Permission", Permission::FullControl.name());
            });
        });
    })
}

/// GetBucketOwnershipControls' `OwnershipControls`: bucket owner enforced, the one setting
/// mantle's buckets have (13 §6.8).
pub fn ownership_controls() -> String {
    Writer::document("OwnershipControls", true, |w| {
        w.element("Rule", |w| {
            w.text("ObjectOwnership", Ownership::BucketOwnerEnforced.name())
        });
    })
}

/// GetBucketLifecycleConfiguration's `LifecycleConfiguration` (13 §6.9): each rule in the
/// order set, with its ID, and its objects named as it named them, by a `Filter` or by the
/// rule's own `Prefix`, as AWS's sample gives a rule-level `Prefix` back. A rule's elements
/// are in the order S3 was recorded writing them, `ID`, `Filter` or `Prefix`, `Status`, then
/// the actions, rather than its Response Syntax's. A `Date` is written as every time in a
/// response is (13 §9.2).
pub fn lifecycle_configuration(rules: &[Rule]) -> Result<String, TimeOutOfRange> {
    let mut dates = Vec::new();
    for rule in rules {
        if let Some(Expiration::Date(day)) = rule.expiration {
            let millis = day.checked_mul(86_400_000).ok_or(TimeOutOfRange(day))?;
            dates.push(iso8601(millis).ok_or(TimeOutOfRange(day))?);
        }
    }
    let mut dates = dates.iter();
    Ok(Writer::document("LifecycleConfiguration", true, |w| {
        for rule in rules {
            w.element("Rule", |w| {
                w.text("ID", &rule.id);
                match &rule.scope {
                    Scope::Filter(filter) => w.element("Filter", |w| lifecycle_filter(w, filter)),
                    Scope::Prefix(prefix) => w.text("Prefix", prefix),
                }
                w.text("Status", if rule.enabled { "Enabled" } else { "Disabled" });
                if let Some(expiration) = rule.expiration {
                    w.element("Expiration", |w| match expiration {
                        Expiration::Date(_) => {
                            w.text("Date", dates.next().map_or("", String::as_str));
                        }
                        Expiration::Days(days) => w.text("Days", &days.to_string()),
                        Expiration::Marker(marker) => {
                            w.text(
                                "ExpiredObjectDeleteMarker",
                                if marker { "true" } else { "false" },
                            );
                        }
                    });
                }
                if let Some(noncurrent) = rule.noncurrent {
                    w.element("NoncurrentVersionExpiration", |w| {
                        if let Some(newer) = noncurrent.newer {
                            w.text("NewerNoncurrentVersions", &newer.to_string());
                        }
                        w.text("NoncurrentDays", &noncurrent.days.to_string());
                    });
                }
                if let Some(days) = rule.abort {
                    w.element("AbortIncompleteMultipartUpload", |w| {
                        w.text("DaysAfterInitiation", &days.to_string());
                    });
                }
            });
        }
    }))
}

/// GetPublicAccessBlock's `PublicAccessBlockConfiguration` (17 §7).
pub fn public_access_block(block: &crate::policy::PublicAccessBlock) -> String {
    let flag = |on: bool| if on { "true" } else { "false" };
    Writer::document("PublicAccessBlockConfiguration", true, |w| {
        w.text("BlockPublicAcls", flag(block.block_public_acls));
        w.text("IgnorePublicAcls", flag(block.ignore_public_acls));
        w.text("BlockPublicPolicy", flag(block.block_public_policy));
        w.text("RestrictPublicBuckets", flag(block.restrict_public_buckets));
    })
}

/// GetBucketPolicyStatus' `PolicyStatus`, `IsPublic` written `true` or `false`: AWS's sample
/// writes `TRUE`, which botocore, reading a boolean as `text == 'true'`, takes for false
/// (17 §2.2).
pub fn policy_status(public: bool) -> String {
    Writer::document("PolicyStatus", true, |w| {
        w.text("IsPublic", if public { "true" } else { "false" });
    })
}

/// GetBucketCors' `CORSConfiguration` (16 §1.2): each rule as set, its lists in the order
/// given, in its Response Syntax's order.
pub fn cors_configuration(rules: &[cors::Rule]) -> String {
    Writer::document("CORSConfiguration", true, |w| {
        for rule in rules {
            w.element("CORSRule", |w| {
                for header in &rule.headers {
                    w.text("AllowedHeader", header);
                }
                for method in &rule.methods {
                    w.text("AllowedMethod", method.name());
                }
                for origin in &rule.origins {
                    w.text("AllowedOrigin", origin);
                }
                for header in &rule.expose {
                    w.text("ExposeHeader", header);
                }
                if let Some(id) = &rule.id {
                    w.text("ID", id);
                }
                if let Some(age) = rule.max_age {
                    w.text("MaxAgeSeconds", &age.to_string());
                }
            });
        }
    })
}

/// A lifecycle rule's `Filter`, holding what it held when set.
fn lifecycle_filter(w: &mut Writer, filter: &Filter) {
    match filter {
        Filter::All => {}
        Filter::And(And {
            prefix,
            tags,
            larger,
            smaller,
        }) => w.element("And", |w| {
            if let Some(size) = larger {
                w.text("ObjectSizeGreaterThan", &size.to_string());
            }
            if let Some(size) = smaller {
                w.text("ObjectSizeLessThan", &size.to_string());
            }
            if let Some(prefix) = prefix {
                w.text("Prefix", prefix);
            }
            for tag in tags {
                lifecycle_tag(w, tag);
            }
        }),
        Filter::Larger(size) => w.text("ObjectSizeGreaterThan", &size.to_string()),
        Filter::Smaller(size) => w.text("ObjectSizeLessThan", &size.to_string()),
        Filter::Prefix(prefix) => w.text("Prefix", prefix),
        Filter::Tag(tag) => lifecycle_tag(w, tag),
    }
}

fn lifecycle_tag(w: &mut Writer, tag: &Tag) {
    w.element("Tag", |w| {
        w.text("Key", &tag.key);
        w.text("Value", &tag.value);
    });
}

/// A listing's `Contents`.
fn contents(w: &mut Writer, page: &Page<Entry>, modified: &[String], owners: bool, url: bool) {
    for ((key, entry), modified) in page.contents.iter().zip(modified) {
        w.element("Contents", |w| {
            checksum_type(w, entry.checksum);
            w.text("ETag", &quoted(&entry.etag));
            w.text("Key", &encoded(key, url));
            w.text("LastModified", modified);
            if owners {
                owner(w, "Owner", &entry.owner);
            }
            w.text("Size", &entry.size.to_string());
            w.text("StorageClass", STORAGE_CLASS);
        });
    }
}

/// `ChecksumAlgorithm` and `ChecksumType`, for an object or upload that has a checksum.
fn checksum_type(w: &mut Writer, checksum: Option<(Algorithm, ChecksumType)>) {
    if let Some((algorithm, kind)) = checksum {
        w.text("ChecksumAlgorithm", algorithm.name());
        w.text("ChecksumType", kind.name());
    }
}

/// An `Owner` or `Initiator`: the ID alone (13 §9.4).
fn owner(w: &mut Writer, name: &str, id: &str) {
    w.element(name, |w| w.text("ID", id));
}

fn common_prefixes(w: &mut Writer, prefixes: &[String], url: bool) {
    for prefix in prefixes {
        w.element("CommonPrefixes", |w| {
            w.text("Prefix", &encoded(prefix, url))
        });
    }
}

/// `Delimiter`, echoed only when there is one.
fn delimiter(w: &mut Writer, delimiter: Option<&str>, url: bool) {
    if let Some(delimiter) = delimited(delimiter) {
        w.text("Delimiter", &encoded(delimiter, url));
    }
}

/// A delimiter that groups keys: an empty one is none (05 §6.2).
fn delimited(delimiter: Option<&str>) -> Option<&str> {
    delimiter.filter(|d| !d.is_empty())
}

fn encoding_type(w: &mut Writer, url: bool) {
    if url {
        w.text("EncodingType", "url");
    }
}

/// A key, prefix, delimiter or key marker as `encoding-type` asks for it.
fn encoded(text: &str, url: bool) -> Cow<'_, str> {
    if url {
        Cow::Owned(url_encode(text))
    } else {
        Cow::Borrowed(text)
    }
}

fn quoted(etag: &str) -> String {
    format!("\"{etag}\"")
}

fn flag(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Each time as a document writes it, before any of the document is written.
fn times(seconds: impl Iterator<Item = i64>) -> Result<Vec<String>, TimeOutOfRange> {
    seconds.map(timestamp).collect()
}

/// Unix seconds as S3 writes a time: `2009-10-12T17:50:30.000Z`.
fn timestamp(seconds: i64) -> Result<String, TimeOutOfRange> {
    seconds
        .checked_mul(1000)
        .and_then(iso8601)
        .ok_or(TimeOutOfRange(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksum::checksum;
    use crate::list::{Paging, Resume, continuation_token};
    use crate::sigv4::percent_decode;
    use crate::xml::NAMESPACE;
    use proptest::prelude::*;

    /// A document's root namespace and its leaf elements as sorted (path, text) pairs: the
    /// form in which a document is compared with AWS's samples, whose element order differs
    /// from sample to sample (13 §9.1). roxmltree, a strict XML 1.0 reader, reads both.
    fn elements(doc: &str) -> (Option<String>, Vec<(String, String)>) {
        let tree = roxmltree::Document::parse(doc).unwrap();
        let root = tree.root_element();
        let mut leaves = Vec::new();
        walk(root, "", &mut leaves);
        leaves.sort();
        (root.tag_name().namespace().map(String::from), leaves)
    }

    fn walk(node: roxmltree::Node<'_, '_>, parent: &str, leaves: &mut Vec<(String, String)>) {
        let path = format!("{parent}/{}", node.tag_name().name());
        let children: Vec<_> = node.children().filter(|c| c.is_element()).collect();
        if children.is_empty() {
            let text: String = node
                .children()
                .filter(|c| c.is_text())
                .map(|c| c.text().unwrap())
                .collect();
            leaves.push((path, text.trim().to_string()));
        } else {
            for child in children {
                walk(child, &path, leaves);
            }
        }
    }

    /// Asserts that `doc` holds exactly what AWS's `sample` holds, and is in `namespace`.
    fn same(doc: &str, sample: &str, namespace: Option<&str>) {
        let (ns, ours) = elements(doc);
        assert_eq!(ns.as_deref(), namespace, "{doc}");
        assert_eq!(ours, elements(sample).1, "{doc}");
    }

    fn entry(modified: i64, etag: &str, size: u64, owner: &str) -> Entry {
        Entry {
            modified,
            etag: etag.into(),
            size,
            owner: owner.into(),
            checksum: None,
        }
    }

    fn request<'a>(
        prefix: &'a str,
        delimiter: Option<&'a str>,
        after: Option<&'a str>,
    ) -> Request<'a> {
        Request {
            prefix,
            delimiter,
            start: after.map(Start::After),
            max_keys: 1000,
            paging: Paging::Token,
        }
    }

    fn page(contents: Vec<(&str, Entry)>, common_prefixes: &[&str]) -> Page<Entry> {
        Page {
            contents: contents
                .into_iter()
                .map(|(k, e)| (k.to_string(), e))
                .collect(),
            common_prefixes: common_prefixes.iter().map(|p| p.to_string()).collect(),
            truncated: false,
            next: None,
        }
    }

    fn v2<'a>(bucket: &'a str, request: Request<'a>) -> ListV2<'a> {
        ListV2 {
            bucket,
            request,
            continuation_token: None,
            start_after: None,
            url: false,
            fetch_owner: false,
        }
    }

    const NS: Option<&str> = Some(NAMESPACE);

    /// ListObjectsV2's samples for a delimiter, and for a prefix and a delimiter.
    #[test]
    fn list_objects_v2_holds_what_aws_samples_hold() {
        let listed = page(
            vec![(
                "sample.jpg",
                entry(
                    1_298_685_380,
                    "bf1d737a4d46a19f3bced6905cc8b902",
                    142_863,
                    "o",
                ),
            )],
            &["photos/"],
        );
        let doc = list_objects_v2(&v2("example-bucket", request("", Some("/"), None)), &listed);
        same(
            &doc.unwrap(),
            r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix></Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>sample.jpg</Key>
    <LastModified>2011-02-26T01:56:20.000Z</LastModified>
    <ETag>"bf1d737a4d46a19f3bced6905cc8b902"</ETag>
    <Size>142863</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes>
    <Prefix>photos/</Prefix>
  </CommonPrefixes>
</ListBucketResult>"#,
            NS,
        );
        let listed = page(vec![], &["photos/2006/February/", "photos/2006/January/"]);
        let list = v2("example-bucket", request("photos/2006/", Some("/"), None));
        same(
            &list_objects_v2(&list, &listed).unwrap(),
            r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix>photos/2006/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>

  <CommonPrefixes>
    <Prefix>photos/2006/February/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>photos/2006/January/</Prefix>
  </CommonPrefixes>
</ListBucketResult>"#,
            NS,
        );
    }

    /// ListObjects' samples for a marker, and for a delimiter.
    #[test]
    fn list_objects_holds_what_aws_samples_hold() {
        let listed = page(
            vec![
                (
                    "Nelson",
                    entry(
                        1_136_116_800,
                        "828ef3fdfa96f00ad9f27c383fc9ac7f",
                        5,
                        "bcaf161ca5fb16fd081034f",
                    ),
                ),
                (
                    "Neo",
                    entry(
                        1_136_116_800,
                        "828ef3fdfa96f00ad9f27c383fc9ac7f",
                        4,
                        "bcaf1ffd86a5fb16fd081034f",
                    ),
                ),
            ],
            &[],
        );
        let list = ListV1 {
            bucket: "quotes",
            request: Request {
                max_keys: 40,
                ..request("N", None, Some("Ned"))
            },
            url: false,
        };
        same(
            &list_objects(&list, &listed).unwrap(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>quotes</Name>
  <Prefix>N</Prefix>
  <Marker>Ned</Marker>
  <MaxKeys>40</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>Nelson</Key>
    <LastModified>2006-01-01T12:00:00.000Z</LastModified>
    <ETag>"828ef3fdfa96f00ad9f27c383fc9ac7f"</ETag>
    <Size>5</Size>
    <StorageClass>STANDARD</StorageClass>
    <Owner>
      <ID>bcaf161ca5fb16fd081034f</ID>
     </Owner>
  </Contents>
  <Contents>
    <Key>Neo</Key>
    <LastModified>2006-01-01T12:00:00.000Z</LastModified>
    <ETag>"828ef3fdfa96f00ad9f27c383fc9ac7f"</ETag>
    <Size>4</Size>
    <StorageClass>STANDARD</StorageClass>
     <Owner>
      <ID>bcaf1ffd86a5fb16fd081034f</ID>
    </Owner>
 </Contents>
</ListBucketResult>"#,
            NS,
        );
        let listed = page(
            vec![(
                "sample.jpg",
                entry(
                    1_298_685_380,
                    "bf1d737a4d46a19f3bced6905cc8b902",
                    142_863,
                    "canonical-user-id",
                ),
            )],
            &["photos/"],
        );
        let list = ListV1 {
            bucket: "example-bucket",
            request: request("", Some("/"), None),
            url: false,
        };
        same(
            &list_objects(&list, &listed).unwrap(),
            r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix></Prefix>
  <Marker></Marker>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>sample.jpg</Key>
    <LastModified>2011-02-26T01:56:20.000Z</LastModified>
    <ETag>"bf1d737a4d46a19f3bced6905cc8b902"</ETag>
    <Size>142863</Size>
    <Owner>
      <ID>canonical-user-id</ID>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes>
    <Prefix>photos/</Prefix>
  </CommonPrefixes>
</ListBucketResult>"#,
            NS,
        );
    }

    /// ListObjectVersions' sample for a key marker.
    #[test]
    fn list_object_versions_holds_what_aws_samples_hold() {
        let owner = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";
        let version = |key: &str, id: &str, latest, marker, modified| Version {
            key: key.into(),
            version_id: id.into(),
            latest,
            marker,
            entry: entry(modified, "396fefef536d5ce46c7537ecf978a360", 217, owner),
        };
        let versions = [
            version(
                "key3",
                "I5VhmK6CDDdQ5Pwfe1gcHZWmHDpcv7gfmfc29UBxsKU.",
                true,
                false,
                1_260_317_944,
            ),
            version(
                "sourcekey",
                "qDhprLU80sAlCFLu2DWgXAEDgKzWarn-HS_JU0TvYqs.",
                true,
                true,
                1_260_463_091,
            ),
            version(
                "sourcekey",
                "wxxQ7ezLaL5JN2Sislq66Syxxo0k7uHTUpb9qiiMxNg.",
                false,
                false,
                1_260_463_064,
            ),
        ];
        let list = ListVersions {
            bucket: "mtp-versioning-fresh",
            prefix: "",
            delimiter: None,
            max_keys: 1000,
            key_marker: Some("key2"),
            version_id_marker: None,
            url: false,
            versions: &versions,
            common_prefixes: &[],
            next: None,
        };
        same(
            &list_object_versions(&list).unwrap(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>mtp-versioning-fresh</Name>
  <Prefix/>
  <KeyMarker>key2</KeyMarker>
  <VersionIdMarker/>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Version>
    <Key>key3</Key>
    <VersionId>I5VhmK6CDDdQ5Pwfe1gcHZWmHDpcv7gfmfc29UBxsKU.</VersionId>
    <IsLatest>true</IsLatest>
    <LastModified>2009-12-09T00:19:04.000Z</LastModified>
    <ETag>"396fefef536d5ce46c7537ecf978a360"</ETag>
    <Size>217</Size>
    <Owner>
      <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
  </Version>
  <DeleteMarker>
    <Key>sourcekey</Key>
    <VersionId>qDhprLU80sAlCFLu2DWgXAEDgKzWarn-HS_JU0TvYqs.</VersionId>
    <IsLatest>true</IsLatest>
    <LastModified>2009-12-10T16:38:11.000Z</LastModified>
    <Owner>
      <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
    </Owner>
  </DeleteMarker>
  <Version>
    <Key>sourcekey</Key>
    <VersionId>wxxQ7ezLaL5JN2Sislq66Syxxo0k7uHTUpb9qiiMxNg.</VersionId>
    <IsLatest>false</IsLatest>
    <LastModified>2009-12-10T16:37:44.000Z</LastModified>
    <ETag>"396fefef536d5ce46c7537ecf978a360"</ETag>
    <Size>217</Size>
    <Owner>
      <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
  </Version>
</ListVersionsResult>"#,
            NS,
        );
    }

    /// ListMultipartUploads' samples for a delimiter, and for a delimiter and a prefix.
    #[test]
    fn list_multipart_uploads_holds_what_aws_samples_hold() {
        let owner = "314133b66967d86f031c7249d1d9a80249109428335cd0ef1cdc487b4566cb1b";
        let id = "Agw4MJT6ZPAVxpY0SAuGN7q4uWJJM22ZYg1N99trdp4tpO88.PT6.MhO0w2E17eutfAvQfQWoajgE_W2gpcxQw--";
        let uploads = [Upload {
            key: "sample.jpg".into(),
            upload_id: id.into(),
            initiated: 1_290_799_457,
            initiator: owner.into(),
            owner: owner.into(),
            checksum: None,
        }];
        let prefixes = ["photos/".to_string(), "videos/".to_string()];
        let list = ListUploads {
            bucket: "example-bucket",
            prefix: "",
            delimiter: Some("/"),
            max_uploads: 1000,
            key_marker: None,
            upload_id_marker: None,
            url: false,
            uploads: &uploads,
            common_prefixes: &prefixes,
            truncated: false,
            next: Next {
                key: "sample.jpg",
                id,
            },
        };
        same(
            &list_multipart_uploads(&list).unwrap(),
            r#"<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>example-bucket</Bucket>
  <KeyMarker/>
  <UploadIdMarker/>
  <NextKeyMarker>sample.jpg</NextKeyMarker>
  <NextUploadIdMarker>Agw4MJT6ZPAVxpY0SAuGN7q4uWJJM22ZYg1N99trdp4tpO88.PT6.MhO0w2E17eutfAvQfQWoajgE_W2gpcxQw--</NextUploadIdMarker>
  <Delimiter>/</Delimiter>
  <Prefix/>
  <MaxUploads>1000</MaxUploads>
  <IsTruncated>false</IsTruncated>
  <Upload>
    <Key>sample.jpg</Key>
    <UploadId>Agw4MJT6ZPAVxpY0SAuGN7q4uWJJM22ZYg1N99trdp4tpO88.PT6.MhO0w2E17eutfAvQfQWoajgE_W2gpcxQw--</UploadId>
    <Initiator>
      <ID>314133b66967d86f031c7249d1d9a80249109428335cd0ef1cdc487b4566cb1b</ID>

    </Initiator>
    <Owner>
      <ID>314133b66967d86f031c7249d1d9a80249109428335cd0ef1cdc487b4566cb1b</ID>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
    <Initiated>2010-11-26T19:24:17.000Z</Initiated>
  </Upload>
  <CommonPrefixes>
    <Prefix>photos/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>videos/</Prefix>
  </CommonPrefixes>
  </ListMultipartUploadsResult>"#,
            NS,
        );
        let prefixes = [
            "photos/2006/February/".to_string(),
            "photos/2006/January/".to_string(),
            "photos/2006/March/".to_string(),
        ];
        let list = ListUploads {
            prefix: "photos/2006/",
            uploads: &[],
            common_prefixes: &prefixes,
            next: Next { key: "", id: "" },
            ..list
        };
        same(
            &list_multipart_uploads(&list).unwrap(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>example-bucket</Bucket>
  <KeyMarker/>
  <UploadIdMarker/>
  <NextKeyMarker/>
  <NextUploadIdMarker/>
  <Delimiter>/</Delimiter>
  <Prefix>photos/2006/</Prefix>
  <MaxUploads>1000</MaxUploads>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes>
    <Prefix>photos/2006/February/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>photos/2006/January/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>photos/2006/March/</Prefix>
  </CommonPrefixes>
</ListMultipartUploadsResult>"#,
            NS,
        );
    }

    #[test]
    fn list_parts_holds_what_aws_samples_hold() {
        let part = |number, modified, etag: &str| Part {
            number,
            modified,
            etag: etag.into(),
            size: 10_485_760,
            checksum: None,
        };
        let parts = [
            part(2, 1_289_422_114, "7778aef83f66abc1fa1e8477f296d394"),
            part(3, 1_289_422_113, "aaaa18db4cc2f85cedef654fccc4a4x8"),
        ];
        let list = ListParts {
            bucket: "example-bucket",
            key: "example-object",
            upload_id: "XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA",
            part_number_marker: 1,
            next: Some(3),
            max_parts: 2,
            parts: &parts,
            initiator: "arn:aws:iam::111122223333:user/some-user-11116a31-17b5-4fb7-9df5-b288870f11xx",
            owner: "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
            checksum: None,
        };
        same(
            &list_parts(&list).unwrap(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>example-bucket</Bucket>
  <Key>example-object</Key>
  <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
  <Initiator>
      <ID>arn:aws:iam::111122223333:user/some-user-11116a31-17b5-4fb7-9df5-b288870f11xx</ID>

  </Initiator>
  <Owner>
    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>

  </Owner>
  <StorageClass>STANDARD</StorageClass>
  <PartNumberMarker>1</PartNumberMarker>
  <NextPartNumberMarker>3</NextPartNumberMarker>
  <MaxParts>2</MaxParts>
  <IsTruncated>true</IsTruncated>
  <Part>
    <PartNumber>2</PartNumber>
    <LastModified>2010-11-10T20:48:34.000Z</LastModified>
    <ETag>"7778aef83f66abc1fa1e8477f296d394"</ETag>
    <Size>10485760</Size>
  </Part>
  <Part>
    <PartNumber>3</PartNumber>
    <LastModified>2010-11-10T20:48:33.000Z</LastModified>
    <ETag>"aaaa18db4cc2f85cedef654fccc4a4x8"</ETag>
    <Size>10485760</Size>
  </Part>
</ListPartsResult>"#,
            NS,
        );
    }

    /// ListBuckets' samples without and with pagination. They write times as
    /// `2019-12-11T23:32:47+00:00` where every other sample writes `.000Z` (13 §9.2); the two
    /// name the same instant, so the samples' times are rewritten before comparing.
    #[test]
    fn list_buckets_holds_what_aws_samples_hold() {
        let bucket = |name: &str, created, region: &str| Bucket {
            name: name.into(),
            created,
            region: region.into(),
        };
        let buckets = [
            bucket("amzn-s3-demo-bucket", 1_576_107_167, "us-east-1"),
            bucket("amzn-s3-demo-bucket1", 1_573_428_733, "us-east-2"),
        ];
        let list = ListBuckets {
            owner: "AIDACKCEVSQ6C2EXAMPLE",
            buckets: &buckets,
            regions: false,
            continuation_token: None,
            prefix: None,
        };
        let sample = r#"<ListAllMyBucketsResult>
   <Buckets>
      <Bucket>
         <CreationDate>2019-12-11T23:32:47+00:00</CreationDate>
         <Name>amzn-s3-demo-bucket</Name>
      </Bucket>
      <Bucket>
         <CreationDate>2019-11-10T23:32:13+00:00</CreationDate>
         <Name>amzn-s3-demo-bucket1</Name>
      </Bucket>
   </Buckets>
   <Owner>
      <ID>AIDACKCEVSQ6C2EXAMPLE</ID>
   </Owner>  
</ListAllMyBucketsResult>"#;
        same(
            &list_buckets(&list).unwrap(),
            &sample.replace("+00:00", ".000Z"),
            NS,
        );
        let buckets = [
            bucket("amzn-s3-demo-bucket", 1_731_627_167, "us-east-1"),
            bucket("amzn-s3-demo-bucket1", 1_731_627_133, "us-east-2"),
        ];
        let list = ListBuckets {
            buckets: &buckets,
            regions: true,
            continuation_token: Some(
                "eyJNYXJrZXIiOiBudWxsLCAiYm90b190cnVuY2F0ZV9hbW91bnQiOiAxfQ==",
            ),
            ..list
        };
        let sample = r#"<ListAllMyBucketsResult>
   <Buckets>
      <Bucket>
         <CreationDate>2024-11-14T23:32:47+00:00</CreationDate>
         <Name>amzn-s3-demo-bucket</Name>
         <BucketRegion>us-east-1</BucketRegion>
      </Bucket>
      <Bucket>
         <CreationDate>2024-11-14T23:32:13+00:00</CreationDate>
         <Name>amzn-s3-demo-bucket1</Name>
         <BucketRegion>us-east-2</BucketRegion>
      </Bucket>
   </Buckets>
   <Owner>
      <ID>AIDACKCEVSQ6C2EXAMPLE</ID>
   </Owner>  
   <ContinuationToken>eyJNYXJrZXIiOiBudWxsLCAiYm90b190cnVuY2F0ZV9hbW91bnQiOiAxfQ==</ContinuationToken>      
</ListAllMyBucketsResult>"#;
        same(
            &list_buckets(&list).unwrap(),
            &sample.replace("+00:00", ".000Z"),
            NS,
        );
    }

    #[test]
    fn multipart_and_copy_results_hold_what_aws_samples_hold() {
        same(
            &initiate_multipart_upload(
                "amzn-s3-demo-bucket",
                "example-object",
                "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA",
            ),
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Bucket>amzn-s3-demo-bucket</Bucket>
              <Key>example-object</Key>
              <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
            </InitiateMultipartUploadResult>"#,
            NS,
        );
        let completed = Completed {
            location: "http://amzn-s3-demo-bucket.s3.<Region>.amazonaws.com/Example-Object",
            bucket: "amzn-s3-demo-bucket",
            key: "Example-Object",
            etag: "3858f62230ac3c915f300c664312c11f-9",
            checksum: None,
        };
        same(
            &complete_multipart_upload(&completed),
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
             <Location>http://amzn-s3-demo-bucket.s3.&lt;Region&gt;.amazonaws.com/Example-Object</Location>
             <Bucket>amzn-s3-demo-bucket</Bucket>
             <Key>Example-Object</Key>
             <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
            </CompleteMultipartUploadResult>"#,
            NS,
        );
        same(
            &copy_object("9b2cf535f27731c974343645a3985328", 1_255_369_830, None).unwrap(),
            r#"<CopyObjectResult>
                  <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                  <ETag>"9b2cf535f27731c974343645a3985328"</ETag>
               </CopyObjectResult>"#,
            NS,
        );
        same(
            &copy_part("9b2cf535f27731c974343645a3985328", 1_302_554_096, None).unwrap(),
            r#"<CopyPartResult>
   <LastModified>2011-04-11T20:34:56.000Z</LastModified>
   <ETag>"9b2cf535f27731c974343645a3985328"</ETag>
</CopyPartResult>"#,
            NS,
        );
    }

    /// DeleteObjects' samples: a mixed result, a simple delete that made a delete marker, a
    /// version deleted, and a delete marker deleted.
    #[test]
    fn delete_results_hold_what_aws_samples_hold() {
        let deleted = |key, version_id, delete_marker| Deletion::Deleted {
            key,
            version_id,
            delete_marker,
        };
        let results = [
            deleted("sample1.txt", None, None),
            Deletion::Failed {
                key: "sample2.txt",
                version_id: None,
                code: "AccessDenied",
                message: "Access Denied",
            },
        ];
        same(
            &delete_result(&results, false),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Deleted>
  <Key>sample1.txt</Key>
</Deleted>
<Error>
<Key>sample2.txt</Key>
<Code>AccessDenied</Code>
<Message>Access Denied</Message>
</Error>
</DeleteResult>"#,
            NS,
        );
        let marker = "NeQt5xeFTfgPJD8B4CGWnkSLtluMr11s";
        same(
            &delete_result(&[deleted("SampleDocument.txt", None, Some(marker))], false),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted>
  <Key>SampleDocument.txt</Key>
    <DeleteMarker>true</DeleteMarker> 
    <DeleteMarkerVersionId>NeQt5xeFTfgPJD8B4CGWnkSLtluMr11s</DeleteMarkerVersionId>
  </Deleted>
</DeleteResult>"#,
            NS,
        );
        let version = "OYcLXagmS.WaD..oyH4KRguB95_YhLs7";
        same(
            &delete_result(&[deleted("SampleDocument.txt", Some(version), None)], false),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted>
    <Key>SampleDocument.txt</Key>
    <VersionId>OYcLXagmS.WaD..oyH4KRguB95_YhLs7</VersionId>
  </Deleted>
</DeleteResult>"#,
            NS,
        );
        same(
            &delete_result(
                &[deleted("SampleDocument.txt", Some(marker), Some(marker))],
                false,
            ),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Deleted>
<Key>SampleDocument.txt</Key>
<VersionId>NeQt5xeFTfgPJD8B4CGWnkSLtluMr11s</VersionId>
<DeleteMarker>true</DeleteMarker>  
<DeleteMarkerVersionId>NeQt5xeFTfgPJD8B4CGWnkSLtluMr11s</DeleteMarkerVersionId> 
</Deleted>
</DeleteResult>"#,
            NS,
        );
        // Quiet mode reports only the failure.
        same(
            &delete_result(&results, true),
            r#"<DeleteResult>
<Error><Key>sample2.txt</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>
</DeleteResult>"#,
            NS,
        );
    }

    /// CompleteMultipartUpload's error sample, and DeleteObjects' for malformed XML.
    #[test]
    fn errors_hold_what_aws_samples_hold() {
        let failure = Failure {
            code: "InternalError",
            message: "We encountered an internal error. Please try again.",
            resource: None,
            details: &[],
            request_id: "656c76696e6727732072657175657374",
            host_id: "Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==",
        };
        same(
            &error(&failure),
            r#"<?xml version="1.0" encoding="UTF-8"?>

         <Error>
          <Code>InternalError</Code>
          <Message>We encountered an internal error. Please try again.</Message>
          <RequestId>656c76696e6727732072657175657374</RequestId>
          <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
         </Error>"#,
            None,
        );
        let doc = error(&Failure {
            code: "NoSuchKey",
            message: "The resource you requested does not exist",
            resource: Some("/mybucket/myfoto.jpg"),
            ..failure
        });
        let (_, leaves) = elements(&doc);
        assert!(leaves.contains(&("/Error/Resource".into(), "/mybucket/myfoto.jpg".into())));
    }

    #[test]
    fn bucket_settings_hold_what_aws_samples_hold() {
        same(
            &location_constraint("us-west-2"),
            r#"<?xml version="1.0" encoding="UTF-8"?>
         <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-west-2</LocationConstraint>"#,
            NS,
        );
        // botocore reads the root's text, which is none for S3's null (13 §9.5).
        let doc = location_constraint("");
        let tree = roxmltree::Document::parse(&doc).unwrap();
        assert_eq!(tree.root_element().text(), None);
        same(
            &versioning_configuration(Some(Versioning::Enabled)),
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Status>Enabled</Status>
     </VersioningConfiguration>"#,
            NS,
        );
        same(
            &versioning_configuration(Some(Versioning::Suspended)),
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Status>Suspended</Status>
     </VersioningConfiguration>"#,
            NS,
        );
        same(
            &versioning_configuration(None),
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#,
            NS,
        );
    }

    /// GetObjectTagging's and GetBucketTagging's samples (13 §9.6), and an empty set, which
    /// an object without tags answers.
    #[test]
    fn tagging_holds_what_aws_samples_hold() {
        let tag = |key: &str, value: &str| Tag {
            key: key.into(),
            value: value.into(),
        };
        same(
            &tagging(&[tag("tag1", "val1"), tag("tag2", "val2")]),
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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
            </Tagging> "#,
            NS,
        );
        same(
            &tagging(&[tag("Project", "Project One"), tag("User", "jsmith")]),
            "<Tagging>
           <TagSet>
              <Tag>
                <Key>Project</Key>
               <Value>Project One</Value>
              </Tag>
              <Tag>
                <Key>User</Key>
                <Value>jsmith</Value>
              </Tag>
           </TagSet>
         </Tagging>",
            NS,
        );
        let empty = tagging(&[]);
        let tree = roxmltree::Document::parse(&empty).unwrap();
        let set = tree.root_element().first_element_child().unwrap();
        assert_eq!(set.tag_name().name(), "TagSet");
        assert_eq!(set.children().count(), 0);
        // What the writer writes, the reader reads back.
        let tags = vec![tag("a&b", "<v>"), tag("k", "")];
        assert_eq!(
            crate::body::tagging(tagging(&tags).as_bytes(), crate::tagging::Tagged::Object)
                .map_err(|e| e.code()),
            Err(("InvalidTag", 400)),
            "escaped, so the reader reaches the tag rules, which refuse & and <"
        );
        let tags = vec![tag("Cost Center", "a+b"), tag("k", "")];
        assert_eq!(
            crate::body::tagging(tagging(&tags).as_bytes(), crate::tagging::Tagged::Object),
            Ok(tags)
        );
    }

    /// GetBucketOwnershipControls' sample for bucket owner enforced (13 §6.8).
    #[test]
    fn ownership_controls_holds_what_aws_samples_hold() {
        same(
            &ownership_controls(),
            r#"<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Rule>
              <ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>
            </Rule>
          </OwnershipControls>"#,
            NS,
        );
        assert_eq!(
            crate::body::ownership_controls(ownership_controls().as_bytes()),
            Ok(Ownership::BucketOwnerEnforced)
        );
    }

    /// GetBucketAcl's sample: the owner and one grant of full control to it, the grantee
    /// typed by `xsi:type` (13 §9.6).
    #[test]
    fn access_control_policy_holds_what_aws_samples_hold() {
        let owner = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";
        let doc = access_control_policy(owner);
        same(
            &doc,
            r#"<AccessControlPolicy>
  <Owner>
    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			xsi:type="CanonicalUser">
        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy> "#,
            NS,
        );
        let tree = roxmltree::Document::parse(&doc).unwrap();
        let grantee = tree
            .descendants()
            .find(|n| n.has_tag_name("Grantee"))
            .unwrap();
        assert_eq!(
            grantee.attribute(("http://www.w3.org/2001/XMLSchema-instance", "type")),
            Some("CanonicalUser")
        );
        // The policy mantle answers is the one it takes back.
        assert_eq!(crate::acl::put_acl(None, doc.as_bytes(), owner), Ok(()));
    }

    /// Every element ListObjectsV2 can write, in the Response Syntax's order (13 §9.1).
    #[test]
    fn list_objects_v2_writes_the_response_syntax_order() {
        let mut listed = page(
            vec![(
                "a/b",
                Entry {
                    checksum: Some((Algorithm::Crc64Nvme, ChecksumType::FullObject)),
                    ..entry(0, "e", 3, "o")
                },
            )],
            &["a/c/"],
        );
        listed.truncated = true;
        listed.next = Some(Resume::After("a/c/".into()));
        let list = ListV2 {
            continuation_token: Some("t"),
            start_after: Some("a/"),
            url: true,
            fetch_owner: true,
            ..v2(
                "b",
                Request {
                    max_keys: 2,
                    ..request("a/", Some("/"), None)
                },
            )
        };
        let token = continuation_token(&Resume::After("a/c/".into()));
        assert_eq!(
            list_objects_v2(&list, &listed).unwrap(),
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <ListBucketResult xmlns=\"{NAMESPACE}\"><IsTruncated>true</IsTruncated>\
                 <Contents><ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm>\
                 <ChecksumType>FULL_OBJECT</ChecksumType><ETag>\"e\"</ETag><Key>a/b</Key>\
                 <LastModified>1970-01-01T00:00:00.000Z</LastModified><Owner><ID>o</ID></Owner>\
                 <Size>3</Size><StorageClass>STANDARD</StorageClass></Contents>\
                 <Name>b</Name><Prefix>a/</Prefix><Delimiter>/</Delimiter><MaxKeys>2</MaxKeys>\
                 <CommonPrefixes><Prefix>a/c/</Prefix></CommonPrefixes>\
                 <EncodingType>url</EncodingType><KeyCount>2</KeyCount>\
                 <ContinuationToken>t</ContinuationToken>\
                 <NextContinuationToken>{token}</NextContinuationToken>\
                 <StartAfter>a/</StartAfter></ListBucketResult>"
            )
        );
    }

    /// ceph s3-tests' expectations of the listing documents (05 §6.2–§6.3).
    #[test]
    fn listings_echo_what_s3_tests_expect() {
        let leaves = |doc: Result<String, TimeOutOfRange>| elements(&doc.unwrap()).1;
        let has = |leaves: &[(String, String)], path: &str| leaves.iter().any(|(p, _)| p == path);
        let text = |leaves: &[(String, String)], path: &str| {
            leaves
                .iter()
                .find(|(p, _)| p == path)
                .map(|(_, t)| t.clone())
        };
        let listed = page(vec![("k", entry(0, "e", 1, "o"))], &["d/"]);

        // V1 writes Marker when none was sent.
        let v1 = |request| ListV1 {
            bucket: "b",
            request,
            url: false,
        };
        let l = leaves(list_objects(&v1(request("", None, None)), &listed));
        assert_eq!(text(&l, "/ListBucketResult/Marker"), Some(String::new()));
        // NextMarker, the last item listed, only on a truncated page with a delimiter.
        let mut truncated = listed.clone();
        truncated.truncated = true;
        truncated.next = Some(Resume::After("d/".into()));
        let l = leaves(list_objects(&v1(request("", Some("/"), None)), &truncated));
        assert_eq!(text(&l, "/ListBucketResult/NextMarker"), Some("d/".into()));
        let l = leaves(list_objects(&v1(request("", None, None)), &truncated));
        assert!(!has(&l, "/ListBucketResult/NextMarker"));
        let l = leaves(list_objects(&v1(request("", Some("/"), None)), &listed));
        assert!(!has(&l, "/ListBucketResult/NextMarker"));
        // V1 lists owners unasked; V2 only with fetch-owner.
        assert!(has(&l, "/ListBucketResult/Contents/Owner/ID"));
        let l = leaves(list_objects_v2(&v2("b", request("", None, None)), &listed));
        assert!(!has(&l, "/ListBucketResult/Contents/Owner/ID"));
        // An empty delimiter is not echoed.
        let l = leaves(list_objects_v2(
            &v2("b", request("", Some(""), None)),
            &listed,
        ));
        assert!(!has(&l, "/ListBucketResult/Delimiter"));
        // An empty continuation token is echoed as sent.
        let list = ListV2 {
            continuation_token: Some(""),
            ..v2("b", request("", None, None))
        };
        let l = leaves(list_objects_v2(&list, &listed));
        assert_eq!(
            text(&l, "/ListBucketResult/ContinuationToken"),
            Some(String::new())
        );
        // KeyCount counts common prefixes with keys; MaxKeys echoes the default.
        assert_eq!(text(&l, "/ListBucketResult/KeyCount"), Some("2".into()));
        assert_eq!(text(&l, "/ListBucketResult/MaxKeys"), Some("1000".into()));
    }

    /// `encoding-type=url` encodes each element AWS names for it, and common prefixes, as
    /// s3-tests expects (05 §6.2–§6.4, §4.7).
    #[test]
    fn url_encoding_reaches_every_element_it_names() {
        let odd = "a b+(1)/";
        let text = |doc: &str, path: &str| {
            elements(doc)
                .1
                .into_iter()
                .filter(|(p, _)| p == path)
                .map(|(_, t)| t)
                .collect::<Vec<_>>()
        };
        let mut listed = page(vec![(odd, entry(0, "e", 1, "o"))], &[odd]);
        listed.truncated = true;
        listed.next = Some(Resume::After(odd.into()));
        let list = ListV2 {
            start_after: Some(odd),
            url: true,
            ..v2("b", request(odd, Some(odd), None))
        };
        let doc = list_objects_v2(&list, &listed).unwrap();
        for path in [
            "/ListBucketResult/Contents/Key",
            "/ListBucketResult/Prefix",
            "/ListBucketResult/Delimiter",
            "/ListBucketResult/CommonPrefixes/Prefix",
            "/ListBucketResult/StartAfter",
        ] {
            assert_eq!(text(&doc, path), ["a%20b%2B%281%29/"], "{path}");
        }
        let list = ListV1 {
            bucket: "b",
            request: request(odd, Some(odd), Some(odd)),
            url: true,
        };
        let doc = list_objects(&list, &listed).unwrap();
        for path in ["/ListBucketResult/Marker", "/ListBucketResult/NextMarker"] {
            assert_eq!(text(&doc, path), ["a%20b%2B%281%29/"], "{path}");
        }
        let versions = [Version {
            key: odd.into(),
            version_id: "v".into(),
            latest: true,
            marker: false,
            entry: entry(0, "e", 1, "o"),
        }];
        let list = ListVersions {
            bucket: "b",
            prefix: odd,
            delimiter: Some(odd),
            max_keys: 1000,
            key_marker: Some(odd),
            version_id_marker: None,
            url: true,
            versions: &versions,
            common_prefixes: &[],
            next: Some(Next { key: odd, id: "v" }),
        };
        let doc = list_object_versions(&list).unwrap();
        for path in [
            "/ListVersionsResult/KeyMarker",
            "/ListVersionsResult/NextKeyMarker",
            "/ListVersionsResult/Prefix",
            "/ListVersionsResult/Delimiter",
            "/ListVersionsResult/Version/Key",
        ] {
            assert_eq!(text(&doc, path), ["a%20b%2B%281%29/"], "{path}");
        }
        let uploads = [Upload {
            key: odd.into(),
            upload_id: "u".into(),
            initiated: 0,
            initiator: "o".into(),
            owner: "o".into(),
            checksum: None,
        }];
        let list = ListUploads {
            bucket: "b",
            prefix: odd,
            delimiter: Some(odd),
            max_uploads: 1000,
            key_marker: Some(odd),
            upload_id_marker: None,
            url: true,
            uploads: &uploads,
            common_prefixes: &[],
            truncated: false,
            next: Next { key: odd, id: "u" },
        };
        let doc = list_multipart_uploads(&list).unwrap();
        for path in [
            "/ListMultipartUploadsResult/KeyMarker",
            "/ListMultipartUploadsResult/NextKeyMarker",
            "/ListMultipartUploadsResult/Prefix",
            "/ListMultipartUploadsResult/Delimiter",
            "/ListMultipartUploadsResult/Upload/Key",
        ] {
            assert_eq!(text(&doc, path), ["a%20b%2B%281%29/"], "{path}");
        }
    }

    /// Checksums: an object's algorithm and type in listings, its value and type in results,
    /// and a part's value (05 §3.3–§3.5).
    #[test]
    fn checksums_are_written_where_aws_places_them() {
        let value = checksum(Algorithm::Crc32, b"hello").unwrap();
        let summary = ObjectChecksum {
            algorithm: Algorithm::Sha256,
            kind: ChecksumType::Composite,
            value: "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
        };
        let completed = Completed {
            location: "l",
            bucket: "b",
            key: "k",
            etag: "b2add96cc9702bbf4efb0ccdfc6b7747-3",
            checksum: Some(summary),
        };
        assert!(complete_multipart_upload(&completed).ends_with(
            "<ETag>\"b2add96cc9702bbf4efb0ccdfc6b7747-3\"</ETag>\
             <ChecksumSHA256>uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3</ChecksumSHA256>\
             <ChecksumType>COMPOSITE</ChecksumType></CompleteMultipartUploadResult>"
        ));
        let full = ObjectChecksum {
            algorithm: Algorithm::Crc64Nvme,
            kind: ChecksumType::FullObject,
            value: "i+6LR0y3eFo=",
        };
        assert!(copy_object("e", 0, Some(full)).unwrap().ends_with(
            "<ChecksumType>FULL_OBJECT</ChecksumType>\
             <ChecksumCRC64NVME>i+6LR0y3eFo=</ChecksumCRC64NVME></CopyObjectResult>"
        ));
        let encoded = value.to_base64();
        assert!(copy_part("e", 0, Some(&value)).unwrap().ends_with(&format!(
            "<ChecksumCRC32>{encoded}</ChecksumCRC32></CopyPartResult>"
        )));
        let parts = [Part {
            number: 1,
            modified: 0,
            etag: "e".into(),
            size: 5,
            checksum: Some(value.clone()),
        }];
        let list = ListParts {
            bucket: "b",
            key: "k",
            upload_id: "u",
            part_number_marker: 0,
            next: None,
            max_parts: 1000,
            parts: &parts,
            initiator: "o",
            owner: "o",
            checksum: Some((Algorithm::Crc32, ChecksumType::FullObject)),
        };
        let doc = list_parts(&list).unwrap();
        assert!(doc.contains(&format!(
            "<Part><ChecksumCRC32>{encoded}</ChecksumCRC32><ETag>"
        )));
        assert!(doc.ends_with(
            "<StorageClass>STANDARD</StorageClass><ChecksumAlgorithm>CRC32</ChecksumAlgorithm>\
             <ChecksumType>FULL_OBJECT</ChecksumType></ListPartsResult>"
        ));
        assert!(!doc.contains("NextPartNumberMarker"));
    }

    #[test]
    fn times_are_whole_seconds_and_a_time_out_of_range_is_refused() {
        assert_eq!(timestamp(0), Ok("1970-01-01T00:00:00.000Z".into()));
        assert_eq!(
            timestamp(1_255_369_830),
            Ok("2009-10-12T17:50:30.000Z".into())
        );
        // 10000-01-01T00:00:00Z, and the largest i64, which overflows as milliseconds.
        assert_eq!(
            timestamp(253_402_300_800),
            Err(TimeOutOfRange(253_402_300_800))
        );
        assert_eq!(timestamp(i64::MAX), Err(TimeOutOfRange(i64::MAX)));
        let listed = page(vec![("k", entry(i64::MIN, "e", 1, "o"))], &[]);
        assert_eq!(
            list_objects_v2(&v2("b", request("", None, None)), &listed),
            Err(TimeOutOfRange(i64::MIN))
        );
        assert_eq!(TimeOutOfRange(0).code(), ("InternalError", 500));
    }

    fn key_strategy() -> impl Strategy<Value = String> {
        // Every character a key may hold, weighted toward those XML and URLs treat specially.
        let special = prop::sample::select(vec![
            '&',
            '<',
            '>',
            '"',
            '\'',
            '\r',
            '\n',
            '\t',
            '\u{1}',
            '\u{1F}',
            '\u{7F}',
            '\u{FFFE}',
            ']',
            '%',
            '+',
            ' ',
            '/',
            'é',
            '中',
            '\u{1F600}',
        ]);
        prop::collection::vec(
            prop_oneof![
                special,
                any::<char>().prop_filter("not NUL", |c| *c != '\0')
            ],
            1..24,
        )
        .prop_map(|chars| chars.into_iter().collect())
    }

    /// AWS's GetBucketLifecycleConfiguration sample, without the two transitions mantle does
    /// not hold: the rule-level `Prefix` comes back as a `Prefix`, not a `Filter` (13 §6.9).
    #[test]
    fn lifecycle_configuration_holds_what_aws_samples_hold() {
        let rule = Rule {
            id: "Archive and then delete rule".into(),
            scope: Scope::Prefix("projectdocs/".into()),
            enabled: true,
            expiration: Some(Expiration::Days(3650)),
            noncurrent: None,
            abort: None,
        };
        same(
            &lifecycle_configuration(&[rule]).unwrap(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               <Rule>
                  <ID>Archive and then delete rule</ID>
                  <Prefix>projectdocs/</Prefix>
                  <Status>Enabled</Status>
                  <Expiration>
                     <Days>3650</Days>
                  </Expiration>
               </Rule>
            </LifecycleConfiguration>"#,
            Some(NAMESPACE),
        );
        // An empty filter comes back empty, as botocore reads `Filter: {}`, and a date as
        // every time is written.
        let dated = Rule {
            id: "r".into(),
            scope: Scope::Filter(Filter::All),
            enabled: false,
            expiration: Some(Expiration::Date(17_436)),
            noncurrent: None,
            abort: Some(7),
        };
        let doc = lifecycle_configuration(&[dated]).unwrap();
        assert!(doc.contains("<Filter></Filter>"), "{doc}");
        assert!(
            doc.contains("<Date>2017-09-27T00:00:00.000Z</Date>"),
            "{doc}"
        );
        assert_eq!(
            lifecycle_configuration(&[Rule {
                expiration: Some(Expiration::Date(i64::MAX / 86_400_000 + 1)),
                ..lifecycle_rule()
            }]),
            Err(TimeOutOfRange(i64::MAX / 86_400_000 + 1))
        );
    }

    fn lifecycle_rule() -> Rule {
        Rule {
            id: "r".into(),
            scope: Scope::Prefix(String::new()),
            enabled: true,
            expiration: Some(Expiration::Days(1)),
            noncurrent: None,
            abort: None,
        }
    }

    /// Text a request document can carry: any character but NUL, U+FFFE and U+FFFF, which
    /// XML cannot hold even as a reference. A configuration's text comes from one.
    fn document_text() -> impl Strategy<Value = String> {
        key_strategy().prop_map(|text| {
            text.chars()
                .filter(|c| !matches!(c, '\u{FFFE}' | '\u{FFFF}'))
                .collect()
        })
    }

    /// Tags whose keys differ.
    fn filter_tags(most: usize) -> impl Strategy<Value = Vec<Tag>> {
        prop::collection::btree_map("[a-zA-Z0-9 _.:/=+@-]{1,6}", document_text(), 0..=most)
            .prop_map(|tags| {
                tags.into_iter()
                    .map(|(key, value)| Tag { key, value })
                    .collect()
            })
    }

    /// Sizes S3 admits in a filter: from `least` to 1000 × 2^40 bytes (13 §6.9).
    fn size_strategy(least: u64) -> impl Strategy<Value = u64> {
        least..=crate::lifecycle::MAX_FILTER_SIZE
    }

    fn filter_strategy() -> impl Strategy<Value = Filter> {
        let and = (
            prop::option::of(document_text()),
            filter_tags(3),
            prop::option::of(0..crate::lifecycle::MAX_FILTER_SIZE / 2),
            prop::option::of(size_strategy(crate::lifecycle::MAX_FILTER_SIZE / 2)),
        )
            .prop_filter_map(
                "an And holds two or more predicates",
                |(prefix, tags, larger, smaller)| {
                    let count = tags.len()
                        + usize::from(prefix.is_some())
                        + usize::from(larger.is_some())
                        + usize::from(smaller.is_some());
                    if count >= 2 {
                        Some(Filter::And(And {
                            prefix,
                            tags,
                            larger,
                            smaller,
                        }))
                    } else {
                        None
                    }
                },
            );
        prop_oneof![
            Just(Filter::All),
            document_text().prop_map(Filter::Prefix),
            filter_tags(1)
                .prop_filter_map("one tag", |mut tags| tags.pop())
                .prop_map(Filter::Tag),
            size_strategy(0).prop_map(Filter::Larger),
            size_strategy(1).prop_map(Filter::Smaller),
            and,
        ]
    }

    /// The actions of a valid rule over `scope`: S3 admits no marker or abort beside a tag or
    /// size predicate, and `NewerNoncurrentVersions` only in Lifecycle V2.
    fn rule_strategy(scope: impl Strategy<Value = Scope>) -> impl Strategy<Value = Rule> {
        let expiration = prop::option::of(prop_oneof![
            (1..=u32::try_from(i32::MAX).unwrap()).prop_map(Expiration::Days),
            (0..=2_932_896i64).prop_map(Expiration::Date),
            any::<bool>().prop_map(Expiration::Marker),
        ]);
        let noncurrent = prop::option::of((1..=1000u32, prop::option::of(1..=1000u32)));
        (
            document_text(),
            scope,
            any::<bool>(),
            expiration,
            noncurrent,
            prop::option::of(1..=1000u32),
        )
            .prop_filter_map(
                "a rule S3 accepts",
                |(id, scope, enabled, expiration, noncurrent, abort)| {
                    let narrowed = match &scope {
                        Scope::Filter(Filter::Tag(_) | Filter::Larger(_) | Filter::Smaller(_)) => {
                            true
                        }
                        Scope::Filter(Filter::And(and)) => {
                            !and.tags.is_empty() || and.larger.is_some() || and.smaller.is_some()
                        }
                        _ => false,
                    };
                    let noncurrent = noncurrent.map(|(days, newer)| crate::lifecycle::Noncurrent {
                        days,
                        newer: newer.filter(|_| matches!(scope, Scope::Filter(_))),
                    });
                    let abort = abort.filter(|_| !narrowed);
                    let expiration =
                        expiration.filter(|e| !(narrowed && matches!(e, Expiration::Marker(_))));
                    let acts = expiration.is_some() || noncurrent.is_some() || abort.is_some();
                    let id = id
                        .chars()
                        .take(crate::lifecycle::MAX_ID / 2)
                        .collect::<String>();
                    if acts && !id.is_empty() {
                        Some(Rule {
                            id,
                            scope,
                            enabled,
                            expiration,
                            noncurrent,
                            abort,
                        })
                    } else {
                        None
                    }
                },
            )
    }

    /// A configuration in one form: every rule with a `Filter`, or every rule with its own
    /// `Prefix`, the prefixes led by each rule's position so that none begins another.
    fn configuration_strategy() -> impl Strategy<Value = Vec<Rule>> {
        let v2 = prop::collection::vec(
            rule_strategy(filter_strategy().prop_map(Scope::Filter)),
            1..6,
        );
        let v1 =
            prop::collection::vec(rule_strategy(document_text().prop_map(Scope::Prefix)), 1..6)
                .prop_map(|mut rules| {
                    for (i, rule) in rules.iter_mut().enumerate() {
                        if let Scope::Prefix(prefix) = &mut rule.scope {
                            *prefix = format!("{i}/{prefix}");
                        }
                    }
                    rules
                });
        prop_oneof![v2, v1]
    }

    /// A refused preflight's error, whose elements S3 was recorded writing as `Code`,
    /// `Message`, `Method`, `ResourceType`, `RequestId`, `HostId` (16 §7).
    #[test]
    fn errors_carry_s3s_further_elements() {
        let doc = error(&Failure {
            code: "AccessForbidden",
            message: "CORSResponse: CORS is not enabled for this bucket.",
            resource: None,
            details: &[("Method", "OPTIONS"), ("ResourceType", "BUCKET")],
            request_id: "Z4ERRATSVNEC7WS7",
            host_id: "h",
        });
        assert!(
            doc.contains(
                "<Message>CORSResponse: CORS is not enabled for this bucket.</Message>\
                 <Method>OPTIONS</Method><ResourceType>BUCKET</ResourceType>\
                 <RequestId>Z4ERRATSVNEC7WS7</RequestId>"
            ),
            "{doc}"
        );
    }

    /// GetPublicAccessBlock's settings read back as they were set, and the policy status in the
    /// lowercase botocore reads (17 §2.2, §7).
    #[test]
    fn public_access_documents_read_back() {
        let block = crate::policy::PublicAccessBlock {
            block_public_acls: true,
            ignore_public_acls: false,
            block_public_policy: true,
            restrict_public_buckets: false,
        };
        let doc = public_access_block(&block);
        assert_eq!(crate::body::public_access_block(doc.as_bytes()), Ok(block));
        let status = policy_status(true);
        assert!(status.contains("<IsPublic>true</IsPublic>"), "{status}");
        assert!(policy_status(false).contains("<IsPublic>false</IsPublic>"));
        let (_, leaves) = elements(&status);
        assert_eq!(
            leaves,
            [("/PolicyStatus/IsPublic".to_string(), "true".to_string())]
        );
    }

    fn cors_rule_strategy() -> impl Strategy<Value = cors::Rule> {
        let method = prop_oneof![
            Just(cors::Method::Get),
            Just(cors::Method::Put),
            Just(cors::Method::Head),
            Just(cors::Method::Post),
            Just(cors::Method::Delete),
        ];
        let one_star = || {
            document_text().prop_map(|text| {
                let mut stars = 0;
                text.chars()
                    .filter(|c| {
                        *c != '*' || {
                            stars += 1;
                            stars == 1
                        }
                    })
                    .collect::<String>()
            })
        };
        (
            prop::option::of(document_text()),
            prop::collection::vec(one_star(), 0..3),
            prop::collection::vec(method, 1..4),
            prop::collection::vec(one_star(), 1..3),
            prop::collection::vec(document_text(), 0..3),
            prop::option::of(0..=u32::try_from(i32::MAX).unwrap()),
        )
            .prop_map(
                |(id, headers, methods, origins, expose, max_age)| cors::Rule {
                    id: id.map(|id| id.chars().take(cors::MAX_ID / 2).collect()),
                    headers,
                    methods,
                    origins,
                    expose,
                    max_age,
                },
            )
    }

    proptest! {
        /// Every configuration mantle accepts reads back from the document it writes exactly,
        /// as the rules were set.
        #[test]
        fn lifecycle_configurations_read_back(rules in configuration_strategy()) {
            let mut rules = rules;
            for (i, rule) in rules.iter_mut().enumerate() {
                rule.id = format!("{i}{}", rule.id);
            }
            let doc = lifecycle_configuration(&rules).unwrap();
            prop_assert_eq!(crate::body::lifecycle(doc.as_bytes()), Ok(rules));
        }

        /// Every CORS configuration mantle accepts reads back from the document it writes
        /// exactly, as the rules were set.
        #[test]
        fn cors_configurations_read_back(rules in prop::collection::vec(cors_rule_strategy(), 1..5)) {
            let mut rules = rules;
            for (i, rule) in rules.iter_mut().enumerate() {
                if let Some(id) = &mut rule.id {
                    *id = format!("{i}{id}");
                }
            }
            let doc = cors_configuration(&rules);
            prop_assert_eq!(crate::body::cors(doc.as_bytes()), Ok(rules));
        }
    }

    proptest! {
        /// Any key a listing holds reads back from the document exactly: as written when XML
        /// 1.0 can carry it, and after percent-decoding with `encoding-type=url`, which is how
        /// a client reads a key XML 1.0 cannot carry (13 §7).
        #[test]
        fn listed_keys_read_back(keys in prop::collection::btree_set(key_strategy(), 1..8)) {
            let listed = Page {
                contents: keys.iter().map(|k| (k.clone(), entry(0, "e", 1, "o"))).collect(),
                common_prefixes: keys.iter().map(|k| format!("{k}/")).collect(),
                truncated: false,
                next: None,
            };
            let read = |doc: &str, path: &str| -> Vec<String> {
                elements(doc).1.into_iter().filter(|(p, _)| p == path).map(|(_, t)| t).collect()
            };
            let list = ListV2 { url: true, ..v2("b", request("", None, None)) };
            let doc = list_objects_v2(&list, &listed).unwrap();
            let decode = |t: String| String::from_utf8(percent_decode(t.as_bytes()).unwrap()).unwrap();
            let mut got: Vec<String> = read(&doc, "/ListBucketResult/Contents/Key").into_iter().map(decode).collect();
            got.sort();
            prop_assert_eq!(got, keys.iter().cloned().collect::<Vec<_>>());

            let xml10 = |k: &String| k.chars().all(|c| matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..));
            let doc = list_objects_v2(&v2("b", request("", None, None)), &listed).unwrap();
            if keys.iter().all(xml10) {
                // Leaves are compared trimmed, so compare keys the same way.
                let mut got = read(&doc, "/ListBucketResult/Contents/Key");
                got.sort();
                let mut want: Vec<String> = keys.iter().map(|k| k.trim().to_string()).collect();
                want.sort();
                prop_assert_eq!(got, want);
            } else {
                // S3 writes such a key as a character reference, which XML 1.0 refuses.
                prop_assert!(roxmltree::Document::parse(&doc).is_err());
            }
        }
    }
}
