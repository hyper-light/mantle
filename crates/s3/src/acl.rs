//! Access control lists (docs/research/13 §6.8).
//!
//! mantle gives every bucket S3's default Object Ownership since April 2023, bucket owner
//! enforced: ACLs are disabled, the bucket owner owns every object, and reading an ACL
//! answers the owner's full control (docs/design/s3-protocol.md §5). A request may still name
//! an ACL, by a canned `x-amz-acl`, by `x-amz-grant-*` headers, or, to PutBucketAcl and
//! PutObjectAcl, by an `AccessControlPolicy` document ([`crate::body::access_control_policy`]).
//! This module reads what a request names and decides, as S3 does under that setting, whether
//! the request goes ahead.

use crate::body::{self, BodyError};

/// "An ACL can have up to 100 grants" (13 §6.8).
pub const MAX_GRANTS: usize = 100;

/// What a grant allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Read,
    Write,
    ReadAcp,
    WriteAcp,
    FullControl,
}

impl Permission {
    pub const ALL: [Self; 5] = [
        Self::Read,
        Self::Write,
        Self::ReadAcp,
        Self::WriteAcp,
        Self::FullControl,
    ];

    /// Its name in a `Permission` element.
    pub fn name(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::ReadAcp => "READ_ACP",
            Self::WriteAcp => "WRITE_ACP",
            Self::FullControl => "FULL_CONTROL",
        }
    }

    /// The request header that grants it.
    pub fn header(self) -> &'static str {
        match self {
            Self::Read => "x-amz-grant-read",
            Self::Write => "x-amz-grant-write",
            Self::ReadAcp => "x-amz-grant-read-acp",
            Self::WriteAcp => "x-amz-grant-write-acp",
            Self::FullControl => "x-amz-grant-full-control",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.name() == name)
    }
}

/// Whom a grant is to, as a request names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grantee {
    /// An account, by its canonical user ID.
    User(String),
    /// An account, by its email address.
    Email(String),
    /// A predefined group, by its URI.
    Group(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub grantee: Grantee,
    pub permission: Permission,
}

/// The canned ACLs (13 §6.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Canned {
    Private,
    PublicRead,
    PublicReadWrite,
    AwsExecRead,
    AuthenticatedRead,
    BucketOwnerRead,
    BucketOwnerFullControl,
    LogDeliveryWrite,
}

impl Canned {
    pub const ALL: [Self; 8] = [
        Self::Private,
        Self::PublicRead,
        Self::PublicReadWrite,
        Self::AwsExecRead,
        Self::AuthenticatedRead,
        Self::BucketOwnerRead,
        Self::BucketOwnerFullControl,
        Self::LogDeliveryWrite,
    ];

    /// Its name in `x-amz-acl`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
            Self::PublicReadWrite => "public-read-write",
            Self::AwsExecRead => "aws-exec-read",
            Self::AuthenticatedRead => "authenticated-read",
            Self::BucketOwnerRead => "bucket-owner-read",
            Self::BucketOwnerFullControl => "bucket-owner-full-control",
            Self::LogDeliveryWrite => "log-delivery-write",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.name() == name)
    }
}

/// Object Ownership's settings (13 §6.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    BucketOwnerPreferred,
    ObjectWriter,
    BucketOwnerEnforced,
}

impl Ownership {
    pub const ALL: [Self; 3] = [
        Self::BucketOwnerPreferred,
        Self::ObjectWriter,
        Self::BucketOwnerEnforced,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::BucketOwnerPreferred => "BucketOwnerPreferred",
            Self::ObjectWriter => "ObjectWriter",
            Self::BucketOwnerEnforced => "BucketOwnerEnforced",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.name() == name)
    }
}

/// An `AccessControlPolicy` document as a request sends it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Policy {
    /// The owner's ID, when the document names one.
    pub owner: Option<String>,
    pub grants: Vec<Grant>,
}

/// The ACL a request's headers name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Named {
    Canned(Canned),
    Grants(Vec<Grant>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AclError {
    #[error("x-amz-acl must name one canned ACL")]
    Canned,
    #[error(
        "an x-amz-grant header must list id=, uri= or emailAddress= grantees, separated by commas"
    )]
    Grant,
    #[error("an ACL holds at most 100 grants")]
    TooManyGrants,
    #[error("specifying both canned ACLs and header grants is not allowed")]
    CannedAndGrants,
    #[error("an ACL may be given in the headers or the body, not both")]
    HeadersAndBody,
    #[error(transparent)]
    Body(#[from] BodyError),
    #[error("the bucket does not allow ACLs")]
    NotSupported,
    #[error("bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting")]
    OwnershipEnforced,
    #[error(
        "x-amz-object-ownership must be BucketOwnerPreferred, ObjectWriter or BucketOwnerEnforced"
    )]
    Ownership,
    #[error("a bucket with ACLs enabled, which mantle does not implement")]
    AclsEnabled,
}

impl AclError {
    /// The S3 error code and status (13 §6.8).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Canned | Self::Grant | Self::TooManyGrants => ("InvalidArgument", 400),
            Self::CannedAndGrants | Self::HeadersAndBody => ("InvalidRequest", 400),
            Self::Body(error) => error.code(),
            Self::NotSupported => ("AccessControlListNotSupported", 400),
            Self::OwnershipEnforced => ("InvalidBucketAclWithObjectOwnership", 400),
            Self::Ownership => ("InvalidArgument", 400),
            Self::AclsEnabled => ("NotImplemented", 501),
        }
    }
}

/// The ACL `headers` name: a canned ACL, grants, or `None` for neither. "You can use either a
/// canned ACL or specify access permissions explicitly. You cannot do both" (13 §6.8).
pub fn named(headers: &[(&str, &str)]) -> Result<Option<Named>, AclError> {
    let mut canned = None;
    let mut grants = Vec::new();
    let mut granted = false;
    for &(name, value) in headers {
        if name.eq_ignore_ascii_case("x-amz-acl") {
            if canned.is_some() {
                return Err(AclError::Canned);
            }
            canned = Some(Canned::from_name(value).ok_or(AclError::Canned)?);
        } else if let Some(permission) = Permission::ALL
            .into_iter()
            .find(|p| name.eq_ignore_ascii_case(p.header()))
        {
            granted = true;
            for grantee in grantees(value)? {
                if grants.len() >= MAX_GRANTS {
                    return Err(AclError::TooManyGrants);
                }
                grants.push(Grant {
                    grantee,
                    permission,
                });
            }
        }
    }
    match (canned, granted) {
        (Some(_), true) => Err(AclError::CannedAndGrants),
        (Some(canned), false) => Ok(Some(Named::Canned(canned))),
        (None, true) => Ok(Some(Named::Grants(grants))),
        (None, false) => Ok(None),
    }
}

/// The grantees one `x-amz-grant-*` header lists: `type=value` pairs separated by commas,
/// the type `id`, `uri` or `emailAddress`, and the value quoted as AWS writes it or bare as
/// s3-tests sends it (13 §6.8).
fn grantees(list: &str) -> Result<Vec<Grantee>, AclError> {
    list.split(',')
        .map(|item| {
            let (kind, value) = item.trim().split_once('=').ok_or(AclError::Grant)?;
            let value = value.trim_start();
            let value = match value.strip_prefix('"') {
                Some(quoted) => quoted.strip_suffix('"').ok_or(AclError::Grant)?,
                None => value,
            };
            if value.is_empty() || value.contains('"') {
                return Err(AclError::Grant);
            }
            let value = value.to_owned();
            match kind.trim_end() {
                "id" => Ok(Grantee::User(value)),
                "uri" => Ok(Grantee::Group(value)),
                "emailAddress" => Ok(Grantee::Email(value)),
                _ => Err(AclError::Grant),
            }
        })
        .collect()
}

/// Whether PutObject, CopyObject or CreateMultipartUpload goes ahead with the ACL its
/// headers name. "The bucket only accepts PUT requests that don't specify an ACL or specify
/// bucket owner full control ACLs (such as the predefined `bucket-owner-full-control` canned
/// ACL or a custom ACL in XML format that grants the same permissions)" (13 §6.8).
pub fn object_write(named: Option<&Named>, bucket_owner: &str) -> Result<(), AclError> {
    match named {
        None => Ok(()),
        Some(named) => owner_full_control(named, bucket_owner),
    }
}

/// Whether PutBucketAcl or PutObjectAcl goes ahead: an ACL named by the headers or, when
/// they name none, by the body, which must be the bucket owner's full control as for
/// [`object_write`]. A body beside header ACLs is refused, as "you cannot specify access
/// permission using both the body and the request headers" (13 §6.8).
pub fn put_acl(named: Option<&Named>, body: &[u8], bucket_owner: &str) -> Result<(), AclError> {
    match named {
        Some(_) if !body.is_empty() => Err(AclError::HeadersAndBody),
        Some(named) => owner_full_control(named, bucket_owner),
        None => {
            let policy = body::access_control_policy(body)?;
            let owned = policy.owner.as_deref().is_none_or(|o| o == bucket_owner);
            if owned && only_owner_full_control(&policy.grants, bucket_owner) {
                Ok(())
            } else {
                Err(AclError::NotSupported)
            }
        }
    }
}

/// Whether CreateBucket goes ahead with the ACL its headers name, for `owner`: one that
/// gives no other account access. "If your `CreateBucket` request sets Bucket owner enforced
/// and specifies a bucket ACL that provides access to an external AWS account, your request
/// fails" (13 §6.8). S3 ignores the two object canned ACLs on a new bucket.
pub fn create_bucket(named: Option<&Named>, owner: &str) -> Result<(), AclError> {
    let own = match named {
        None => true,
        Some(Named::Canned(canned)) => matches!(
            canned,
            Canned::Private | Canned::BucketOwnerRead | Canned::BucketOwnerFullControl
        ),
        Some(Named::Grants(grants)) => grants
            .iter()
            .all(|g| matches!(&g.grantee, Grantee::User(id) if id == owner)),
    };
    if own {
        Ok(())
    } else {
        Err(AclError::OwnershipEnforced)
    }
}

/// Whether a bucket takes `ownership`, from PutBucketOwnershipControls or CreateBucket's
/// `x-amz-object-ownership`: bucket owner enforced, the one setting mantle's buckets have,
/// goes ahead, and the two that enable ACLs are `NotImplemented`. DeleteBucketOwnershipControls
/// is [`AclError::AclsEnabled`] too, since a bucket without controls has ACLs enabled (13 §6.8).
pub fn ownership(ownership: Ownership) -> Result<(), AclError> {
    match ownership {
        Ownership::BucketOwnerEnforced => Ok(()),
        Ownership::BucketOwnerPreferred | Ownership::ObjectWriter => Err(AclError::AclsEnabled),
    }
}

/// CreateBucket's `x-amz-object-ownership`, absent for S3's default, bucket owner enforced.
pub fn object_ownership(header: Option<&str>) -> Result<(), AclError> {
    match header {
        None => Ok(()),
        Some(name) => ownership(Ownership::from_name(name).ok_or(AclError::Ownership)?),
    }
}

/// The `bucket-owner-full-control` canned ACL, or grants of full control to the bucket owner
/// alone. Every other canned ACL is refused, `private` among them, though it grants the same
/// once the bucket owner owns the object: s3-tests expects `private` refused (13 §6.8).
fn owner_full_control(named: &Named, bucket_owner: &str) -> Result<(), AclError> {
    let accepted = match named {
        Named::Canned(canned) => *canned == Canned::BucketOwnerFullControl,
        Named::Grants(grants) => only_owner_full_control(grants, bucket_owner),
    };
    if accepted {
        Ok(())
    } else {
        Err(AclError::NotSupported)
    }
}

fn only_owner_full_control(grants: &[Grant], bucket_owner: &str) -> bool {
    !grants.is_empty()
        && grants.iter().all(|g| {
            g.permission == Permission::FullControl
                && matches!(&g.grantee, Grantee::User(id) if id == bucket_owner)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";

    fn user(id: &str, permission: Permission) -> Grant {
        Grant {
            grantee: Grantee::User(id.into()),
            permission,
        }
    }

    /// The header forms AWS's pages print (13 §6.8).
    #[test]
    fn grant_headers_read_as_aws_writes_them() {
        let headers = [
            (
                "x-amz-grant-write",
                "uri=\"http://acs.amazonaws.com/groups/s3/LogDelivery\", id=\"111122223333\", id=\"555566667777\"",
            ),
            (
                "X-Amz-Grant-Read",
                "emailAddress=\"xyz@amazon.com\", emailAddress=\"abc@amazon.com\"",
            ),
        ];
        let Some(Named::Grants(grants)) = named(&headers).unwrap() else {
            panic!("grants expected");
        };
        assert_eq!(
            grants,
            [
                Grant {
                    grantee: Grantee::Group(
                        "http://acs.amazonaws.com/groups/s3/LogDelivery".into()
                    ),
                    permission: Permission::Write,
                },
                user("111122223333", Permission::Write),
                user("555566667777", Permission::Write),
                Grant {
                    grantee: Grantee::Email("xyz@amazon.com".into()),
                    permission: Permission::Read,
                },
                Grant {
                    grantee: Grantee::Email("abc@amazon.com".into()),
                    permission: Permission::Read,
                },
            ]
        );
        // s3-tests sends its IDs bare.
        assert_eq!(
            named(&[("x-amz-grant-full-control", "id=testid")]),
            Ok(Some(Named::Grants(vec![user(
                "testid",
                Permission::FullControl
            )])))
        );
        assert_eq!(named(&[("x-amz-meta-acl", "private")]), Ok(None));
    }

    #[test]
    fn header_refusals_are_s3s() {
        let code = |headers: &[(&str, &str)]| named(headers).map_err(|e| e.code().0);
        // s3-tests' test_bucket_put_bad_canned_acl sends `public-ready`.
        assert_eq!(
            code(&[("x-amz-acl", "public-ready")]),
            Err("InvalidArgument")
        );
        assert_eq!(
            code(&[("x-amz-acl", "private"), ("x-amz-acl", "public-read")]),
            Err("InvalidArgument")
        );
        // What S3 answered a canned ACL beside header grants (13 §6.8).
        assert_eq!(
            code(&[("x-amz-acl", "private"), ("x-amz-grant-read", "id=\"a\"")]),
            Err("InvalidRequest")
        );
        for bad in [
            "",
            "id",
            "id=",
            "id=\"a",
            "name=\"a\"",
            "id=\"a\"\"b\"",
            "id=\"a\",,",
        ] {
            assert_eq!(
                code(&[("x-amz-grant-read", bad)]),
                Err("InvalidArgument"),
                "{bad}"
            );
        }
        let many = vec!["id=\"a\""; MAX_GRANTS + 1].join(",");
        assert_eq!(code(&[("x-amz-grant-read", &many)]), Err("InvalidArgument"));
        let most = vec!["id=\"a\""; MAX_GRANTS].join(",");
        assert!(code(&[("x-amz-grant-read", &most)]).is_ok());
    }

    /// s3-tests' bucket owner enforced cases: no ACL or `bucket-owner-full-control` goes
    /// ahead, and `private` is `AccessControlListNotSupported` (13 §6.8).
    #[test]
    fn object_writes_take_only_the_owners_full_control() {
        let canned = |c| Some(Named::Canned(c));
        assert_eq!(object_write(None, OWNER), Ok(()));
        assert_eq!(
            object_write(canned(Canned::BucketOwnerFullControl).as_ref(), OWNER),
            Ok(())
        );
        for other in Canned::ALL
            .into_iter()
            .filter(|c| *c != Canned::BucketOwnerFullControl)
        {
            assert_eq!(
                object_write(canned(other).as_ref(), OWNER).map_err(|e| e.code()),
                Err(("AccessControlListNotSupported", 400)),
                "{}",
                other.name()
            );
        }
        let grants = |g: Vec<Grant>| Some(Named::Grants(g));
        let full = user(OWNER, Permission::FullControl);
        assert_eq!(
            object_write(grants(vec![full.clone()]).as_ref(), OWNER),
            Ok(())
        );
        for refused in [
            vec![full.clone(), user("other", Permission::Read)],
            vec![user(OWNER, Permission::Read)],
            vec![user("other", Permission::FullControl)],
        ] {
            assert_eq!(
                object_write(grants(refused).as_ref(), OWNER),
                Err(AclError::NotSupported)
            );
        }
    }

    #[test]
    fn put_acl_takes_the_headers_or_the_body() {
        let full = format!(
            "<AccessControlPolicy><AccessControlList><Grant>\
             <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
             <ID>{OWNER}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
             </AccessControlList><Owner><ID>{OWNER}</ID></Owner></AccessControlPolicy>"
        );
        assert_eq!(put_acl(None, full.as_bytes(), OWNER), Ok(()));
        let other_owner = full.replace(&format!("<Owner><ID>{OWNER}"), "<Owner><ID>x");
        assert_eq!(
            put_acl(None, other_owner.as_bytes(), OWNER),
            Err(AclError::NotSupported)
        );
        // s3-tests' test_bucket_acl_revoke_all: an owner and no grants.
        let none =
            format!("<AccessControlPolicy><Owner><ID>{OWNER}</ID></Owner></AccessControlPolicy>");
        assert_eq!(
            put_acl(None, none.as_bytes(), OWNER),
            Err(AclError::NotSupported)
        );
        let canned = Named::Canned(Canned::BucketOwnerFullControl);
        assert_eq!(put_acl(Some(&canned), b"", OWNER), Ok(()));
        assert_eq!(
            put_acl(Some(&canned), full.as_bytes(), OWNER),
            Err(AclError::HeadersAndBody)
        );
        let private = Named::Canned(Canned::Private);
        assert_eq!(
            put_acl(Some(&private), b"", OWNER),
            Err(AclError::NotSupported)
        );
        assert_eq!(
            put_acl(None, b"", OWNER).map_err(|e| e.code().0),
            Err("MalformedACLError")
        );
    }

    #[test]
    fn a_new_bucket_takes_no_acl_that_reaches_another_account() {
        let canned = |c| Some(Named::Canned(c));
        for fine in [
            Canned::Private,
            Canned::BucketOwnerRead,
            Canned::BucketOwnerFullControl,
        ] {
            assert_eq!(create_bucket(canned(fine).as_ref(), OWNER), Ok(()));
        }
        for refused in [
            Canned::PublicRead,
            Canned::PublicReadWrite,
            Canned::AwsExecRead,
            Canned::AuthenticatedRead,
            Canned::LogDeliveryWrite,
        ] {
            assert_eq!(
                create_bucket(canned(refused).as_ref(), OWNER).map_err(|e| e.code()),
                Err(("InvalidBucketAclWithObjectOwnership", 400))
            );
        }
        let own = Named::Grants(vec![user(OWNER, Permission::Read)]);
        assert_eq!(create_bucket(Some(&own), OWNER), Ok(()));
        let group = Named::Grants(vec![Grant {
            grantee: Grantee::Group("http://acs.amazonaws.com/groups/global/AllUsers".into()),
            permission: Permission::Read,
        }]);
        assert_eq!(
            create_bucket(Some(&group), OWNER),
            Err(AclError::OwnershipEnforced)
        );
        assert_eq!(create_bucket(None, OWNER), Ok(()));
    }

    #[test]
    fn buckets_are_bucket_owner_enforced() {
        let code = |header| object_ownership(header).map_err(|e| e.code());
        assert_eq!(code(None), Ok(()));
        assert_eq!(code(Some("BucketOwnerEnforced")), Ok(()));
        assert_eq!(code(Some("ObjectWriter")), Err(("NotImplemented", 501)));
        assert_eq!(
            code(Some("BucketOwnerPreferred")),
            Err(("NotImplemented", 501))
        );
        assert_eq!(
            code(Some("bucketownerenforced")),
            Err(("InvalidArgument", 400))
        );
    }
}
