//! What a task says it is doing, by which the runtime's table of what held a shard counts each long
//! poll beside the task's spawn site (hyper-rt `slow`): every S3 connection is spawned at one site,
//! and every operation through the native door runs in the node's own task, so the label is what
//! tells their operations apart.
//!
//! - **An S3 connection's task** is labelled with its request's S3 operation once the request is
//!   routed, and cleared as the request ends (`listener::conn`).
//! - **The node's task** is labelled, as each turn of its object host ends, with the operation
//!   whose step held that turn longest, through either door; a turn that steps none clears it
//!   (`node::host`). A long poll of the node's task is counted under the operation that cost it
//!   most.
//!
//! Each operation has one label of its own, a `static` named as its variant: the table tells
//! labels apart by address and its readers show the name.

use hyper_rt::slow::Label;
use mantle_protocol::object::Op;
use mantle_s3::route::Operation;

/// One `static` label for each variant listed, named as the variant, and `$name`, which gives a
/// value's: its match names every variant, so one added to the enum fails to compile until it is
/// listed here.
macro_rules! labels {
    ($(#[$doc:meta])* $name:ident($enum:ident) { $($variant:ident),* $(,)? }) => {
        $(#[$doc])*
        pub(crate) fn $name(of: &$enum) -> &'static Label {
            match of {
                $($enum::$variant { .. } => {
                    static LABEL: Label = Label::new(stringify!($variant));
                    &LABEL
                })*
            }
        }
    };
}

labels! {
    /// The label of an S3 request's operation, as `mantle_s3::route` routes it.
    s3(Operation) {
        ListBuckets, CreateBucket, DeleteBucket, HeadBucket, GetBucketLocation,
        GetBucketVersioning, PutBucketVersioning, GetBucketTagging, PutBucketTagging,
        DeleteBucketTagging, GetBucketAcl, PutBucketAcl, GetBucketOwnershipControls,
        PutBucketOwnershipControls, DeleteBucketOwnershipControls,
        GetBucketLifecycleConfiguration, PutBucketLifecycleConfiguration, DeleteBucketLifecycle,
        GetBucketCors, PutBucketCors, DeleteBucketCors, GetBucketPolicy, PutBucketPolicy,
        DeleteBucketPolicy, GetBucketPolicyStatus, GetPublicAccessBlock, PutPublicAccessBlock,
        DeletePublicAccessBlock, GetObjectLockConfiguration, PutObjectLockConfiguration,
        GetBucketEncryption, PutBucketEncryption, DeleteBucketEncryption, Preflight, ListObjects,
        ListObjectsV2, ListObjectVersions, ListMultipartUploads, DeleteObjects, PutObject,
        PostObject, CopyObject, GetObject, HeadObject, DeleteObject, GetObjectAttributes,
        GetObjectTagging, PutObjectTagging, DeleteObjectTagging, GetObjectAcl, PutObjectAcl,
        GetObjectRetention, PutObjectRetention, GetObjectLegalHold, PutObjectLegalHold,
        CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload,
        AbortMultipartUpload, ListParts,
    }
}

labels! {
    /// The label of an object host's operation (`mantle_protocol::object::Op`), as the native door
    /// decodes it or the S3 listener hands it over.
    host(Op) {
        Create, Put, Get, Head, Delete, List, HeadBucket, ListBuckets, DeleteBucket, PutVersioning,
        GetVersioning, CreateUpload, UploadPart, Complete, Abort, ListParts, ListUploads, Open,
        Append, Offset, Finish, Copy, CopyPart, DeleteObjects, ListVersions, CreatePrincipal,
        PutPolicy, DeletePrincipal, ListPrincipals, GetPrincipal, CreateKey, RevokeKey,
    }
}

/// Labels the task being polled `label`; `None` clears it.
pub(crate) fn set(label: Option<&'static Label>) {
    // Refused only where no task of a shard is being polled, where no table counts a poll: there
    // is nothing to label.
    let _ = hyper_rt::futures::label(label);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every S3 operation the router names.
    const OPERATIONS: [Operation; 61] = [
        Operation::ListBuckets,
        Operation::CreateBucket,
        Operation::DeleteBucket,
        Operation::HeadBucket,
        Operation::GetBucketLocation,
        Operation::GetBucketVersioning,
        Operation::PutBucketVersioning,
        Operation::GetBucketTagging,
        Operation::PutBucketTagging,
        Operation::DeleteBucketTagging,
        Operation::GetBucketAcl,
        Operation::PutBucketAcl,
        Operation::GetBucketOwnershipControls,
        Operation::PutBucketOwnershipControls,
        Operation::DeleteBucketOwnershipControls,
        Operation::GetBucketLifecycleConfiguration,
        Operation::PutBucketLifecycleConfiguration,
        Operation::DeleteBucketLifecycle,
        Operation::GetBucketCors,
        Operation::PutBucketCors,
        Operation::DeleteBucketCors,
        Operation::GetBucketPolicy,
        Operation::PutBucketPolicy,
        Operation::DeleteBucketPolicy,
        Operation::GetBucketPolicyStatus,
        Operation::GetPublicAccessBlock,
        Operation::PutPublicAccessBlock,
        Operation::DeletePublicAccessBlock,
        Operation::GetObjectLockConfiguration,
        Operation::PutObjectLockConfiguration,
        Operation::GetBucketEncryption,
        Operation::PutBucketEncryption,
        Operation::DeleteBucketEncryption,
        Operation::Preflight,
        Operation::ListObjects,
        Operation::ListObjectsV2,
        Operation::ListObjectVersions,
        Operation::ListMultipartUploads,
        Operation::DeleteObjects,
        Operation::PutObject,
        Operation::PostObject,
        Operation::CopyObject,
        Operation::GetObject,
        Operation::HeadObject,
        Operation::DeleteObject,
        Operation::GetObjectAttributes,
        Operation::GetObjectTagging,
        Operation::PutObjectTagging,
        Operation::DeleteObjectTagging,
        Operation::GetObjectAcl,
        Operation::PutObjectAcl,
        Operation::GetObjectRetention,
        Operation::PutObjectRetention,
        Operation::GetObjectLegalHold,
        Operation::PutObjectLegalHold,
        Operation::CreateMultipartUpload,
        Operation::UploadPart,
        Operation::UploadPartCopy,
        Operation::CompleteMultipartUpload,
        Operation::AbortMultipartUpload,
        Operation::ListParts,
    ];

    /// Each label is named as its operation, and is its operation's alone, by address, as the
    /// table tells them apart, and by name, as its readers show them.
    fn one_to_one(labelled: &[(String, &'static Label)]) {
        for (at, (name, label)) in labelled.iter().enumerate() {
            assert_eq!(label.name, name, "{name}'s label");
            for (_, theirs) in labelled.iter().skip(at + 1) {
                assert!(!std::ptr::eq(*label, *theirs), "{name} and {}", theirs.name);
                assert_ne!(label.name, theirs.name);
            }
        }
    }

    /// Every S3 operation has a label of its own, named as the operation.
    #[test]
    fn every_s3_operation_has_its_own_label() {
        let labelled: Vec<(String, &'static Label)> = OPERATIONS
            .iter()
            .map(|op| (format!("{op:?}"), s3(op)))
            .collect();
        one_to_one(&labelled);
        assert!(std::ptr::eq(
            s3(&Operation::GetObject),
            s3(&Operation::GetObject)
        ));
    }

    /// The host's operations have labels of their own, named as the operation, whatever their
    /// fields.
    #[test]
    fn the_hosts_operations_have_their_own_labels() {
        let name = || "a".to_owned();
        let ops = [
            Op::Create {
                bucket: name(),
                id: 1,
            },
            Op::HeadBucket { bucket: name() },
            Op::DeleteBucket {
                bucket: name(),
                id: 2,
            },
            Op::GetVersioning { bucket: name() },
            Op::CreatePrincipal {
                name: name(),
                policy: name(),
            },
            Op::PutPolicy {
                name: name(),
                policy: name(),
            },
            Op::DeletePrincipal { name: name() },
            Op::ListPrincipals {
                after: None,
                max: 3,
            },
            Op::GetPrincipal { name: name() },
            Op::CreateKey {
                principal: name(),
                rotating: true,
            },
            Op::RevokeKey { id: name() },
        ];
        let labelled: Vec<(String, &'static Label)> = ops
            .iter()
            .map(|op| {
                let shown = format!("{op:?}");
                let variant: String = shown
                    .chars()
                    .take_while(char::is_ascii_alphanumeric)
                    .collect();
                (variant, host(op))
            })
            .collect();
        one_to_one(&labelled);
        let other = Op::Create {
            bucket: "b".to_owned(),
            id: 9,
        };
        assert!(std::ptr::eq(host(&ops[0]), host(&other)));
    }
}
