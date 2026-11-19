//! Bucket policies (docs/research/17): the JSON document PutBucketPolicy sets, read and checked
//! as S3 checks it. Every fault is `400 MalformedPolicy`; where S3 was recorded answering one,
//! the message is S3's (17 §2.3).
//!
//! A bucket policy names accounts, each as AWS names one, by its 12-digit ID or the ARN of its
//! root, or by its canonical user ID, and the anonymous requester. It names no IAM user, role or
//! federated principal: a policy naming one is refused as S3 refuses a principal that does not
//! exist, since PutBucketPolicy is not yet served beside the principals a cell keeps.
//!
//! A principal's identity policy ([`identity`], docs/research/91) is the same language as IAM
//! reads an IAM user's inline policy: no `Principal`, since "the principal is implicitly the
//! identity where the policy is attached", any S3 resource, and the actions on no resource,
//! `s3:ListAllMyBuckets` among them, which name the resource `*`.

pub mod catalog;
mod evaluate;

pub use evaluate::{Request, Requester, Verdict, authorize, permits, principal_keys, user_arn};

use crate::json::{self, View};

/// "Bucket policies are limited to 20 KB in size", measured on the document normalized: S3
/// answered "Normalized policy document exceeds the maximum allowed size of 20480 bytes"
/// (17 §2.3). The normalized form is taken as the compact one, without white space between
/// tokens (DERIVED).
pub const MAX_SIZE: usize = 20_480;

/// The largest body PutBucketPolicy reads: the normalized limit, and as much white space
/// again, as the XML bodies' limits allow (docs/design/s3-protocol.md §2).
pub const BODY_LIMIT: usize = 2 * MAX_SIZE;

/// The partition and service every resource a bucket policy names begins with.
const ARN_PREFIX: &str = "arn:aws:s3:::";

/// The most characters a principal's identity policy holds, white space between its tokens not
/// counted: IAM's bound on a user's inline policies, "User policy size can't exceed 2,048
/// characters. ... IAM doesn't count white space" (research/91 §2), a principal holding one.
pub const IDENTITY_CHARS: usize = 2_048;

/// The most characters a policy document is sent in, its white space counted: IAM's
/// `PolicyDocument`, "Maximum length of 131072" (PutUserPolicy; research/91 §2).
pub const DOCUMENT_CHARS: usize = 131_072;

/// The most octets a sent document of [`DOCUMENT_CHARS`] takes in UTF-8: IAM's pattern admits no
/// character past U+00FF, each at most two octets (RFC 3629 §3).
pub const DOCUMENT_BYTES: usize = 2 * DOCUMENT_CHARS;

/// The resource an action on no resource is judged against: S3's whole namespace, which `*` and
/// `arn:aws:s3:::*` name, as AWS's own policies grant `s3:ListAllMyBuckets` by either, and no
/// bucket's or object's ARN does (research/91 §3).
pub const ACCOUNT_RESOURCE: &str = "arn:aws:s3:::*";

/// The kind of resource an action acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Bucket,
    Object,
    /// No resource: the statement names `*` (`catalog::ACCOUNT_ACTIONS`).
    Account,
}

/// A condition key's type, as the Service Authorization Reference gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Arn,
    ArrayOfString,
    Bool,
    Date,
    Numeric,
    String,
}

/// An S3 action a bucket policy can name: its name after `s3:`, the kind of resource it acts
/// on, and the S3 condition keys that come with it (17 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Action {
    pub name: &'static str,
    pub kind: Kind,
    pub keys: &'static [&'static str],
}

/// A bucket's Block Public Access settings (17 §7). S3 turns all four on for every new bucket
/// since April 2023; with ACLs disabled on every bucket, the two for ACLs change nothing, and
/// are kept so a configuration reads back as it was set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicAccessBlock {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    /// Refuse a public policy: "Setting this element to `TRUE` causes Amazon S3 to reject calls
    /// to PUT Bucket policy if the specified bucket policy allows public access."
    pub block_public_policy: bool,
    /// Limit what a public policy grants to the owner's own account: "public and cross-account
    /// access within any public bucket policy, including non-public delegation to specific
    /// accounts, is blocked."
    pub restrict_public_buckets: bool,
}

impl PublicAccessBlock {
    /// A new bucket's: every setting on.
    pub const NEW_BUCKET: Self = Self {
        block_public_acls: true,
        ignore_public_acls: true,
        block_public_policy: true,
        restrict_public_buckets: true,
    };
}

/// PutBucketPolicy refused for a public policy under BlockPublicPolicy: `403 AccessDenied`
/// (17 §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "Access Denied because public policies are prevented by the BlockPublicPolicy setting in S3 Block Public Access."
)]
pub struct PublicPolicyBlocked;

impl PublicPolicyBlocked {
    pub fn code(&self) -> (&'static str, u16) {
        ("AccessDenied", 403)
    }
}

/// Whether `policy` may be set on a bucket with the settings `block`, if it has any.
pub fn may_set(
    policy: &Policy,
    block: Option<&PublicAccessBlock>,
) -> Result<(), PublicPolicyBlocked> {
    if block.is_some_and(|block| block.block_public_policy) && policy.is_public() {
        Err(PublicPolicyBlocked)
    } else {
        Ok(())
    }
}

/// The action a request asks for, as S3 authorizes its operation (17 §5): the `Version` form
/// when the request names a version ID. The operations with more than one resource are judged
/// by parts: a copy as `s3:PutObject` on its destination, its source judged as a GetObject;
/// DeleteObjects for each key as a DeleteObject; a POST upload as `s3:PutObject` on the key its
/// form names (19 §2.1). `None` for a preflight, answered from the
/// bucket's CORS rules without authorization (16 §3), and for ListBuckets, whose
/// `s3:ListAllMyBuckets` names no bucket and so no bucket policy.
pub fn action(operation: crate::route::Operation, versioned: bool) -> Option<&'static str> {
    use crate::route::Operation as O;
    let either =
        |plain: &'static str, version: &'static str| Some(if versioned { version } else { plain });
    match operation {
        O::ListBuckets | O::Preflight => None,
        O::CreateBucket => Some("s3:CreateBucket"),
        O::DeleteBucket => Some("s3:DeleteBucket"),
        O::HeadBucket | O::ListObjects | O::ListObjectsV2 => Some("s3:ListBucket"),
        O::ListObjectVersions => Some("s3:ListBucketVersions"),
        O::ListMultipartUploads => Some("s3:ListBucketMultipartUploads"),
        O::GetBucketLocation => Some("s3:GetBucketLocation"),
        O::GetBucketVersioning => Some("s3:GetBucketVersioning"),
        O::PutBucketVersioning => Some("s3:PutBucketVersioning"),
        O::GetBucketTagging => Some("s3:GetBucketTagging"),
        O::PutBucketTagging | O::DeleteBucketTagging => Some("s3:PutBucketTagging"),
        O::GetBucketAcl => Some("s3:GetBucketAcl"),
        O::PutBucketAcl => Some("s3:PutBucketAcl"),
        O::GetBucketOwnershipControls => Some("s3:GetBucketOwnershipControls"),
        O::PutBucketOwnershipControls | O::DeleteBucketOwnershipControls => {
            Some("s3:PutBucketOwnershipControls")
        }
        O::GetBucketLifecycleConfiguration => Some("s3:GetLifecycleConfiguration"),
        O::PutBucketLifecycleConfiguration | O::DeleteBucketLifecycle => {
            Some("s3:PutLifecycleConfiguration")
        }
        O::GetBucketCors => Some("s3:GetBucketCORS"),
        O::PutBucketCors | O::DeleteBucketCors => Some("s3:PutBucketCORS"),
        O::GetBucketPolicy => Some("s3:GetBucketPolicy"),
        O::PutBucketPolicy => Some("s3:PutBucketPolicy"),
        O::DeleteBucketPolicy => Some("s3:DeleteBucketPolicy"),
        O::GetBucketPolicyStatus => Some("s3:GetBucketPolicyStatus"),
        O::GetPublicAccessBlock => Some("s3:GetBucketPublicAccessBlock"),
        O::PutPublicAccessBlock | O::DeletePublicAccessBlock => {
            Some("s3:PutBucketPublicAccessBlock")
        }
        O::GetObject | O::HeadObject | O::GetObjectAttributes => {
            either("s3:GetObject", "s3:GetObjectVersion")
        }
        O::PutObject
        | O::PostObject
        | O::CopyObject
        | O::CreateMultipartUpload
        | O::UploadPart
        | O::UploadPartCopy
        | O::CompleteMultipartUpload => Some("s3:PutObject"),
        O::DeleteObject | O::DeleteObjects => either("s3:DeleteObject", "s3:DeleteObjectVersion"),
        O::GetObjectTagging => either("s3:GetObjectTagging", "s3:GetObjectVersionTagging"),
        O::PutObjectTagging => either("s3:PutObjectTagging", "s3:PutObjectVersionTagging"),
        O::DeleteObjectTagging => either("s3:DeleteObjectTagging", "s3:DeleteObjectVersionTagging"),
        O::GetObjectAcl => either("s3:GetObjectAcl", "s3:GetObjectVersionAcl"),
        O::PutObjectAcl => either("s3:PutObjectAcl", "s3:PutObjectVersionAcl"),
        O::GetObjectLockConfiguration => Some("s3:GetBucketObjectLockConfiguration"),
        O::GetBucketEncryption => Some("s3:GetEncryptionConfiguration"),
        O::PutBucketEncryption | O::DeleteBucketEncryption => Some("s3:PutEncryptionConfiguration"),
        O::PutObjectLockConfiguration => Some("s3:PutBucketObjectLockConfiguration"),
        O::GetObjectRetention => Some("s3:GetObjectRetention"),
        O::PutObjectRetention => Some("s3:PutObjectRetention"),
        O::GetObjectLegalHold => Some("s3:GetObjectLegalHold"),
        O::PutObjectLegalHold => Some("s3:PutObjectLegalHold"),
        O::AbortMultipartUpload => Some("s3:AbortMultipartUpload"),
        O::ListParts => Some("s3:ListMultipartUploadParts"),
    }
}

/// The ARN a policy names a request's resource by: the bucket's, or with a key the object's
/// (17 §5).
pub fn resource(bucket: &str, key: Option<&str>) -> String {
    match key {
        Some(key) => format!("{ARN_PREFIX}{bucket}/{key}"),
        None => format!("{ARN_PREFIX}{bucket}"),
    }
}

/// The action a principal's request asks for, as its identity policy names it: ListBuckets's
/// `s3:ListAllMyBuckets`, on [`ACCOUNT_RESOURCE`] ("the following example ... `s3:ListAllMyBuckets`"
/// research/91 §3), and every other operation's [`action`].
pub fn identity_action(
    operation: crate::route::Operation,
    versioned: bool,
) -> Option<&'static str> {
    match operation {
        crate::route::Operation::ListBuckets => Some("s3:ListAllMyBuckets"),
        other => action(other, versioned),
    }
}

/// Whether `action`, `s3:` and its name, carries the S3 condition key `key` in the Service
/// Authorization Reference's catalog: a request's own S3 keys are those its action carries, as
/// IAM's authorization context holds them (17 §5).
pub fn carries(action: &str, key: &str) -> bool {
    let Some(name) = action.strip_prefix("s3:") else {
        return false;
    };
    catalog::ACTIONS
        .iter()
        .chain(catalog::ACCOUNT_ACTIONS.iter())
        .find(|known| known.name == name)
        .is_some_and(|known| known.keys.iter().any(|k| k.eq_ignore_ascii_case(key)))
}

/// A policy, checked: a bucket's, or a principal's identity policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// `Version` `2012-10-17`, under which `${...}` names a policy variable; `2008-10-17`, the
    /// default, treats it as text (17 §3.1).
    pub variables: bool,
    /// A principal's identity policy, whose statements name no principal and apply to the
    /// identity it is attached to (research/91 §3).
    pub identity: bool,
    pub statements: Vec<Statement>,
}

/// A statement, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// `Effect` `Allow`; otherwise `Deny`.
    pub allow: bool,
    pub principals: Principals,
    /// `NotAction`: the statement applies to every action but these.
    pub not_action: bool,
    /// Action patterns, lowercase, as actions compare without regard to case (17 §3.2).
    pub actions: Vec<String>,
    /// `NotResource`: the statement applies to every resource but these.
    pub not_resource: bool,
    /// Resource patterns: ARNs, compared with regard to case.
    pub resources: Vec<String>,
    pub conditions: Vec<Condition>,
}

/// A statement's `Principal`, or with `not` its `NotPrincipal`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principals {
    pub not: bool,
    /// `"*"` or `{"AWS": "*"}`: every requester, the anonymous one included (17 §6).
    pub everyone: bool,
    /// Accounts by their 12-digit IDs.
    pub accounts: Vec<String>,
    /// `CanonicalUser` IDs.
    pub canonical: Vec<String>,
    /// `Service` principals, which no requester of mantle's is.
    pub services: Vec<String>,
}

/// A condition: its operator applied to one key and its values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    pub operator: Operator,
    /// `ForAllValues:` or `ForAnyValue:`.
    pub set: Option<Set>,
    /// `...IfExists`: a key the request lacks satisfies the condition.
    pub if_exists: bool,
    /// The key, lowercase: key names compare without regard to case (17 §3.3).
    pub key: String,
    /// The values, numbers and booleans as written.
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Set {
    All,
    Any,
}

/// The condition operators of the IAM policy language (17 §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringNotEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
    Bool,
    BinaryEquals,
    IpAddress,
    NotIpAddress,
    ArnEquals,
    ArnLike,
    ArnNotEquals,
    ArnNotLike,
    Null,
}

impl Operator {
    const ALL: [(&'static str, Self); 27] = [
        ("StringEquals", Self::StringEquals),
        ("StringNotEquals", Self::StringNotEquals),
        ("StringEqualsIgnoreCase", Self::StringEqualsIgnoreCase),
        ("StringNotEqualsIgnoreCase", Self::StringNotEqualsIgnoreCase),
        ("StringLike", Self::StringLike),
        ("StringNotLike", Self::StringNotLike),
        ("NumericEquals", Self::NumericEquals),
        ("NumericNotEquals", Self::NumericNotEquals),
        ("NumericLessThan", Self::NumericLessThan),
        ("NumericLessThanEquals", Self::NumericLessThanEquals),
        ("NumericGreaterThan", Self::NumericGreaterThan),
        ("NumericGreaterThanEquals", Self::NumericGreaterThanEquals),
        ("DateEquals", Self::DateEquals),
        ("DateNotEquals", Self::DateNotEquals),
        ("DateLessThan", Self::DateLessThan),
        ("DateLessThanEquals", Self::DateLessThanEquals),
        ("DateGreaterThan", Self::DateGreaterThan),
        ("DateGreaterThanEquals", Self::DateGreaterThanEquals),
        ("Bool", Self::Bool),
        ("BinaryEquals", Self::BinaryEquals),
        ("IpAddress", Self::IpAddress),
        ("NotIpAddress", Self::NotIpAddress),
        ("ArnEquals", Self::ArnEquals),
        ("ArnLike", Self::ArnLike),
        ("ArnNotEquals", Self::ArnNotEquals),
        ("ArnNotLike", Self::ArnNotLike),
        ("Null", Self::Null),
    ];

    /// The operator a condition type names, with its set prefix and `IfExists` suffix: the
    /// names are exact, as S3 refused `stringequals` (17 §2.3), and `Null` takes no suffix.
    fn from_type(name: &str) -> Option<(Self, Option<Set>, bool)> {
        let (set, rest) = if let Some(rest) = name.strip_prefix("ForAllValues:") {
            (Some(Set::All), rest)
        } else if let Some(rest) = name.strip_prefix("ForAnyValue:") {
            (Some(Set::Any), rest)
        } else {
            (None, name)
        };
        let (base, if_exists) = match rest.strip_suffix("IfExists") {
            Some(base) => (base, true),
            None => (rest, false),
        };
        let operator = Self::ALL
            .iter()
            .find(|(written, _)| *written == base)
            .map(|(_, operator)| *operator)?;
        if if_exists && operator == Self::Null {
            return None;
        }
        Some((operator, set, if_exists))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("Policies must be valid JSON and the first byte must be '{{'")]
    NotJson,
    #[error("This policy contains invalid Json")]
    InvalidJson,
    #[error("Normalized policy document exceeds the maximum allowed size of 20480 bytes")]
    TooLarge,
    #[error("Invalid policy syntax.")]
    Syntax,
    #[error("The policy must contain a valid version string")]
    Version,
    #[error("Missing required field Statement")]
    MissingStatement,
    #[error("Could not parse the policy: Statement is empty!")]
    EmptyStatement,
    #[error("Missing required field Effect")]
    MissingEffect,
    #[error("Invalid effect: {0}")]
    Effect(String),
    #[error("Missing required field Principal")]
    MissingPrincipal,
    #[error("Invalid principal in policy")]
    Principal,
    #[error("Missing required field Action")]
    MissingAction,
    #[error("Policy has invalid action")]
    Action,
    #[error("Missing required field Resource")]
    MissingResource,
    #[error("Policy has invalid resource")]
    Resource,
    #[error("Action does not apply to any resource(s) in statement")]
    NotApplicable,
    #[error("Invalid Condition type : {0}")]
    ConditionType(String),
    #[error("Policy has an invalid condition key")]
    ConditionKey,
    #[error("Conditions do not apply to combination of actions and resources in statement")]
    ConditionNotApplicable,
    #[error("Invalid IP address in Conditions")]
    IpAddress,
    #[error("Invalid value in Conditions for operator {0}")]
    ConditionValue(&'static str),
    /// An identity policy naming a principal: IAM's MalformedPolicyDocument (research/91 §3).
    #[error("Policy document should not specify a principal.")]
    IdentityPrincipal,
    /// An identity policy past IAM's 2,048 characters for a user (research/91 §2).
    #[error("Maximum policy size of 2048 characters, white space not counted, exceeded")]
    IdentityTooLarge,
    /// A document past IAM's `PolicyDocument`: longer than 131,072 characters, or holding a
    /// character its pattern `[\u0009\u000A\u000D -ÿ]+` refuses (research/91 §2).
    #[error(
        "The policy document is not 1 to 131072 characters of tab, line feed, carriage return and U+0020 to U+00FF"
    )]
    Document,
}

impl PolicyError {
    /// `400 MalformedPolicy`, S3's code for every fault of a policy (17 §2.3).
    pub fn code(&self) -> (&'static str, u16) {
        ("MalformedPolicy", 400)
    }
}

/// What a policy is checked as: a bucket's, its resources within that bucket, or a principal's
/// identity policy.
#[derive(Clone, Copy)]
enum Grammar<'a> {
    Bucket(&'a str),
    Identity,
}

/// A PutBucketPolicy body for `bucket`, read and checked. The first fault found is the answer:
/// the body's form as JSON, its normalized size, then the document's members, then each
/// statement in order (17 §2.3, §3). The caller bounds the body by [`BODY_LIMIT`] and keeps it
/// as sent, since GetBucketPolicy gives back the bytes set, as s3-tests expects (17 §2.2).
pub fn parse(body: &[u8], bucket: &str) -> Result<Policy, PolicyError> {
    if body.first() != Some(&b'{') {
        return Err(PolicyError::NotJson);
    }
    let document = json::parse(body).map_err(|_| PolicyError::InvalidJson)?;
    if document.compact().len() > MAX_SIZE {
        return Err(PolicyError::TooLarge);
    }
    checked(document.view(), Grammar::Bucket(bucket))
}

/// A principal's identity policy (research/91 §3), read and checked as IAM checks a user's inline
/// policy: the document IAM's `PolicyDocument` takes, at most 131,072 characters of its pattern;
/// JSON whose compact form, white space between its tokens not counted, holds at most 2,048
/// characters; then its members and statements as a bucket policy's, but that a statement names
/// no principal, its resources are `*` or any S3 ARN, and its actions may be those on no
/// resource. The policy and its compact form, the text a cell keeps of it.
pub fn identity(document: &[u8]) -> Result<(Policy, String), PolicyError> {
    let text = std::str::from_utf8(document).map_err(|_| PolicyError::Document)?;
    let admitted = |c: char| matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{ff}');
    if text.is_empty() || text.chars().count() > DOCUMENT_CHARS || !text.chars().all(admitted) {
        return Err(PolicyError::Document);
    }
    if document.first() != Some(&b'{') {
        return Err(PolicyError::NotJson);
    }
    let parsed = json::parse(document).map_err(|_| PolicyError::InvalidJson)?;
    let compact = parsed.compact();
    if compact.chars().count() > IDENTITY_CHARS {
        return Err(PolicyError::IdentityTooLarge);
    }
    Ok((checked(parsed.view(), Grammar::Identity)?, compact))
}

/// A parsed document's members and statements, checked as `grammar` says.
fn checked(document: View<'_>, grammar: Grammar<'_>) -> Result<Policy, PolicyError> {
    let View::Object(members) = document else {
        return Err(PolicyError::NotJson);
    };
    let (mut variables, mut statement) = (false, None);
    for (name, value) in members.iter() {
        match name {
            "Version" => {
                variables = match value {
                    View::String("2012-10-17") => true,
                    View::String("2008-10-17") => false,
                    _ => return Err(PolicyError::Version),
                };
            }
            "Id" if matches!(value, View::String(_)) => {}
            "Statement" => statement = Some(value),
            _ => return Err(PolicyError::Syntax),
        }
    }
    let statements = match statement.ok_or(PolicyError::MissingStatement)? {
        View::Array(items) if items.is_empty() => return Err(PolicyError::EmptyStatement),
        View::Array(items) => items
            .iter()
            .map(|item| self::statement(item, grammar))
            .collect::<Result<Vec<_>, _>>()?,
        single @ View::Object(_) => vec![self::statement(single, grammar)?],
        _ => return Err(PolicyError::Syntax),
    };
    Ok(Policy {
        variables,
        identity: matches!(grammar, Grammar::Identity),
        statements,
    })
}

/// A principal's identity policy as a cell keeps it, its compact form ([`identity`]), read back:
/// its JSON and its grammar checked again, IAM's bounds on the document it came in having held
/// when it was set. A kept policy that does not read is the record's corruption.
pub fn kept(compact: &str) -> Result<Policy, PolicyError> {
    let parsed = json::parse(compact.as_bytes()).map_err(|_| PolicyError::InvalidJson)?;
    checked(parsed.view(), Grammar::Identity)
}

impl Policy {
    /// Whether the policy is public, as S3 judges it: "Amazon S3 begins by assuming that the
    /// policy is public. ... To be considered non-public, a bucket policy must grant access
    /// only to fixed values (values that don't contain a wildcard or an AWS Identity and Access
    /// Management Policy Variable)" of a principal, a source address range no broader than `/8`
    /// for IPv4 and `/32` for IPv6, or one of the keys that fix a source (17 §7). An allowing
    /// statement to everyone is public unless one of its conditions fixes such a key; one that
    /// names principals is not. A denying statement grants nothing.
    pub fn is_public(&self) -> bool {
        self.statements.iter().any(|statement| {
            statement.allow
                && statement.principals.everyone
                && !statement.conditions.iter().any(fixes)
        })
    }
}

/// Whether a condition holds a request to fixed values of a key S3 counts as confining it
/// (17 §7). A condition a request without the key satisfies, `...IfExists` or `ForAllValues`,
/// confines nothing.
fn fixes(condition: &Condition) -> bool {
    use Operator as O;
    if condition.if_exists || condition.set == Some(Set::All) {
        return false;
    }
    let fixed = condition
        .values
        .iter()
        .all(|value| !value.contains(['*', '?']) && !value.contains("${"));
    match condition.key.as_str() {
        "aws:sourceip" => {
            condition.operator == O::IpAddress
                && condition.values.iter().all(|value| {
                    cidr(value).is_some_and(|(address, prefix)| {
                        prefix >= if address.is_ipv4() { 8 } else { 32 }
                    })
                })
        }
        "aws:sourcearn"
        | "aws:sourcevpc"
        | "aws:sourcevpce"
        | "aws:sourceowner"
        | "aws:sourceaccount"
        | "aws:principalorgid"
        | "aws:principalarn"
        | "aws:principalaccount"
        | "aws:userid"
        | "s3:dataaccesspointarn"
        | "s3:dataaccesspointaccount" => {
            fixed
                && matches!(
                    condition.operator,
                    O::StringEquals
                        | O::StringEqualsIgnoreCase
                        | O::StringLike
                        | O::ArnEquals
                        | O::ArnLike
                )
        }
        _ => false,
    }
}

/// One statement: its elements, each pair of an element and its `Not` form exclusive, and
/// Principal, Effect, Action and Resource required (17 §3.1); an identity policy's names no
/// principal, "You cannot use the `Principal` element in an identity-based policy" (research/91
/// §3).
fn statement(value: View<'_>, grammar: Grammar<'_>) -> Result<Statement, PolicyError> {
    let View::Object(members) = value else {
        return Err(PolicyError::Syntax);
    };
    let (mut effect, mut principal, mut action, mut resource, mut condition) =
        (None, None, None, None, None);
    for (name, value) in members.iter() {
        let (field, not) = match name {
            "Sid" if matches!(value, View::String(_)) => continue,
            "Effect" => (&mut effect, false),
            "Principal" => (&mut principal, false),
            "NotPrincipal" => (&mut principal, true),
            "Action" => (&mut action, false),
            "NotAction" => (&mut action, true),
            "Resource" => (&mut resource, false),
            "NotResource" => (&mut resource, true),
            "Condition" => (&mut condition, false),
            _ => return Err(PolicyError::Syntax),
        };
        if field.replace((value, not)).is_some() {
            return Err(PolicyError::Syntax);
        }
    }
    let allow = match effect.ok_or(PolicyError::MissingEffect)?.0 {
        View::String("Allow") => true,
        View::String("Deny") => false,
        View::String(effect) => return Err(PolicyError::Effect(effect.to_owned())),
        _ => return Err(PolicyError::Syntax),
    };
    let principals = match (grammar, principal) {
        (Grammar::Identity, Some(_)) => return Err(PolicyError::IdentityPrincipal),
        (Grammar::Identity, None) => Principals::default(),
        (Grammar::Bucket(_), None) => return Err(PolicyError::MissingPrincipal),
        (Grammar::Bucket(_), Some((principal, not_principal))) => {
            let mut principals = principals(principal)?;
            principals.not = not_principal;
            // "`NotPrincipal` must be used with `"Effect":"Deny"`" (17 §6).
            if not_principal && allow {
                return Err(PolicyError::Principal);
            }
            principals
        }
    };
    // The actions a statement may name: a bucket policy's on a bucket or an object, an identity
    // policy's those too and the ones on no resource (research/91 §3).
    let known: &[&[Action]] = match grammar {
        Grammar::Bucket(_) => &[&catalog::ACTIONS],
        Grammar::Identity => &[&catalog::ACTIONS, &catalog::ACCOUNT_ACTIONS],
    };
    let catalogued = || known.iter().flat_map(|actions| actions.iter());
    let (action, not_action) = action.ok_or(PolicyError::MissingAction)?;
    let actions = strings(action)?
        .into_iter()
        .map(|pattern| action_pattern(&pattern, catalogued()))
        .collect::<Result<Vec<_>, _>>()?;
    let (resource, not_resource) = resource.ok_or(PolicyError::MissingResource)?;
    let resources = strings(resource)?;
    if resources.is_empty() {
        return Err(PolicyError::Resource);
    }
    for pattern in &resources {
        match grammar {
            Grammar::Bucket(bucket) => check_resource(pattern, bucket)?,
            Grammar::Identity => check_identity_resource(pattern)?,
        }
    }
    let named: Vec<&Action> = if not_action {
        catalogued().collect()
    } else {
        catalogued()
            .filter(|known| actions.iter().any(|pattern| names(pattern, known)))
            .collect()
    };
    // S3 refuses a bucket policy's statement none of whose actions acts on its resources; IAM
    // keeps such an identity statement, which grants nothing (research/91 §3).
    if matches!(grammar, Grammar::Bucket(_)) && !not_action && !not_resource {
        let kinds: Vec<Kind> = [Kind::Bucket, Kind::Object]
            .into_iter()
            .filter(|kind| resources.iter().any(|pattern| reaches(pattern, *kind)))
            .collect();
        let applies = actions.iter().all(|pattern| {
            named
                .iter()
                .any(|known| names(pattern, known) && kinds.contains(&known.kind))
        });
        if !applies {
            return Err(PolicyError::NotApplicable);
        }
    }
    let conditions = match condition {
        None => Vec::new(),
        Some((condition, _)) => conditions(condition, &named)?,
    };
    Ok(Statement {
        allow,
        principals,
        not_action,
        actions,
        not_resource,
        resources,
        conditions,
    })
}

/// An element's value: a string, or an array of them, "If the element takes an array ... but
/// only one value is included, the brackets are optional" (17 §3.1).
fn strings(value: View<'_>) -> Result<Vec<String>, PolicyError> {
    match value {
        View::String(text) => Ok(vec![text.to_owned()]),
        View::Array(items) => items
            .iter()
            .map(|item| match item {
                View::String(text) => Ok(text.to_owned()),
                _ => Err(PolicyError::Syntax),
            })
            .collect(),
        _ => Err(PolicyError::Syntax),
    }
}

/// A `Principal`: `"*"`, or a map of `AWS`, `CanonicalUser` and `Service` to one value or a
/// list (17 §3.1, §6).
fn principals(value: View<'_>) -> Result<Principals, PolicyError> {
    let mut principals = Principals::default();
    let members = match value {
        View::String("*") => {
            principals.everyone = true;
            return Ok(principals);
        }
        View::Object(members) if !members.is_empty() => members,
        _ => return Err(PolicyError::Principal),
    };
    for (name, value) in members.iter() {
        let listed = strings(value).map_err(|_| PolicyError::Principal)?;
        if listed.is_empty() || listed.iter().any(String::is_empty) {
            return Err(PolicyError::Principal);
        }
        match name {
            "AWS" if listed == ["*"] => principals.everyone = true,
            "AWS" => {
                for named in &listed {
                    principals.accounts.push(account(named)?);
                }
            }
            "CanonicalUser" => principals.canonical.extend(listed),
            "Service" if !listed.iter().any(|service| service == "*") => {
                principals.services.extend(listed);
            }
            _ => return Err(PolicyError::Principal),
        }
    }
    Ok(principals)
}

/// An account an `AWS` principal names: its 12-digit ID, or its root's ARN, which "behave the
/// same way" (17 §6).
fn account(named: &str) -> Result<String, PolicyError> {
    let id = named
        .strip_prefix("arn:aws:iam::")
        .and_then(|rest| rest.strip_suffix(":root"))
        .unwrap_or(named);
    if crate::account::valid_id(id) {
        Ok(id.to_owned())
    } else {
        Err(PolicyError::Principal)
    }
}

/// An action pattern: `*`, or `s3:` and a name holding `*` and `?` as wildcards, compared
/// without regard to case, which names at least one of the actions `known`. S3 refused an action
/// it does not define, "Policy has invalid action" (17 §2.3, §3.2).
fn action_pattern<'k>(
    pattern: &str,
    mut known: impl Iterator<Item = &'k Action>,
) -> Result<String, PolicyError> {
    let lower = pattern.to_ascii_lowercase();
    let lower = if lower == "*" {
        "s3:*".to_owned()
    } else {
        lower
    };
    if !lower.starts_with("s3:") || !known.any(|known| names(&lower, known)) {
        return Err(PolicyError::Action);
    }
    Ok(lower)
}

/// Whether a lowercase action pattern names `known`.
fn names(pattern: &str, known: &Action) -> bool {
    let name = format!("s3:{}", known.name.to_ascii_lowercase());
    wildcard(pattern, &name)
}

/// A resource: an S3 ARN whose bucket, the part before any `/`, can be this policy's bucket.
/// S3 refused `*`, and a bucket other than the policy's, "Policy has invalid resource"
/// (17 §2.3). A bucket part holding a policy variable is judged when the policy is applied.
fn check_resource(pattern: &str, bucket: &str) -> Result<(), PolicyError> {
    let rest = pattern
        .strip_prefix(ARN_PREFIX)
        .filter(|rest| !rest.is_empty())
        .ok_or(PolicyError::Resource)?;
    let named = rest.split_once('/').map_or(rest, |(named, _)| named);
    if named.contains("${") || wildcard(named, bucket) {
        Ok(())
    } else {
        Err(PolicyError::Resource)
    }
}

/// An identity policy's resource: `*`, or an S3 ARN naming anything after its prefix, any bucket
/// and any key. A cell serves S3 alone, so another service's ARN names nothing it has, and is
/// refused as invalid where IAM would keep a statement that grants nothing here (research/91 §3).
fn check_identity_resource(pattern: &str) -> Result<(), PolicyError> {
    if pattern == "*"
        || pattern
            .strip_prefix(ARN_PREFIX)
            .is_some_and(|rest| !rest.is_empty())
    {
        Ok(())
    } else {
        Err(PolicyError::Resource)
    }
}

/// Whether a resource pattern can match a resource of `kind`: a bucket's ARN holds no `/`, an
/// object's holds one after the bucket, and a `*` matches any run, `/` included (17 §3.2). No
/// pattern a bucket policy can name is S3's whole namespace, which only an identity policy grants.
fn reaches(pattern: &str, kind: Kind) -> bool {
    let rest = pattern.strip_prefix(ARN_PREFIX).unwrap_or(pattern);
    match kind {
        Kind::Bucket => !rest.contains('/'),
        Kind::Object => rest.contains('/') || rest.contains('*'),
        Kind::Account => false,
    }
}

/// A statement's `Condition`: operators, each a map of keys to values (17 §3.3). `named` is
/// every action the statement can apply to.
fn conditions(value: View<'_>, named: &[&Action]) -> Result<Vec<Condition>, PolicyError> {
    let View::Object(operators) = value else {
        return Err(PolicyError::Syntax);
    };
    let mut conditions = Vec::new();
    for (written, keys) in operators.iter() {
        let (operator, set, if_exists) = Operator::from_type(written)
            .ok_or_else(|| PolicyError::ConditionType(written.to_owned()))?;
        let View::Object(keys) = keys else {
            return Err(PolicyError::Syntax);
        };
        for (key, values) in keys.iter() {
            let key = key.to_ascii_lowercase();
            check_key(&key, named)?;
            let values = condition_values(values)?;
            for value in &values {
                check_value(operator, value)?;
            }
            conditions.push(Condition {
                operator,
                set,
                if_exists,
                key,
                values,
            });
        }
    }
    Ok(conditions)
}

/// A condition key: any global `aws:` key, whose list AWS extends; or one of S3's own keys that
/// comes with at least one of the statement's actions. S3 refused a key it does not define,
/// "Policy has an invalid condition key" (17 §2.3, §5).
fn check_key(key: &str, named: &[&Action]) -> Result<(), PolicyError> {
    if key
        .strip_prefix("aws:")
        .is_some_and(|rest| !rest.is_empty())
    {
        return Ok(());
    }
    if !key.starts_with("s3:") || !catalog::KEYS.iter().any(|(known, _)| same_key(known, key)) {
        return Err(PolicyError::ConditionKey);
    }
    let carried = named
        .iter()
        .any(|action| action.keys.iter().any(|known| same_key(known, key)));
    if carried {
        Ok(())
    } else {
        Err(PolicyError::ConditionNotApplicable)
    }
}

/// Whether `key`, lowercase, is the catalog's `known` key, or one of the family it names, such
/// as `s3:ExistingObjectTag/<key>` for `s3:existingobjecttag/security`.
fn same_key(known: &str, key: &str) -> bool {
    let known = known.to_ascii_lowercase();
    for family in ["/<key>", "/${tagkey}"] {
        if let Some(stem) = known.strip_suffix(family) {
            return key
                .strip_prefix(stem)
                .and_then(|rest| rest.strip_prefix('/'))
                .is_some_and(|tag| !tag.is_empty());
        }
    }
    known == key
}

/// A condition's values: strings, or numbers and booleans, for which "Quotation marks are
/// optional" (17 §3.1), one or a list of them.
fn condition_values(value: View<'_>) -> Result<Vec<String>, PolicyError> {
    let one = |value: View<'_>| match value {
        View::String(text) => Ok(text.to_owned()),
        View::Number(written) => Ok(written.to_owned()),
        View::Bool(true) => Ok("true".to_owned()),
        View::Bool(false) => Ok("false".to_owned()),
        _ => Err(PolicyError::Syntax),
    };
    match value {
        View::Array(items) if items.is_empty() => Err(PolicyError::Syntax),
        View::Array(items) => items.iter().map(one).collect(),
        single => Ok(vec![one(single)?]),
    }
}

/// A value its operator can compare: a number, a date, a boolean, an address range, or base64,
/// as the operator's kind requires (17 §3.3).
fn check_value(operator: Operator, value: &str) -> Result<(), PolicyError> {
    use Operator as O;
    let fits = match operator {
        O::NumericEquals
        | O::NumericNotEquals
        | O::NumericLessThan
        | O::NumericLessThanEquals
        | O::NumericGreaterThan
        | O::NumericGreaterThanEquals => number(value).is_some(),
        O::DateEquals
        | O::DateNotEquals
        | O::DateLessThan
        | O::DateLessThanEquals
        | O::DateGreaterThan
        | O::DateGreaterThanEquals => date(value).is_some(),
        O::Bool | O::Null => matches!(value.to_ascii_lowercase().as_str(), "true" | "false"),
        O::BinaryEquals => base64_valid(value),
        O::IpAddress | O::NotIpAddress => {
            return cidr(value).map(|_| ()).ok_or(PolicyError::IpAddress);
        }
        _ => true,
    };
    if fits {
        Ok(())
    } else {
        Err(PolicyError::ConditionValue(operator_name(operator)))
    }
}

fn operator_name(operator: Operator) -> &'static str {
    Operator::ALL
        .iter()
        .find(|(_, known)| *known == operator)
        .map_or("", |(name, _)| name)
}

/// A decimal number, integer or fraction, as numeric conditions compare them.
pub(crate) fn number(text: &str) -> Option<f64> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    let well_formed = !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && digits.bytes().filter(|b| *b == b'.').count() <= 1
        && digits != ".";
    if well_formed { text.parse().ok() } else { None }
}

/// A date as date conditions take one: an ISO 8601 time with its zone, a day as `YYYY-MM-DD`,
/// midnight UTC, or Unix seconds (17 §3.3); as Unix seconds.
pub(crate) fn date(text: &str) -> Option<i64> {
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        return text.parse().ok();
    }
    if let Some((seconds, _)) = crate::time::parse_iso8601(text) {
        return Some(seconds);
    }
    crate::time::parse_iso8601(&format!("{text}T00:00:00Z")).map(|(seconds, _)| seconds)
}

fn base64_valid(text: &str) -> bool {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .is_ok()
}

/// An address range: an IPv4 or IPv6 address with an optional prefix length, which defaults to
/// the whole address, `/32` for IPv4 as IAM documents (17 §3.3), and `/128` for IPv6 by the same
/// rule (DERIVED).
pub(crate) fn cidr(text: &str) -> Option<(std::net::IpAddr, u8)> {
    let (address, prefix) = match text.split_once('/') {
        Some((address, prefix)) => (address, Some(prefix)),
        None => (text, None),
    };
    let address: std::net::IpAddr = address.parse().ok()?;
    let most = if address.is_ipv4() { 32 } else { 128 };
    let prefix = match prefix {
        None => most,
        Some(prefix) if !prefix.is_empty() && prefix.bytes().all(|b| b.is_ascii_digit()) => {
            prefix.parse().ok().filter(|p| *p <= most)?
        }
        Some(_) => return None,
    };
    Some((address, prefix))
}

/// Whether `text` matches `pattern`, in which `*` stands for any run of characters, the empty
/// one and `/` and `:` included, and `?` for any one character (17 §3.2). A greedy match that
/// returns to the last `*` on a mismatch: its work is at most the product of the two lengths.
pub(crate) fn wildcard(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let (mut p, mut t) = (0usize, 0usize);
    let mut resume: Option<(usize, usize)> = None;
    while t < text.len() {
        match (pattern.get(p), text.get(t)) {
            (Some('*'), _) => {
                p = p.saturating_add(1);
                resume = Some((p, t));
            }
            (Some(c), Some(d)) if *c == '?' || c == d => {
                p = p.saturating_add(1);
                t = t.saturating_add(1);
            }
            _ => match resume {
                Some((after_star, from)) => {
                    let from = from.saturating_add(1);
                    p = after_star;
                    t = from;
                    resume = Some((after_star, from));
                }
                None => return false,
            },
        }
    }
    pattern
        .get(p..)
        .is_some_and(|rest| rest.iter().all(|c| *c == '*'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(policy: &str) -> Result<Policy, PolicyError> {
        parse(policy.as_bytes(), "bucket")
    }

    /// A statement as s3-tests' `make_json_policy` writes one, through Python's `json.dumps`.
    fn statement_with(action: &str, resource: &str, extra: &str) -> String {
        format!(
            "{{\"Version\": \"2012-10-17\", \"Statement\": [{{\"Action\": {action}, \"Principal\": \
             {{\"AWS\": \"*\"}}, \"Effect\": \"Allow\", \"Resource\": {resource}{extra}}}]}}"
        )
    }

    /// AWS's PutBucketPolicy sample, and the policy s3-tests' `test_bucket_policy` sets.
    #[test]
    fn samples_are_read() {
        let sample = r#"{
"Version":"2008-10-17",
"Id":"aaaa-bbbb-cccc-dddd",
"Statement" : [
    {
        "Effect":"Allow",
        "Sid":"1",
        "Principal" : {
            "AWS":["111122223333","444455556666"]
        },
        "Action":["s3:*"],
        "Resource":"arn:aws:s3:::bucket/*"
    }
 ]
}"#;
        let policy = read(sample).unwrap();
        assert!(!policy.variables);
        let statement = &policy.statements[0];
        assert!(statement.allow);
        assert_eq!(
            statement.principals.accounts,
            ["111122223333", "444455556666"]
        );
        assert_eq!(statement.actions, ["s3:*"]);
        let listing = statement_with(
            "\"s3:ListBucket\"",
            "[\"arn:aws:s3:::bucket\", \"arn:aws:s3:::bucket/*\"]",
            "",
        );
        let policy = read(&listing).unwrap();
        assert!(policy.variables);
        assert!(policy.statements[0].principals.everyone);
        // s3-tests' `test_multipart_upload_on_a_bucket_with_policy`: every action.
        let every = statement_with(
            "\"*\"",
            "[\"arn:aws:s3:::bucket\", \"arn:aws:s3:::bucket/*\"]",
            "",
        );
        assert!(read(&every).is_ok());
        // A single statement need not be in a list.
        let single = r#"{"Statement": {"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/secret/*"}}"#;
        assert!(!read(single).unwrap().statements[0].allow);
    }

    /// S3's recorded answers to documents that are not policies (17 §2.3).
    #[test]
    fn what_s3_refused_is_refused_as_s3_refused_it() {
        let message = |policy: &str| read(policy).unwrap_err().to_string();
        assert_eq!(
            message(""),
            "Policies must be valid JSON and the first byte must be '{'"
        );
        assert_eq!(
            message("invalid json"),
            "Policies must be valid JSON and the first byte must be '{'"
        );
        assert_eq!(message("{}"), "Missing required field Statement");
        assert_eq!(
            message("{\"Statement\": []}"),
            "Could not parse the policy: Statement is empty!"
        );
        assert_eq!(
            read("{\"Statement\": [}").unwrap_err(),
            PolicyError::InvalidJson
        );
        assert_eq!(
            read("{\"Statement\": 1, \"Statement\": 2}").unwrap_err(),
            PolicyError::InvalidJson
        );
        let other_bucket = statement_with("\"s3:GetObject\"", "\"arn:aws:s3:::other/*\"", "");
        assert_eq!(message(&other_bucket), "Policy has invalid resource");
        let star = statement_with("\"s3:GetObject\"", "[\"*\"]", "");
        assert_eq!(message(&star), "Policy has invalid resource");
        let on_bucket = statement_with("\"s3:PutObject\"", "\"arn:aws:s3:::bucket\"", "");
        assert_eq!(
            message(&on_bucket),
            "Action does not apply to any resource(s) in statement"
        );
        let unknown_key = statement_with(
            "\"s3:GetObject\"",
            "\"arn:aws:s3:::bucket/*\"",
            ", \"Condition\": {\"StringEquals\": {\"s3:VersionStatus\": \"Enabled\"}}",
        );
        assert_eq!(message(&unknown_key), "Policy has an invalid condition key");
        let unknown_action = statement_with(
            "\"s3:PutObjectLockConfiguration\"",
            "\"arn:aws:s3:::bucket\"",
            "",
        );
        assert_eq!(message(&unknown_action), "Policy has invalid action");
        let null = statement_with("null", "\"arn:aws:s3:::bucket\"", "");
        assert_eq!(message(&null), "Invalid policy syntax.");
        for error in [
            PolicyError::NotJson,
            PolicyError::Resource,
            PolicyError::Syntax,
        ] {
            assert_eq!(error.code(), ("MalformedPolicy", 400));
        }
    }

    /// The size limit is on the compact form, so white space does not count (17 §2.3).
    #[test]
    fn the_size_limit_is_on_the_normalized_document() {
        let resources = |count: usize| {
            let listed: Vec<String> = (0..count)
                .map(|i| format!("\"arn:aws:s3:::bucket/{i:04}/*\""))
                .collect();
            format!("[{}]", listed.join(",          "))
        };
        let within = statement_with("\"s3:GetObject\"", &resources(600), "");
        assert!(within.len() > MAX_SIZE);
        assert!(read(&within).is_ok());
        let over = statement_with("\"s3:GetObject\"", &resources(800), "");
        assert_eq!(read(&over), Err(PolicyError::TooLarge));
    }

    #[test]
    fn statements_are_checked_element_by_element() {
        let code = |policy: &str| read(policy).map(|_| ()).map_err(|e| e.to_string());
        let base = |effect: &str, principal: &str| {
            format!(
                "{{\"Statement\": [{{\"Effect\": \"{effect}\", {principal}, \"Action\": \
                 \"s3:GetObject\", \"Resource\": \"arn:aws:s3:::bucket/*\"}}]}}"
            )
        };
        assert_eq!(
            code(&base("allow", "\"Principal\": \"*\"")),
            Err("Invalid effect: allow".into())
        );
        // s3-tests' `test_bucket_policy_allow_notprincipal`.
        assert_eq!(
            code(&base(
                "Allow",
                "\"NotPrincipal\": {\"AWS\": \"111122223333\"}"
            )),
            Err("Invalid principal in policy".into())
        );
        assert!(
            code(&base(
                "Deny",
                "\"NotPrincipal\": {\"AWS\": \"111122223333\"}"
            ))
            .is_ok()
        );
        for principal in [
            "\"Principal\": {\"AWS\": \"arn:aws:iam::111122223333:user/alice\"}",
            "\"Principal\": {\"AWS\": \"AIDAJQABLZS4A3QDU576Q\"}",
            "\"Principal\": {\"AWS\": [\"*\", \"111122223333\"]}",
            "\"Principal\": {\"AWS\": []}",
            "\"Principal\": {\"Federated\": \"cognito-identity.amazonaws.com\"}",
            "\"Principal\": {\"Service\": \"*\"}",
            "\"Principal\": \"someone\"",
            "\"Principal\": {}",
        ] {
            assert_eq!(
                code(&base("Allow", principal)),
                Err("Invalid principal in policy".into()),
                "{principal}"
            );
        }
        for principal in [
            "\"Principal\": {\"AWS\": \"arn:aws:iam::111122223333:root\"}",
            "\"Principal\": {\"CanonicalUser\": \"79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be\"}",
            "\"Principal\": {\"Service\": \"logging.s3.amazonaws.com\"}",
        ] {
            assert!(code(&base("Allow", principal)).is_ok(), "{principal}");
        }
        let versioned = |version: &str| {
            format!(
                "{{\"Version\": \"{version}\", \"Statement\": [{{\"Effect\": \"Allow\", \"Principal\": \"*\", \"Action\": \"s3:GetObject\", \"Resource\": \"arn:aws:s3:::bucket/*\"}}]}}"
            )
        };
        assert_eq!(
            code(&versioned("2012-10-16")),
            Err("The policy must contain a valid version string".into())
        );
        let missing = "{\"Statement\": [{\"Effect\": \"Allow\", \"Action\": \"s3:GetObject\", \"Resource\": \"arn:aws:s3:::bucket/*\"}]}";
        assert_eq!(
            code(missing),
            Err("Missing required field Principal".into())
        );
        let both = "{\"Statement\": [{\"Effect\": \"Allow\", \"Principal\": \"*\", \"Action\": \"s3:GetObject\", \"NotAction\": \"s3:PutObject\", \"Resource\": \"arn:aws:s3:::bucket/*\"}]}";
        assert_eq!(code(both), Err("Invalid policy syntax.".into()));
        let unknown = "{\"Statement\": [{\"Effect\": \"Allow\", \"Principal\": \"*\", \"Action\": \"s3:GetObject\", \"Resource\": \"arn:aws:s3:::bucket/*\", \"Extra\": 1}]}";
        assert_eq!(code(unknown), Err("Invalid policy syntax.".into()));
    }

    /// Condition types, keys and values, and the keys that come with the statement's actions
    /// (17 §2.3, §3.3, §5), with the conditions s3-tests' policies use.
    #[test]
    fn conditions_are_checked() {
        let with = |action: &str, condition: &str| {
            read(&statement_with(
                action,
                "[\"arn:aws:s3:::bucket\", \"arn:aws:s3:::bucket/*\"]",
                &format!(", \"Condition\": {condition}"),
            ))
            .map(|policy| policy.statements[0].conditions.clone())
            .map_err(|e| e.to_string())
        };
        let accepted = [
            (
                "\"s3:GetObject\"",
                r#"{"StringEquals": {"s3:ExistingObjectTag/security": "public"}}"#,
            ),
            (
                "\"s3:PutObject\"",
                r#"{"StringLike": {"s3:x-amz-copy-source": "bucket/public/*"}}"#,
            ),
            (
                "\"s3:PutObject\"",
                r#"{"StringEquals": {"s3:x-amz-metadata-directive": "COPY"}}"#,
            ),
            (
                "\"s3:PutObject\"",
                r#"{"StringLike": {"s3:x-amz-acl": ["public*"]}}"#,
            ),
            (
                "\"s3:GetObject\"",
                r#"{"StringLikeIfExists": {"aws:Referer": "http://www.example.com/*"}}"#,
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"StringLike": {"s3:prefix": "public/*"}, "NumericLessThanEquals": {"s3:max-keys": 10}}"#,
            ),
            (
                "\"s3:PutObject\"",
                r#"{"Null": {"s3:x-amz-server-side-encryption": "true"}}"#,
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"IpAddress": {"aws:SourceIp": ["10.0.0.0/32", "2001:db8::/64", "192.0.2.1"]}}"#,
            ),
            (
                "\"s3:GetObject\"",
                r#"{"Bool": {"aws:SecureTransport": false}}"#,
            ),
            (
                "\"s3:GetObject\"",
                r#"{"DateGreaterThan": {"aws:CurrentTime": "2019-07-16T12:00:00Z"}}"#,
            ),
            (
                "\"s3:PutObject\"",
                r#"{"ForAnyValue:StringEquals": {"s3:RequestObjectTagKeys": ["a", "b"]}}"#,
            ),
            ("\"*\"", r#"{"StringLike": {"s3:prefix": "x"}}"#),
        ];
        for (action, condition) in accepted {
            assert!(
                with(action, condition).is_ok(),
                "{action} {condition}: {:?}",
                with(action, condition)
            );
        }
        let conditions = with(
            "\"s3:ListBucket\"",
            r#"{"StringLike": {"S3:Prefix": ["a", "b"]}}"#,
        )
        .unwrap();
        assert_eq!(conditions[0].key, "s3:prefix");
        assert_eq!(conditions[0].values, ["a", "b"]);
        let refused = [
            (
                "\"s3:GetObject\"",
                r#"{"stringequals": {"aws:Referer": "x"}}"#,
                "Invalid Condition type : stringequals",
            ),
            (
                "\"s3:GetObject\"",
                r#"{"NullIfExists": {"aws:Referer": "true"}}"#,
                "Invalid Condition type : NullIfExists",
            ),
            (
                "\"s3:GetObject\"",
                r#"{"ForSomeValues:StringEquals": {"aws:Referer": "x"}}"#,
                "Invalid Condition type : ForSomeValues:StringEquals",
            ),
            (
                "\"s3:GetObject\"",
                r#"{"StringEquals": {"iam:PassedToService": "x"}}"#,
                "Policy has an invalid condition key",
            ),
            (
                "\"s3:GetObject\"",
                r#"{"StringLike": {"s3:prefix": "x"}}"#,
                "Conditions do not apply to combination of actions and resources in statement",
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"IpAddress": {"aws:SourceIp": "10.0.0.0/33"}}"#,
                "Invalid IP address in Conditions",
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"NumericLessThan": {"s3:max-keys": "ten"}}"#,
                "Invalid value in Conditions for operator NumericLessThan",
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"StringEquals": {"s3:prefix": null}}"#,
                "Invalid policy syntax.",
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"StringEquals": {"s3:prefix": []}}"#,
                "Invalid policy syntax.",
            ),
            // An object or array where a value belongs is read, at the parser's deepest
            // level, and refused by the check (json::MAX_DEPTH).
            (
                "\"s3:ListBucket\"",
                r#"{"StringEquals": {"s3:prefix": [{}]}}"#,
                "Invalid policy syntax.",
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"StringEquals": {"s3:prefix": [[]]}}"#,
                "Invalid policy syntax.",
            ),
            (
                "\"s3:ListBucket\"",
                r#"{"StringEquals": {"s3:prefix": [[[]]]}}"#,
                "This policy contains invalid Json",
            ),
        ];
        for (action, condition, message) in refused {
            assert_eq!(
                with(action, condition),
                Err(message.to_owned()),
                "{condition}"
            );
        }
    }

    /// IAM's wildcard example: `*` spans `/`, and a pattern must be matched whole (17 §3.2).
    #[test]
    fn wildcards_match_as_iam_describes() {
        let pattern = "arn:aws:s3:::amzn-s3-demo-bucket/*/test/*";
        for matched in [
            "arn:aws:s3:::amzn-s3-demo-bucket/1///test///object.jpg",
            "arn:aws:s3:::amzn-s3-demo-bucket//test/object.jpg",
            "arn:aws:s3:::amzn-s3-demo-bucket/1/test/",
        ] {
            assert!(wildcard(pattern, matched), "{matched}");
        }
        for unmatched in [
            "arn:aws:s3:::amzn-s3-demo-bucket/1-test/object.jpg",
            "arn:aws:s3:::amzn-s3-demo-bucket/test/object.jpg",
            "arn:aws:s3:::amzn-s3-demo-bucket/1/2/test.jpg",
        ] {
            assert!(!wildcard(pattern, unmatched), "{unmatched}");
        }
        assert!(wildcard("s3:get*", "s3:getobject"));
        assert!(wildcard("s3:getobjec?", "s3:getobject"));
        assert!(!wildcard("s3:getobjec?", "s3:getobjectacl"));
        assert!(wildcard("a*b*c", "aXbYbZc"));
        assert!(wildcard("*", ""));
        assert!(!wildcard("?", ""));
        assert!(wildcard("é?", "éé"));
        let long = "a".repeat(2000);
        assert!(!wildcard(&"*a".repeat(100), &format!("{long}b")));
    }

    fn allow(principal: &str, condition: &str) -> Policy {
        let condition = if condition.is_empty() {
            String::new()
        } else {
            format!(", \"Condition\": {condition}")
        };
        read(&format!(
            "{{\"Version\": \"2012-10-17\", \"Statement\": [{{\"Effect\": \"Allow\", \
             \"Principal\": {principal}, \"Action\": \"s3:PutObject\", \
             \"Resource\": \"arn:aws:s3:::bucket/*\"{condition}}}]}}"
        ))
        .unwrap()
    }

    /// "The meaning of public" and its examples, and s3-tests' policy-status cases (17 §7, §9).
    #[test]
    fn public_is_as_s3_means_it() {
        let everyone = "{\"AWS\": \"*\"}";
        assert!(allow("\"*\"", "").is_public());
        assert!(allow(everyone, "").is_public());
        assert!(!allow("{\"AWS\": \"111122223333\"}", "").is_public());
        assert!(
            !allow(
                everyone,
                r#"{"IpAddress": {"aws:SourceIp": "10.0.0.0/32"}}"#
            )
            .is_public()
        );
        assert!(!allow(everyone, r#"{"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}}"#).is_public());
        assert!(allow(everyone, r#"{"IpAddress": {"aws:SourceIp": "0.0.0.0/1"}}"#).is_public());
        assert!(
            allow(
                everyone,
                r#"{"IpAddress": {"aws:SourceIp": "2001:db8::/31"}}"#
            )
            .is_public()
        );
        assert!(
            allow(
                everyone,
                r#"{"NotIpAddress": {"aws:SourceIp": "10.0.0.0/8"}}"#
            )
            .is_public()
        );
        assert!(
            !allow(
                everyone,
                r#"{"StringEquals": {"aws:SourceVpc": "vpc-91237329"}}"#
            )
            .is_public()
        );
        assert!(allow(everyone, r#"{"StringLike": {"aws:SourceVpc": "vpc-*"}}"#).is_public());
        assert!(
            allow(
                everyone,
                r#"{"StringEqualsIfExists": {"aws:SourceVpc": "vpc-91237329"}}"#
            )
            .is_public()
        );
        assert!(
            allow(
                everyone,
                r#"{"StringEquals": {"aws:Referer": "http://example.com/"}}"#
            )
            .is_public()
        );
        assert!(
            !allow(
                everyone,
                r#"{"StringEquals": {"aws:PrincipalOrgID": "o-a1b2c3"}}"#
            )
            .is_public()
        );
        // A denying statement grants nothing: s3-tests puts one under BlockPublicPolicy.
        let deny = read(
            r#"{"Statement": [{"Effect": "Deny", "Principal": {"AWS": "*"}, "Action":
                "s3:GetBucketPublicAccessBlock", "Resource": "arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        assert!(!deny.is_public());
    }

    /// s3-tests' `test_block_public_policy` and `..._with_principal`: BlockPublicPolicy refuses a
    /// public policy, 403 `AccessDenied`, and admits one naming its principals (17 §7, §9).
    #[test]
    fn block_public_policy_refuses_public_policies() {
        let block = PublicAccessBlock::NEW_BUCKET;
        let public = allow("{\"AWS\": \"*\"}", "");
        assert_eq!(may_set(&public, Some(&block)), Err(PublicPolicyBlocked));
        assert_eq!(PublicPolicyBlocked.code(), ("AccessDenied", 403));
        assert!(may_set(&allow("{\"AWS\": \"111122223333\"}", ""), Some(&block)).is_ok());
        let off = PublicAccessBlock {
            block_public_policy: false,
            ..block
        };
        assert!(may_set(&public, Some(&off)).is_ok());
        assert!(may_set(&public, None).is_ok());
    }

    /// Every action a routed operation asks for is one the catalog lists, on the kind of
    /// resource the operation names: a bucket, or with a key an object.
    #[test]
    fn every_operation_asks_for_an_action_the_catalog_lists() {
        use crate::route::Operation as O;
        let bucket_level = [
            O::CreateBucket,
            O::DeleteBucket,
            O::HeadBucket,
            O::GetBucketLocation,
            O::GetBucketVersioning,
            O::PutBucketVersioning,
            O::GetBucketTagging,
            O::PutBucketTagging,
            O::DeleteBucketTagging,
            O::GetBucketAcl,
            O::PutBucketAcl,
            O::GetBucketOwnershipControls,
            O::PutBucketOwnershipControls,
            O::DeleteBucketOwnershipControls,
            O::GetBucketLifecycleConfiguration,
            O::PutBucketLifecycleConfiguration,
            O::DeleteBucketLifecycle,
            O::GetBucketCors,
            O::PutBucketCors,
            O::DeleteBucketCors,
            O::GetBucketPolicy,
            O::PutBucketPolicy,
            O::DeleteBucketPolicy,
            O::GetBucketPolicyStatus,
            O::GetPublicAccessBlock,
            O::PutPublicAccessBlock,
            O::DeletePublicAccessBlock,
            O::ListObjects,
            O::ListObjectsV2,
            O::ListObjectVersions,
            O::ListMultipartUploads,
            O::GetObjectLockConfiguration,
            O::PutObjectLockConfiguration,
            O::GetBucketEncryption,
            O::PutBucketEncryption,
            O::DeleteBucketEncryption,
        ];
        let object_level = [
            O::DeleteObjects,
            O::PutObject,
            O::PostObject,
            O::CopyObject,
            O::GetObject,
            O::HeadObject,
            O::DeleteObject,
            O::GetObjectAttributes,
            O::GetObjectTagging,
            O::PutObjectTagging,
            O::DeleteObjectTagging,
            O::GetObjectAcl,
            O::PutObjectAcl,
            O::CreateMultipartUpload,
            O::UploadPart,
            O::UploadPartCopy,
            O::CompleteMultipartUpload,
            O::AbortMultipartUpload,
            O::ListParts,
            O::GetObjectRetention,
            O::PutObjectRetention,
            O::GetObjectLegalHold,
            O::PutObjectLegalHold,
        ];
        let kind_of = |name: &str| {
            catalog::ACTIONS
                .iter()
                .find(|known| format!("s3:{}", known.name) == name)
                .map(|known| known.kind)
        };
        for (operations, kind) in [
            (&bucket_level[..], Kind::Bucket),
            (&object_level[..], Kind::Object),
        ] {
            for operation in operations {
                for versioned in [false, true] {
                    let action = action(*operation, versioned).unwrap();
                    assert_eq!(kind_of(action), Some(kind), "{operation:?} {action}");
                }
            }
        }
        assert_eq!(action(O::GetObject, true), Some("s3:GetObjectVersion"));
        assert_eq!(action(O::ListBuckets, false), None);
        assert_eq!(action(O::Preflight, false), None);
        assert_eq!(resource("b", Some("a/b:c")), "arn:aws:s3:::b/a/b:c");
        assert_eq!(resource("b", None), "arn:aws:s3:::b");
        // A principal's ListBuckets asks for the action on no resource (research/91 §3).
        assert_eq!(
            identity_action(O::ListBuckets, false),
            Some("s3:ListAllMyBuckets")
        );
        assert_eq!(identity_action(O::Preflight, false), None);
        assert_eq!(
            identity_action(O::GetObject, true),
            Some("s3:GetObjectVersion")
        );
        let account = catalog::ACCOUNT_ACTIONS
            .iter()
            .find(|known| known.name == "ListAllMyBuckets")
            .map(|known| known.kind);
        assert_eq!(account, Some(Kind::Account));
        // A listing's keys come with the listings that carry them, and with no other.
        for key in ["s3:prefix", "s3:delimiter", "s3:max-keys"] {
            assert!(carries("s3:ListBucket", key), "{key}");
            assert!(carries("s3:ListBucketVersions", key), "{key}");
            assert!(!carries("s3:ListBucketMultipartUploads", key), "{key}");
            assert!(!carries("s3:GetObject", key), "{key}");
        }
        assert!(carries("s3:ListAllMyBuckets", "s3:authType"));
        assert!(!carries("ListBucket", "s3:prefix"));
    }

    fn identity_of(document: &str) -> Result<Policy, PolicyError> {
        identity(document.as_bytes()).map(|(policy, _)| policy)
    }

    /// AWS's own identity policies read as IAM reads them (research/91 §3): the home-directory
    /// example's three statements, the evaluation example's, and the variables example's.
    #[test]
    fn aws_identity_policies_are_read() {
        let home = r#"{"Version":"2012-10-17","Statement":[
            {"Sid":"S3ConsoleAccess","Effect":"Allow","Action":["s3:GetAccountPublicAccessBlock",
             "s3:GetBucketAcl","s3:GetBucketLocation","s3:GetBucketPolicyStatus",
             "s3:GetBucketPublicAccessBlock","s3:ListAccessPoints","s3:ListAllMyBuckets"],
             "Resource":"*"},
            {"Sid":"ListObjectsInBucket","Effect":"Allow","Action":"s3:ListBucket",
             "Resource":"arn:aws:s3:::amzn-s3-demo-bucket",
             "Condition":{"StringLike":{"s3:prefix":["","home/","home/${aws:username}/*"]}}},
            {"Effect":"Allow","Action":"s3:*","Resource":[
             "arn:aws:s3:::amzn-s3-demo-bucket/home/${aws:username}",
             "arn:aws:s3:::amzn-s3-demo-bucket/home/${aws:username}/*"]}]}"#;
        let policy = identity_of(home).unwrap();
        assert!(policy.identity && policy.variables);
        assert_eq!(policy.statements.len(), 3);
        assert_eq!(policy.statements[0].resources, ["*"]);
        assert_eq!(policy.statements[1].conditions[0].key, "s3:prefix");
        assert!(!policy.is_public());

        let carlos = r#"{"Version":"2012-10-17","Statement":[
            {"Sid":"AllowS3ListRead","Effect":"Allow","Action":["s3:GetBucketLocation",
             "s3:GetAccountPublicAccessBlock","s3:ListAccessPoints","s3:ListAllMyBuckets"],
             "Resource":"arn:aws:s3:::*"},
            {"Sid":"AllowS3Self","Effect":"Allow","Action":"s3:*","Resource":[
             "arn:aws:s3:::amzn-s3-demo-bucket-carlossalazar/*",
             "arn:aws:s3:::amzn-s3-demo-bucket-carlossalazar"]},
            {"Sid":"DenyS3Logs","Effect":"Deny","Action":"s3:*","Resource":"arn:aws:s3:::*log*"}]}"#;
        assert_eq!(identity_of(carlos).unwrap().statements.len(), 3);

        let marketing = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:ListBucket"],
             "Resource":["arn:aws:s3:::amzn-s3-demo-bucket"],
             "Condition":{"StringLike":{"s3:prefix":["marketing/*"]}}},
            {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],
             "Resource":["arn:aws:s3:::amzn-s3-demo-bucket/marketing/*"]}]}"#;
        let (policy, compact) = identity(marketing.as_bytes()).unwrap();
        // The text a cell keeps is the compact form, which reads back as the same policy.
        assert!(!compact.contains('\n') && !compact.contains("  "));
        assert_eq!(kept(&compact).unwrap(), policy);
    }

    /// What IAM refuses of a user's policy is refused (research/91 §2, §3): a principal, a
    /// resource of another service, an action S3 does not define, a document past IAM's bounds.
    #[test]
    fn what_iam_refuses_of_an_identity_policy_is_refused() {
        let with =
            |statement: &str| format!(r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#);
        let refused = |statement: &str| identity_of(&with(statement)).unwrap_err();
        assert_eq!(
            refused(r#"{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}"#),
            PolicyError::IdentityPrincipal
        );
        assert_eq!(
            refused(
                r#"{"Effect":"Deny","NotPrincipal":{"AWS":"111122223333"},"Action":"s3:*","Resource":"*"}"#
            ),
            PolicyError::IdentityPrincipal
        );
        assert_eq!(
            refused(
                r#"{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:sqs:us-east-2:111122223333:queue1"}"#
            ),
            PolicyError::Resource
        );
        assert_eq!(
            refused(r#"{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::"}"#),
            PolicyError::Resource
        );
        assert_eq!(
            refused(r#"{"Effect":"Allow","Action":"s3:GetObjekt","Resource":"*"}"#),
            PolicyError::Action
        );
        assert_eq!(
            refused(r#"{"Effect":"Allow","Action":"sqs:SendMessage","Resource":"*"}"#),
            PolicyError::Action
        );
        assert_eq!(
            refused(r#"{"Effect":"Allow","Action":"s3:GetObject"}"#),
            PolicyError::MissingResource
        );
        // A statement whose actions act on none of its resources is kept, granting nothing, as
        // IAM keeps one; a bucket policy's is refused (17 §2.3).
        assert!(
            identity_of(&with(
                r#"{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b"}"#
            ))
            .is_ok()
        );
        // The account-level actions take their own condition keys, and no bucket's.
        assert!(
            identity_of(&with(
                r#"{"Effect":"Allow","Action":"s3:ListAllMyBuckets","Resource":"*",
                    "Condition":{"StringEquals":{"s3:authType":"REST-HEADER"}}}"#
            ))
            .is_ok()
        );
        assert_eq!(
            refused(
                r#"{"Effect":"Allow","Action":"s3:ListAllMyBuckets","Resource":"*",
                    "Condition":{"StringLike":{"s3:prefix":"a/*"}}}"#
            ),
            PolicyError::ConditionNotApplicable
        );

        // 2,048 characters of compact form, white space not counted, and no more.
        let at = |chars: usize| {
            let start = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::"#;
            let end = r#""}]}"#;
            let fill = chars - start.len() - end.len();
            format!("{start}{}{end}", "a".repeat(fill))
        };
        assert!(identity_of(&at(IDENTITY_CHARS)).is_ok());
        assert_eq!(
            identity_of(&at(IDENTITY_CHARS + 1)).unwrap_err(),
            PolicyError::IdentityTooLarge
        );
        let spaced = at(IDENTITY_CHARS).replace(",", ",      ");
        assert!(identity_of(&spaced).is_ok(), "white space is not counted");
        // IAM's PolicyDocument: 131,072 characters of its pattern at most.
        let padded = format!("{}{}", at(256), " ".repeat(DOCUMENT_CHARS - 256));
        assert!(identity_of(&padded).is_ok());
        let over = format!("{} ", padded);
        assert_eq!(identity_of(&over).unwrap_err(), PolicyError::Document);
        for refused_char in ['\u{0}', '\u{b}', '\u{100}', '\u{1F600}'] {
            let document = at(256).replace("Allow", &format!("Al{refused_char}low"));
            assert_eq!(
                identity_of(&document).unwrap_err(),
                PolicyError::Document,
                "{refused_char:?}"
            );
        }
        assert_eq!(identity_of("").unwrap_err(), PolicyError::Document);
        assert_eq!(
            identity(&[b'{', 0xff, b'}']).unwrap_err(),
            PolicyError::Document
        );
        // Latin-1 characters are IAM's, each a character however many octets it takes.
        assert!(identity_of(&at(256).replace(":::a", ":::é")).is_ok());
    }
}
