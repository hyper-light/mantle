//! Every error code an S3 response can name (docs/research/86 [3]): the 80 of the S3 API
//! Reference's `Error` type, and the 14 more mantle's S3 listener answers with where S3 names a
//! code its reference does not list (`mantle_node::listener`; `mantle_s3`), each a variant, so an
//! error the S3 door answers is typed as S3 names it. A code past these is
//! [`crate::client::Error::UnlistedCode`], said as it came.

/// An S3 error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    /// Answered by mantle's S3 listener; not in the reference's list.
    AccessControlListNotSupported,
    AccessDenied,
    /// Answered by mantle's S3 listener; not in the reference's list.
    AccessForbidden,
    AccountProblem,
    AllAccessDisabled,
    AmbiguousGrantByEmailAddress,
    AuthorizationHeaderMalformed,
    /// Answered by mantle's S3 listener; not in the reference's list.
    AuthorizationQueryParametersError,
    BadDigest,
    /// Answered by mantle's S3 listener; not in the reference's list.
    BadRequest,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    CredentialsNotSupported,
    CrossLocationLoggingProhibited,
    EntityTooLarge,
    EntityTooSmall,
    ExpiredToken,
    /// Answered by mantle's S3 listener; not in the reference's list.
    HttpVersionNotSupported,
    /// Answered by mantle's S3 listener; not in the reference's list.
    IllegalLocationConstraintException,
    IllegalVersioningConfigurationException,
    IncompleteBody,
    IncorrectNumberOfFilesInPostRequest,
    InlineDataTooLarge,
    /// Answered by mantle's S3 listener; not in the reference's list.
    InsufficientStorage,
    InternalError,
    InvalidAccessKeyId,
    InvalidAddressingHeader,
    InvalidArgument,
    /// Answered by mantle's S3 listener; not in the reference's list.
    InvalidBucketAclWithObjectOwnership,
    InvalidBucketName,
    /// Answered by mantle's S3 listener; not in the reference's list.
    InvalidBucketOwnerAWSAccountID,
    InvalidBucketState,
    InvalidDigest,
    InvalidEncryptionAlgorithmError,
    InvalidLocationConstraint,
    InvalidObjectState,
    InvalidPart,
    InvalidPartOrder,
    InvalidPayer,
    InvalidPolicyDocument,
    InvalidRange,
    InvalidRequest,
    InvalidSOAPRequest,
    InvalidSecurity,
    InvalidStorageClass,
    /// Answered by mantle's S3 listener; not in the reference's list.
    InvalidTag,
    InvalidTargetBucketForLogging,
    InvalidToken,
    InvalidURI,
    KeyTooLongError,
    MalformedACLError,
    MalformedPOSTRequest,
    /// Answered by mantle's S3 listener; not in the reference's list.
    MalformedPolicy,
    MalformedXML,
    MaxMessageLengthExceeded,
    /// Answered by mantle's S3 listener; not in the reference's list.
    MaxPostPreDataLengthExceeded,
    MaxPostPreDataLengthExceededError,
    MetadataTooLarge,
    MethodNotAllowed,
    MissingAttachment,
    MissingContentLength,
    MissingRequestBodyError,
    MissingSecurityElement,
    MissingSecurityHeader,
    NoLoggingStatusForKey,
    NoSuchBucket,
    NoSuchBucketPolicy,
    NoSuchKey,
    NoSuchLifecycleConfiguration,
    NoSuchUpload,
    NoSuchVersion,
    NotImplemented,
    NotSignedUp,
    OperationAborted,
    PermanentRedirect,
    PreconditionFailed,
    Redirect,
    /// Answered by mantle's S3 listener; not in the reference's list.
    RequestHeaderSectionTooLarge,
    RequestIsNotMultiPartContent,
    RequestTimeTooSkewed,
    RequestTimeout,
    RequestTorrentOfBucketError,
    RestoreAlreadyInProgress,
    ServiceUnavailable,
    SignatureDoesNotMatch,
    SlowDown,
    TemporaryRedirect,
    TokenRefreshRequired,
    TooManyBuckets,
    UnexpectedContent,
    UnresolvableGrantByEmailAddress,
    UserKeyMustBeSpecified,
    /// Answered by mantle's S3 listener; not in the reference's list.
    XAmzContentSHA256Mismatch,
}

/// Each code and its name as a response's `Code` element carries it.
const CODES: [(Code, &str); 94] = [
    (
        Code::AccessControlListNotSupported,
        "AccessControlListNotSupported",
    ),
    (Code::AccessDenied, "AccessDenied"),
    (Code::AccessForbidden, "AccessForbidden"),
    (Code::AccountProblem, "AccountProblem"),
    (Code::AllAccessDisabled, "AllAccessDisabled"),
    (
        Code::AmbiguousGrantByEmailAddress,
        "AmbiguousGrantByEmailAddress",
    ),
    (
        Code::AuthorizationHeaderMalformed,
        "AuthorizationHeaderMalformed",
    ),
    (
        Code::AuthorizationQueryParametersError,
        "AuthorizationQueryParametersError",
    ),
    (Code::BadDigest, "BadDigest"),
    (Code::BadRequest, "BadRequest"),
    (Code::BucketAlreadyExists, "BucketAlreadyExists"),
    (Code::BucketAlreadyOwnedByYou, "BucketAlreadyOwnedByYou"),
    (Code::BucketNotEmpty, "BucketNotEmpty"),
    (Code::CredentialsNotSupported, "CredentialsNotSupported"),
    (
        Code::CrossLocationLoggingProhibited,
        "CrossLocationLoggingProhibited",
    ),
    (Code::EntityTooLarge, "EntityTooLarge"),
    (Code::EntityTooSmall, "EntityTooSmall"),
    (Code::ExpiredToken, "ExpiredToken"),
    (Code::HttpVersionNotSupported, "HttpVersionNotSupported"),
    (
        Code::IllegalLocationConstraintException,
        "IllegalLocationConstraintException",
    ),
    (
        Code::IllegalVersioningConfigurationException,
        "IllegalVersioningConfigurationException",
    ),
    (Code::IncompleteBody, "IncompleteBody"),
    (
        Code::IncorrectNumberOfFilesInPostRequest,
        "IncorrectNumberOfFilesInPostRequest",
    ),
    (Code::InlineDataTooLarge, "InlineDataTooLarge"),
    (Code::InsufficientStorage, "InsufficientStorage"),
    (Code::InternalError, "InternalError"),
    (Code::InvalidAccessKeyId, "InvalidAccessKeyId"),
    (Code::InvalidAddressingHeader, "InvalidAddressingHeader"),
    (Code::InvalidArgument, "InvalidArgument"),
    (
        Code::InvalidBucketAclWithObjectOwnership,
        "InvalidBucketAclWithObjectOwnership",
    ),
    (Code::InvalidBucketName, "InvalidBucketName"),
    (
        Code::InvalidBucketOwnerAWSAccountID,
        "InvalidBucketOwnerAWSAccountID",
    ),
    (Code::InvalidBucketState, "InvalidBucketState"),
    (Code::InvalidDigest, "InvalidDigest"),
    (
        Code::InvalidEncryptionAlgorithmError,
        "InvalidEncryptionAlgorithmError",
    ),
    (Code::InvalidLocationConstraint, "InvalidLocationConstraint"),
    (Code::InvalidObjectState, "InvalidObjectState"),
    (Code::InvalidPart, "InvalidPart"),
    (Code::InvalidPartOrder, "InvalidPartOrder"),
    (Code::InvalidPayer, "InvalidPayer"),
    (Code::InvalidPolicyDocument, "InvalidPolicyDocument"),
    (Code::InvalidRange, "InvalidRange"),
    (Code::InvalidRequest, "InvalidRequest"),
    (Code::InvalidSOAPRequest, "InvalidSOAPRequest"),
    (Code::InvalidSecurity, "InvalidSecurity"),
    (Code::InvalidStorageClass, "InvalidStorageClass"),
    (Code::InvalidTag, "InvalidTag"),
    (
        Code::InvalidTargetBucketForLogging,
        "InvalidTargetBucketForLogging",
    ),
    (Code::InvalidToken, "InvalidToken"),
    (Code::InvalidURI, "InvalidURI"),
    (Code::KeyTooLongError, "KeyTooLongError"),
    (Code::MalformedACLError, "MalformedACLError"),
    (Code::MalformedPOSTRequest, "MalformedPOSTRequest"),
    (Code::MalformedPolicy, "MalformedPolicy"),
    (Code::MalformedXML, "MalformedXML"),
    (Code::MaxMessageLengthExceeded, "MaxMessageLengthExceeded"),
    (
        Code::MaxPostPreDataLengthExceeded,
        "MaxPostPreDataLengthExceeded",
    ),
    (
        Code::MaxPostPreDataLengthExceededError,
        "MaxPostPreDataLengthExceededError",
    ),
    (Code::MetadataTooLarge, "MetadataTooLarge"),
    (Code::MethodNotAllowed, "MethodNotAllowed"),
    (Code::MissingAttachment, "MissingAttachment"),
    (Code::MissingContentLength, "MissingContentLength"),
    (Code::MissingRequestBodyError, "MissingRequestBodyError"),
    (Code::MissingSecurityElement, "MissingSecurityElement"),
    (Code::MissingSecurityHeader, "MissingSecurityHeader"),
    (Code::NoLoggingStatusForKey, "NoLoggingStatusForKey"),
    (Code::NoSuchBucket, "NoSuchBucket"),
    (Code::NoSuchBucketPolicy, "NoSuchBucketPolicy"),
    (Code::NoSuchKey, "NoSuchKey"),
    (
        Code::NoSuchLifecycleConfiguration,
        "NoSuchLifecycleConfiguration",
    ),
    (Code::NoSuchUpload, "NoSuchUpload"),
    (Code::NoSuchVersion, "NoSuchVersion"),
    (Code::NotImplemented, "NotImplemented"),
    (Code::NotSignedUp, "NotSignedUp"),
    (Code::OperationAborted, "OperationAborted"),
    (Code::PermanentRedirect, "PermanentRedirect"),
    (Code::PreconditionFailed, "PreconditionFailed"),
    (Code::Redirect, "Redirect"),
    (
        Code::RequestHeaderSectionTooLarge,
        "RequestHeaderSectionTooLarge",
    ),
    (
        Code::RequestIsNotMultiPartContent,
        "RequestIsNotMultiPartContent",
    ),
    (Code::RequestTimeTooSkewed, "RequestTimeTooSkewed"),
    (Code::RequestTimeout, "RequestTimeout"),
    (
        Code::RequestTorrentOfBucketError,
        "RequestTorrentOfBucketError",
    ),
    (Code::RestoreAlreadyInProgress, "RestoreAlreadyInProgress"),
    (Code::ServiceUnavailable, "ServiceUnavailable"),
    (Code::SignatureDoesNotMatch, "SignatureDoesNotMatch"),
    (Code::SlowDown, "SlowDown"),
    (Code::TemporaryRedirect, "TemporaryRedirect"),
    (Code::TokenRefreshRequired, "TokenRefreshRequired"),
    (Code::TooManyBuckets, "TooManyBuckets"),
    (Code::UnexpectedContent, "UnexpectedContent"),
    (
        Code::UnresolvableGrantByEmailAddress,
        "UnresolvableGrantByEmailAddress",
    ),
    (Code::UserKeyMustBeSpecified, "UserKeyMustBeSpecified"),
    (Code::XAmzContentSHA256Mismatch, "XAmzContentSHA256Mismatch"),
];

impl Code {
    /// The code a response's `Code` element names, if it is one S3 names.
    pub fn parse(name: &str) -> Option<Self> {
        CODES
            .iter()
            .find(|(_, listed)| *listed == name)
            .map(|(code, _)| *code)
    }

    /// The code's name, as S3 writes it.
    pub fn name(self) -> &'static str {
        CODES
            .iter()
            .find(|(code, _)| *code == self)
            .map_or("", |(_, name)| name)
    }
}

impl std::fmt::Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_reads_back_as_its_name() {
        for (code, name) in CODES {
            assert_eq!(Code::parse(name), Some(code));
            assert_eq!(code.name(), name);
        }
        assert_eq!(Code::parse("NoSuchThing"), None);
    }
}
