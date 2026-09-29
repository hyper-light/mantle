//! Server-side encryption as requests name it (docs/research/20; docs/design/encryption.md): the
//! headers of object writes and reads, each checked as S3 was recorded checking it, and a
//! bucket's encryption configuration.
//!
//! Every object is sealed at rest; what the headers choose is whose key wraps its data key: the
//! node's root key for SSE-S3, the customer's for SSE-C. SSE-KMS and DSSE-KMS name a key
//! management service mantle does not have, and are not implemented.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use zeroize::Zeroizing;

use crate::crypto::{self, CryptoError};

/// The one algorithm SSE-S3 and SSE-C name (20 §1.1, §3.1).
pub const AES256: &str = "AES256";

const SSE: &str = "x-amz-server-side-encryption";

/// A customer's key for SSE-C: 256 bits, held for the request and wiped when dropped.
pub struct CustomerKey {
    key: Zeroizing<[u8; 32]>,
    md5: String,
}

impl CustomerKey {
    pub fn key(&self) -> &[u8; 32] {
        &self.key
    }

    /// The base64 MD5 of the key, which responses echo in
    /// `x-amz-server-side-encryption-customer-key-MD5` (20 §3.4).
    pub fn md5(&self) -> &str {
        &self.md5
    }
}

impl std::fmt::Debug for CustomerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key is the customer's secret.
        f.debug_struct("CustomerKey")
            .field("md5", &self.md5)
            .finish_non_exhaustive()
    }
}

/// Whose key an object write asks to wrap its data key.
#[derive(Debug)]
pub enum Encryption {
    /// SSE-S3: the node's root key, every object's state unless it names SSE-C (20 §1.3).
    S3,
    Customer(CustomerKey),
}

/// How a stored object was encrypted, which its reads must match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stored {
    S3,
    Customer,
}

/// The SSE-C headers of a request, or of its copy source.
#[derive(Debug, Clone, Copy, Default)]
pub struct CustomerHeaders<'a> {
    pub algorithm: Option<&'a str>,
    pub key: Option<&'a str>,
    pub key_md5: Option<&'a str>,
}

/// An object request's encryption headers.
#[derive(Debug, Clone, Copy, Default)]
pub struct Headers<'a> {
    /// `x-amz-server-side-encryption`.
    pub algorithm: Option<&'a str>,
    /// `x-amz-server-side-encryption-aws-kms-key-id`.
    pub kms_key: Option<&'a str>,
    pub customer: CustomerHeaders<'a>,
    /// The `x-amz-copy-source-server-side-encryption-customer-*` headers of a copy.
    pub copy_source: CustomerHeaders<'a>,
}

impl<'a> Headers<'a> {
    /// The encryption headers among a request's, named in any case.
    pub fn from(headers: &[(&'a str, &'a str)]) -> Self {
        let get = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| *v)
        };
        let trio = |prefix: &str| CustomerHeaders {
            algorithm: get(&format!("{prefix}-customer-algorithm")),
            key: get(&format!("{prefix}-customer-key")),
            key_md5: get(&format!("{prefix}-customer-key-md5")),
        };
        Self {
            algorithm: get(SSE),
            kms_key: get("x-amz-server-side-encryption-aws-kms-key-id"),
            customer: trio(SSE),
            copy_source: trio("x-amz-copy-source-server-side-encryption"),
        }
    }
}

/// Why a request's encryption is refused, each with S3's answer where one is recorded (20
/// §1.4, §2.5, §3.5, §4.3).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SseError {
    /// `x-amz-server-side-encryption` holds a value S3 does not take, empty included.
    #[error("The encryption method specified is not supported")]
    Unsupported(String),
    /// SSE-KMS or DSSE-KMS, which need a key management service.
    #[error("{0}, which mantle does not implement")]
    NotImplemented(&'static str),
    /// A KMS key named without `aws:kms`.
    #[error(
        "Server Side Encryption with AWS KMS managed key requires HTTP header x-amz-server-side-encryption : aws:kms"
    )]
    KmsKeyWithoutKms,
    /// SSE-C's headers with `x-amz-server-side-encryption`.
    #[error(
        "Server Side Encryption with Customer provided key is incompatible with the encryption method specified"
    )]
    Incompatible(String),
    #[error("The Encryption request you specified is not valid. Supported value: AES256.")]
    Algorithm(String),
    /// SSE-C's key or its MD5 without the algorithm.
    #[error(
        "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm."
    )]
    NoAlgorithm,
    #[error(
        "Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key."
    )]
    NoKey,
    #[error("The calculated MD5 hash of the key did not match the hash that was provided.")]
    Md5Mismatch,
    /// A key that is not 256 bits of base64.
    #[error("The secret key was invalid for the specified algorithm.")]
    InvalidKey,
    /// SSE-C over plain HTTP: "Amazon S3 rejects any requests made over HTTP when using SSE-C"
    /// (20 §3.3), in IONOS's words, S3's being unrecorded.
    #[error(
        "Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection."
    )]
    Insecure,
    /// `x-amz-server-side-encryption` on a read, which S3 answers 400 (20 §1.2).
    #[error("x-amz-server-side-encryption header is not supported for this operation.")]
    NotForReads,
    /// An SSE-C object read without its key.
    #[error(
        "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object."
    )]
    KeyRequired,
    /// SSE-C headers for an object that is not SSE-C.
    #[error("The encryption parameters are not applicable to this object.")]
    NotApplicable,
    /// An SSE-C object read with another key.
    #[error(
        "Requests specifying Server Side Encryption with Customer provided keys must provide the correct secret key."
    )]
    WrongKey,
    /// A part whose SSE-C headers the upload's creation did not name, or the reverse.
    #[error(
        "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters."
    )]
    PartMismatch,
    /// A part with another key than the upload's.
    #[error("The provided encryption parameters did not match the ones used originally.")]
    PartKey,
    /// An SSE-C write to a bucket that blocks SSE-C, with S3's message for the requester, the
    /// action and the object (20 §3.5).
    #[error(
        "User: {principal} is not authorized to perform: {action} on resource: \"{resource}\" because this bucket has blocked upload requests that specify Server Side Encryption with Customer provided keys (SSE-C). Please specify a different server-side encryption type"
    )]
    Blocked {
        principal: String,
        action: String,
        resource: String,
    },
    /// A bucket default that names a KMS key for another algorithm.
    #[error(
        "a KMSMasterKeyID is not applicable if the default sse algorithm is not aws:kms or aws:kms:dsse"
    )]
    KmsKeyNotApplicable,
    #[error("We encountered an internal error. Please try again.")]
    Internal(#[from] CryptoError),
}

impl SseError {
    /// The S3 error code and status, as S3 answered each (20 §3.5, §4.3).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Unsupported(_)
            | Self::KmsKeyWithoutKms
            | Self::Incompatible(_)
            | Self::NoAlgorithm
            | Self::NoKey
            | Self::Md5Mismatch
            | Self::InvalidKey
            | Self::Insecure
            | Self::NotForReads
            | Self::KmsKeyNotApplicable => ("InvalidArgument", 400),
            Self::Algorithm(_) => ("InvalidEncryptionAlgorithmError", 400),
            Self::NotImplemented(_) => ("NotImplemented", 501),
            Self::KeyRequired | Self::NotApplicable | Self::PartMismatch | Self::PartKey => {
                ("InvalidRequest", 400)
            }
            Self::WrongKey | Self::Blocked { .. } => ("AccessDenied", 403),
            Self::Internal(_) => ("InternalError", 500),
        }
    }

    /// The `ArgumentName` and `ArgumentValue` S3's error carries, as recorded: SSE-C's errors
    /// name `x-amz-server-side-encryption`, and `null` where the value is not the header's
    /// (20 §3.5).
    pub fn argument(&self) -> Option<(&'static str, &str)> {
        match self {
            Self::Unsupported(value) | Self::Incompatible(value) | Self::Algorithm(value) => {
                Some((SSE, value))
            }
            Self::NoAlgorithm | Self::NoKey | Self::Md5Mismatch | Self::InvalidKey => {
                Some((SSE, "null"))
            }
            Self::KmsKeyNotApplicable => Some(("ApplyServerSideEncryptionByDefault", "")),
            _ => None,
        }
    }
}

/// The encryption an object write asks for (PutObject, CopyObject's destination,
/// CreateMultipartUpload, a POST's fields), checked in the order S3 was recorded checking it:
/// SSE-C's headers against `x-amz-server-side-encryption` first, then SSE-C's own (20 §3.5).
/// Without headers a write is SSE-S3, the base every object takes (20 §1.3).
pub fn write(headers: &Headers<'_>) -> Result<Encryption, SseError> {
    let c = headers.customer;
    if c.algorithm.is_some() || c.key.is_some() || c.key_md5.is_some() {
        if let Some(value) = headers.algorithm {
            return Err(SseError::Incompatible(value.to_owned()));
        }
        return Ok(customer(&c)?.map_or(Encryption::S3, Encryption::Customer));
    }
    match headers.algorithm {
        None | Some(AES256) if headers.kms_key.is_some() => Err(SseError::KmsKeyWithoutKms),
        None | Some(AES256) => Ok(Encryption::S3),
        Some("aws:kms") => Err(SseError::NotImplemented("SSE-KMS")),
        Some("aws:kms:dsse") => Err(SseError::NotImplemented("DSSE-KMS")),
        Some(other) => Err(SseError::Unsupported(other.to_owned())),
    }
}

/// The customer's key a read (GetObject, HeadObject, GetObjectAttributes) names, if any. "Do not
/// send encryption request headers for `GET` requests and `HEAD` requests if your object uses
/// SSE-S3, or you'll get an HTTP status code 400" (20 §1.2).
pub fn read(headers: &Headers<'_>) -> Result<Option<CustomerKey>, SseError> {
    if headers.algorithm.is_some() {
        return Err(SseError::NotForReads);
    }
    customer(&headers.customer)
}

/// Whether a read's key suits the object it reads: an SSE-C object needs one, and any other
/// object none (20 §3.5). Whether it is the right key is the unwrapping's to say.
pub fn suits(stored: Stored, given: Option<&CustomerKey>) -> Result<(), SseError> {
    match (stored, given) {
        (Stored::Customer, None) => Err(SseError::KeyRequired),
        (Stored::S3, Some(_)) => Err(SseError::NotApplicable),
        _ => Ok(()),
    }
}

/// Whether a part's key suits its upload: an SSE-C upload's parts each name a key, and no other
/// upload's parts may (20 §3.5).
pub fn part(stored: Stored, given: Option<&CustomerKey>) -> Result<(), SseError> {
    match (stored, given) {
        (Stored::Customer, None) | (Stored::S3, Some(_)) => Err(SseError::PartMismatch),
        _ => Ok(()),
    }
}

/// A customer's key from SSE-C's three headers (20 §3.1, §3.5): the algorithm, which must be
/// `AES256`, then the key, then its MD5, which S3 checks before the key's length. A request that
/// leaves out the MD5 is taken, as clients that compute it cannot always send it (20 §3.5), and
/// the MD5 responses echo is computed.
pub fn customer(headers: &CustomerHeaders<'_>) -> Result<Option<CustomerKey>, SseError> {
    let Some(algorithm) = headers.algorithm else {
        return if headers.key.is_some() || headers.key_md5.is_some() {
            Err(SseError::NoAlgorithm)
        } else {
            Ok(None)
        };
    };
    if algorithm != AES256 {
        return Err(SseError::Algorithm(algorithm.to_owned()));
    }
    let key = headers.key.ok_or(SseError::NoKey)?;
    let decoded = Zeroizing::new(
        BASE64
            .decode(key.trim())
            .map_err(|_| SseError::InvalidKey)?,
    );
    let md5 = BASE64.encode(md5(&decoded)?);
    if let Some(given) = headers.key_md5
        && given.trim() != md5
    {
        return Err(SseError::Md5Mismatch);
    }
    let key: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| SseError::InvalidKey)?;
    Ok(Some(CustomerKey {
        key: Zeroizing::new(key),
        md5,
    }))
}

fn md5(bytes: &[u8]) -> Result<Vec<u8>, SseError> {
    let mut digest = crypto::Digest::new(&crypto::MD5)?;
    digest.update(bytes)?;
    Ok(digest.finish()?)
}

/// SSE-C needs a secure connection (20 §3.3).
pub fn secure(given: Option<&CustomerKey>, tls: bool) -> Result<(), SseError> {
    if given.is_some() && !tls {
        Err(SseError::Insecure)
    } else {
        Ok(())
    }
}

/// The headers a response about an object of `stored` encryption carries, on the operations S3
/// lists (20 §1.2): `AES256` for SSE-S3, and SSE-C's algorithm and the key's MD5 for SSE-C,
/// which never names `x-amz-server-side-encryption` (20 §3.4).
pub fn response(stored: Stored, key: Option<&CustomerKey>) -> Vec<(&'static str, String)> {
    match (stored, key) {
        (Stored::Customer, Some(key)) => vec![
            (
                "x-amz-server-side-encryption-customer-algorithm",
                AES256.to_owned(),
            ),
            (
                "x-amz-server-side-encryption-customer-key-MD5",
                key.md5().to_owned(),
            ),
        ],
        (Stored::Customer, None) => Vec::new(),
        (Stored::S3, _) => vec![(SSE, AES256.to_owned())],
    }
}

/// A bucket's encryption configuration (20 §4): its default is SSE-S3, the only default mantle
/// serves; whether writes may name SSE-C; and the Bucket Key flag, kept so it reads back as set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Configuration {
    pub bucket_key: bool,
    pub customer_blocked: bool,
}

impl Configuration {
    /// A new bucket's: SSE-S3, and SSE-C blocked, as S3's since April 2026 (20 §3.6).
    pub const NEW_BUCKET: Self = Self {
        bucket_key: false,
        customer_blocked: true,
    };

    /// The configuration after DeleteBucketEncryption, which "resets the default encryption for
    /// the bucket as ... (SSE-S3)" (20 §4.2). Whether it lifts a block S3 does not say, and the
    /// block stays.
    pub fn deleted(self) -> Self {
        Self {
            bucket_key: false,
            ..self
        }
    }

    /// The configuration after PutBucketEncryption sets `rule`. A rule without
    /// `BlockedEncryptionTypes` leaves the block as it was, as S3 was recorded doing (20 §3.6).
    pub fn put(self, rule: Rule) -> Self {
        Self {
            bucket_key: rule.bucket_key,
            customer_blocked: rule.customer_blocked.unwrap_or(self.customer_blocked),
        }
    }
}

/// A PutBucketEncryption rule, read (`body::server_side_encryption_configuration`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    pub bucket_key: bool,
    /// `BlockedEncryptionTypes`: `SSE-C` blocks, `NONE` unblocks, absent leaves it.
    pub customer_blocked: Option<bool>,
}

/// A default algorithm a rule names (20 §4.1): `AES256`, SSE-KMS and DSSE-KMS not implemented,
/// and a KMS key only with those. `None` for a value S3's schema does not hold.
pub fn default_algorithm(algorithm: &str, kms_key: bool) -> Option<Result<(), SseError>> {
    Some(match algorithm {
        AES256 if kms_key => Err(SseError::KmsKeyNotApplicable),
        AES256 => Ok(()),
        "aws:kms" => Err(SseError::NotImplemented("SSE-KMS")),
        "aws:kms:dsse" => Err(SseError::NotImplemented("DSSE-KMS")),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ceph s3-tests' two keys and their MD5s (20 §6b).
    const KEY_A: &str = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=";
    const MD5_A: &str = "DWygnHRtgiJ77HCm+1rvHw==";
    const KEY_B: &str = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=";
    const MD5_B: &str = "arxBvwY2V4SiOne6yppVPQ==";

    fn trio<'a>(
        algorithm: Option<&'a str>,
        key: Option<&'a str>,
        md5: Option<&'a str>,
    ) -> Headers<'a> {
        Headers {
            customer: CustomerHeaders {
                algorithm,
                key,
                key_md5: md5,
            },
            ..Headers::default()
        }
    }

    #[test]
    fn the_test_keys_decode_to_their_md5s() {
        for (key, md5) in [(KEY_A, MD5_A), (KEY_B, MD5_B)] {
            let Encryption::Customer(given) =
                write(&trio(Some(AES256), Some(key), Some(md5))).unwrap()
            else {
                panic!("not SSE-C");
            };
            assert_eq!(given.md5(), md5);
        }
        // botocore's key of 32 `a`s and the MD5 it computes (20 §6a).
        let given = customer(&CustomerHeaders {
            algorithm: Some(AES256),
            key: Some("YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE="),
            key_md5: None,
        })
        .unwrap()
        .unwrap();
        assert_eq!(given.md5(), "Xsqb0+sHwAbNQ65I395/0w==");
        assert_eq!(given.key(), &[b'a'; 32]);
        assert!(!format!("{given:?}").contains("aaaa"));
    }

    /// SSE-C's refusals in the order S3 was recorded making them (20 §3.5).
    #[test]
    fn customer_keys_are_refused_as_s3_refuses_them() {
        let refused = |h: Headers<'_>| write(&h).unwrap_err();
        let incompatible = Headers {
            algorithm: Some(AES256),
            ..trio(Some("AES512"), Some(KEY_A), Some(MD5_A))
        };
        assert_eq!(refused(incompatible), SseError::Incompatible(AES256.into()));
        let algorithm = refused(trio(Some("AES512"), Some(KEY_A), Some(MD5_A)));
        assert_eq!(algorithm.code(), ("InvalidEncryptionAlgorithmError", 400));
        assert_eq!(algorithm.argument(), Some((SSE, "AES512")));
        assert_eq!(
            refused(trio(None, Some(KEY_A), Some(MD5_A))),
            SseError::NoAlgorithm
        );
        assert_eq!(
            refused(trio(None, None, Some(MD5_A))),
            SseError::NoAlgorithm
        );
        assert_eq!(
            refused(trio(Some(AES256), None, Some(MD5_A))),
            SseError::NoKey
        );
        let mismatch = refused(trio(
            Some(AES256),
            Some(KEY_A),
            Some("AAAAAAAAAAAAAAAAAAAAAA=="),
        ));
        assert_eq!(mismatch, SseError::Md5Mismatch);
        assert_eq!(mismatch.argument(), Some((SSE, "null")));
        // A 24-byte key with the wrong MD5 is the MD5's fault first, as S3 answered it.
        let short = BASE64.encode([7u8; 24]);
        assert_eq!(
            refused(trio(Some(AES256), Some(&short), Some(MD5_A))),
            SseError::Md5Mismatch
        );
        let short_md5 = BASE64.encode(md5(&[7u8; 24]).unwrap());
        assert_eq!(
            refused(trio(Some(AES256), Some(&short), Some(&short_md5))),
            SseError::InvalidKey
        );
        assert_eq!(
            refused(trio(Some(AES256), Some("not base64!"), None)),
            SseError::InvalidKey
        );
    }

    #[test]
    fn sse_s3_and_kms_headers_are_judged_as_s3_judges_them() {
        let with = |algorithm, kms_key| {
            write(&Headers {
                algorithm,
                kms_key,
                ..Headers::default()
            })
        };
        assert!(matches!(with(None, None), Ok(Encryption::S3)));
        assert!(matches!(with(Some(AES256), None), Ok(Encryption::S3)));
        assert_eq!(
            with(Some(AES256), Some("key")).unwrap_err(),
            SseError::KmsKeyWithoutKms
        );
        assert_eq!(
            with(None, Some("key")).unwrap_err(),
            SseError::KmsKeyWithoutKms
        );
        assert_eq!(
            with(Some("aws:kms"), None).unwrap_err().code(),
            ("NotImplemented", 501)
        );
        assert_eq!(
            with(Some("aws:kms:dsse"), Some("k")).unwrap_err().code(),
            ("NotImplemented", 501)
        );
        for bad in ["", "aes:kms", "aes256", "AES512", "aws:fsx"] {
            let error = with(Some(bad), None).unwrap_err();
            assert_eq!(error, SseError::Unsupported(bad.into()), "{bad}");
            assert_eq!(error.code(), ("InvalidArgument", 400));
            assert_eq!(error.argument(), Some((SSE, bad)));
        }
    }

    /// Reads name no SSE-S3 header, and a key only for an SSE-C object; parts name a key only
    /// for an SSE-C upload (20 §1.2, §3.5).
    #[test]
    fn reads_and_parts_match_what_is_stored() {
        assert_eq!(
            read(&Headers {
                algorithm: Some(AES256),
                ..Headers::default()
            })
            .unwrap_err(),
            SseError::NotForReads
        );
        let key = read(&trio(Some(AES256), Some(KEY_A), Some(MD5_A))).unwrap();
        assert!(key.is_some());
        assert_eq!(suits(Stored::Customer, key.as_ref()), Ok(()));
        assert_eq!(suits(Stored::Customer, None), Err(SseError::KeyRequired));
        assert_eq!(
            suits(Stored::S3, key.as_ref()),
            Err(SseError::NotApplicable)
        );
        assert_eq!(suits(Stored::S3, None), Ok(()));
        assert_eq!(part(Stored::Customer, None), Err(SseError::PartMismatch));
        assert_eq!(part(Stored::S3, key.as_ref()), Err(SseError::PartMismatch));
        assert_eq!(secure(key.as_ref(), false), Err(SseError::Insecure));
        assert_eq!(secure(key.as_ref(), true), Ok(()));
        assert_eq!(SseError::WrongKey.code(), ("AccessDenied", 403));
        assert_eq!(
            response(Stored::Customer, key.as_ref()),
            vec![
                (
                    "x-amz-server-side-encryption-customer-algorithm",
                    AES256.to_owned()
                ),
                (
                    "x-amz-server-side-encryption-customer-key-MD5",
                    MD5_A.to_owned()
                ),
            ]
        );
        assert_eq!(response(Stored::S3, None), vec![(SSE, AES256.to_owned())]);
    }

    #[test]
    fn headers_are_found_in_any_case_with_the_copy_source_s_apart() {
        let request = [
            ("X-Amz-Server-Side-Encryption-Customer-Algorithm", AES256),
            ("x-amz-server-side-encryption-customer-key", KEY_A),
            ("X-AMZ-SERVER-SIDE-ENCRYPTION-CUSTOMER-KEY-MD5", MD5_A),
            (
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                AES256,
            ),
            (
                "x-amz-copy-source-server-side-encryption-customer-key",
                KEY_B,
            ),
            (
                "x-amz-copy-source-server-side-encryption-customer-key-MD5",
                MD5_B,
            ),
        ];
        let h = Headers::from(&request);
        assert_eq!(h.algorithm, None);
        assert_eq!(h.customer.key, Some(KEY_A));
        assert_eq!(customer(&h.copy_source).unwrap().unwrap().md5(), MD5_B);
    }

    #[test]
    fn a_bucket_s_configuration_follows_s3() {
        let c = Configuration::NEW_BUCKET;
        assert!(c.customer_blocked);
        let unblocked = c.put(Rule {
            bucket_key: true,
            customer_blocked: Some(false),
        });
        assert_eq!(
            unblocked,
            Configuration {
                bucket_key: true,
                customer_blocked: false
            }
        );
        // A rule without BlockedEncryptionTypes leaves the block, and a delete keeps it.
        let kept = unblocked.put(Rule {
            bucket_key: false,
            customer_blocked: None,
        });
        assert!(!kept.customer_blocked);
        assert_eq!(c.deleted(), c);
        assert_eq!(default_algorithm(AES256, false), Some(Ok(())));
        assert_eq!(
            default_algorithm(AES256, true),
            Some(Err(SseError::KmsKeyNotApplicable))
        );
        assert_eq!(
            default_algorithm("aws:kms", true)
                .unwrap()
                .unwrap_err()
                .code(),
            ("NotImplemented", 501)
        );
        assert_eq!(default_algorithm("aws:fsx", false), None);
        let blocked = SseError::Blocked {
            principal: "arn:aws:iam::123456789012:root".into(),
            action: "s3:PutObject".into(),
            resource: "arn:aws:s3:::b/k".into(),
        };
        assert_eq!(blocked.code(), ("AccessDenied", 403));
        assert!(blocked.to_string().starts_with(
            "User: arn:aws:iam::123456789012:root is not authorized to perform: s3:PutObject on resource: \"arn:aws:s3:::b/k\" because this bucket has blocked"
        ));
    }
}
