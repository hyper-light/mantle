//! POST Object (docs/research/19): an object uploaded from an HTML form, whose fields stand for
//! PutObject's headers and whose authority is a policy the form carries, signed with Signature
//! Version 4, rather than a signed request.
//!
//! A form is checked as S3 was recorded checking one (19 §8.3): the fields it must hold; then,
//! for a signed form, the signature over the policy as sent, the policy's form, its expiration,
//! each condition in turn, and that a condition covers every field. The file's size is checked
//! against the policy's `content-length-range` once the file has arrived. A form with no
//! signature is anonymous, and uploads only where a bucket policy lets anyone write.

use base64::Engine as _;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};

use crate::crypto::{self, CryptoError};
use crate::form::Form;
use crate::json::{self, Value};
use crate::route::MAX_KEY;
use crate::sigv4::{self, AuthError};
use crate::time;

/// Base64 as a policy is sent: the standard alphabet, padded or not, since the signature covers
/// the text as sent however it is padded.
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// The fields a signed form must hold besides `key`, in the order S3 names the first one
/// missing: `X-Amz-Algorithm` before `X-Amz-Credential` (19 §8.1). Any of the four `x-amz-`
/// fields makes a form a signed one; a `policy` alone does not (19 §8.1).
const SIGNED: [&str; 5] = [
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Signature",
    "Policy",
];

/// Fields no condition need cover: "Each form field that you specify in a form (except
/// `x-amz-signature`, `file`, `policy`, and field names that have an `x-ignore-` prefix) must
/// appear in the list of conditions" (19 §4.4).
const UNCOVERED: [&str; 3] = ["x-amz-signature", "file", "policy"];
const IGNORED: &str = "x-ignore-";

/// The fields that sign a form, which stand for no header of the upload.
const AUTHORIZING: [&str; 5] = [
    "x-amz-algorithm",
    "x-amz-credential",
    "x-amz-date",
    "x-amz-signature",
    "x-amz-security-token",
];

/// The fields other than `x-amz-*` that stand for PutObject's headers of the same names: "The
/// REST-specific headers" (19 §2.2), and `Content-MD5` (19 §2.1).
const CONTENT: [&str; 7] = [
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-md5",
    "content-type",
    "expires",
];

/// Linear whitespace, around the items of a list.
const WS: [char; 2] = [' ', '\t'];

/// Why a form may not upload, each with S3's answer (19 §8, §9).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostError {
    /// A field the form must hold is missing, or follows the file, which hides it (19 §8.2).
    #[error(
        "Bucket POST must contain a field named '{0}'.  If it is specified, please check the order of the fields."
    )]
    Missing(&'static str),
    /// A signing field holds a value S3 does not take (19 §13).
    #[error("{message}")]
    Field {
        name: &'static str,
        value: String,
        message: String,
    },
    /// The credential's scope names another region than the endpoint's (19 §13).
    #[error("the region '{given}' is wrong; expecting '{expected}'")]
    Region {
        credential: String,
        given: String,
        expected: String,
    },
    /// Signature Version 2's fields, which mantle does not serve (05 §1).
    #[error(
        "The request is using the wrong signature version. Use AWS4-HMAC-SHA256 (Signature Version 4)."
    )]
    SignatureVersion,
    #[error("Your key is too long.")]
    KeyTooLong,
    #[error("The AWS Access Key Id you provided does not exist in our records.")]
    UnknownKey(String),
    /// The signature is not the policy's, as sent, under the access key's secret.
    #[error(
        "The request signature we calculated does not match the signature you provided. Check your key and signing method."
    )]
    Mismatch {
        access_key: String,
        provided: String,
        policy: String,
    },
    /// The policy is not a policy (19 §8.2).
    #[error("Invalid Policy: {0}")]
    Policy(String),
    #[error("Invalid according to Policy: Policy expired.")]
    Expired,
    /// A condition fails, written as S3 writes it (19 §8.1).
    #[error("Invalid according to Policy: Policy Condition failed: {0}")]
    Condition(String),
    /// A field no condition covers, named as the form sent it (19 §13).
    #[error("Invalid according to Policy: Extra input fields: {0}")]
    Extra(String),
    #[error("Your proposed upload exceeds the maximum allowed size")]
    TooLarge { proposed: u64, max: u64 },
    #[error("Your proposed upload is smaller than the minimum allowed size")]
    TooSmall { proposed: u64, min: u64 },
    /// A header of the request disagrees with the field of the same meaning (19 §2.6).
    #[error("Conflicting values provided in HTTP headers and POST form fields.")]
    Conflict(String),
    #[error("We encountered an internal error. Please try again.")]
    Internal(#[from] CryptoError),
}

impl PostError {
    /// The S3 error code and status, as S3 answered each (19 §9): a failed condition is
    /// `403 AccessDenied`, where the error table says `400 InvalidPolicyDocument`, and a missing
    /// field `InvalidArgument`, where it says `UserKeyMustBeSpecified` (19 §10 items 1, 2).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Missing(_) | Self::Field { .. } | Self::Region { .. } => ("InvalidArgument", 400),
            Self::SignatureVersion | Self::Conflict(_) => ("InvalidRequest", 400),
            Self::KeyTooLong => ("KeyTooLongError", 400),
            Self::UnknownKey(_) => ("InvalidAccessKeyId", 403),
            Self::Mismatch { .. } => ("SignatureDoesNotMatch", 403),
            Self::Policy(_) => ("InvalidPolicyDocument", 400),
            Self::Expired | Self::Condition(_) | Self::Extra(_) => ("AccessDenied", 403),
            Self::TooLarge { .. } => ("EntityTooLarge", 400),
            Self::TooSmall { .. } => ("EntityTooSmall", 400),
            Self::Internal(_) => ("InternalError", 500),
        }
    }

    /// The elements S3's error carries after its message, as recorded (19 §8, §13).
    pub fn details(&self) -> Vec<(&'static str, String)> {
        match self {
            Self::Missing(name) => vec![
                ("ArgumentName", (*name).to_owned()),
                ("ArgumentValue", String::new()),
            ],
            Self::Field { name, value, .. } => vec![
                ("ArgumentName", (*name).to_owned()),
                ("ArgumentValue", value.clone()),
            ],
            Self::Region {
                credential,
                expected,
                ..
            } => vec![
                ("ArgumentName", "X-Amz-Credential".to_owned()),
                ("ArgumentValue", credential.clone()),
                ("Region", expected.clone()),
            ],
            Self::UnknownKey(key) => vec![("AWSAccessKeyId", key.clone())],
            Self::Mismatch {
                access_key,
                provided,
                policy,
            } => vec![
                ("AWSAccessKeyId", access_key.clone()),
                ("StringToSign", policy.clone()),
                ("SignatureProvided", provided.clone()),
                ("StringToSignBytes", spaced_hex(policy.as_bytes())),
            ],
            Self::TooLarge { proposed, max } => vec![
                ("ProposedSize", proposed.to_string()),
                ("MaxSizeAllowed", max.to_string()),
            ],
            Self::TooSmall { proposed, min } => vec![
                ("ProposedSize", proposed.to_string()),
                ("MinSizeAllowed", min.to_string()),
            ],
            _ => Vec::new(),
        }
    }
}

/// Bytes as S3's `StringToSignBytes` writes them: two lowercase hex digits each, spaced.
fn spaced_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::new();
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push(char::from(
            HEX.get(usize::from(b >> 4)).copied().unwrap_or(b'0'),
        ));
        out.push(char::from(
            HEX.get(usize::from(b & 15)).copied().unwrap_or(b'0'),
        ));
    }
    out
}

/// What a form's upload is allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorized {
    /// The access key whose policy the form carries; `None` for an anonymous upload.
    pub access_key: Option<String>,
    /// The file sizes the policy allows.
    pub size: Size,
}

/// The sizes a policy's `content-length-range` conditions allow a file, bounds included, every
/// condition holding (19 §4.2, §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Size {
    pub min: u64,
    pub max: u64,
}

impl Size {
    pub const ANY: Self = Self {
        min: 0,
        max: u64::MAX,
    };

    /// Whether a file of `length` bytes is allowed: 12 bytes against `[5, 10]` is
    /// `EntityTooLarge`, 1 byte `EntityTooSmall`, and 5 and 10 bytes go ahead (19 §8.1).
    pub fn check(self, length: u64) -> Result<(), PostError> {
        if length > self.max {
            Err(PostError::TooLarge {
                proposed: length,
                max: self.max,
            })
        } else if length < self.min {
            Err(PostError::TooSmall {
                proposed: length,
                min: self.min,
            })
        } else {
            Ok(())
        }
    }
}

/// The key a form uploads to, its `${filename}` expanded: "The name of the uploaded key" (19
/// §2.2), required of every form. An empty key names no object, and is answered as a missing
/// one.
pub fn key(form: &Form) -> Result<&str, PostError> {
    let key = form
        .get("key")
        .filter(|key| !key.is_empty())
        .ok_or(PostError::Missing("key"))?;
    if key.len() > MAX_KEY {
        return Err(PostError::KeyTooLong);
    }
    Ok(key)
}

/// Checks a form's authority to upload to `bucket`, the bucket its URL names, at an endpoint
/// serving `region`, at `now` in Unix milliseconds; `secret` finds an access key's secret.
pub fn authorize(
    form: &Form,
    bucket: &str,
    region: &str,
    now: i64,
    secret: impl Fn(&str) -> Option<String>,
) -> Result<Authorized, PostError> {
    key(form)?;
    let signed = SIGNED.iter().take(4).any(|field| form.get(field).is_some());
    if !signed {
        if form.get("AWSAccessKeyId").is_some() || form.get("Signature").is_some() {
            return Err(PostError::SignatureVersion);
        }
        return Ok(Authorized {
            access_key: None,
            size: Size::ANY,
        });
    }
    let mut values = [""; 5];
    for (slot, field) in values.iter_mut().zip(SIGNED) {
        *slot = form.get(field).ok_or(PostError::Missing(field))?;
    }
    let [algorithm, credential, date, signature, policy] = values;
    if algorithm != sigv4::ALGORITHM {
        return Err(PostError::Field {
            name: "X-Amz-Algorithm",
            value: algorithm.to_owned(),
            message: "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\"".to_owned(),
        });
    }
    if time::parse_amz_date(date).is_none() {
        return Err(PostError::Field {
            name: "X-Amz-Date",
            value: date.to_owned(),
            message: "X-Amz-Date must be formated via ISO8601 Long format".to_owned(),
        });
    }
    let scope = Scope::parse(credential, region)?;
    let secret = secret(scope.access_key)
        .ok_or_else(|| PostError::UnknownKey(scope.access_key.to_owned()))?;
    // "Create the signature as an HMAC-SHA256 hash of the string to sign", the policy as sent
    // (19 §3.2): checked before the policy is read, as S3 answers a policy changed after
    // signing (19 §8.1).
    let key =
        sigv4::signing_key(&secret, scope.date, region, "s3").map_err(|error| match error {
            AuthError::Internal(error) => PostError::Internal(error),
            _ => PostError::Internal(CryptoError),
        })?;
    let expected = crypto::hmac_sha256(&key, policy.as_bytes())?;
    let matches = match sigv4::unhex(signature) {
        Some(provided) => crypto::equal(&expected, &provided)?,
        None => false,
    };
    if !matches {
        return Err(PostError::Mismatch {
            access_key: scope.access_key.to_owned(),
            provided: signature.to_owned(),
            policy: policy.to_owned(),
        });
    }
    let document = document(policy)?;
    let policy = Policy::read(&document)?;
    if policy.expired(now) {
        return Err(PostError::Expired);
    }
    let mut size = Size::ANY;
    for condition in &policy.conditions {
        condition.check(form, bucket, &mut size)?;
    }
    for (name, _) in form.fields() {
        let exempt = UNCOVERED
            .iter()
            .any(|field| name.eq_ignore_ascii_case(field))
            || name
                .get(..IGNORED.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(IGNORED));
        if !exempt && !policy.conditions.iter().any(|c| c.covers(name)) {
            return Err(PostError::Extra(name.clone()));
        }
    }
    Ok(Authorized {
        access_key: Some(scope.access_key.to_owned()),
        size,
    })
}

/// A credential's scope, `<access-key>/<date>/<region>/s3/aws4_request` (19 §3.1).
struct Scope<'a> {
    access_key: &'a str,
    date: &'a str,
}

impl<'a> Scope<'a> {
    /// The scope `credential` names, which must be this endpoint's: "The bucket must be in the
    /// region that you specified in the credential scope" (19 §3.1). Each fault is
    /// `InvalidArgument` naming the credential, with S3's message where one is recorded, and
    /// otherwise the one S3 gives a presigned URL without its prefix, as versitygw words them
    /// (19 §13). The day's agreement with `x-amz-date`, which AWS's text asks for, is left to
    /// the signature: S3's enforcement of it is unrecorded, and the signing key is the day's.
    fn parse(credential: &'a str, region: &str) -> Result<Self, PostError> {
        let fault = |message: String| PostError::Field {
            name: "X-Amz-Credential",
            value: credential.to_owned(),
            message,
        };
        let parts: Vec<&str> = credential.rsplitn(5, '/').collect();
        let [terminator, service, scoped, day, access_key] = parts.as_slice() else {
            return Err(fault(
                "the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".".to_owned(),
            ));
        };
        if access_key.is_empty() {
            return Err(fault(
                "a non-empty Access Key (AKID) must be provided in the credential.".to_owned(),
            ));
        }
        if day.len() != 8 || time::parse_amz_date(&format!("{day}T000000Z")).is_none() {
            return Err(fault(format!(
                "incorrect date format \"{day}\". This date in the credential must be in the format \"yyyyMMdd\"."
            )));
        }
        if *scoped != region {
            return Err(PostError::Region {
                credential: credential.to_owned(),
                given: (*scoped).to_owned(),
                expected: region.to_owned(),
            });
        }
        if *service != "s3" {
            return Err(fault(format!(
                "incorrect service \"{service}\". This endpoint belongs to \"s3\"."
            )));
        }
        if *terminator != "aws4_request" {
            return Err(fault(format!(
                "incorrect terminal \"{terminator}\". This endpoint uses \"aws4_request\"."
            )));
        }
        Ok(Self {
            access_key,
            date: day,
        })
    }
}

/// A POST policy (19 §4), its conditions borrowed from the document they were read from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Policy<'a> {
    /// The instant, Unix seconds and nanoseconds, after which the policy is not valid.
    expiration: (i64, u32),
    conditions: Vec<Condition<'a>>,
}

/// One of a policy's conditions, and how it was written, for the error that names it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Condition<'a> {
    test: Test<'a>,
    source: Source<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Test<'a> {
    /// `{"name": "value"}`, or `["eq", "$name", "value"]`: "an alternate way" (19 §4.2).
    Eq {
        field: Option<&'a str>,
        value: &'a str,
    },
    /// `["starts-with", "$name", "prefix"]`.
    StartsWith {
        field: Option<&'a str>,
        prefix: &'a str,
    },
    /// `["content-length-range", min, max]`.
    Range { min: Bound, max: Bound },
}

/// A condition as the policy writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source<'a> {
    Simple { name: &'a str, value: &'a str },
    List(&'a [Value]),
}

/// A bound of `content-length-range`: a number, or a string holding one (19 §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    Size(u64),
    /// A string that holds no size: the condition fails, as S3 failed `["content-length-range",
    /// "test", "10"]` (19 §8.1).
    Unreadable,
}

/// The document a policy's text encodes: base64 of JSON.
fn document(text: &str) -> Result<Value, PostError> {
    let decoded = BASE64
        .decode(text)
        .map_err(|_| PostError::Policy("invalid Base64 encoding.".to_owned()))?;
    json::parse(&decoded).map_err(|_| PostError::Policy("Invalid JSON.".to_owned()))
}

impl<'a> Policy<'a> {
    /// The policy `document` holds: an object with `expiration` and `conditions`, named exactly
    /// so, which s3-tests expects of `EXPIRATION` and `CONDITIONS` (19 §7 items 21, 22). S3
    /// answered a member of another name "Unexpected", naming it in lowercase, before asking
    /// for either (19 §13).
    fn read(document: &'a Value) -> Result<Self, PostError> {
        let Value::Object(members) = document else {
            return Err(PostError::Policy("Invalid JSON.".to_owned()));
        };
        if let Some((name, _)) = members
            .iter()
            .find(|(name, _)| name != "expiration" && name != "conditions")
        {
            return Err(PostError::Policy(format!(
                "Unexpected: '{}'",
                name.to_lowercase()
            )));
        }
        let expiration = match document.member("expiration") {
            None => return Err(PostError::Policy("Policy missing expiration.".to_owned())),
            Some(Value::String(text)) => expiration(text).ok_or_else(|| {
                PostError::Policy(format!("Invalid 'expiration' value: '{text}'"))
            })?,
            Some(other) => {
                return Err(PostError::Policy(format!(
                    "Invalid 'expiration' value: '{}'",
                    other.compact()
                )));
            }
        };
        let conditions = match document.member("conditions") {
            None => return Err(PostError::Policy("Policy missing conditions.".to_owned())),
            Some(Value::Array(items)) => items
                .iter()
                .map(Condition::read)
                .collect::<Result<_, _>>()?,
            Some(_) => {
                return Err(PostError::Policy(
                    "Invalid 'conditions' value: must be a List.".to_owned(),
                ));
            }
        };
        Ok(Self {
            expiration,
            conditions,
        })
    }

    /// "specifies that the POST policy is not valid after" its time (19 §4.1).
    fn expired(&self, now: i64) -> bool {
        let (seconds, nanos) = self.expiration;
        let now = i128::from(now).saturating_mul(1_000_000);
        let until = i128::from(seconds)
            .saturating_mul(1_000_000_000)
            .saturating_add(i128::from(nanos));
        now > until
    }
}

/// An expiration "in ISO8601 GMT date format" (19 §4.1): a time in UTC, `Z`, with or without
/// fractional seconds, as AWS's example and botocore write it. S3 refused one with an offset,
/// `+02:00`, and one with a space for the `T` (19 §13).
fn expiration(text: &str) -> Option<(i64, u32)> {
    if text.ends_with('Z') {
        time::parse_iso8601(text)
    } else {
        None
    }
}

impl<'a> Condition<'a> {
    /// A condition as the policy writes it, with S3's answer to each shape it refuses where one
    /// is recorded (19 §8.2, §13), and versitygw's wording of S3's where none is: JSON's `null`,
    /// and a value of another type where a string or a whole size belongs, S3 answered as
    /// "Invalid JSON.", as it did `{"success_action_redirect": null}` and a bound of `512.0`.
    fn read(value: &'a Value) -> Result<Self, PostError> {
        let policy = |message: &str| PostError::Policy(message.to_owned());
        let invalid_json = || policy("Invalid JSON.");
        match value {
            Value::Object(members) => {
                let [(name, value)] = members.as_slice() else {
                    return Err(policy(
                        "Invalid Simple-Condition: Simple-Conditions must have exactly one property specified.",
                    ));
                };
                let value = match value {
                    Value::String(value) => value,
                    Value::Null => return Err(invalid_json()),
                    _ => {
                        return Err(policy("Invalid Simple-Condition: value must be a string."));
                    }
                };
                Ok(Self {
                    test: Test::Eq {
                        field: Some(name),
                        value,
                    },
                    source: Source::Simple { name, value },
                })
            }
            Value::Array(items) => {
                let Some(Value::String(operator)) = items.first() else {
                    return Err(policy("Invalid Condition: missing operation identifier."));
                };
                let arity =
                    || PostError::Policy(format!("Invalid {operator}: wrong number of arguments."));
                let test = if operator.eq_ignore_ascii_case("content-length-range") {
                    let [_, min, max] = items.as_slice() else {
                        return Err(arity());
                    };
                    Test::Range {
                        min: Bound::read(min)?,
                        max: Bound::read(max)?,
                    }
                } else if operator.eq_ignore_ascii_case("eq")
                    || operator.eq_ignore_ascii_case("starts-with")
                {
                    let [_, name, operand] = items.as_slice() else {
                        return Err(arity());
                    };
                    let (Value::String(name), Value::String(operand)) = (name, operand) else {
                        return Err(invalid_json());
                    };
                    // A name without `$` names no field, and the condition fails (19 §8.1).
                    let field = name.strip_prefix('$');
                    if operator.eq_ignore_ascii_case("eq") {
                        Test::Eq {
                            field,
                            value: operand,
                        }
                    } else {
                        Test::StartsWith {
                            field,
                            prefix: operand,
                        }
                    }
                } else {
                    return Err(PostError::Policy(format!(
                        "Invalid Condition: unknown operation '{operator}'."
                    )));
                };
                Ok(Self {
                    test,
                    source: Source::List(items),
                })
            }
            _ => Err(policy("Invalid condition test: must be a List or Object.")),
        }
    }

    /// Checks the condition against the form, whose fields are named in any case and compared
    /// in theirs (19 §8.1), and narrows `size` by a `content-length-range`. A field the form
    /// does not hold fails its condition, even `starts-with ""`, as on S3 (19 §8.2).
    fn check(&self, form: &Form, bucket: &str, size: &mut Size) -> Result<(), PostError> {
        let holds = match self.test {
            Test::Eq { field, value } => lookup(form, bucket, field) == Some(value),
            Test::StartsWith { field, prefix } => match lookup(form, bucket, field) {
                None => false,
                // "Content-Types values for a starts-with condition that include commas are
                // interpreted as lists. Each value in the list must meet the condition" (19
                // §4.2).
                Some(value) if field.is_some_and(|f| f.eq_ignore_ascii_case("content-type")) => {
                    value
                        .split(',')
                        .all(|item| item.trim_matches(WS).starts_with(prefix))
                }
                Some(value) => value.starts_with(prefix),
            },
            Test::Range { min, max } => match (min, max) {
                (Bound::Size(min), Bound::Size(max)) => {
                    size.min = size.min.max(min);
                    size.max = size.max.min(max);
                    true
                }
                _ => false,
            },
        };
        if holds {
            Ok(())
        } else {
            Err(PostError::Condition(self.source.written()))
        }
    }

    /// Whether the condition names the field `name`.
    fn covers(&self, name: &str) -> bool {
        match self.test {
            Test::Eq { field, .. } | Test::StartsWith { field, .. } => {
                field.is_some_and(|f| f.eq_ignore_ascii_case(name))
            }
            Test::Range { .. } => false,
        }
    }
}

impl Source<'_> {
    /// The condition as S3 writes it in an error: its items as JSON, separated by `, `, and a
    /// simple condition as the `eq` it stands for (19 §8.1, §8.2).
    fn written(self) -> String {
        let items: Vec<String> = match self {
            Self::Simple { name, value } => vec![
                Value::String("eq".to_owned()).compact(),
                Value::String(format!("${name}")).compact(),
                Value::String(value.to_owned()).compact(),
            ],
            Self::List(items) => items.iter().map(Value::compact).collect(),
        };
        format!("[{}]", items.join(", "))
    }
}

impl Bound {
    /// A bound is a whole size: S3 answered `512.0` as "Invalid JSON." (19 §13), and so is
    /// anything else that is not one, a size below zero among them, which s3-tests expects
    /// refused (19 §7 item 30). A string is read as S3 was recorded reading `"5"`, and one
    /// holding no size fails the condition, as `"test"` did (19 §8.1).
    fn read(value: &Value) -> Result<Self, PostError> {
        let invalid = || PostError::Policy("Invalid JSON.".to_owned());
        match value {
            Value::Number(written) => {
                let digits = written.strip_prefix('-').unwrap_or(written);
                let size = digits
                    .bytes()
                    .all(|b| b.is_ascii_digit())
                    .then(|| digits.parse::<u64>().ok())
                    .flatten()
                    .ok_or_else(invalid)?;
                if size > 0 && written.starts_with('-') {
                    return Err(invalid());
                }
                Ok(Self::Size(size))
            }
            Value::String(text) => Ok(
                if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
                    text.parse().map_or(Self::Unreadable, Self::Size)
                } else {
                    Self::Unreadable
                },
            ),
            _ => Err(invalid()),
        }
    }
}

/// The value a condition's field has: `bucket` is the bucket the request's URL names, which a
/// policy's `bucket` condition is checked against (19 §4.3, §7 item 24); any other the form's
/// field of that name.
fn lookup<'a>(form: &'a Form, bucket: &'a str, field: Option<&str>) -> Option<&'a str> {
    let field = field?;
    if field.eq_ignore_ascii_case("bucket") {
        return Some(bucket);
    }
    form.get(field)
}

/// The PutObject headers a form's fields stand for, names lowercase: "Parameters that are passed
/// to `PUT` through HTTP headers are instead passed as form fields" (19 §2.1). `acl` is
/// `x-amz-acl`; `tagging`, a `Tagging` document, is read as PutObjectTagging's body is; the
/// fields that sign the form or shape its answer stand for none.
pub fn headers(form: &Form) -> Vec<(String, &str)> {
    let mut headers = Vec::new();
    for (name, value) in form.fields() {
        let lower = name.to_ascii_lowercase();
        let header = if lower == "acl" {
            "x-amz-acl".to_owned()
        } else if CONTENT.contains(&lower.as_str())
            || (lower.starts_with("x-amz-") && !AUTHORIZING.contains(&lower.as_str()))
        {
            lower
        } else {
            continue;
        };
        headers.push((header, value.as_str()));
    }
    headers
}

/// Refuses a request whose own `x-amz-` headers disagree with the fields standing for them:
/// "Conflicting values provided in HTTP headers and POST form fields" (19 §2.6). An upload's
/// settings are its fields, which its policy covers; its headers set none.
pub fn conflict(form: &Form, request: &[(&str, &str)]) -> Result<(), PostError> {
    for (header, value) in headers(form) {
        if !header.starts_with("x-amz-") {
            continue;
        }
        let differs = request
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case(&header))
            .any(|(_, sent)| sent.trim_matches(WS) != value);
        if differs {
            return Err(PostError::Conflict(header));
        }
    }
    Ok(())
}

/// How a successful upload is answered (19 §2.2, §2.5, §8, §13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// `303` to a redirect; else `200`, `201` or `204`.
    pub status: u16,
    /// The `Location` header: where a redirect goes, or the object's URL, which S3 sends with a
    /// 201 and a 204 alike.
    pub location: String,
    /// The `ETag` header, quoted, which S3 sends with the object's URL; no redirect of S3's is
    /// recorded with one.
    pub etag: Option<String>,
    /// The `PostResponse` a 201 carries; every other answer's body is empty.
    pub body: Option<String>,
}

/// The answer to a form whose file was stored as `key` in `bucket` with `etag`, unquoted, at
/// the URL `location` (see [`location`]). A `success_action_redirect`, or the deprecated
/// `redirect`, that S3 can interpret is followed with the bucket, key and ETag appended; one
/// it cannot is ignored, "as if the field is not present" (19 §2.2). Then
/// `success_action_status` `201` is a `PostResponse`, `200` an empty 200, and anything else
/// "an empty document with a 204 status code" (19 §2.2).
pub fn answer(form: &Form, bucket: &str, key: &str, etag: &str, location: &str) -> Answer {
    let redirect = form
        .get("success_action_redirect")
        .or_else(|| form.get("redirect"));
    if let Some(url) = redirect.and_then(|url| redirect_to(url, bucket, key, etag)) {
        return Answer {
            status: 303,
            location: url,
            etag: None,
            body: None,
        };
    }
    let (status, body) = match form.get("success_action_status") {
        Some("200") => (200, None),
        Some("201") => (
            201,
            Some(crate::response::post_response(location, bucket, key, etag)),
        ),
        _ => (204, None),
    };
    Answer {
        status,
        location: location.to_owned(),
        etag: Some(format!("\"{etag}\"")),
        body,
    }
}

/// The URL of the object `key` under `base`, the bucket's URL ending in `/`: the key
/// percent-encoded, `/` included, as S3 writes `Location` in a 201's body and a 204's header
/// (19 §8.2, §13).
pub fn location(base: &str, key: &str) -> String {
    format!("{base}{}", sigv4::uri_encode(key.as_bytes(), true))
}

/// `url` with `bucket`, `key` and the quoted `etag` appended to its query, as S3 appends them:
/// after a query the URL has, kept as sent, with `&`, and each value form-encoded, a key's `/`
/// as `%2F` and a space as `+` (19 §13). `None` for a URL S3 would not interpret: one that is
/// not absolute `http` or `https`, as S3 ignored a relative one (19 §8.1), or one a `Location`
/// header cannot carry, holding a control character or a byte outside ASCII. A fragment stays
/// last, where RFC 3986 §3 puts it.
fn redirect_to(url: &str, bucket: &str, key: &str, etag: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return None;
    }
    let host = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if host.is_empty()
        || host.contains(' ')
        || !url.bytes().all(|b| b == b' ' || b.is_ascii_graphic())
    {
        return None;
    }
    let (base, fragment) = match url.split_once('#') {
        Some((base, fragment)) => (base, Some(fragment)),
        None => (url, None),
    };
    let mut out = String::from(base);
    out.push(if base.contains('?') { '&' } else { '?' });
    out.push_str("bucket=");
    form_encode(&mut out, bucket);
    out.push_str("&key=");
    form_encode(&mut out, key);
    out.push_str("&etag=");
    form_encode(&mut out, &format!("\"{etag}\""));
    if let Some(fragment) = fragment {
        out.push('#');
        out.push_str(fragment);
    }
    Some(out)
}

/// `text` as `application/x-www-form-urlencoded` writes a value (WHATWG URL §5.2): letters,
/// digits and `*-._` as they are, a space as `+`, and every other byte of its UTF-8 as `%XX`,
/// which is how S3 wrote `8329%2F1391639479.7579765%2FUntitled+copy.sketch` (19 §13).
fn form_encode(out: &mut String, text: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &b in text.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'*' | b'-' | b'.' | b'_') {
            out.push(char::from(b));
        } else if b == b' ' {
            out.push('+');
        } else {
            out.push('%');
            out.push(char::from(
                HEX.get(usize::from(b >> 4)).copied().unwrap_or(b'0'),
            ));
            out.push(char::from(
                HEX.get(usize::from(b & 15)).copied().unwrap_or(b'0'),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::Decoder;

    const KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const CREDENTIAL: &str = "AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request";
    const DATE: &str = "20151229T000000Z";

    fn secret(key: &str) -> Option<String> {
        (key == KEY).then(|| SECRET.to_owned())
    }

    /// Unix milliseconds of an ISO 8601 time.
    fn at(text: &str) -> i64 {
        time::parse_iso8601(text).unwrap().0 * 1000
    }

    /// A form's fields as a browser sends them, then a file named `filename` holding `data`.
    fn form(fields: &[(&str, &str)], filename: Option<&str>, data: &[u8]) -> (Form, u64) {
        let mut body = Vec::new();
        for (name, value) in fields {
            body.extend_from_slice(
                format!(
                    "--B\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
                )
                .as_bytes(),
            );
        }
        let filename = filename
            .map(|f| format!("; filename=\"{f}\""))
            .unwrap_or_default();
        body.extend_from_slice(
            format!("--B\r\nContent-Disposition: form-data; name=\"file\"{filename}\r\nContent-Type: image/jpeg\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(data);
        body.extend_from_slice(b"\r\n--B\r\nContent-Disposition: form-data; name=\"submit\"\r\n\r\nUpload\r\n--B--\r\n");
        let mut decoder = Decoder::new(Some("multipart/form-data; boundary=B")).unwrap();
        let mut out = Vec::new();
        decoder.feed(&body, &mut out).unwrap();
        assert_eq!(out, data);
        decoder.finish().unwrap()
    }

    /// The base64 of `policy` and its signature under the example key on 2015-12-29.
    fn sign(policy: &str) -> (String, String) {
        let encoded = base64::engine::general_purpose::STANDARD.encode(policy);
        let key = sigv4::signing_key(SECRET, "20151229", "us-east-1", "s3").unwrap();
        let signature = sigv4::hex(&crypto::hmac_sha256(&key, encoded.as_bytes()).unwrap());
        (encoded, signature)
    }

    /// AWS's SigV4 POST example, byte for byte: its policy's base64 is the string to sign, and
    /// the signature is AWS's (19 §3.3).
    const AWS_POLICY: &str = "{ \"expiration\": \"2015-12-30T12:00:00.000Z\",\r\n  \"conditions\": [\r\n    {\"bucket\": \"sigv4examplebucket\"},\r\n    [\"starts-with\", \"$key\", \"user/user1/\"],\r\n    {\"acl\": \"public-read\"},\r\n    {\"success_action_redirect\": \"http://sigv4examplebucket.s3.amazonaws.com/successful_upload.html\"},\r\n    [\"starts-with\", \"$Content-Type\", \"image/\"],\r\n    {\"x-amz-meta-uuid\": \"14365123651274\"},\r\n    {\"x-amz-server-side-encryption\": \"AES256\"},\r\n    [\"starts-with\", \"$x-amz-meta-tag\", \"\"],\r\n\r\n    {\"x-amz-credential\": \"AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request\"},\r\n    {\"x-amz-algorithm\": \"AWS4-HMAC-SHA256\"},\r\n    {\"x-amz-date\": \"20151229T000000Z\" }\r\n  ]\r\n}";
    const AWS_BASE64: &str = "eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLA0KICAiY29uZGl0aW9ucyI6IFsNCiAgICB7ImJ1Y2tldCI6ICJzaWd2NGV4YW1wbGVidWNrZXQifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS8iXSwNCiAgICB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LA0KICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL3NpZ3Y0ZXhhbXBsZWJ1Y2tldC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRDb250ZW50LVR5cGUiLCAiaW1hZ2UvIl0sDQogICAgeyJ4LWFtei1tZXRhLXV1aWQiOiAiMTQzNjUxMjM2NTEyNzQifSwNCiAgICB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sDQogICAgWyJzdGFydHMtd2l0aCIsICIkeC1hbXotbWV0YS10YWciLCAiIl0sDQoNCiAgICB7IngtYW16LWNyZWRlbnRpYWwiOiAiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAxNTEyMjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LA0KICAgIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwNCiAgICB7IngtYW16LWRhdGUiOiAiMjAxNTEyMjlUMDAwMDAwWiIgfQ0KICBdDQp9";
    const AWS_SIGNATURE: &str = "8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e";

    fn aws_fields() -> Vec<(&'static str, &'static str)> {
        vec![
            ("key", "user/user1/${filename}"),
            ("acl", "public-read"),
            (
                "success_action_redirect",
                "http://sigv4examplebucket.s3.amazonaws.com/successful_upload.html",
            ),
            ("Content-Type", "image/jpeg"),
            ("x-amz-meta-uuid", "14365123651274"),
            ("x-amz-server-side-encryption", "AES256"),
            ("X-Amz-Credential", CREDENTIAL),
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Date", DATE),
            ("x-amz-meta-tag", ""),
            ("Policy", AWS_BASE64),
            ("X-Amz-Signature", AWS_SIGNATURE),
        ]
    }

    #[test]
    fn aws_example_signs_as_aws_computed() {
        assert_eq!(AWS_POLICY.len(), 648);
        let (encoded, signature) = sign(AWS_POLICY);
        assert_eq!(encoded, AWS_BASE64);
        assert_eq!(signature, AWS_SIGNATURE);
        let key = sigv4::signing_key(SECRET, "20151229", "us-east-1", "s3").unwrap();
        assert_eq!(
            sigv4::hex(&key),
            "cbcef1ebeaefc82cce6530b9f0a9ae598846065f5c5bae0674bd5ebc4ba52d28"
        );
    }

    /// AWS's example form, as its sample HTML sends it, uploads until the policy expires, and
    /// is answered with the redirect it names (19 §3.3, §2.5).
    #[test]
    fn aws_example_form_uploads_until_it_expires() {
        let (form, length) = form(
            &aws_fields(),
            Some("C:\\Users\\me\\photo.jpg"),
            b"jpeg bytes",
        );
        assert_eq!(key(&form), Ok("user/user1/photo.jpg"));
        let authorized = authorize(
            &form,
            "sigv4examplebucket",
            "us-east-1",
            at("2015-12-29T12:00:00Z"),
            secret,
        )
        .unwrap();
        assert_eq!(authorized.access_key.as_deref(), Some(KEY));
        assert_eq!(authorized.size, Size::ANY);
        authorized.size.check(length).unwrap();
        assert_eq!(
            authorize(
                &form,
                "sigv4examplebucket",
                "us-east-1",
                at("2015-12-30T12:00:00Z"),
                secret
            )
            .map(|a| a.size),
            Ok(Size::ANY)
        );
        assert_eq!(
            authorize(
                &form,
                "sigv4examplebucket",
                "us-east-1",
                at("2015-12-30T12:00:01Z"),
                secret
            ),
            Err(PostError::Expired)
        );
        // Another bucket fails the policy's bucket condition, whatever the form says.
        assert_eq!(
            authorize(
                &form,
                "otherbucket",
                "us-east-1",
                at("2015-12-29T12:00:00Z"),
                secret
            ),
            Err(PostError::Condition(
                "[\"eq\", \"$bucket\", \"sigv4examplebucket\"]".into()
            ))
        );
        assert_eq!(
            answer(&form, "sigv4examplebucket", "user/user1/photo.jpg", "39d459dfbc0faabbb5e179358dfb94c3", ""),
            Answer {
                status: 303,
                location: "http://sigv4examplebucket.s3.amazonaws.com/successful_upload.html?bucket=sigv4examplebucket&key=user%2Fuser1%2Fphoto.jpg&etag=%2239d459dfbc0faabbb5e179358dfb94c3%22".into(),
                etag: None,
                body: None,
            }
        );
    }

    /// botocore's `generate_presigned_post` at a fixed clock, reproduced offline (19 §6.1):
    /// lowercase fields, a compact policy of `json.dumps`, and its signature.
    #[test]
    fn botocore_presigned_post_verifies() {
        let policy = "{\"expiration\": \"2015-12-29T01:00:00Z\", \"conditions\": [{\"bucket\": \"sigv4examplebucket\"}, {\"key\": \"user/user1/photo.jpg\"}, {\"x-amz-algorithm\": \"AWS4-HMAC-SHA256\"}, {\"x-amz-credential\": \"AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request\"}, {\"x-amz-date\": \"20151229T000000Z\"}]}";
        let (encoded, signature) = sign(policy);
        assert_eq!(
            encoded,
            "eyJleHBpcmF0aW9uIjogIjIwMTUtMTItMjlUMDE6MDA6MDBaIiwgImNvbmRpdGlvbnMiOiBbeyJidWNrZXQiOiAic2lndjRleGFtcGxlYnVja2V0In0sIHsia2V5IjogInVzZXIvdXNlcjEvcGhvdG8uanBnIn0sIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwgeyJ4LWFtei1jcmVkZW50aWFsIjogIkFLSUFJT1NGT0ROTjdFWEFNUExFLzIwMTUxMjI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSwgeyJ4LWFtei1kYXRlIjogIjIwMTUxMjI5VDAwMDAwMFoifV19"
        );
        assert_eq!(
            signature,
            "3364624b39010d9b06f5d9b8c9a18a6e478d261c048159c92f2fd6fa885f14e6"
        );
        let fields = [
            ("key", "user/user1/photo.jpg"),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", CREDENTIAL),
            ("x-amz-date", DATE),
            ("policy", encoded.as_str()),
            ("x-amz-signature", signature.as_str()),
        ];
        let (form, _) = form(&fields, Some("photo.jpg"), b"x");
        let now = at("2015-12-29T00:30:00Z");
        assert!(authorize(&form, "sigv4examplebucket", "us-east-1", now, secret).is_ok());
        assert_eq!(
            authorize(&form, "sigv4examplebucket", "eu-west-1", now, secret).map_err(|e| e.code()),
            Err(("InvalidArgument", 400))
        );
        assert_eq!(
            authorize(&form, "sigv4examplebucket", "us-east-1", now, |_| None),
            Err(PostError::UnknownKey(KEY.into()))
        );
    }

    /// A policy for the example credentials, signed, and the fields that carry it.
    fn signed(policy: &str, fields: &[(&str, &str)]) -> Form {
        let (encoded, signature) = sign(policy);
        let mut all: Vec<(&str, &str)> = fields.to_vec();
        all.extend([
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", CREDENTIAL),
            ("x-amz-date", DATE),
            ("policy", encoded.as_str()),
            ("x-amz-signature", signature.as_str()),
        ]);
        form(&all, Some("foo.txt"), b"bar").0
    }

    /// The fields every signed test policy covers, then `conditions`.
    fn policy(conditions: &str) -> String {
        format!(
            "{{\"expiration\": \"2015-12-30T00:00:00Z\", \"conditions\": [{{\"bucket\": \"b\"}}, [\"starts-with\", \"$key\", \"\"], {{\"x-amz-algorithm\": \"AWS4-HMAC-SHA256\"}}, {{\"x-amz-credential\": \"{CREDENTIAL}\"}}, {{\"x-amz-date\": \"{DATE}\"}}{conditions}]}}"
        )
    }

    fn check(form: &Form) -> Result<Authorized, PostError> {
        authorize(form, "b", "us-east-1", at("2015-12-29T12:00:00Z"), secret)
    }

    /// LocalStack's recordings of S3 (19 §8.1): the fields a form must hold, named as S3 names
    /// them, and a form with no signature is anonymous.
    #[test]
    fn required_fields_are_named_as_s3_names_them() {
        let (encoded, signature) = sign(&policy(""));
        let full = [
            ("key", "k"),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", CREDENTIAL),
            ("x-amz-date", DATE),
            ("policy", encoded.as_str()),
            ("x-amz-signature", signature.as_str()),
        ];
        let without = |names: &[&str]| {
            let fields: Vec<_> = full
                .iter()
                .copied()
                .filter(|(n, _)| !names.contains(n))
                .collect();
            check(&form(&fields, None, b"").0)
        };
        assert!(without(&[]).is_ok());
        let missing = without(&["x-amz-signature"]).unwrap_err();
        assert_eq!(missing, PostError::Missing("X-Amz-Signature"));
        assert_eq!(
            missing.to_string(),
            "Bucket POST must contain a field named 'X-Amz-Signature'.  If it is specified, please check the order of the fields."
        );
        assert_eq!(missing.code(), ("InvalidArgument", 400));
        assert_eq!(
            missing.details(),
            vec![
                ("ArgumentName", "X-Amz-Signature".to_owned()),
                ("ArgumentValue", String::new())
            ]
        );
        assert_eq!(
            without(&["x-amz-algorithm", "x-amz-credential"]),
            Err(PostError::Missing("X-Amz-Algorithm"))
        );
        assert_eq!(without(&["policy"]), Err(PostError::Missing("Policy")));
        assert_eq!(without(&["key"]), Err(PostError::Missing("key")));
        assert_eq!(
            without(&[
                "x-amz-algorithm",
                "x-amz-credential",
                "x-amz-date",
                "x-amz-signature"
            ]),
            Ok(Authorized {
                access_key: None,
                size: Size::ANY
            })
        );
        let v2 = form(
            &[
                ("key", "k"),
                ("AWSAccessKeyId", KEY),
                ("signature", "x"),
                ("policy", "p"),
            ],
            None,
            b"",
        )
        .0;
        assert_eq!(check(&v2), Err(PostError::SignatureVersion));
        // The signing fields' values, each answered as S3 answered it (19 §13).
        let with = |field: &str, value: &str| {
            let fields: Vec<_> = full
                .iter()
                .map(|&(n, v)| if n == field { (n, value) } else { (n, v) })
                .collect();
            check(&form(&fields, None, b"").0).unwrap_err()
        };
        let argument = |error: PostError| {
            let details = error.details();
            (
                error.to_string(),
                details[0].1.clone(),
                details[1].1.clone(),
            )
        };
        assert_eq!(
            argument(with("x-amz-date", "2017-10-22T03:21:54+00:00")),
            (
                "X-Amz-Date must be formated via ISO8601 Long format".into(),
                "X-Amz-Date".into(),
                "2017-10-22T03:21:54+00:00".into()
            )
        );
        let empty = "/20240616/us-east-1/s3/aws4_request";
        assert_eq!(
            argument(with("x-amz-credential", empty)),
            (
                "a non-empty Access Key (AKID) must be provided in the credential.".into(),
                "X-Amz-Credential".into(),
                empty.into()
            )
        );
        let west = "AKIAIOSFODNN7EXAMPLE/20151229/us-west-1/s3/aws4_request";
        let region = with("x-amz-credential", west);
        assert_eq!(
            region.to_string(),
            "the region 'us-west-1' is wrong; expecting 'us-east-1'"
        );
        assert_eq!(
            region.details(),
            vec![
                ("ArgumentName", "X-Amz-Credential".to_owned()),
                ("ArgumentValue", west.to_owned()),
                ("Region", "us-east-1".to_owned())
            ]
        );
        for (credential, message) in [
            (
                "no-slashes",
                "the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
            ),
            (
                "AKID/2015122/us-east-1/s3/aws4_request",
                "incorrect date format \"2015122\". This date in the credential must be in the format \"yyyyMMdd\".",
            ),
            (
                "AKID/20151229/us-east-1/s4/aws4_request",
                "incorrect service \"s4\". This endpoint belongs to \"s3\".",
            ),
            (
                "AKID/20151229/us-east-1/s3/aws5_request",
                "incorrect terminal \"aws5_request\". This endpoint uses \"aws4_request\".",
            ),
        ] {
            assert_eq!(with("x-amz-credential", credential).to_string(), message);
        }
        assert_eq!(
            with("x-amz-algorithm", "AWS4-ECDSA-P256-SHA256").code(),
            ("InvalidArgument", 400)
        );
        let unknown = with(
            "x-amz-credential",
            "AKIDUNKNOWN/20151229/us-east-1/s3/aws4_request",
        );
        assert_eq!(
            (unknown.to_string(), unknown.code(), unknown.details()),
            (
                "The AWS Access Key Id you provided does not exist in our records.".into(),
                ("InvalidAccessKeyId", 403),
                vec![("AWSAccessKeyId", "AKIDUNKNOWN".to_owned())]
            )
        );
        let long = "k".repeat(MAX_KEY + 1);
        assert_eq!(
            key(&form(&[("key", &long)], None, b"").0),
            Err(PostError::KeyTooLong)
        );
        assert_eq!(
            key(&form(&[("key", "${filename}")], Some(""), b"").0),
            Err(PostError::Missing("key"))
        );
    }

    /// A policy changed after signing is `SignatureDoesNotMatch`, with the policy as sent as the
    /// string to sign (19 §8.1).
    #[test]
    fn a_changed_policy_does_not_match_its_signature() {
        let (encoded, signature) = sign(&policy(""));
        let truncated = encoded.get(..encoded.len() - 2).unwrap();
        let fields = [
            ("key", "k"),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-credential", CREDENTIAL),
            ("x-amz-date", DATE),
            ("policy", truncated),
            ("x-amz-signature", signature.as_str()),
        ];
        let refused = check(&form(&fields, None, b"").0).unwrap_err();
        assert_eq!(refused.code(), ("SignatureDoesNotMatch", 403));
        let details = refused.details();
        assert_eq!(details[0], ("AWSAccessKeyId", KEY.to_owned()));
        assert_eq!(details[1], ("StringToSign", truncated.to_owned()));
        assert_eq!(details[2], ("SignatureProvided", signature.clone()));
        assert!(details[3].1.starts_with("65 79 4a 6c"));
    }

    /// S3's recorded answers to policies and conditions (19 §8.1, §8.2).
    #[test]
    fn conditions_are_judged_as_s3_judges_them() {
        let redirect = ", [\"eq\", \"$success_action_redirect\", \"http://localhost.test/random\"]";
        let form_with = |conditions: &str, fields: &[(&str, &str)]| {
            let mut all = vec![("key", "k")];
            all.extend_from_slice(fields);
            check(&signed(&policy(conditions), &all))
        };
        assert!(
            form_with(
                redirect,
                &[("success_action_redirect", "http://localhost.test/random")]
            )
            .is_ok()
        );
        let failed = form_with(
            redirect,
            &[("success_action_redirect", "http://localhost.test/other")],
        )
        .unwrap_err();
        assert_eq!(
            failed.to_string(),
            "Invalid according to Policy: Policy Condition failed: [\"eq\", \"$success_action_redirect\", \"http://localhost.test/random\"]"
        );
        assert_eq!(failed.code(), ("AccessDenied", 403));
        // Values compare with case; names without.
        assert!(
            form_with(
                redirect,
                &[("success_action_redirect", "HTTP://localhost.test/random")]
            )
            .is_err()
        );
        assert!(
            form_with(
                ", [\"eq\", \"$success_Action_REDIRECT\", \"x\"]",
                &[("Success_Action_Redirect", "x")]
            )
            .is_ok()
        );
        assert!(
            form_with(
                ", {\"x-amz-meta-test-2\": \"v\"}",
                &[("x-amz-meta-TEST-2", "v")]
            )
            .is_ok()
        );
        // A name without `$` names no field.
        assert_eq!(
            form_with(
                ", [\"eq\", \"success_action_redirect\", \"x\"]",
                &[("success_action_redirect", "x")]
            ),
            Err(PostError::Condition(
                "[\"eq\", \"success_action_redirect\", \"x\"]".into()
            ))
        );
        // A field the form lacks fails its condition, even `starts-with ""`.
        assert_eq!(
            form_with(", [\"starts-with\", \"$Content-Type\", \"\"]", &[]),
            Err(PostError::Condition(
                "[\"starts-with\", \"$Content-Type\", \"\"]".into()
            ))
        );
        // A simple condition is written as the `eq` it stands for.
        assert_eq!(
            form_with(", {\"acl\": \"private\"}", &[("acl", "public-read")]),
            Err(PostError::Condition(
                "[\"eq\", \"$acl\", \"private\"]".into()
            ))
        );
        // A field no condition covers, named as sent, as S3 named `StorageClass` (19 §13).
        assert_eq!(
            form_with("", &[("StorageClass", "x")]).map_err(|e| e.to_string()),
            Err("Invalid according to Policy: Extra input fields: StorageClass".into())
        );
        // A condition's text keeps the policy's spelling and escapes its quotes (19 §13).
        assert_eq!(
            form_with(
                ", [\"eq\", \"$Content-Disposition\", \"filename=\\\"test.png\\\"\"]",
                &[("Content-Disposition", "inline")]
            )
            .map_err(|e| e.to_string()),
            Err("Invalid according to Policy: Policy Condition failed: [\"eq\", \"$Content-Disposition\", \"filename=\\\"test.png\\\"\"]".into())
        );
        // Ignored and exempt fields need no condition.
        assert!(form_with("", &[("x-ignore-foo", "bar"), ("X-Ignore-Bar", "baz")]).is_ok());
        // Content types in a comma list must each start with the prefix.
        let image = ", [\"starts-with\", \"$Content-Type\", \"image/\"]";
        assert!(form_with(image, &[("Content-Type", "image/jpg,image/png, image/gif")]).is_ok());
        assert!(form_with(image, &[("Content-Type", "image/jpg,text/plain")]).is_err());
        let other = ", [\"starts-with\", \"$x-amz-meta-list\", \"a\"]";
        assert!(form_with(other, &[("x-amz-meta-list", "a,b")]).is_ok());
    }

    #[test]
    fn malformed_policies_are_refused_with_s3_s_messages() {
        let refused = |policy: &str| check(&signed(policy, &[("key", "k")])).unwrap_err();
        let invalid = |policy: &str| {
            let error = refused(policy);
            assert_eq!(error.code(), ("InvalidPolicyDocument", 400), "{policy}");
            error.to_string()
        };
        assert_eq!(invalid("not json"), "Invalid Policy: Invalid JSON.");
        assert_eq!(
            invalid(&policy(
                ", {\"bucket\": \"b\", \"success_action_redirect\": \"r\"}"
            )),
            "Invalid Policy: Invalid Simple-Condition: Simple-Conditions must have exactly one property specified."
        );
        assert_eq!(
            invalid(&policy(", {}")),
            "Invalid Policy: Invalid Simple-Condition: Simple-Conditions must have exactly one property specified."
        );
        assert_eq!(
            invalid(&policy(", {\"Content-Length\": 6199034}")),
            "Invalid Policy: Invalid Simple-Condition: value must be a string."
        );
        // s3-tests' shapes (19 §5): names in the wrong case, missing members, bad bounds.
        invalid("{\"EXPIRATION\": \"2015-12-30T00:00:00Z\", \"conditions\": []}");
        invalid("{\"expiration\": \"2015-12-30T00:00:00Z\", \"CONDITIONS\": []}");
        invalid("{\"expiration\": \"2015-12-30T00:00:00Z\"}");
        invalid("{\"conditions\": []}");
        invalid("{\"expiration\": \"2015-12-30 00:00:00.123456+00:00\", \"conditions\": []}");
        invalid(&policy(", [\"content-length-range\", 0]"));
        invalid(&policy(", [\"content-length-range\", -1, 0]"));
        invalid(&policy(", [\"content-length-range\", 1.5, 10]"));
        invalid(&policy(", [\"ne\", \"$key\", \"k\"]"));
        invalid(&policy(", [\"eq\", \"$key\"]"));
        invalid(&policy(", \"key\""));
        // S3's recorded answers to other shapes (19 §13).
        let message = |policy: &str| refused(policy).to_string();
        assert_eq!(
            message(&policy(", {\"success_action_redirect\": null}")),
            "Invalid Policy: Invalid JSON."
        );
        assert_eq!(
            message(&policy(", [\"content-length-range\", 0, 512.0]")),
            "Invalid Policy: Invalid JSON."
        );
        assert_eq!(
            message(&policy(", [\"eq\", \"$key\", 5]")),
            "Invalid Policy: Invalid JSON."
        );
        assert_eq!(
            message(&policy(", []")),
            "Invalid Policy: Invalid Condition: missing operation identifier."
        );
        assert_eq!(
            message(&policy(", [\"starts-with\", \"$key\"]")),
            "Invalid Policy: Invalid starts-with: wrong number of arguments."
        );
        assert_eq!(
            message(&policy(", [\"ne\", \"$key\", \"k\"]")),
            "Invalid Policy: Invalid Condition: unknown operation 'ne'."
        );
        assert_eq!(
            message("{\"Version\": \"2012-10-17\", \"Statement\": []}"),
            "Invalid Policy: Unexpected: 'version'"
        );
        assert_eq!(
            message("{\"expiration\": \"2011-09-13T07:52:58+02:00\", \"conditions\": []}"),
            "Invalid Policy: Invalid 'expiration' value: '2011-09-13T07:52:58+02:00'"
        );
        assert_eq!(
            message("{\"expiration\": \"2015-10-17 03:15:59 UTC\", \"conditions\": []}"),
            "Invalid Policy: Invalid 'expiration' value: '2015-10-17 03:15:59 UTC'"
        );
    }

    /// `content-length-range` bounds the file, inclusive, as S3 was recorded bounding it (19
    /// §8.1), from numbers or strings holding them.
    #[test]
    fn content_length_range_bounds_the_file() {
        let size = |range: &str| check(&signed(&policy(range), &[("key", "k")])).map(|a| a.size);
        let bounded = size(", [\"content-length-range\", 5, 10]").unwrap();
        assert_eq!(bounded, Size { min: 5, max: 10 });
        assert_eq!(
            size(", [\"content-length-range\", \"5\", \"10\"]"),
            Ok(bounded)
        );
        assert_eq!(
            size(", [\"content-length-range\", 0, 100], [\"content-length-range\", 5, 1000]"),
            Ok(Size { min: 5, max: 100 })
        );
        for ok in [5, 10] {
            assert_eq!(bounded.check(ok), Ok(()));
        }
        let large = bounded.check(12).unwrap_err();
        assert_eq!(large.code(), ("EntityTooLarge", 400));
        assert_eq!(
            large.to_string(),
            "Your proposed upload exceeds the maximum allowed size"
        );
        assert_eq!(
            large.details(),
            vec![
                ("ProposedSize", "12".to_owned()),
                ("MaxSizeAllowed", "10".to_owned())
            ]
        );
        let small = bounded.check(1).unwrap_err();
        assert_eq!(
            small.to_string(),
            "Your proposed upload is smaller than the minimum allowed size"
        );
        assert_eq!(
            small.details(),
            vec![
                ("ProposedSize", "1".to_owned()),
                ("MinSizeAllowed", "5".to_owned())
            ]
        );
        assert_eq!(
            size(", [\"content-length-range\", \"test\", \"10\"]"),
            Err(PostError::Condition(
                "[\"content-length-range\", \"test\", \"10\"]".into()
            ))
        );
    }

    /// s3-tests' POST tests, signed with Signature Version 4 in place of the Version 2 they
    /// use (19 §5).
    #[test]
    fn s3_tests_policies_judge_as_expected() {
        let c0 = ", [\"starts-with\", \"$key\", \"foo\"], {\"acl\": \"private\"}, [\"starts-with\", \"$Content-Type\", \"text/plain\"], [\"content-length-range\", 0, 1024]";
        let f0 = [
            ("key", "foo.txt"),
            ("acl", "private"),
            ("Content-Type", "text/plain"),
        ];
        assert!(check(&signed(&policy(c0), &f0)).is_ok());
        // case_insensitive_condition_fields
        let mixed = format!(
            "{{\"expiration\": \"2015-12-30T00:00:00Z\", \"conditions\": [{{\"bUcKeT\": \"b\"}}, [\"StArTs-WiTh\", \"$KeY\", \"foo\"], {{\"AcL\": \"private\"}}, [\"StArTs-WiTh\", \"$CoNtEnT-TyPe\", \"text/plain\"], {{\"x-amz-algorithm\": \"AWS4-HMAC-SHA256\"}}, {{\"x-amz-credential\": \"{CREDENTIAL}\"}}, {{\"x-amz-date\": \"{DATE}\"}}]}}"
        );
        assert!(
            check(&signed(
                &mixed,
                &[
                    ("kEy", "foo.txt"),
                    ("aCl", "private"),
                    ("Content-Type", "text/plain")
                ]
            ))
            .is_ok()
        );
        // escaped_field_values: `\$` is no escape.
        let escaped = ", [\"starts-with\", \"$key\", \"\\\\$foo\"]";
        let form = signed(&policy(escaped), &[("key", "\\$foo.txt")]);
        assert!(check(&form).is_ok());
        assert_eq!(key(&form), Ok("\\$foo.txt"));
        // invalid_request_field_value
        assert!(matches!(
            check(&signed(
                &policy(", [\"eq\", \"$x-amz-meta-foo\", \"\"]"),
                &[("key", "k"), ("x-amz-meta-foo", "barclamp")]
            )),
            Err(PostError::Condition(_))
        ));
        // request_missing_policy_specified_field
        assert!(matches!(
            check(&signed(
                &policy(", [\"starts-with\", \"$x-amz-meta-foo\", \"bar\"]"),
                &[("key", "k")]
            )),
            Err(PostError::Condition(_))
        ));
        // set_key_from_filename
        let form = signed(&policy(""), &[("key", "${filename}")]);
        assert_eq!(key(&form), Ok("foo.txt"));
        assert!(check(&form).is_ok());
    }

    #[test]
    fn answers_follow_the_form() {
        let url = location("https://b.s3.example.com/", "dir/my key+é");
        let answer_to = |fields: &[(&str, &str)]| {
            let (form, _) = form(fields, None, b"");
            answer(&form, "b", "dir/my key+é", "e", &url)
        };
        let plain = |status| Answer {
            status,
            location: url.clone(),
            etag: Some("\"e\"".into()),
            body: None,
        };
        // A 204 carries the object's ETag and Location, as S3's does (19 §13).
        assert_eq!(answer_to(&[]), plain(204));
        assert_eq!(answer_to(&[("success_action_status", "404")]), plain(204));
        assert_eq!(answer_to(&[("success_action_status", "200")]), plain(200));
        let created = answer_to(&[("success_action_status", "201")]);
        assert_eq!((created.status, &created.location), (201, &url));
        assert_eq!(
            created.body.unwrap(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse><Location>https://b.s3.example.com/dir%2Fmy%20key%2B%C3%A9</Location><Bucket>b</Bucket><Key>dir/my key+é</Key><ETag>\"e\"</ETag></PostResponse>"
        );
        // A relative redirect is ignored, as S3 ignored one; the deprecated field is followed.
        assert_eq!(
            answer_to(&[
                ("success_action_redirect", "/wrong/redirect/relative"),
                ("success_action_status", "201")
            ]),
            answer_to(&[("success_action_status", "201")])
        );
        let redirect = |url: &str| Answer {
            status: 303,
            location: url.into(),
            etag: None,
            body: None,
        };
        assert_eq!(
            answer_to(&[("redirect", "https://example.com/done?x=1#top")]),
            redirect(
                "https://example.com/done?x=1&bucket=b&key=dir%2Fmy+key%2B%C3%A9&etag=%22e%22#top"
            )
        );
        // An existing query is kept as sent, raw space and all, as S3 kept one (19 §13).
        assert_eq!(
            answer_to(&[("success_action_redirect", "https://example.com/up?name=a b")]),
            redirect(
                "https://example.com/up?name=a b&bucket=b&key=dir%2Fmy+key%2B%C3%A9&etag=%22e%22"
            )
        );
        for ignored in [
            "ftp://example.com/",
            "http:///path",
            "https://exa mple.com/",
            "example.com",
            "https://example.com/\r\nSet-Cookie: x",
            "https://example.com/é",
        ] {
            assert_eq!(
                answer_to(&[("success_action_redirect", ignored)]),
                plain(204),
                "{ignored}"
            );
        }
    }

    #[test]
    fn fields_stand_for_put_object_s_headers() {
        let (form, _) = form(
            &[
                ("key", "k"),
                ("acl", "public-read"),
                ("Content-Type", "image/jpeg"),
                ("x-amz-meta-TEST-2", "v"),
                ("X-Amz-Storage-Class", "STANDARD_IA"),
                ("tagging", "<Tagging/>"),
                ("success_action_status", "201"),
                ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
                ("x-amz-security-token", "t"),
                ("policy", "p"),
                ("x-ignore-foo", "bar"),
            ],
            None,
            b"",
        );
        assert_eq!(
            headers(&form),
            vec![
                ("x-amz-acl".to_owned(), "public-read"),
                ("content-type".to_owned(), "image/jpeg"),
                ("x-amz-meta-test-2".to_owned(), "v"),
                ("x-amz-storage-class".to_owned(), "STANDARD_IA"),
            ]
        );
        assert_eq!(
            conflict(
                &form,
                &[
                    ("Content-Type", "multipart/form-data; boundary=B"),
                    ("X-Amz-Acl", "public-read")
                ]
            ),
            Ok(())
        );
        assert_eq!(
            conflict(&form, &[("x-amz-acl", "private")]),
            Err(PostError::Conflict("x-amz-acl".into()))
        );
    }
}
