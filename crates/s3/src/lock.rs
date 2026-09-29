//! Object Lock (docs/research/18) as requests carry it: a bucket's configuration, a version's
//! retention and legal hold, and the headers an object write names them in, each checked as S3
//! checks it. Where locks are kept and enforced, on the versions themselves, is the metadata
//! layer's.
//!
//! Event holds, which S3 added in September 2026 with no recorded answers yet (18 §2.6), are
//! refused as not implemented.

use crate::time::{iso8601, parse_iso8601};

/// The longest default retention, in days: "The maximum retention period is 100 years", a year
/// of 365 days as S3 counts one for a retention duration (18 §2.4, §2.6).
pub const MAX_DAYS: u32 = 36_500;

/// The longest default retention, in years (18 §2.4).
pub const MAX_YEARS: u32 = 100;

/// Milliseconds in a day.
const DAY: i64 = 86_400_000;

/// A retention mode (18 §2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Changed or bypassed only with `s3:BypassGovernanceRetention`.
    Governance,
    /// Never shortened, changed or bypassed.
    Compliance,
}

impl Mode {
    /// The mode a value names, exactly: s3-tests expects `governance` refused (18 §4).
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "GOVERNANCE" => Some(Self::Governance),
            "COMPLIANCE" => Some(Self::Compliance),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Governance => "GOVERNANCE",
            Self::Compliance => "COMPLIANCE",
        }
    }
}

/// A default retention period: days or years, never both (18 §1.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Period {
    Days(u32),
    Years(u32),
}

impl Period {
    /// The period in milliseconds, a year counted as 365 days (18 §2.6).
    pub fn millis(self) -> i64 {
        let days = match self {
            Self::Days(days) => i64::from(days),
            Self::Years(years) => i64::from(years).saturating_mul(365),
        };
        days.saturating_mul(DAY)
    }
}

/// A bucket's Object Lock configuration: Object Lock is on, and new versions may take a
/// default retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Configuration {
    pub default: Option<(Mode, Period)>,
}

impl Configuration {
    /// The retention a version created at `created` (Unix milliseconds) takes from the default:
    /// "adding the specified duration to the object version's creation timestamp" (18 §2.4).
    pub fn default_for(&self, created: i64) -> Option<Retention> {
        self.default.map(|(mode, period)| Retention {
            mode,
            until: created.saturating_add(period.millis()),
        })
    }
}

/// A version's retention: its mode and the instant it lasts until, Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub mode: Mode,
    pub until: i64,
}

impl Retention {
    /// `RetainUntilDate`, and `x-amz-object-lock-retain-until-date`, as S3 writes a time
    /// (13 §9.2): `2030-01-01T00:00:00.000Z`. `None` past the year 9999.
    pub fn until_text(&self) -> Option<String> {
        iso8601(self.until)
    }
}

/// What PutObjectRetention asks for: a retention, or with an empty `Retention` none, which
/// removes a GOVERNANCE retention under bypass (18 §2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionRequest {
    Set(Retention),
    Remove,
}

/// The lock an object write's headers ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Requested {
    pub retention: Option<Retention>,
    /// `x-amz-object-lock-legal-hold`, `ON` or `OFF`, if sent.
    pub legal_hold: Option<bool>,
}

impl Requested {
    /// Whether the write names any lock, which with a bucket default makes it a write "with
    /// Object Lock parameters" (18 §5).
    pub fn any(&self) -> bool {
        self.retention.is_some() || self.legal_hold.is_some()
    }
}

/// Object Lock's faults, with S3's recorded messages (18 §5).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LockError {
    #[error("x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied")]
    Unpaired { missing: &'static str },
    #[error("The retain until date must be provided in ISO 8601 format")]
    DateFormat(String),
    #[error("The retain until date must be in the future!")]
    Past {
        argument: &'static str,
        value: String,
    },
    #[error("Unknown wormMode directive.")]
    Mode(String),
    #[error("Legal Hold must be either of 'ON' or 'OFF'")]
    LegalHold(String),
    #[error("Default retention period must be a positive integer value.")]
    PeriodNotPositive(&'static str),
    #[error("Default retention period too large.")]
    PeriodTooLarge(&'static str),
    #[error(
        "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters"
    )]
    Integrity,
    #[error(
        "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Part requests with Object Lock parameters"
    )]
    PartIntegrity,
    #[error("Put Object requests with Object Lock parameters require AWS Signature Version 4")]
    Anonymous,
    #[error("x-amz-bypass-governance-retention is only applicable to Object Lock enabled buckets.")]
    Bypass,
    #[error("Bucket is missing Object Lock Configuration")]
    NotEnabled,
    #[error("Bucket is missing ObjectLockConfiguration")]
    WriteNotEnabled,
    #[error("event holds, which mantle does not implement")]
    EventHold,
}

impl LockError {
    /// The S3 error code and status (18 §5).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Unpaired { .. }
            | Self::DateFormat(_)
            | Self::Past { .. }
            | Self::Mode(_)
            | Self::LegalHold(_)
            | Self::PeriodNotPositive(_)
            | Self::PeriodTooLarge(_)
            | Self::Anonymous
            | Self::Bypass => ("InvalidArgument", 400),
            Self::Integrity | Self::PartIntegrity | Self::NotEnabled | Self::WriteNotEnabled => {
                ("InvalidRequest", 400)
            }
            Self::EventHold => ("NotImplemented", 501),
        }
    }

    /// The `ArgumentName` and `ArgumentValue` S3's error carries, as recorded (18 §5).
    pub fn argument(&self) -> Option<(&'static str, Option<&str>)> {
        const DATE: &str = "x-amz-object-lock-retain-until-date";
        match self {
            Self::Unpaired { missing } => Some((missing, None)),
            Self::DateFormat(value) => Some((DATE, Some(value))),
            Self::Past { argument, value } => Some((argument, Some(value))),
            Self::Mode(value) => Some(("x-amz-object-lock-mode", Some(value))),
            Self::LegalHold(value) => Some(("x-amz-object-lock-legal-hold", Some(value))),
            Self::PeriodNotPositive(argument) | Self::PeriodTooLarge(argument) => {
                Some((argument, None))
            }
            Self::Anonymous => Some(("Authorization", None)),
            Self::Bypass => Some(("x-amz-bypass-governance-retention", None)),
            _ => None,
        }
    }
}

/// The lock PutObject's, CopyObject's or CreateMultipartUpload's headers name, at `now` (Unix
/// milliseconds), checked in the order S3 was recorded answering: a mode and a date named
/// together; the date's form, then that it is ahead; the legal hold; the mode (18 §5).
pub fn requested(
    mode: Option<&str>,
    until: Option<&str>,
    legal_hold: Option<&str>,
    now: i64,
) -> Result<Requested, LockError> {
    let retention = match (mode, until) {
        (None, None) => None,
        (Some(_), None) => {
            return Err(LockError::Unpaired {
                missing: "x-amz-object-lock-retain-until-date",
            });
        }
        (None, Some(_)) => {
            return Err(LockError::Unpaired {
                missing: "x-amz-object-lock-mode",
            });
        }
        (Some(mode), Some(until)) => Some((mode, until)),
    };
    let dated = match retention {
        None => None,
        Some((mode, until)) => {
            let at = instant(until).ok_or_else(|| LockError::DateFormat(until.to_owned()))?;
            if at <= now {
                return Err(LockError::Past {
                    argument: "x-amz-object-lock-retain-until-date",
                    value: until.to_owned(),
                });
            }
            Some((mode, at))
        }
    };
    let legal_hold = legal_hold
        .map(|value| match value {
            "ON" => Ok(true),
            "OFF" => Ok(false),
            other => Err(LockError::LegalHold(other.to_owned())),
        })
        .transpose()?;
    let retention = dated
        .map(|(mode, until)| {
            Mode::from_name(mode)
                .map(|mode| Retention { mode, until })
                .ok_or_else(|| LockError::Mode(mode.to_owned()))
        })
        .transpose()?;
    Ok(Requested {
        retention,
        legal_hold,
    })
}

/// Whether a retention set by PutObjectRetention at `now` is ahead: "The retain until date must
/// be in the future!" (18 §5).
pub fn ahead(request: RetentionRequest, written: &str, now: i64) -> Result<(), LockError> {
    match request {
        RetentionRequest::Set(retention) if retention.until <= now => Err(LockError::Past {
            argument: "RetainUntilDate",
            value: written.to_owned(),
        }),
        _ => Ok(()),
    }
}

/// An ISO 8601 instant, as a timestamp header carries one, as Unix milliseconds; a time finer
/// than a millisecond is truncated, as botocore truncates a header's to whole seconds (18 §3).
pub fn instant(text: &str) -> Option<i64> {
    let (seconds, nanos) = parse_iso8601(text)?;
    seconds
        .checked_mul(1000)?
        .checked_add(i64::from(nanos / 1_000_000))
}

/// What an object write with Object Lock parameters must carry to be written: "Content-MD5 OR
/// x-amz-checksum- HTTP header", a trailer counting and a SigV4 payload hash not, and a
/// signature (18 §5). `locked` is whether the write names a lock or the bucket has a default
/// retention; `part` whether it is an UploadPart of such an upload.
pub fn integrity(
    locked: bool,
    part: bool,
    content_md5: bool,
    checksum: bool,
    signed: bool,
) -> Result<(), LockError> {
    if !locked || content_md5 || checksum {
        if locked && !signed && !part {
            return Err(LockError::Anonymous);
        }
        return Ok(());
    }
    Err(if part {
        LockError::PartIntegrity
    } else {
        LockError::Integrity
    })
}

/// `x-amz-bypass-governance-retention`: whether it asks to bypass, `true` in any case, as a
/// Java boolean reads. On a bucket without Object Lock it is refused whatever its value, as S3
/// refused it, although s3-tests' teardown sends it to every bucket (18 §4, §5).
pub fn bypass(header: Option<&str>, lock_enabled: bool) -> Result<bool, LockError> {
    match header {
        None => Ok(false),
        Some(_) if !lock_enabled => Err(LockError::Bypass),
        Some(value) => Ok(value.eq_ignore_ascii_case("true")),
    }
}

/// A default retention period's days or years: at least 1, as S3 answered 0 and -1, and at
/// most 100 years, the longest retention S3 documents (18 §2.4, §5).
pub fn period(days: Option<i32>, years: Option<i32>) -> Result<Option<Period>, PeriodFault> {
    match (days, years) {
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(PeriodFault::Both),
        (Some(days), None) => bounded(days, MAX_DAYS, "Days").map(|d| Some(Period::Days(d))),
        (None, Some(years)) => bounded(years, MAX_YEARS, "Years").map(|y| Some(Period::Years(y))),
    }
}

/// Why a default retention period is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeriodFault {
    /// `Days` and `Years` together, which the schema refuses: `MalformedXML` (18 §5).
    Both,
    Error(LockError),
}

fn bounded(value: i32, most: u32, argument: &'static str) -> Result<u32, PeriodFault> {
    let value = u32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(PeriodFault::Error(LockError::PeriodNotPositive(argument)))?;
    if value > most {
        return Err(PeriodFault::Error(LockError::PeriodTooLarge(argument)));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_790_000_000_000;

    /// LocalStack's and the issue trackers' recordings of S3's answers to lock headers (18 §5).
    #[test]
    fn headers_are_refused_as_s3_refused_them() {
        let future = "2099-01-01T00:00:00Z";
        let code = |mode, until, hold| requested(mode, until, hold, NOW).unwrap_err();
        let only_mode = code(Some("GOVERNANCE"), None, None);
        assert_eq!(
            only_mode.to_string(),
            "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied"
        );
        assert_eq!(
            only_mode.argument(),
            Some(("x-amz-object-lock-retain-until-date", None))
        );
        assert_eq!(
            code(Some("BAD-VALUE"), None, None).argument(),
            Some(("x-amz-object-lock-retain-until-date", None))
        );
        assert_eq!(
            code(None, Some(future), None).argument(),
            Some(("x-amz-object-lock-mode", None))
        );
        let bad_mode = code(Some("BAD-VALUE"), Some(future), None);
        assert_eq!(bad_mode.to_string(), "Unknown wormMode directive.");
        assert_eq!(
            bad_mode.argument(),
            Some(("x-amz-object-lock-mode", Some("BAD-VALUE")))
        );
        let bad_date = code(Some("abc"), Some("abc"), None);
        assert_eq!(
            bad_date.to_string(),
            "The retain until date must be provided in ISO 8601 format"
        );
        let past = code(Some("abc"), Some("2025-12-25T12:00:00Z"), None);
        assert_eq!(
            past.to_string(),
            "The retain until date must be in the future!"
        );
        assert_eq!(
            past.argument(),
            Some((
                "x-amz-object-lock-retain-until-date",
                Some("2025-12-25T12:00:00Z")
            ))
        );
        let hold = code(None, None, Some("wrong"));
        assert_eq!(
            hold.to_string(),
            "Legal Hold must be either of 'ON' or 'OFF'"
        );
        for error in [only_mode, bad_mode, bad_date, past, hold] {
            assert_eq!(error.code(), ("InvalidArgument", 400));
        }
        let fine = requested(Some("COMPLIANCE"), Some(future), Some("ON"), NOW).unwrap();
        assert_eq!(fine.retention.unwrap().mode, Mode::Compliance);
        assert_eq!(fine.legal_hold, Some(true));
        assert!(fine.any());
        assert!(!requested(None, None, None, NOW).unwrap().any());
    }

    /// A default retention period's bounds: S3 answered 0 and -1 "must be a positive integer
    /// value" and 999999999 "too large"; days and years together is malformed (18 §5).
    #[test]
    fn default_periods_are_bounded() {
        assert_eq!(period(Some(1), None), Ok(Some(Period::Days(1))));
        assert_eq!(period(None, Some(100)), Ok(Some(Period::Years(100))));
        assert_eq!(period(Some(1), Some(1)), Err(PeriodFault::Both));
        for days in [0, -1] {
            assert_eq!(
                period(Some(days), None),
                Err(PeriodFault::Error(LockError::PeriodNotPositive("Days")))
            );
        }
        assert_eq!(
            period(Some(999_999_999), None),
            Err(PeriodFault::Error(LockError::PeriodTooLarge("Days")))
        );
        assert_eq!(
            period(None, Some(101)),
            Err(PeriodFault::Error(LockError::PeriodTooLarge("Years")))
        );
        assert_eq!(
            LockError::PeriodNotPositive("Days").code(),
            ("InvalidArgument", 400)
        );
    }

    /// "adding the specified duration to the object version's creation timestamp" (18 §2.4).
    #[test]
    fn defaults_count_from_creation() {
        let config = Configuration {
            default: Some((Mode::Governance, Period::Days(1))),
        };
        assert_eq!(
            config.default_for(NOW),
            Some(Retention {
                mode: Mode::Governance,
                until: NOW + DAY
            })
        );
        let years = Configuration {
            default: Some((Mode::Compliance, Period::Years(2))),
        };
        assert_eq!(years.default_for(0).unwrap().until, 730 * DAY);
        assert_eq!(Configuration::default().default_for(NOW), None);
        let written = Retention {
            mode: Mode::Governance,
            until: instant("2030-01-01T00:00:00Z").unwrap(),
        };
        assert_eq!(
            written.until_text().as_deref(),
            Some("2030-01-01T00:00:00.000Z")
        );
    }

    /// The integrity rule and the signature rule for writes with lock parameters, and the
    /// bypass header's refusal on a bucket without Object Lock (18 §5).
    #[test]
    fn writes_with_locks_carry_what_s3_requires() {
        assert_eq!(integrity(false, false, false, false, false), Ok(()));
        assert_eq!(integrity(true, false, true, false, true), Ok(()));
        assert_eq!(integrity(true, false, false, true, true), Ok(()));
        assert_eq!(
            integrity(true, false, false, false, true),
            Err(LockError::Integrity)
        );
        assert_eq!(
            integrity(true, true, false, false, true),
            Err(LockError::PartIntegrity)
        );
        assert_eq!(
            integrity(true, false, true, false, false),
            Err(LockError::Anonymous)
        );
        assert_eq!(LockError::Integrity.code(), ("InvalidRequest", 400));
        assert_eq!(bypass(None, false), Ok(false));
        assert_eq!(bypass(Some("true"), true), Ok(true));
        assert_eq!(bypass(Some("True"), true), Ok(true));
        assert_eq!(bypass(Some("false"), true), Ok(false));
        for value in ["true", "false"] {
            let refused = bypass(Some(value), false).unwrap_err();
            assert_eq!(
                refused.to_string(),
                "x-amz-bypass-governance-retention is only applicable to Object Lock enabled buckets."
            );
            assert_eq!(refused.code(), ("InvalidArgument", 400));
        }
        let past = RetentionRequest::Set(Retention {
            mode: Mode::Governance,
            until: NOW - 1,
        });
        assert_eq!(
            ahead(past, "2019-12-31T16:00:00Z", NOW)
                .unwrap_err()
                .argument(),
            Some(("RetainUntilDate", Some("2019-12-31T16:00:00Z")))
        );
        assert_eq!(ahead(RetentionRequest::Remove, "", NOW), Ok(()));
    }
}
