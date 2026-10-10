//! Bucket lifecycle configuration (docs/research/13 §6.9): the rules
//! PutBucketLifecycleConfiguration sets, checked as S3 checks them, and when each rule's action
//! falls due for an object version or a multipart upload.
//!
//! The actions mantle takes are S3's expirations: of current versions, of noncurrent versions,
//! of a delete marker left with no versions beneath it, and of incomplete multipart uploads.
//! mantle stores every object in one storage class, so a rule that transitions objects between
//! classes is refused as not implemented, once the rest of the configuration is found valid.

use std::borrow::Cow;
use std::collections::BTreeSet;

use crate::sigv4::uri_encode;
use crate::tagging::{Tag, Tagged};
use crate::time::http_date;

/// "An Amazon S3 Lifecycle configuration can have up to 1,000 rules. This limit is not
/// adjustable" (13 §6.9).
pub const MAX_RULES: usize = 1000;

/// "ID length is limited to 255 characters" (13 §6.9), counted in UTF-16 code units as S3
/// counts a tag's characters (13 §6.7).
pub const MAX_ID: usize = 255;

/// The most tags one filter names: an object holds at most 10 (13 §6.7), so a filter that
/// requires more matches no object.
pub const MAX_FILTER_TAGS: usize = Tagged::Object.limit();

/// The largest object size a filter names, 1000 × 2^40 bytes: S3 answered a size outside it
/// "'ObjectSizeLessThan' should be between 1 and 1099511627776000." (13 §6.9).
pub const MAX_FILTER_SIZE: u64 = 1_099_511_627_776_000;

/// The classes a transition may name: `TransitionStorageClass` (13 §6.9).
pub const CLASSES: [&str; 6] = [
    "GLACIER",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "DEEP_ARCHIVE",
    "GLACIER_IR",
];

/// The classes S3 transitions an object to only once it is 30 days old (13 §6.9).
const INFREQUENT_ACCESS: [&str; 2] = ["STANDARD_IA", "ONEZONE_IA"];

/// Milliseconds in a day, the unit of every rule's age.
const DAY: i64 = 86_400_000;

/// One of a bucket's lifecycle rules, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    /// The ID given, or one made for a rule given none.
    pub id: String,
    pub scope: Scope,
    /// `Status` `Enabled`. A disabled rule takes no action.
    pub enabled: bool,
    pub expiration: Option<Expiration>,
    pub noncurrent: Option<Noncurrent>,
    /// `AbortIncompleteMultipartUpload`'s `DaysAfterInitiation`, at least 1.
    pub abort: Option<u32>,
}

/// The objects a rule applies to, in the form the rule gave it:
/// GetBucketLifecycleConfiguration gives a rule back as it was written (13 §6.9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// The rule's own `Prefix`, the form of the deprecated PutBucketLifecycle and of what S3
    /// calls a configuration before "Lifecycle V2".
    Prefix(String),
    Filter(Filter),
}

/// A `Filter`: empty, which applies to every object, or exactly one predicate (13 §6.9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    All,
    Prefix(String),
    Tag(Tag),
    /// `ObjectSizeGreaterThan`, in bytes: sizes above it.
    Larger(u64),
    /// `ObjectSizeLessThan`, in bytes: sizes below it.
    Smaller(u64),
    And(And),
}

/// `And`: two or more predicates, every one of which an object must meet (13 §6.9).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct And {
    pub prefix: Option<String>,
    /// Each with its own key, in the order given.
    pub tags: Vec<Tag>,
    pub larger: Option<u64>,
    pub smaller: Option<u64>,
}

/// A rule's `Expiration`: one of its three forms (13 §6.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiration {
    /// `Days`, at least 1: a current version expires this many days after it was created.
    Days(u32),
    /// `Date`, a midnight UTC, as days from 1970-01-01: current versions expire from then on.
    Date(i64),
    /// `ExpiredObjectDeleteMarker`: `true` removes a delete marker that no noncurrent version
    /// is left beneath; `false` takes no action.
    Marker(bool),
}

/// `NoncurrentVersionExpiration` (13 §6.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Noncurrent {
    /// `NoncurrentDays`, at least 1: days from when a version became noncurrent.
    pub days: u32,
    /// `NewerNoncurrentVersions`, at least 1: the noncurrent versions kept whatever their age.
    pub newer: Option<u32>,
}

/// A rule as a document gives it: read against S3's schema, not yet checked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Given {
    pub id: Option<String>,
    /// The rule's own `Prefix`.
    pub prefix: Option<String>,
    pub filter: Option<Filter>,
    pub enabled: bool,
    pub expiration: Option<GivenExpiration>,
    pub transitions: Vec<GivenTransition>,
    pub noncurrent_transitions: Vec<GivenTransition>,
    pub noncurrent: Option<GivenNoncurrent>,
    /// `AbortIncompleteMultipartUpload`, and its `DaysAfterInitiation` if given.
    pub abort: Option<Option<i32>>,
}

/// An `Expiration` as given: each of its elements, if present.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GivenExpiration {
    /// Unix seconds and nanoseconds.
    pub date: Option<(i64, u32)>,
    pub days: Option<i32>,
    pub marker: Option<bool>,
}

/// A `Transition` or a `NoncurrentVersionTransition` as given. A noncurrent transition has
/// no `Date`, and its `NoncurrentDays` is `days`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GivenTransition {
    pub date: Option<(i64, u32)>,
    pub days: Option<i32>,
    pub newer: Option<i32>,
    pub class: Option<String>,
}

/// A `NoncurrentVersionExpiration` as given.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GivenNoncurrent {
    pub days: Option<i32>,
    pub newer: Option<i32>,
}

/// A configuration's faults. Where S3 was recorded answering one, the message is S3's
/// (13 §6.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleError {
    #[error("At least one lifecycle rule must be specified.")]
    NoRules,
    #[error("The number of lifecycle rules must not exceed the allowed limit of 1000 rules.")]
    TooManyRules,
    #[error("ID length should not exceed allowed limit of 255")]
    IdTooLong,
    #[error("Rule ID must be unique. Found same ID for more than one rule")]
    DuplicateId,
    #[error("a rule names its objects by neither a Filter nor a Prefix")]
    NoScope,
    #[error("Filter element can only be used in Lifecycle V2.")]
    FilterInV1,
    #[error(
        "Base level prefix cannot be used in Lifecycle V2, prefixes are only supported in the Filter."
    )]
    PrefixInV2,
    #[error("NewerNoncurrentVersions element can only be used in Lifecycle V2.")]
    NewerInV1,
    #[error("Found overlapping prefixes for same action type '{0}'")]
    Overlapping(&'static str),
    #[error("At least one action needs to be specified in a rule")]
    NoAction,
    #[error("a prefix is longer than the longest key, 1,024 bytes")]
    PrefixTooLong,
    #[error("a filter names more tags than an object holds")]
    TooManyTags,
    #[error("Duplicate Tag Keys are not allowed.")]
    DuplicateTagKey,
    #[error("a tag in a filter has an empty key")]
    EmptyTagKey,
    #[error("'ObjectSizeGreaterThan' should be between 0 and 1099511627776000.")]
    LargerRange,
    #[error("'ObjectSizeLessThan' should be between 1 and 1099511627776000.")]
    SmallerRange,
    #[error("'ObjectSizeLessThan' has to be a value greater than 'ObjectSizeGreaterThan'.")]
    SizeOrder,
    #[error(
        "an Expiration names none, or more than one, of Date, Days and ExpiredObjectDeleteMarker"
    )]
    ExpirationForm,
    #[error("'Days' for Expiration action must be a positive integer")]
    ExpirationDays,
    #[error("'Date' must be at midnight GMT")]
    Midnight,
    #[error("a NoncurrentVersionExpiration without NoncurrentDays")]
    NoncurrentForm,
    #[error("'NoncurrentDays' for NoncurrentVersionExpiration action must be a positive integer")]
    NoncurrentDays,
    #[error("'NewerNoncurrentVersions' must be a positive integer")]
    NewerVersions,
    #[error("an AbortIncompleteMultipartUpload without DaysAfterInitiation")]
    AbortForm,
    #[error(
        "'DaysAfterInitiation' for AbortIncompleteMultipartUpload action must be a positive integer"
    )]
    AbortDays,
    #[error("ExpiredObjectDeleteMarker cannot be specified with Tags.")]
    MarkerWithTags,
    #[error("ExpiredObjectDeleteMarker cannot be specified with Object Size.")]
    MarkerWithSize,
    #[error("AbortIncompleteMultipartUpload cannot be specified with Tags.")]
    AbortWithTags,
    #[error("AbortIncompleteMultipartUpload cannot be specified with Object Size.")]
    AbortWithSize,
    #[error("a transition names neither a Date nor Days, or both, or no class S3 defines")]
    TransitionForm,
    #[error("'Days' in a transition must not be negative")]
    TransitionDays,
    #[error(
        "'Days' in Transition action must be greater than or equal to 30 for storageClass '{0}'"
    )]
    InfrequentAccessDays(&'static str),
    #[error("'StorageClass' must be different for 'Transition' actions in same 'Rule'")]
    DuplicateClass,
    #[error(
        "Found mixed 'Date' and 'Days' based Expiration and Transition actions in lifecycle rule"
    )]
    MixedDates,
    #[error(
        "transitions between storage classes, which mantle, holding one class, does not implement"
    )]
    Transition,
    #[error("Invalid TransitionDefaultMinimumObjectSize found")]
    MinimumSize,
}

impl LifecycleError {
    /// The S3 error code and status. Most are S3's recorded answers, one each is its error
    /// table's and s3-tests', and the rest follow the recorded answer to the nearest fault
    /// (13 §6.9).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::NoRules
            | Self::TooManyRules
            | Self::FilterInV1
            | Self::PrefixInV2
            | Self::NewerInV1
            | Self::Overlapping(_)
            | Self::NoAction
            | Self::DuplicateTagKey
            | Self::LargerRange
            | Self::SmallerRange
            | Self::SizeOrder
            | Self::MarkerWithTags
            | Self::MarkerWithSize
            | Self::AbortWithTags
            | Self::AbortWithSize
            | Self::DuplicateClass
            | Self::MixedDates
            | Self::MinimumSize => ("InvalidRequest", 400),
            Self::IdTooLong
            | Self::DuplicateId
            | Self::PrefixTooLong
            | Self::TooManyTags
            | Self::ExpirationDays
            | Self::Midnight
            | Self::NoncurrentDays
            | Self::NewerVersions
            | Self::AbortDays
            | Self::TransitionDays
            | Self::InfrequentAccessDays(_) => ("InvalidArgument", 400),
            Self::NoScope
            | Self::EmptyTagKey
            | Self::ExpirationForm
            | Self::NoncurrentForm
            | Self::AbortForm
            | Self::TransitionForm => ("MalformedXML", 400),
            Self::Transition => ("NotImplemented", 501),
        }
    }
}

/// `x-amz-transition-default-minimum-object-size`: which objects too small to transition are
/// kept from it by default (13 §6.9). mantle transitions nothing, and keeps the setting so a
/// configuration reads back as it was set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MinimumSize {
    /// `all_storage_classes_128K`: no object below 128 KB transitions. S3 answers it for a
    /// configuration set without the header.
    #[default]
    AllClasses,
    /// `varies_by_storage_class`.
    ByClass,
}

impl MinimumSize {
    /// The setting a PUT's header names; absent is the default.
    pub fn from_header(value: Option<&str>) -> Result<Self, LifecycleError> {
        match value {
            None | Some("all_storage_classes_128K") => Ok(Self::AllClasses),
            Some("varies_by_storage_class") => Ok(Self::ByClass),
            Some(_) => Err(LifecycleError::MinimumSize),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::AllClasses => "all_storage_classes_128K",
            Self::ByClass => "varies_by_storage_class",
        }
    }
}

/// Rules as a document gives them, checked, each with an ID. The first fault found is the
/// answer: the rules' number and IDs, then each rule in order, then the prefixes of a
/// configuration before Lifecycle V2. Transitions are refused last, so a configuration S3
/// would refuse is answered as S3 answers it.
///
/// A configuration is in Lifecycle V2 if its first rule has a `Filter`. S3 then refuses a rule
/// with its own `Prefix`, and otherwise a rule with a `Filter` or `NewerNoncurrentVersions`,
/// naming the version in its answer (13 §6.9).
pub fn check(given: Vec<Given>) -> Result<Vec<Rule>, LifecycleError> {
    if given.is_empty() {
        return Err(LifecycleError::NoRules);
    }
    if given.len() > MAX_RULES {
        return Err(LifecycleError::TooManyRules);
    }
    let mut ids = BTreeSet::new();
    for rule in &given {
        if let Some(id) = rule.id.as_deref().filter(|id| !id.is_empty()) {
            if id.encode_utf16().count() > MAX_ID {
                return Err(LifecycleError::IdTooLong);
            }
            if !ids.insert(id) {
                return Err(LifecycleError::DuplicateId);
            }
        }
    }
    let v2 = given.first().is_some_and(|rule| rule.filter.is_some());
    let mut rules = Vec::new();
    for rule in &given {
        rules.push(checked(rule, v2)?);
    }
    if !v2 {
        overlapping(&given)?;
    }
    if given
        .iter()
        .any(|rule| !rule.transitions.is_empty() || !rule.noncurrent_transitions.is_empty())
    {
        return Err(LifecycleError::Transition);
    }
    name(&mut rules, &given, &ids);
    Ok(rules)
}

/// One rule checked, without its ID.
fn checked(rule: &Given, v2: bool) -> Result<Rule, LifecycleError> {
    let scope = match (&rule.prefix, &rule.filter) {
        (None, None) => return Err(LifecycleError::NoScope),
        (Some(_), _) if v2 => return Err(LifecycleError::PrefixInV2),
        (_, Some(_)) if !v2 => return Err(LifecycleError::FilterInV1),
        (Some(prefix), _) => Scope::Prefix(prefix.clone()),
        (None, Some(filter)) => Scope::Filter(filter.clone()),
    };
    scope.check()?;
    if rule.expiration.is_none()
        && rule.transitions.is_empty()
        && rule.noncurrent_transitions.is_empty()
        && rule.noncurrent.is_none()
        && rule.abort.is_none()
    {
        return Err(LifecycleError::NoAction);
    }
    let expiration = rule.expiration.map(expiration).transpose()?;
    let noncurrent = rule
        .noncurrent
        .map(|given| noncurrent(given, v2))
        .transpose()?;
    let abort = match rule.abort {
        None => None,
        Some(None) => return Err(LifecycleError::AbortForm),
        Some(Some(days)) => Some(positive(days).ok_or(LifecycleError::AbortDays)?),
    };
    let (tags, sizes) = (scope.uses_tags(), scope.uses_sizes());
    let marker = matches!(expiration, Some(Expiration::Marker(_)));
    if marker && tags {
        return Err(LifecycleError::MarkerWithTags);
    }
    if marker && sizes {
        return Err(LifecycleError::MarkerWithSize);
    }
    if abort.is_some() && tags {
        return Err(LifecycleError::AbortWithTags);
    }
    if abort.is_some() && sizes {
        return Err(LifecycleError::AbortWithSize);
    }
    transitions(rule, expiration, v2)?;
    Ok(Rule {
        id: String::new(),
        scope,
        enabled: rule.enabled,
        expiration,
        noncurrent,
        abort,
    })
}

fn expiration(given: GivenExpiration) -> Result<Expiration, LifecycleError> {
    match (given.date, given.days, given.marker) {
        (Some(date), None, None) => Ok(Expiration::Date(midnight(date)?)),
        (None, Some(days), None) => positive(days)
            .map(Expiration::Days)
            .ok_or(LifecycleError::ExpirationDays),
        (None, None, Some(marker)) => Ok(Expiration::Marker(marker)),
        _ => Err(LifecycleError::ExpirationForm),
    }
}

/// A `NoncurrentVersionExpiration`: S3 found one without `NoncurrentDays` malformed, even
/// with `NewerNoncurrentVersions` (13 §6.9).
fn noncurrent(given: GivenNoncurrent, v2: bool) -> Result<Noncurrent, LifecycleError> {
    let days = given.days.ok_or(LifecycleError::NoncurrentForm)?;
    Ok(Noncurrent {
        days: positive(days).ok_or(LifecycleError::NoncurrentDays)?,
        newer: newer(given.newer, v2)?,
    })
}

/// `NewerNoncurrentVersions`: at least 1, and only in Lifecycle V2. The documented maximum of
/// 100 is not enforced: S3 accepted 500 (13 §6.9).
fn newer(given: Option<i32>, v2: bool) -> Result<Option<u32>, LifecycleError> {
    let Some(count) = given else {
        return Ok(None);
    };
    if !v2 {
        return Err(LifecycleError::NewerInV1);
    }
    positive(count)
        .map(Some)
        .ok_or(LifecycleError::NewerVersions)
}

/// A rule's transitions, checked as S3 checks them before mantle refuses them: each names one
/// of a date or a day count and a class S3 defines; `STANDARD_IA` and `ONEZONE_IA` wait at
/// least 30 days; no class is named twice; and a rule does not mix dates and day counts across
/// its expiration and transitions (13 §6.9). The order S3 requires between classes is not
/// checked, since no transition is taken.
fn transitions(
    rule: &Given,
    expiration: Option<Expiration>,
    v2: bool,
) -> Result<(), LifecycleError> {
    let mut dated = matches!(expiration, Some(Expiration::Date(_)));
    let mut counted = matches!(expiration, Some(Expiration::Days(_)));
    let mut classes = BTreeSet::new();
    for transition in &rule.transitions {
        let class = class(transition)?;
        match (transition.date, transition.days) {
            (Some(date), None) => {
                midnight(date)?;
                dated = true;
            }
            (None, Some(days)) if days < 0 => return Err(LifecycleError::TransitionDays),
            (None, Some(days)) => {
                if days < 30 && INFREQUENT_ACCESS.contains(&class) {
                    return Err(LifecycleError::InfrequentAccessDays(class));
                }
                counted = true;
            }
            _ => return Err(LifecycleError::TransitionForm),
        }
        if !classes.insert(class) {
            return Err(LifecycleError::DuplicateClass);
        }
    }
    if dated && counted {
        return Err(LifecycleError::MixedDates);
    }
    let mut classes = BTreeSet::new();
    for transition in &rule.noncurrent_transitions {
        let class = class(transition)?;
        match transition.days {
            None => return Err(LifecycleError::TransitionForm),
            Some(days) if days < 0 => return Err(LifecycleError::TransitionDays),
            Some(_) => {}
        }
        newer(transition.newer, v2)?;
        if !classes.insert(class) {
            return Err(LifecycleError::DuplicateClass);
        }
    }
    Ok(())
}

/// A transition's class, one S3 defines: an enumeration, so any other value does not validate
/// against the schema.
fn class(transition: &GivenTransition) -> Result<&'static str, LifecycleError> {
    let given = transition
        .class
        .as_deref()
        .ok_or(LifecycleError::TransitionForm)?;
    CLASSES
        .into_iter()
        .find(|class| *class == given)
        .ok_or(LifecycleError::TransitionForm)
}

/// A kind of action as S3 names it, and whether a rule takes it.
type ActionKind = (&'static str, fn(&Given) -> bool);

/// In a configuration before Lifecycle V2, no two rules whose prefixes overlap may take the
/// same kind of action: S3 answered "Found overlapping prefixes '' and 'a' for same action
/// type 'Expiration'" (13 §6.9). Prefixes overlap when one begins the other, and in the sorted
/// prefixes of one kind of action every prefix that begins another begins the one after it,
/// so neighbours are all that need comparing.
fn overlapping(given: &[Given]) -> Result<(), LifecycleError> {
    let kinds: [ActionKind; 5] = [
        ("Expiration", |rule| rule.expiration.is_some()),
        ("Transition", |rule| !rule.transitions.is_empty()),
        ("NoncurrentVersionTransition", |rule| {
            !rule.noncurrent_transitions.is_empty()
        }),
        ("NoncurrentVersionExpiration", |rule| {
            rule.noncurrent.is_some()
        }),
        ("AbortIncompleteMultipartUpload", |rule| {
            rule.abort.is_some()
        }),
    ];
    for (kind, takes) in kinds {
        let mut prefixes: Vec<&str> = given
            .iter()
            .filter(|rule| takes(rule))
            .filter_map(|rule| rule.prefix.as_deref())
            .collect();
        prefixes.sort_unstable();
        if prefixes.array_windows::<2>().any(|[a, b]| b.starts_with(a)) {
            return Err(LifecycleError::Overlapping(kind));
        }
    }
    Ok(())
}

/// A day count that must be "a positive integer" (13 §6.9).
fn positive(days: i32) -> Option<u32> {
    u32::try_from(days).ok().filter(|days| *days > 0)
}

/// A `Date`, which is "always midnight UTC" (13 §6.9), as days from 1970-01-01.
fn midnight((seconds, nanos): (i64, u32)) -> Result<i64, LifecycleError> {
    const SECONDS_PER_DAY: i64 = 86_400;
    if nanos != 0 || seconds.checked_rem_euclid(SECONDS_PER_DAY) != Some(0) {
        return Err(LifecycleError::Midnight);
    }
    seconds
        .checked_div_euclid(SECONDS_PER_DAY)
        .ok_or(LifecycleError::Midnight)
}

/// Gives each rule the ID it was given, or else the lowest `rule-N`, counting from 1, that no
/// rule was given. Of the first N such names, where N counts the rules, no more are taken than
/// there are rules with IDs, so as many are free as there are rules without: every rule gets
/// one.
fn name(rules: &mut [Rule], given: &[Given], taken: &BTreeSet<&str>) {
    let mut free = (1..=given.len())
        .map(|n| format!("rule-{n}"))
        .filter(|id| !taken.contains(id.as_str()));
    for (rule, given) in rules.iter_mut().zip(given) {
        rule.id = match given.id.as_deref().filter(|id| !id.is_empty()) {
            Some(id) => id.to_owned(),
            None => free.next().unwrap_or_default(),
        };
    }
}

impl Scope {
    fn check(&self) -> Result<(), LifecycleError> {
        match self {
            Self::Prefix(prefix) | Self::Filter(Filter::Prefix(prefix)) => check_prefix(prefix),
            Self::Filter(Filter::Tag(tag)) => check_tags(std::slice::from_ref(tag)),
            Self::Filter(Filter::All | Filter::Larger(_) | Filter::Smaller(_)) => Ok(()),
            Self::Filter(Filter::And(and)) => {
                if let Some(prefix) = &and.prefix {
                    check_prefix(prefix)?;
                }
                check_tags(&and.tags)?;
                match (and.larger, and.smaller) {
                    (Some(larger), Some(smaller)) if larger >= smaller => {
                        Err(LifecycleError::SizeOrder)
                    }
                    _ => Ok(()),
                }
            }
        }
    }

    fn uses_tags(&self) -> bool {
        match self {
            Self::Filter(Filter::Tag(_)) => true,
            Self::Filter(Filter::And(and)) => !and.tags.is_empty(),
            _ => false,
        }
    }

    fn uses_sizes(&self) -> bool {
        match self {
            Self::Filter(Filter::Larger(_) | Filter::Smaller(_)) => true,
            Self::Filter(Filter::And(and)) => and.larger.is_some() || and.smaller.is_some(),
            _ => false,
        }
    }

    /// Whether `object` is one this rule applies to.
    fn matches(&self, object: &Object<'_>) -> bool {
        let has = |tag: &Tag| object.tags.contains(tag);
        match self {
            Self::Prefix(prefix) | Self::Filter(Filter::Prefix(prefix)) => {
                object.key.starts_with(prefix.as_str())
            }
            Self::Filter(Filter::All) => true,
            Self::Filter(Filter::Tag(tag)) => has(tag),
            Self::Filter(Filter::Larger(size)) => object.size > *size,
            Self::Filter(Filter::Smaller(size)) => object.size < *size,
            Self::Filter(Filter::And(and)) => {
                and.prefix
                    .as_deref()
                    .is_none_or(|prefix| object.key.starts_with(prefix))
                    && and.tags.iter().all(has)
                    && and.larger.is_none_or(|size| object.size > size)
                    && and.smaller.is_none_or(|size| object.size < size)
            }
        }
    }
}

/// A prefix longer than the longest key matches none (05 §10.1).
fn check_prefix(prefix: &str) -> Result<(), LifecycleError> {
    if prefix.len() > crate::route::MAX_KEY {
        return Err(LifecycleError::PrefixTooLong);
    }
    Ok(())
}

/// "When you specify multiple tags in a filter, each tag key must be unique" (13 §6.9).
fn check_tags(tags: &[Tag]) -> Result<(), LifecycleError> {
    if tags.len() > MAX_FILTER_TAGS {
        return Err(LifecycleError::TooManyTags);
    }
    if tags.iter().any(|tag| tag.key.is_empty()) {
        return Err(LifecycleError::EmptyTagKey);
    }
    let mut keys: Vec<&str> = tags.iter().map(|tag| tag.key.as_str()).collect();
    keys.sort_unstable();
    if keys.array_windows::<2>().any(|[a, b]| a == b) {
        return Err(LifecycleError::DuplicateTagKey);
    }
    Ok(())
}

/// What a rule's filter reads of an object version. A delete marker has no tags, and its size
/// is 0, as the user guide's rule for noncurrent delete markers, `ObjectSizeLessThan` 1,
/// treats it (13 §6.9).
#[derive(Debug, Clone, Copy)]
pub struct Object<'a> {
    pub key: &'a str,
    pub tags: &'a [Tag],
    pub size: u64,
}

/// When an action falls due, and the rule that sets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Due<'a> {
    /// Unix milliseconds, a midnight UTC. `i64::MAX` stands for a time past what an `i64`
    /// counts, which no clock reaches.
    pub at: i64,
    pub rule: &'a str,
}

/// `days` after `from`, rounded up to midnight UTC: "Amazon S3 calculates the time by adding
/// the number of days specified in the rule to the object creation time and rounding up the
/// resulting time to the next day at midnight UTC" (13 §6.9). A time already at midnight is
/// its own ceiling. Saturates at `i64::MAX`, the time no clock reaches.
fn after(from: i64, days: u32) -> i64 {
    i64::from(days)
        .checked_mul(DAY)
        .and_then(|span| from.checked_add(span))
        .and_then(|time| {
            let into = time.checked_rem_euclid(DAY)?;
            if into == 0 {
                Some(time)
            } else {
                time.checked_sub(into)?.checked_add(DAY)
            }
        })
        .unwrap_or(i64::MAX)
}

/// The earliest of `candidates`, the first given on a tie: "if two expiration policies
/// overlap, the shorter expiration policy is honored" (13 §6.9).
fn earliest<'a>(candidates: impl Iterator<Item = Due<'a>>) -> Option<Due<'a>> {
    candidates.fold(None, |best, due| match best {
        Some(best) if best.at <= due.at => Some(best),
        _ => Some(due),
    })
}

/// The enabled rules that apply to `object`.
fn applying<'a>(rules: &'a [Rule], object: &Object<'_>) -> impl Iterator<Item = &'a Rule> {
    let object = *object;
    rules
        .iter()
        .filter(move |rule| rule.enabled && rule.scope.matches(&object))
}

/// When the current version `object`, created at `created` (Unix milliseconds), expires. An
/// expiration applies only to a current version that is an object, not a delete marker. Once
/// it is due, the version is deleted as a DeleteObject without a version ID deletes it: gone
/// in a bucket never versioned, beneath a new delete marker in a versioned one (13 §6.9).
pub fn expiry<'a>(rules: &'a [Rule], object: &Object<'_>, created: i64) -> Option<Due<'a>> {
    earliest(applying(rules, object).filter_map(|rule| {
        let at = match rule.expiration? {
            Expiration::Days(days) => after(created, days),
            Expiration::Date(day) => day.checked_mul(DAY).unwrap_or(i64::MAX),
            Expiration::Marker(_) => return None,
        };
        Some(Due { at, rule: &rule.id })
    }))
}

/// When a noncurrent version `object` is deleted. `since` is when its successor was created,
/// the time it became noncurrent. `newer` counts its key's noncurrent versions newer than it,
/// delete markers among them. A rule with `NewerNoncurrentVersions` N keeps the N newest
/// whatever their age: "This value specifies how many newer noncurrent versions must exist
/// before Amazon S3 can expire a given version" (13 §6.9).
pub fn noncurrent_expiry<'a>(
    rules: &'a [Rule],
    object: &Object<'_>,
    since: i64,
    newer: usize,
) -> Option<Due<'a>> {
    earliest(applying(rules, object).filter_map(|rule| {
        let noncurrent = rule.noncurrent?;
        let kept = noncurrent
            .newer
            .is_some_and(|keep| usize::try_from(keep).is_ok_and(|keep| newer < keep));
        (!kept).then(|| Due {
            at: after(since, noncurrent.days),
            rule: &rule.id,
        })
    }))
}

/// When a delete marker with no noncurrent version beneath it, created at `created`, is
/// removed: at once under `ExpiredObjectDeleteMarker`, and under an `Expiration` of `Days`
/// once it is that old: "When you specify the Days tag, Amazon S3 automatically performs
/// ExpiredObjectDeleteMarker cleanup when the delete markers are old enough to satisfy the age
/// criteria" (13 §6.9). "At once" is the first midnight after the marker was created, which
/// has passed by the time the marker is found alone if it was not alone from the start.
pub fn marker_expiry<'a>(rules: &'a [Rule], key: &str, created: i64) -> Option<Due<'a>> {
    let marker = Object {
        key,
        tags: &[],
        size: 0,
    };
    earliest(applying(rules, &marker).filter_map(|rule| {
        let at = match rule.expiration? {
            Expiration::Marker(true) => after(created, 0),
            Expiration::Days(days) => after(created, days),
            Expiration::Marker(false) | Expiration::Date(_) => return None,
        };
        Some(Due { at, rule: &rule.id })
    }))
}

/// When an incomplete multipart upload of `key`, initiated at `initiated`, is aborted, under
/// the first-due enabled rule whose prefix the key has. [`check`] admits no tag or size
/// predicate beside an abort, so a prefix is all such a rule filters by (13 §6.9).
pub fn abort<'a>(rules: &'a [Rule], key: &str, initiated: i64) -> Option<Due<'a>> {
    let upload = Object {
        key,
        tags: &[],
        size: 0,
    };
    earliest(applying(rules, &upload).filter_map(|rule| {
        Some(Due {
            at: after(initiated, rule.abort?),
            rule: &rule.id,
        })
    }))
}

/// `x-amz-expiration` for a version that expires as `due` says: `expiry-date="Fri, 21 Dec 2012
/// 00:00:00 GMT", rule-id="Rule for testfile.txt"`, the form of AWS's sample and of every
/// recorded answer (13 §6.9). `None` past 9999-12-31, the last day an HTTP-date holds.
pub fn expiration_header(due: &Due<'_>) -> Option<String> {
    let date = http_date(due.at.checked_div_euclid(1000)?)?;
    Some(format!(
        "expiry-date=\"{date}\", rule-id=\"{}\"",
        header_id(due.rule)
    ))
}

/// `x-amz-abort-date` and `x-amz-abort-rule-id` for an upload aborted as `due` says: the date
/// an HTTP-date, as S3 was recorded sending it, and the rule's ID as `x-amz-expiration` carries
/// it (13 §6.9). `None` past 9999-12-31.
pub fn abort_headers<'a>(due: &Due<'a>) -> Option<(String, Cow<'a, str>)> {
    let date = http_date(due.at.checked_div_euclid(1000)?)?;
    Some((date, header_id(due.rule)))
}

/// A rule's ID as a header carries it. S3 was recorded sending IDs as they are, spaces and
/// all, though it documents the ID as URL-encoded (13 §6.9). An ID a header cannot carry as it
/// is, one holding a line break, a control or non-ASCII character, `"` or `\`, or a space at
/// either end, is percent-encoded whole, as documented (RFC 9110 §5.5, §5.6.4).
fn header_id(id: &str) -> Cow<'_, str> {
    let plain = !id.starts_with(' ')
        && !id.ends_with(' ')
        && id
            .bytes()
            .all(|b| matches!(b, b' '..=b'~') && b != b'"' && b != b'\\');
    if plain {
        Cow::Borrowed(id)
    } else {
        Cow::Owned(uri_encode(id.as_bytes(), true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::parse_amz_date;

    /// Unix milliseconds of a `YYYYMMDDTHHMMSSZ` time.
    fn at(text: &str) -> i64 {
        parse_amz_date(text).unwrap() * 1000
    }

    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: key.into(),
            value: value.into(),
        }
    }

    fn object<'a>(key: &'a str, tags: &'a [Tag], size: u64) -> Object<'a> {
        Object { key, tags, size }
    }

    /// A rule named `id` for objects under `prefix`, given as a `Filter`.
    fn rule(id: &str, prefix: &str) -> Given {
        Given {
            id: Some(id.into()),
            filter: Some(Filter::Prefix(prefix.into())),
            enabled: true,
            ..Given::default()
        }
    }

    fn days(given: Given, days: i32) -> Given {
        Given {
            expiration: Some(GivenExpiration {
                days: Some(days),
                ..GivenExpiration::default()
            }),
            ..given
        }
    }

    fn checked(given: Vec<Given>) -> Vec<Rule> {
        check(given).unwrap()
    }

    fn refused(given: Vec<Given>) -> &'static str {
        check(given).unwrap_err().code().0
    }

    /// "if an object was created on 1/15/2014 at 10:30 AM UTC and you specify 3 days in a
    /// transition rule, then the transition date of the object would be calculated as
    /// 1/19/2014 00:00 UTC", and the troubleshooting page's object created January 2 at
    /// 00:05 UTC is eligible for a one-day rule at 00:00 UTC on January 4 (13 §6.9).
    #[test]
    fn days_are_counted_from_creation_and_rounded_up_to_midnight() {
        let rules = checked(vec![days(rule("r", ""), 3)]);
        let a = object("a", &[], 1);
        let due = expiry(&rules, &a, at("20140115T103000Z")).unwrap();
        assert_eq!(due.at, at("20140119T000000Z"));
        assert_eq!(due.rule, "r");
        let one = checked(vec![days(rule("r", ""), 1)]);
        assert_eq!(
            expiry(&one, &a, at("20140102T000500Z")).unwrap().at,
            at("20140104T000000Z")
        );
        // A time already at midnight is its own ceiling.
        assert_eq!(
            expiry(&one, &a, at("20140102T000000Z")).unwrap().at,
            at("20140103T000000Z")
        );
        assert_eq!(after(i64::MAX - 1, 1), i64::MAX);
        assert_eq!(after(-1, 0), 0);
    }

    /// The user guide's noncurrent example: a version whose successor was created 1/15/2014
    /// at 10:30 AM UTC, with 3 days, is due 1/19/2014 00:00 UTC; and `photo.gif`, deleted
    /// 1/2/2014 at 11:30 AM UTC, goes on 1/8/2014 at 00:00 UTC, five days after it became
    /// noncurrent (13 §6.9).
    #[test]
    fn noncurrent_days_are_counted_from_the_successor() {
        let rule_for = |days: i32| Given {
            noncurrent: Some(GivenNoncurrent {
                days: Some(days),
                newer: None,
            }),
            ..rule("r", "")
        };
        let three = checked(vec![rule_for(3)]);
        let version = object("photo.gif", &[], 1);
        let due = noncurrent_expiry(&three, &version, at("20140115T103000Z"), 0).unwrap();
        assert_eq!(due.at, at("20140119T000000Z"));
        let five = checked(vec![rule_for(5)]);
        let due = noncurrent_expiry(&five, &version, at("20140102T113000Z"), 0).unwrap();
        assert_eq!(due.at, at("20140108T000000Z"));
        // A current version is not a noncurrent one: no expiration is set for it.
        assert_eq!(expiry(&five, &version, 0), None);
    }

    /// s3-tests' `test_lifecycle_expiration_newer_noncurrent`: of ten versions, with five
    /// newer noncurrent versions kept, six remain: the current version and five noncurrent.
    #[test]
    fn newer_noncurrent_versions_keep_the_newest() {
        let rules = checked(vec![Given {
            noncurrent: Some(GivenNoncurrent {
                days: Some(1),
                newer: Some(5),
            }),
            ..rule("r", "")
        }]);
        let version = object("k", &[], 1);
        let due: Vec<usize> = (0..9)
            .filter(|&newer| noncurrent_expiry(&rules, &version, 0, newer).is_some())
            .collect();
        assert_eq!(due, [5, 6, 7, 8]);
    }

    /// "if two expiration policies overlap, the shorter expiration policy is honored"; a
    /// date-based rule is due on its date for objects created before and after it; a disabled
    /// rule takes no action (13 §6.9).
    #[test]
    fn the_first_due_of_the_rules_that_apply_is_the_answer() {
        let date = Given {
            expiration: Some(GivenExpiration {
                date: Some((parse_amz_date("20150101T000000Z").unwrap(), 0)),
                ..GivenExpiration::default()
            }),
            ..rule("dated", "past/")
        };
        let disabled = Given {
            enabled: false,
            ..days(rule("off", "logs/"), 1)
        };
        let rules = checked(vec![
            days(rule("year", ""), 365),
            days(rule("month", "logs/"), 30),
            date,
            disabled,
        ]);
        let created = at("20260101T120000Z");
        let log = object("logs/a", &[], 1);
        let due = expiry(&rules, &log, created).unwrap();
        assert_eq!((due.at, due.rule), (at("20260201T000000Z"), "month"));
        let other = object("docs/a", &[], 1);
        assert_eq!(expiry(&rules, &other, created).unwrap().rule, "year");
        let past = object("past/a", &[], 1);
        let due = expiry(&rules, &past, created).unwrap();
        assert_eq!((due.at, due.rule), (at("20150101T000000Z"), "dated"));
        let tie = checked(vec![
            days(rule("first", ""), 1),
            days(rule("second", ""), 1),
        ]);
        assert_eq!(expiry(&tie, &other, created).unwrap().rule, "first");
    }

    /// Tags must match key and value exactly; an object with more tags still matches; a key
    /// given alone matches the tag with no value. Sizes exclude their bounds: "objects that
    /// are exactly 1024 KB and 128 KB won't transition" (13 §6.9).
    #[test]
    fn filters_match_as_s3_describes() {
        let filtered = |id: &str, filter: Filter| Given {
            filter: Some(filter),
            ..days(rule(id, ""), 1)
        };
        let rules = checked(vec![
            filtered("tag", Filter::Tag(tag("tag2", "value2"))),
            filtered(
                "and",
                Filter::And(And {
                    prefix: Some("docs/".into()),
                    tags: vec![tag("key1", "tag1"), tag("key5", "tag6")],
                    ..And::default()
                }),
            ),
            filtered(
                "sized",
                Filter::And(And {
                    prefix: Some("sized/".into()),
                    larger: Some(128 * 1024),
                    smaller: Some(1024 * 1024),
                    ..And::default()
                }),
            ),
            filtered("keyed", Filter::Tag(tag("flag", ""))),
        ]);
        let expiring = |key: &str, tags: &[Tag], size: u64| {
            expiry(&rules, &object(key, tags, size), 0).map(|due| due.rule)
        };
        let both = [tag("tag1", "value1"), tag("tag2", "value2")];
        assert_eq!(expiring("a", &both, 1), Some("tag"));
        assert_eq!(expiring("a", &[tag("tag2", "value3")], 1), None);
        assert_eq!(expiring("a", &[tag("TAG2", "value2")], 1), None);
        // s3-tests' test_lifecycle_expiration_header_and_tags_head: key5 is tag5, not tag6.
        let object_tags = [tag("key1", "tag1"), tag("key5", "tag5")];
        assert_eq!(expiring("docs/x", &object_tags, 1), None);
        let object_tags = [tag("key1", "tag1"), tag("key5", "tag6")];
        assert_eq!(expiring("docs/x", &object_tags, 1), Some("and"));
        assert_eq!(expiring("other/x", &object_tags, 1), None);
        assert_eq!(expiring("sized/x", &[], 128 * 1024), None);
        assert_eq!(expiring("sized/x", &[], 128 * 1024 + 1), Some("sized"));
        assert_eq!(expiring("sized/x", &[], 1024 * 1024 - 1), Some("sized"));
        assert_eq!(expiring("sized/x", &[], 1024 * 1024), None);
        assert_eq!(expiring("a", &[tag("flag", "")], 1), Some("keyed"));
        assert_eq!(expiring("a", &[tag("flag", "up")], 1), None);
    }

    /// s3-tests' `test_lifecycle_deletemarker_expiration_with_days_tag`: with NoncurrentDays
    /// 1 and Days 5, the noncurrent version goes after a day and the marker left alone after
    /// five; `ExpiredObjectDeleteMarker` removes a lone marker at once (13 §6.9).
    #[test]
    fn lone_delete_markers_go_under_days_or_at_once() {
        let with_days = Given {
            noncurrent: Some(GivenNoncurrent {
                days: Some(1),
                newer: None,
            }),
            ..days(rule("days", "test1/"), 5)
        };
        let marker = Given {
            expiration: Some(GivenExpiration {
                marker: Some(true),
                ..GivenExpiration::default()
            }),
            ..rule("marker", "test2/")
        };
        let kept = Given {
            expiration: Some(GivenExpiration {
                marker: Some(false),
                ..GivenExpiration::default()
            }),
            ..rule("kept", "test3/")
        };
        let rules = checked(vec![with_days, marker, kept]);
        let deleted = at("20260101T103000Z");
        let due = marker_expiry(&rules, "test1/a", deleted).unwrap();
        assert_eq!((due.at, due.rule), (at("20260107T000000Z"), "days"));
        let due = marker_expiry(&rules, "test2/a", deleted).unwrap();
        assert_eq!((due.at, due.rule), (at("20260102T000000Z"), "marker"));
        assert_eq!(marker_expiry(&rules, "test3/a", deleted), None);
        // Tag and size predicates read a marker as having no tags and no size.
        let tagged = checked(vec![Given {
            filter: Some(Filter::Tag(tag("k", "v"))),
            ..days(rule("tagged", ""), 1)
        }]);
        assert_eq!(marker_expiry(&tagged, "a", deleted), None);
        let small = checked(vec![Given {
            filter: Some(Filter::Smaller(1)),
            ..days(rule("small", ""), 1)
        }]);
        assert!(marker_expiry(&small, "a", deleted).is_some());
    }

    /// s3-tests' `test_lifecycle_multipart_expiration`: uploads under `test1/` are aborted
    /// after DaysAfterInitiation 2; uploads elsewhere are left alone.
    #[test]
    fn uploads_are_aborted_by_prefix() {
        let abort_rule = |id: &str, prefix: &str, days: i32| Given {
            abort: Some(Some(days)),
            ..rule(id, prefix)
        };
        let rules = checked(vec![
            abort_rule("slow", "", 7),
            abort_rule("fast", "test1/", 2),
            Given {
                enabled: false,
                ..abort_rule("off", "", 1)
            },
        ]);
        let started = at("20260929T101500Z");
        let due = abort(&rules, "test1/a", started).unwrap();
        assert_eq!((due.at, due.rule), (at("20261002T000000Z"), "fast"));
        assert_eq!(abort(&rules, "test2/", started).unwrap().rule, "slow");
        // S3 refuses a size filter beside an abort, so no abort rule filters by size.
        let sized = Given {
            abort: Some(Some(1)),
            filter: Some(Filter::Larger(1 << 20)),
            ..rule("sized", "")
        };
        assert_eq!(check(vec![sized]), Err(LifecycleError::AbortWithSize));
    }

    /// AWS's samples: `x-amz-expiration: expiry-date="Fri, 21 Dec 2012 00:00:00 GMT",
    /// rule-id="Rule for testfile.txt"`, the ID as it is, as S3 was recorded sending IDs with
    /// spaces. The other samples' "Fri, 23 Dec 2012" names a Sunday (13 §6.9).
    #[test]
    fn headers_are_written_as_aws_writes_them() {
        let due = Due {
            at: at("20121223T000000Z"),
            rule: "picture-deletion-rule",
        };
        assert_eq!(
            expiration_header(&due).unwrap(),
            "expiry-date=\"Sun, 23 Dec 2012 00:00:00 GMT\", rule-id=\"picture-deletion-rule\""
        );
        let spaced = Due {
            at: at("20121221T000000Z"),
            rule: "Rule for testfile.txt",
        };
        assert_eq!(
            expiration_header(&spaced).unwrap(),
            "expiry-date=\"Fri, 21 Dec 2012 00:00:00 GMT\", rule-id=\"Rule for testfile.txt\""
        );
        // An ID a header cannot carry as it is goes percent-encoded, as documented.
        for (id, sent) in [
            (" padded", "%20padded"),
            ("naïve", "na%C3%AFve"),
            ("back\\slash", "back%5Cslash"),
            ("50%off", "50%off"),
        ] {
            let due = Due { at: 0, rule: id };
            assert_eq!(
                expiration_header(&due).unwrap(),
                format!("expiry-date=\"Thu, 01 Jan 1970 00:00:00 GMT\", rule-id=\"{sent}\"")
            );
        }
        let (date, id) = abort_headers(&Due {
            at: at("20261002T000000Z"),
            rule: "a\"b\r\nc",
        })
        .unwrap();
        assert_eq!(date, "Fri, 02 Oct 2026 00:00:00 GMT");
        assert_eq!(id, "a%22b%0D%0Ac");
        let never = Due {
            at: i64::MAX,
            rule: "r",
        };
        assert_eq!(expiration_header(&never), None);
        assert_eq!(abort_headers(&never), None);
    }

    /// s3-tests' `check_lifecycle_expiration_header`: for Days 1, the header's date is one
    /// whole day, by Python's `timedelta.days`, after the time taken before the PUT.
    #[test]
    fn the_header_date_is_one_whole_day_on_for_days_1() {
        let rules = checked(vec![days(rule("rule1", "days1/"), 1)]);
        let a = object("days1/foo", &[], 3);
        for start in ["20260929T000001Z", "20260929T103000Z", "20260929T235959Z"] {
            let start = at(start);
            let due = expiry(&rules, &a, start + 150).unwrap();
            let whole_days = (due.at - start).div_euclid(DAY);
            assert_eq!(whole_days, 1, "{start}");
        }
    }

    #[test]
    fn rules_are_checked_as_s3_checks_them() {
        assert_eq!(refused(Vec::new()), "InvalidRequest");
        let many: Vec<Given> = (0..=MAX_RULES)
            .map(|i| days(rule(&i.to_string(), ""), 1))
            .collect();
        assert_eq!(refused(many), "InvalidRequest");
        assert_eq!(refused(vec![rule("r", "")]), "InvalidRequest");
        // s3-tests: an ID of 256 characters, and an ID given twice, are InvalidArgument.
        let long = "a".repeat(MAX_ID + 1);
        assert_eq!(refused(vec![days(rule(&long, ""), 1)]), "InvalidArgument");
        let most = "\u{10400}".repeat(MAX_ID / 2);
        assert!(check(vec![days(rule(&most, ""), 1)]).is_ok());
        let twice = vec![days(rule("rule1", "a/"), 1), days(rule("rule1", "b/"), 2)];
        assert_eq!(refused(twice), "InvalidArgument");
        // s3-tests: Days 0 in an expiration is InvalidArgument.
        assert_eq!(refused(vec![days(rule("r", ""), 0)]), "InvalidArgument");
        assert_eq!(refused(vec![days(rule("r", ""), -1)]), "InvalidArgument");
        let not_midnight = Given {
            expiration: Some(GivenExpiration {
                date: Some((20_200_101, 0)),
                ..GivenExpiration::default()
            }),
            ..rule("r", "")
        };
        assert_eq!(refused(vec![not_midnight]), "InvalidArgument");
        let newer_by_prefix = Given {
            id: Some("r".into()),
            prefix: Some(String::new()),
            enabled: true,
            noncurrent: Some(GivenNoncurrent {
                days: Some(1),
                newer: Some(5),
            }),
            ..Given::default()
        };
        assert_eq!(refused(vec![newer_by_prefix]), "InvalidRequest");
    }

    /// A transition is refused as not implemented, but only once the configuration is
    /// otherwise valid: s3-tests' `test_lifecycle_transition_set_invalid_date` expects 400.
    #[test]
    fn transitions_are_refused_after_the_rest_is_checked() {
        let transition = |date: Option<(i64, u32)>, count: Option<i32>, class: &str| Given {
            transitions: vec![GivenTransition {
                date,
                days: count,
                newer: None,
                class: Some(class.into()),
            }],
            ..days(rule("r", ""), 3650)
        };
        let valid = transition(None, Some(30), "GLACIER");
        assert_eq!(check(vec![valid]), Err(LifecycleError::Transition));
        assert_eq!(LifecycleError::Transition.code(), ("NotImplemented", 501));
        let zero = transition(None, Some(0), "GLACIER");
        assert_eq!(check(vec![zero]), Err(LifecycleError::Transition));
        let bad_date = transition(Some((20_220_927, 0)), None, "GLACIER");
        assert_eq!(check(vec![bad_date]), Err(LifecycleError::Midnight));
        let bad_class = transition(None, Some(30), "COLD");
        assert_eq!(check(vec![bad_class]), Err(LifecycleError::TransitionForm));
        assert_eq!(LifecycleError::TransitionForm.code().0, "MalformedXML");
        let later = vec![
            transition(None, Some(30), "GLACIER"),
            days(rule("s", ""), 0),
        ];
        assert_eq!(check(later), Err(LifecycleError::ExpirationDays));
    }

    #[test]
    fn rules_given_no_id_are_named() {
        let unnamed = |prefix: &str| Given {
            id: None,
            ..days(rule("", prefix), 1)
        };
        let rules = checked(vec![
            unnamed("a/"),
            days(rule("rule-2", "b/"), 1),
            unnamed("c/"),
            Given {
                id: Some(String::new()),
                ..unnamed("d/")
            },
        ]);
        let ids: Vec<&str> = rules.iter().map(|rule| rule.id.as_str()).collect();
        assert_eq!(ids, ["rule-1", "rule-2", "rule-3", "rule-4"]);
        // Names a rule was given are skipped, however many there are.
        let rules = checked(vec![
            unnamed("a/"),
            days(rule("rule-1", "b/"), 1),
            unnamed("c/"),
            days(rule("rule-3", "d/"), 1),
        ]);
        let ids: Vec<&str> = rules.iter().map(|rule| rule.id.as_str()).collect();
        assert_eq!(ids, ["rule-2", "rule-1", "rule-4", "rule-3"]);
        let taken: Vec<Given> = (1..=MAX_RULES)
            .map(|n| {
                if n % 2 == 0 {
                    days(rule(&format!("rule-{}", n / 2), ""), 1)
                } else {
                    unnamed("")
                }
            })
            .collect();
        let rules = checked(taken);
        let named: BTreeSet<&str> = rules.iter().map(|rule| rule.id.as_str()).collect();
        assert_eq!(named.len(), MAX_RULES);
        assert!(!named.contains(""));
    }

    /// A configuration is in Lifecycle V2 if its first rule has a `Filter`; S3 refuses the other
    /// form beside it, and in the form before V2 refuses overlapping prefixes for one kind of
    /// action (13 §6.9).
    #[test]
    fn a_configuration_keeps_one_form() {
        let v1 = |id: &str, prefix: &str| Given {
            id: Some(id.into()),
            prefix: Some(prefix.into()),
            enabled: true,
            ..Given::default()
        };
        let code = |given: Vec<Given>| {
            check(given)
                .map(|rules| rules.len())
                .map_err(|e| e.code().0)
        };
        assert_eq!(
            check(vec![days(v1("a", "a/"), 1), days(rule("b", "b/"), 1)]),
            Err(LifecycleError::FilterInV1)
        );
        assert_eq!(
            check(vec![days(rule("a", "a/"), 1), days(v1("b", "b/"), 1)]),
            Err(LifecycleError::PrefixInV2)
        );
        let both = Given {
            prefix: Some("a/".into()),
            ..days(rule("a", "a/"), 1)
        };
        assert_eq!(code(vec![both]), Err("InvalidRequest"));
        let neither = Given {
            filter: None,
            ..days(rule("a", ""), 1)
        };
        assert_eq!(code(vec![neither]), Err("MalformedXML"));
        // "Found overlapping prefixes '' and 'a' for same action type 'Expiration'".
        assert_eq!(
            check(vec![days(v1("all", ""), 1), days(v1("a", "a"), 2)]),
            Err(LifecycleError::Overlapping("Expiration"))
        );
        assert_eq!(
            check(vec![days(v1("x", "logs/"), 1), days(v1("y", "logs/"), 2)]),
            Err(LifecycleError::Overlapping("Expiration"))
        );
        let noncurrent = Given {
            noncurrent: Some(GivenNoncurrent {
                days: Some(1),
                newer: None,
            }),
            ..v1("n", "")
        };
        assert_eq!(code(vec![days(v1("e", ""), 1), noncurrent]), Ok(2));
        assert_eq!(
            code(vec![days(v1("a", "test1/"), 1), days(v1("b", "test2/"), 2)]),
            Ok(2)
        );
        // In V2, overlapping filters are resolved when the actions fall due.
        assert_eq!(
            code(vec![days(rule("all", ""), 365), days(rule("a", "a"), 1)]),
            Ok(2)
        );
    }

    /// What S3 refuses beside `ExpiredObjectDeleteMarker` and `AbortIncompleteMultipartUpload`,
    /// with its recorded messages; the marker is refused beside a size filter even when false
    /// (13 §6.9).
    #[test]
    fn markers_and_aborts_filter_by_prefix_alone() {
        let marker = |value: bool, filter: Filter| Given {
            filter: Some(filter),
            expiration: Some(GivenExpiration {
                marker: Some(value),
                ..GivenExpiration::default()
            }),
            ..rule("r", "")
        };
        let abort_by = |filter: Filter| Given {
            filter: Some(filter),
            abort: Some(Some(7)),
            ..rule("r", "")
        };
        let tag = || Filter::Tag(tag("k", "v"));
        assert_eq!(
            check(vec![marker(true, tag())]),
            Err(LifecycleError::MarkerWithTags)
        );
        assert_eq!(
            check(vec![marker(false, Filter::Smaller(10))]),
            Err(LifecycleError::MarkerWithSize)
        );
        assert_eq!(
            check(vec![abort_by(tag())]),
            Err(LifecycleError::AbortWithTags)
        );
        let sized = Filter::And(And {
            prefix: Some("p3".into()),
            larger: Some(11),
            ..And::default()
        });
        assert_eq!(
            check(vec![abort_by(sized)]),
            Err(LifecycleError::AbortWithSize)
        );
        for error in [
            LifecycleError::MarkerWithTags,
            LifecycleError::MarkerWithSize,
            LifecycleError::AbortWithTags,
            LifecycleError::AbortWithSize,
        ] {
            assert_eq!(error.code(), ("InvalidRequest", 400));
        }
        assert!(check(vec![marker(true, Filter::Prefix("p/".into()))]).is_ok());
        assert!(check(vec![abort_by(Filter::All)]).is_ok());
    }

    /// The recorded answers to noncurrent expirations, aborts and filter tags (13 §6.9).
    #[test]
    fn actions_are_checked_as_s3_answered() {
        let noncurrent = |days: Option<i32>, newer: Option<i32>| Given {
            noncurrent: Some(GivenNoncurrent { days, newer }),
            ..rule("r", "")
        };
        let refused = |given: Given| check(vec![given]).map(|_| ()).unwrap_err();
        assert_eq!(
            refused(noncurrent(None, Some(5))),
            LifecycleError::NoncurrentForm
        );
        assert_eq!(refused(noncurrent(None, None)).code().0, "MalformedXML");
        assert_eq!(
            refused(noncurrent(Some(0), None)).code().0,
            "InvalidArgument"
        );
        assert_eq!(
            refused(noncurrent(Some(1), Some(0))),
            LifecycleError::NewerVersions
        );
        // S3 accepted 500, past the documented 100.
        let kept = check(vec![noncurrent(Some(1), Some(500))]).unwrap();
        assert_eq!(kept[0].noncurrent.unwrap().newer, Some(500));
        let unset = Given {
            abort: Some(None),
            ..rule("r", "")
        };
        assert_eq!(refused(unset).code().0, "MalformedXML");
        let zero = Given {
            abort: Some(Some(0)),
            ..rule("r", "")
        };
        assert_eq!(refused(zero).code().0, "InvalidArgument");
        let twice = Given {
            filter: Some(Filter::And(And {
                tags: vec![tag("k", "1"), tag("k", "2")],
                ..And::default()
            })),
            ..days(rule("r", ""), 1)
        };
        assert_eq!(refused(twice), LifecycleError::DuplicateTagKey);
        assert_eq!(LifecycleError::DuplicateTagKey.code().0, "InvalidRequest");
        let empty_key = Given {
            filter: Some(Filter::Tag(tag("", "v"))),
            ..days(rule("r", ""), 1)
        };
        assert_eq!(refused(empty_key).code().0, "MalformedXML");
        let order = Given {
            filter: Some(Filter::And(And {
                larger: Some(1000),
                smaller: Some(1000),
                ..And::default()
            })),
            ..days(rule("r", ""), 1)
        };
        assert_eq!(refused(order), LifecycleError::SizeOrder);
    }

    /// Transitions are checked as S3 checks them before they are refused (13 §6.9).
    #[test]
    fn transitions_are_checked_before_they_are_refused() {
        let to = |class: &str, days: i32| GivenTransition {
            days: Some(days),
            class: Some(class.into()),
            ..GivenTransition::default()
        };
        let with = |transitions: Vec<GivenTransition>| Given {
            transitions,
            ..rule("r", "")
        };
        let refused = |given: Given| check(vec![given]).unwrap_err();
        assert_eq!(
            refused(with(vec![to("ONEZONE_IA", 0)])),
            LifecycleError::InfrequentAccessDays("ONEZONE_IA")
        );
        assert_eq!(
            LifecycleError::InfrequentAccessDays("STANDARD_IA").to_string(),
            "'Days' in Transition action must be greater than or equal to 30 for storageClass \
             'STANDARD_IA'"
        );
        assert_eq!(
            refused(with(vec![to("GLACIER", 30), to("GLACIER", 60)])),
            LifecycleError::DuplicateClass
        );
        assert_eq!(
            refused(with(vec![to("GLACIER", -1)])),
            LifecycleError::TransitionDays
        );
        let mixed = Given {
            expiration: Some(GivenExpiration {
                date: Some((1_893_456_000, 0)),
                ..GivenExpiration::default()
            }),
            ..with(vec![to("GLACIER", 30)])
        };
        assert_eq!(refused(mixed), LifecycleError::MixedDates);
        // Days 0 is legal in a transition.
        assert_eq!(
            refused(with(vec![to("GLACIER_IR", 0)])),
            LifecycleError::Transition
        );
        assert_eq!(
            refused(with(vec![to("STANDARD_IA", 30), to("GLACIER", 60)])),
            LifecycleError::Transition
        );
    }

    /// `x-amz-transition-default-minimum-object-size`: absent is S3's default, and a value S3
    /// does not define is refused as S3 refused it (13 §6.9).
    #[test]
    fn the_minimum_size_header_reads_s3s_values() {
        assert_eq!(MinimumSize::from_header(None), Ok(MinimumSize::AllClasses));
        assert_eq!(
            MinimumSize::from_header(Some("varies_by_storage_class")),
            Ok(MinimumSize::ByClass)
        );
        let refused = MinimumSize::from_header(Some("value")).unwrap_err();
        assert_eq!(refused.code(), ("InvalidRequest", 400));
        for setting in [MinimumSize::AllClasses, MinimumSize::ByClass] {
            assert_eq!(MinimumSize::from_header(Some(setting.name())), Ok(setting));
        }
    }
}
