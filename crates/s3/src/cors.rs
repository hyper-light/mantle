//! Bucket CORS (docs/research/16): the rules PutBucketCors sets, checked as S3 checks them,
//! and how a request carrying an `Origin` is answered: a preflight `OPTIONS` allowed or
//! refused, and the headers an actual request's response carries when a rule allows it.
//!
//! A rule matches a request whose origin one of its `AllowedOrigin`s matches, whose method is
//! one of its `AllowedMethod`s, and each of whose requested headers one of its `AllowedHeader`s
//! matches. "Amazon S3 ... uses the first `CORSRule` rule that matches" (16 §1.1).

use std::collections::BTreeSet;

/// "You can add up to 100 rules to the configuration" (16 §1.3).
pub const MAX_RULES: usize = 100;

/// A rule's ID "cannot be longer than 255 characters" (16 §1.3), counted in UTF-16 code units
/// as S3 counts a tag's characters (13 §6.7).
pub const MAX_ID: usize = 255;

/// "The document is limited to 64 KB in size" (16 §1.1).
pub const LIMIT: usize = 64 * 1024;

/// What a response that varies with CORS names, as S3 sends it (16 §2, §7).
pub const VARY: &str = "Origin, Access-Control-Request-Headers, Access-Control-Request-Method";

/// A method a rule may allow: "Valid values are `GET`, `PUT`, `HEAD`, `POST`, and `DELETE`"
/// (16 §1.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Put,
    Head,
    Post,
    Delete,
}

impl Method {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "GET" => Some(Self::Get),
            "PUT" => Some(Self::Put),
            "HEAD" => Some(Self::Head),
            "POST" => Some(Self::Post),
            "DELETE" => Some(Self::Delete),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Delete => "DELETE",
        }
    }
}

/// One of a bucket's CORS rules, checked; every list as given, in the order given, which
/// GetBucketCors gives back (16 §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub id: Option<String>,
    /// `AllowedHeader`s: patterns for requested header names, each with at most one `*`.
    pub headers: Vec<String>,
    pub methods: Vec<Method>,
    /// `AllowedOrigin`s: patterns for origins, each with at most one `*`.
    pub origins: Vec<String>,
    /// `ExposeHeader`s, which S3 does not check as header names (16 §7).
    pub expose: Vec<String>,
    /// `MaxAgeSeconds`.
    pub max_age: Option<u32>,
}

/// A rule as a document gives it: read against S3's schema, not yet checked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Given {
    pub id: Option<String>,
    pub headers: Vec<String>,
    pub methods: Vec<String>,
    pub origins: Vec<String>,
    pub expose: Vec<String>,
    pub max_age: Option<i32>,
}

/// A configuration's faults. Where S3 was recorded answering one, the message is S3's
/// (16 §7).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CorsError {
    #[error("a CORS configuration without rules")]
    NoRules,
    #[error("a CORS configuration holds at most 100 rules")]
    TooManyRules,
    #[error("a CORS rule without an AllowedOrigin")]
    NoOrigin,
    #[error("a CORS rule without an AllowedMethod")]
    NoMethod,
    #[error("Found unsupported HTTP method in CORS config. Unsupported method is {0}")]
    Method(String),
    #[error("AllowedOrigin \"{0}\" can not have more than one wildcard.")]
    OriginWildcards(String),
    #[error("AllowedHeader \"{0}\" can not have more than one wildcard.")]
    HeaderWildcards(String),
    #[error("a CORS rule ID is longer than 255 characters")]
    IdTooLong,
    #[error("two CORS rules have one ID")]
    DuplicateId,
    #[error("MaxAgeSeconds must not be negative")]
    MaxAge,
}

impl CorsError {
    /// The S3 error code and status: S3's recorded answer where there is one; otherwise the
    /// one it gives the nearest recorded fault (16 §7).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::NoRules | Self::TooManyRules | Self::NoOrigin | Self::NoMethod => {
                ("MalformedXML", 400)
            }
            Self::Method(_) | Self::OriginWildcards(_) | Self::HeaderWildcards(_) => {
                ("InvalidRequest", 400)
            }
            Self::IdTooLong | Self::DuplicateId | Self::MaxAge => ("InvalidArgument", 400),
        }
    }
}

/// Rules as a document gives them, checked in order: the first fault found is the answer.
///
/// - At least one rule and at most 100; each with an origin and a method, as S3 answered
///   (16 §7).
/// - Methods among the five S3 allows, as it answered `MYMETHOD`, `OPTIONS` and the empty
///   method.
/// - At most one `*` in an origin, as S3 answered `*example*.com`, and in a header, as S3
///   documents it (16 §2).
/// - An ID of at most 255 characters, unique, as the documentation describes it.
/// - A `MaxAgeSeconds` that `Access-Control-Max-Age`, "delta-seconds", can carry: not
///   negative (16 §4).
pub fn check(given: Vec<Given>) -> Result<Vec<Rule>, CorsError> {
    if given.is_empty() {
        return Err(CorsError::NoRules);
    }
    if given.len() > MAX_RULES {
        return Err(CorsError::TooManyRules);
    }
    let mut ids = BTreeSet::new();
    let mut rules = Vec::new();
    for rule in given {
        if let Some(id) = &rule.id {
            if id.encode_utf16().count() > MAX_ID {
                return Err(CorsError::IdTooLong);
            }
            if !ids.insert(id.clone()) {
                return Err(CorsError::DuplicateId);
            }
        }
        if rule.origins.is_empty() {
            return Err(CorsError::NoOrigin);
        }
        if rule.methods.is_empty() {
            return Err(CorsError::NoMethod);
        }
        let methods = rule
            .methods
            .iter()
            .map(|name| Method::from_name(name).ok_or_else(|| CorsError::Method(name.clone())))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(origin) = rule.origins.iter().find(|origin| wildcards(origin) > 1) {
            return Err(CorsError::OriginWildcards(origin.clone()));
        }
        if let Some(header) = rule.headers.iter().find(|header| wildcards(header) > 1) {
            return Err(CorsError::HeaderWildcards(header.clone()));
        }
        let max_age = rule
            .max_age
            .map(|age| u32::try_from(age).map_err(|_| CorsError::MaxAge))
            .transpose()?;
        rules.push(Rule {
            id: rule.id,
            headers: rule.headers,
            methods,
            origins: rule.origins,
            expose: rule.expose,
            max_age,
        });
    }
    Ok(rules)
}

fn wildcards(pattern: &str) -> usize {
    pattern.matches('*').count()
}

/// What a response allows, by the rule that matched a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allowed<'a> {
    pub rule: &'a Rule,
    /// The origin as the request gave it.
    origin: &'a str,
    /// The rule allows the origin only by `*`: the answer is `*`, which a browser shares only
    /// with a request made without credentials (16 §4).
    any: bool,
    /// The headers asked for, lowercase, in the order asked.
    requested: Vec<String>,
}

impl Allowed<'_> {
    /// The response's CORS headers, in the order of S3's recorded answers (16 §2, §7):
    ///
    /// - `Access-Control-Allow-Origin`: the origin, or `*` when the rule allows it only by
    ///   `*`; S3 answered both ways.
    /// - `Access-Control-Allow-Methods`: every method of the rule, as S3 answered rather than
    ///   the one requested, as its OPTIONS page says.
    /// - `Access-Control-Allow-Headers`: the headers requested, lowercase, as S3 answered.
    /// - `Access-Control-Expose-Headers` and `Access-Control-Max-Age`, when the rule has them.
    /// - `Access-Control-Allow-Credentials: true` with a named origin, as S3 answered, and not
    ///   with `*`, which a request with credentials may not be answered (16 §4).
    /// - `Vary`, as S3 sends it.
    pub fn headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![(
            "Access-Control-Allow-Origin",
            if self.any {
                "*".to_owned()
            } else {
                self.origin.to_owned()
            },
        )];
        let methods: Vec<&str> = self.rule.methods.iter().map(|m| m.name()).collect();
        headers.push(("Access-Control-Allow-Methods", methods.join(", ")));
        if !self.requested.is_empty() {
            headers.push(("Access-Control-Allow-Headers", self.requested.join(", ")));
        }
        if !self.rule.expose.is_empty() {
            headers.push(("Access-Control-Expose-Headers", self.rule.expose.join(", ")));
        }
        if let Some(age) = self.rule.max_age {
            headers.push(("Access-Control-Max-Age", age.to_string()));
        }
        if !self.any {
            headers.push(("Access-Control-Allow-Credentials", "true".to_owned()));
        }
        headers.push(("Vary", VARY.to_owned()));
        headers
    }
}

/// The first rule allowing `origin` to use `method` with the headers `requested` names, an
/// `Access-Control-Request-Headers` value (16 §1.1).
///
/// - An origin matches a pattern exactly, byte for byte, as a browser compares the answer
///   with its origin (16 §4), or, where the pattern holds `*`, with any run of characters in
///   its place, the empty run included, as s3-tests expects `start*end` to match `startend`
///   (16 §6).
/// - A method matches exactly: browsers send the five a rule may allow in capitals (16 §4).
/// - A requested header matches a pattern whatever its case, as S3 answered (16 §7).
pub fn allowing<'a>(
    rules: &'a [Rule],
    origin: &'a str,
    method: &str,
    requested: Option<&str>,
) -> Option<Allowed<'a>> {
    let requested = requested.map(header_names).unwrap_or_default();
    let method = Method::from_name(method)?;
    rules.iter().find_map(|rule| {
        let named = rule
            .origins
            .iter()
            .any(|pattern| pattern != "*" && wildcard(pattern, origin, false));
        let any = !named && rule.origins.iter().any(|pattern| pattern == "*");
        let allowed = (named || any)
            && rule.methods.contains(&method)
            && requested.iter().all(|name| {
                rule.headers
                    .iter()
                    .any(|pattern| wildcard(pattern, name, true))
            });
        allowed.then(|| Allowed {
            rule,
            origin,
            any,
            requested: requested.clone(),
        })
    })
}

/// Why a preflight is refused, and what S3 answers (16 §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// No `Origin`.
    NoOrigin,
    /// The bucket has no CORS configuration.
    NotEnabled,
    /// An `Origin` without `Access-Control-Request-Method`, to a bucket with rules.
    NoMethod,
    /// No rule allows it.
    NotAllowed,
}

impl Refusal {
    /// The code, status and message. S3's are recorded for all but `NoMethod`, which s3-tests
    /// expects answered 400 and whose message follows S3's for a missing `Origin`. "evalution"
    /// is S3's spelling.
    pub fn code(self) -> (&'static str, u16, &'static str) {
        match self {
            Self::NoOrigin => (
                "BadRequest",
                400,
                "Insufficient information. Origin request header needed.",
            ),
            Self::NotEnabled => (
                "AccessForbidden",
                403,
                "CORSResponse: CORS is not enabled for this bucket.",
            ),
            Self::NoMethod => (
                "BadRequest",
                400,
                "Insufficient information. Access-Control-Request-Method request header needed.",
            ),
            Self::NotAllowed => (
                "AccessForbidden",
                403,
                "CORSResponse: This CORS request is not allowed. This is usually because the \
                 evalution of Origin, request method / Access-Control-Request-Method or \
                 Access-Control-Request-Headers are not whitelisted by the resource's CORS spec.",
            ),
        }
    }
}

/// A preflight `OPTIONS`, answered from the bucket's rules, if it has any. Its refusal
/// names in its `Method` element the method requested, or `OPTIONS` when none was, as S3's
/// does (16 §7).
pub fn preflight<'a>(
    rules: Option<&'a [Rule]>,
    origin: Option<&'a str>,
    method: Option<&str>,
    requested: Option<&str>,
) -> Result<Allowed<'a>, Refusal> {
    let origin = origin.ok_or(Refusal::NoOrigin)?;
    let rules = rules.ok_or(Refusal::NotEnabled)?;
    let method = method.ok_or(Refusal::NoMethod)?;
    allowing(rules, origin, method, requested).ok_or(Refusal::NotAllowed)
}

/// An actual request that carries `origin`: the rule it matches, if any, whose headers its
/// response carries whatever its outcome. S3 matches such a request by its
/// `Access-Control-Request-Method` and `Access-Control-Request-Headers` when it carries them,
/// as it matches a preflight, and otherwise by its own method; it neither refuses the request
/// nor checks its real headers (16 §7).
pub fn actual<'a>(
    rules: &'a [Rule],
    origin: &'a str,
    method: &str,
    requested_method: Option<&str>,
    requested_headers: Option<&str>,
) -> Option<Allowed<'a>> {
    allowing(
        rules,
        origin,
        requested_method.unwrap_or(method),
        requested_headers,
    )
}

/// The names an `Access-Control-Request-Headers` value lists, lowercase: a comma-separated
/// list with optional white space, whose empty elements are ignored (RFC 9110 §5.6.1).
fn header_names(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|name| name.trim_matches([' ', '\t']))
        .filter(|name| !name.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Whether `text` matches `pattern`, which holds at most one `*`, standing for any run of
/// characters; `fold` compares ASCII letters without regard to case.
fn wildcard(pattern: &str, text: &str, fold: bool) -> bool {
    let same = |a: &str, b: &str| {
        if fold {
            a.eq_ignore_ascii_case(b)
        } else {
            a == b
        }
    };
    let Some((head, tail)) = pattern.split_once('*') else {
        return same(pattern, text);
    };
    let Some(fixed) = head.len().checked_add(tail.len()) else {
        return false;
    };
    let Some(end) = text.len().checked_sub(tail.len()) else {
        return false;
    };
    text.len() >= fixed
        && text
            .get(..head.len())
            .is_some_and(|start| same(start, head))
        && text.get(end..).is_some_and(|finish| same(finish, tail))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(methods: &[&str], origins: &[&str]) -> Given {
        Given {
            methods: methods.iter().map(|m| m.to_string()).collect(),
            origins: origins.iter().map(|o| o.to_string()).collect(),
            ..Given::default()
        }
    }

    fn with_headers(given: Given, headers: &[&str]) -> Given {
        Given {
            headers: headers.iter().map(|h| h.to_string()).collect(),
            ..given
        }
    }

    fn checked(given: Vec<Given>) -> Vec<Rule> {
        check(given).unwrap()
    }

    fn header<'a>(headers: &'a [(&'static str, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.as_str())
    }

    /// S3's recorded answers to bad rules (16 §7).
    #[test]
    fn rules_are_checked_as_s3_answered() {
        let refused = |given: Vec<Given>| check(given).unwrap_err();
        assert_eq!(refused(Vec::new()).code().0, "MalformedXML");
        let mymethod = refused(vec![rule(&["GET", "PUT", "HEAD", "MYMETHOD"], &["*"])]);
        assert_eq!(mymethod.code(), ("InvalidRequest", 400));
        assert_eq!(
            mymethod.to_string(),
            "Found unsupported HTTP method in CORS config. Unsupported method is MYMETHOD"
        );
        for method in ["", "OPTIONS", "get"] {
            assert_eq!(
                refused(vec![rule(&[method], &["*"])]),
                CorsError::Method(method.into())
            );
        }
        let wildcards = refused(vec![rule(&["GET"], &["*example*.com"])]);
        assert_eq!(
            wildcards.to_string(),
            "AllowedOrigin \"*example*.com\" can not have more than one wildcard."
        );
        assert_eq!(wildcards.code().0, "InvalidRequest");
        assert_eq!(
            refused(vec![with_headers(rule(&["GET"], &["*"]), &["x-*-*"])])
                .code()
                .0,
            "InvalidRequest"
        );
        assert_eq!(refused(vec![rule(&["GET"], &[])]).code().0, "MalformedXML");
        assert_eq!(refused(vec![rule(&[], &["*"])]).code().0, "MalformedXML");
        let many: Vec<Given> = (0..=MAX_RULES).map(|_| rule(&["GET"], &["*"])).collect();
        assert_eq!(refused(many).code().0, "MalformedXML");
        let named = |id: &str| Given {
            id: Some(id.into()),
            ..rule(&["GET"], &["*"])
        };
        assert_eq!(
            refused(vec![named(&"i".repeat(256))]).code().0,
            "InvalidArgument"
        );
        assert_eq!(
            refused(vec![named("a"), named("a")]).code().0,
            "InvalidArgument"
        );
        let negative = Given {
            max_age: Some(-1),
            ..rule(&["GET"], &["*"])
        };
        assert_eq!(refused(vec![negative]), CorsError::MaxAge);
        // An empty origin is accepted, as S3 accepted it; so is an origin that is no URL.
        assert!(check(vec![rule(&["GET"], &["", "localhost"])]).is_ok());
    }

    /// s3-tests' `test_cors_origin_response`: `*` stands for any run, the empty one included,
    /// and a pattern is anchored at both ends (16 §6).
    #[test]
    fn origins_match_as_s3_tests_expects() {
        let rules = checked(vec![
            rule(&["GET"], &["*suffix"]),
            rule(&["GET"], &["start*end"]),
            rule(&["GET"], &["prefix*"]),
            rule(&["PUT"], &["*.put"]),
        ]);
        let get = |origin: &'static str| {
            allowing(&rules, origin, "GET", None).map(|allowed| allowed.headers())
        };
        for origin in [
            "foo.suffix",
            "startend",
            "start1end",
            "start12end",
            "prefix",
            "prefix.suffix",
        ] {
            let headers = get(origin).unwrap();
            assert_eq!(
                header(&headers, "Access-Control-Allow-Origin"),
                Some(origin)
            );
            assert_eq!(
                header(&headers, "Access-Control-Allow-Methods"),
                Some("GET")
            );
        }
        for origin in [
            "foo.bar",
            "foo.suffix.get",
            "0start12end",
            "bla.prefix",
            "foo.put",
        ] {
            assert_eq!(get(origin), None, "{origin}");
        }
        let put = allowing(&rules, "foo.put", "PUT", None).unwrap().headers();
        assert_eq!(header(&put, "Access-Control-Allow-Methods"), Some("PUT"));
        assert!(allowing(&rules, "foo.suffix", "PUT", None).is_none());
        assert!(allowing(&rules, "foo.suffix", "DELETE", None).is_none());
    }

    /// LocalStack's recordings of S3 (16 §7): a named origin is echoed with credentials, max
    /// age and `Vary`; a scheme or a trailing slash of difference matches nothing; a rule of
    /// `*` answers `*` without credentials, as s3-tests' `test_cors_origin_wildcard` expects.
    #[test]
    fn origins_are_answered_as_s3_answers_them() {
        let rules = checked(vec![Given {
            max_age: Some(3000),
            ..with_headers(rule(&["GET", "PUT"], &["https://localhost:4200"]), &["*"])
        }]);
        let headers = allowing(&rules, "https://localhost:4200", "PUT", None)
            .unwrap()
            .headers();
        assert_eq!(
            headers,
            [
                (
                    "Access-Control-Allow-Origin",
                    "https://localhost:4200".into()
                ),
                ("Access-Control-Allow-Methods", "GET, PUT".into()),
                ("Access-Control-Max-Age", "3000".into()),
                ("Access-Control-Allow-Credentials", "true".into()),
                ("Vary", VARY.into()),
            ]
        );
        assert!(allowing(&rules, "http://localhost:4200", "PUT", None).is_none());
        let partial = checked(vec![rule(&["GET", "PUT"], &["http://*.origin.com"])]);
        assert!(allowing(&partial, "http://test.origin.com", "GET", None).is_some());
        assert!(allowing(&partial, "http://test.origin.com/", "GET", None).is_none());
        let any = checked(vec![rule(&["GET", "PUT"], &["*"])]);
        let headers = allowing(&any, "http://random:1234", "GET", None)
            .unwrap()
            .headers();
        assert_eq!(header(&headers, "Access-Control-Allow-Origin"), Some("*"));
        assert_eq!(header(&headers, "Access-Control-Allow-Credentials"), None);
        // A named origin beside `*` is echoed, with credentials: `*` alone answers `*`.
        let both = checked(vec![rule(&["GET"], &["*", "https://app.test.com"])]);
        let named = allowing(&both, "https://app.test.com", "GET", None)
            .unwrap()
            .headers();
        assert_eq!(
            header(&named, "Access-Control-Allow-Origin"),
            Some("https://app.test.com")
        );
        let other = allowing(&both, "https://other.test.com", "GET", None)
            .unwrap()
            .headers();
        assert_eq!(header(&other, "Access-Control-Allow-Origin"), Some("*"));
    }

    /// LocalStack's `test_cors_match_headers` and `test_put_cors_default_values`, and s3-tests'
    /// `test_cors_header_option` (16 §6, §7).
    #[test]
    fn requested_headers_match_whatever_their_case() {
        let star = checked(vec![with_headers(rule(&["GET"], &["*"]), &["*"])]);
        let one = allowing(&star, "o", "GET", Some("x-amz-request-payer")).unwrap();
        assert_eq!(
            header(&one.headers(), "Access-Control-Allow-Headers"),
            Some("x-amz-request-payer")
        );
        let two = allowing(
            &star,
            "o",
            "GET",
            Some("x-amz-request-payer, x-amz-expected-bucket-owner"),
        )
        .unwrap();
        assert_eq!(
            header(&two.headers(), "Access-Control-Allow-Headers"),
            Some("x-amz-request-payer, x-amz-expected-bucket-owner")
        );
        let listed = checked(vec![with_headers(
            rule(&["GET"], &["*"]),
            &[
                "x-amz-expected-bucket-owner",
                "x-amz-server-side-encryption-customer-algorithm",
                "x-AMZ-server-SIDE-encryption",
            ],
        )]);
        assert!(allowing(&listed, "o", "GET", Some("x-amz-request-payer")).is_none());
        for requested in [
            "x-AMZ-expected-BUCKET-owner, x-amz-server-side-encryption",
            "x-amz-expected-bucket-owner,x-amz-server-side-encryption",
            " x-amz-expected-bucket-owner ,, x-amz-server-side-encryption,",
        ] {
            let allowed = allowing(&listed, "o", "GET", Some(requested)).unwrap();
            assert_eq!(
                header(&allowed.headers(), "Access-Control-Allow-Headers"),
                Some("x-amz-expected-bucket-owner, x-amz-server-side-encryption"),
                "{requested}"
            );
        }
        let prefixed = checked(vec![with_headers(rule(&["PUT"], &["*"]), &["x-amz-*"])]);
        assert!(allowing(&prefixed, "o", "PUT", Some("x-amz-meta-a, X-Amz-Date")).is_some());
        assert!(allowing(&prefixed, "o", "PUT", Some("content-md5")).is_none());
        let none = checked(vec![Given {
            expose: vec!["x-amz-meta-header1".into()],
            ..rule(&["GET"], &["*"])
        }]);
        assert!(allowing(&none, "o", "GET", Some("x-amz-meta-header2")).is_none());
        let exposed = allowing(&none, "o", "GET", None).unwrap().headers();
        assert_eq!(
            header(&exposed, "Access-Control-Expose-Headers"),
            Some("x-amz-meta-header1")
        );
    }

    /// The user guide's preflight sample: the rule's methods, the requested header, the exposed
    /// header, credentials and `Vary` (16 §2). The header comes back lowercase, as S3 answered
    /// in 2025, where the 2024 sample kept its case.
    #[test]
    fn a_preflight_answers_as_the_user_guides_sample() {
        let rules = checked(vec![Given {
            expose: vec!["x-amz-meta-custom-header".into()],
            ..with_headers(
                rule(
                    &["GET", "PUT", "POST", "DELETE"],
                    &["http://www.example1.com"],
                ),
                &["Authorization"],
            )
        }]);
        let allowed = preflight(
            Some(&rules),
            Some("http://www.example1.com"),
            Some("PUT"),
            Some("Authorization"),
        )
        .unwrap();
        assert_eq!(
            allowed.headers(),
            [
                (
                    "Access-Control-Allow-Origin",
                    "http://www.example1.com".into()
                ),
                (
                    "Access-Control-Allow-Methods",
                    "GET, PUT, POST, DELETE".into()
                ),
                ("Access-Control-Allow-Headers", "authorization".into()),
                (
                    "Access-Control-Expose-Headers",
                    "x-amz-meta-custom-header".into()
                ),
                ("Access-Control-Allow-Credentials", "true".into()),
                ("Vary", VARY.into()),
            ]
        );
    }

    /// What S3 answers a preflight it refuses (16 §7), and s3-tests' 400 for an `Origin`
    /// without a requested method (16 §6).
    #[test]
    fn preflights_are_refused_as_s3_refuses_them() {
        let rules = checked(vec![rule(&["GET"], &["https://a.example"])]);
        assert_eq!(
            preflight(Some(&rules), None, Some("GET"), None),
            Err(Refusal::NoOrigin)
        );
        assert_eq!(
            Refusal::NoOrigin.code(),
            (
                "BadRequest",
                400,
                "Insufficient information. Origin request header needed."
            )
        );
        assert_eq!(
            preflight(None, Some("https://a.example"), Some("GET"), None),
            Err(Refusal::NotEnabled)
        );
        assert_eq!(
            Refusal::NotEnabled.code().2,
            "CORSResponse: CORS is not enabled for this bucket."
        );
        assert_eq!(
            preflight(Some(&rules), Some("https://a.example"), None, None),
            Err(Refusal::NoMethod)
        );
        assert_eq!(Refusal::NoMethod.code().1, 400);
        for (origin, method) in [("https://b.example", "GET"), ("https://a.example", "PUT")] {
            assert_eq!(
                preflight(Some(&rules), Some(origin), Some(method), None),
                Err(Refusal::NotAllowed)
            );
        }
        assert_eq!(Refusal::NotAllowed.code().0, "AccessForbidden");
        assert!(preflight(Some(&rules), Some("https://a.example"), Some("PATCH"), None).is_err());
    }

    /// An actual request is matched by the method it asks about when it carries one, as S3
    /// matched GET with `Access-Control-Request-Method: PUT` by PUT, and s3-tests expects a
    /// PUT carrying `GET` to be answered for GET (16 §6, §7).
    #[test]
    fn actual_requests_match_by_the_method_they_ask_about() {
        let rules = checked(vec![rule(&["GET"], &["https://localhost:4200"])]);
        let origin = "https://localhost:4200";
        assert!(actual(&rules, origin, "GET", None, None).is_some());
        assert!(actual(&rules, origin, "GET", Some("PUT"), None).is_none());
        assert!(actual(&rules, origin, "PUT", None, None).is_none());
        assert!(actual(&rules, origin, "PUT", Some("GET"), None).is_some());
        let star = checked(vec![rule(&["GET"], &["*"])]);
        assert!(actual(&star, origin, "GET", None, Some("x-amz-request-payer")).is_none());
    }
}
