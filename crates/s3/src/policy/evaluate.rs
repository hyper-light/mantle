//! How a request is judged against a bucket policy (docs/research/17 §3.3, §3.4, §4): which
//! statements apply to it, and whether it goes ahead.

use super::{Operator, Policy, Principals, Set, Statement, cidr, date, number};

/// Who makes a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requester<'a> {
    /// An unsigned request: "All unauthenticated requests are made by the anonymous user"
    /// (17 §4).
    Anonymous,
    /// An account: its 12-digit ID, as a policy names it, and its canonical user ID.
    Account { id: &'a str, canonical: &'a str },
}

/// A request as a policy judges it.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub requester: Requester<'a>,
    /// The action, `s3:` and its name as the catalog writes it, e.g. `s3:GetObject`.
    pub action: &'a str,
    /// The resource's ARN: `arn:aws:s3:::bucket`, or `arn:aws:s3:::bucket/key` for an object.
    pub resource: &'a str,
    /// The request's condition keys, lowercase, each with its values; a key the request lacks
    /// is absent.
    pub context: &'a [(String, Vec<String>)],
}

impl Request<'_> {
    fn values(&self, key: &str) -> Option<&[String]> {
        self.context
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, values)| values.as_slice())
    }
}

/// The keys that describe the requester, as IAM gives them for an account's root and for an
/// anonymous caller (17 §3.4, §5): `aws:PrincipalAccount`, `aws:PrincipalType` and
/// `aws:userid` for every request, and `aws:PrincipalArn` for a signed one.
pub fn principal_keys(requester: Requester<'_>) -> Vec<(String, Vec<String>)> {
    let key = |name: &str, value: String| (name.to_owned(), vec![value]);
    match requester {
        Requester::Anonymous => vec![
            key("aws:principalaccount", "anonymous".to_owned()),
            key("aws:principaltype", "Anonymous".to_owned()),
            key("aws:userid", "anonymous".to_owned()),
        ],
        Requester::Account { id, .. } => vec![
            key("aws:principalaccount", id.to_owned()),
            key("aws:principalarn", format!("arn:aws:iam::{id}:root")),
            key("aws:principaltype", "Account".to_owned()),
            key("aws:userid", id.to_owned()),
        ],
    }
}

/// What a policy's statements say of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// A statement denying it applies: "If the enforcement code finds even one explicit deny
    /// that applies, the enforcement code returns a final decision of Deny" (17 §4).
    Deny,
    /// No denying statement applies, and an allowing one does.
    Allow,
    /// No statement applies: an implicit deny unless something else allows it.
    None,
}

impl Policy {
    /// The verdict of the policy's statements on `request`.
    pub fn verdict(&self, request: &Request<'_>) -> Verdict {
        let mut allowed = false;
        for statement in &self.statements {
            if self.applies(statement, request) {
                if !statement.allow {
                    return Verdict::Deny;
                }
                allowed = true;
            }
        }
        if allowed {
            Verdict::Allow
        } else {
            Verdict::None
        }
    }

    fn applies(&self, statement: &Statement, request: &Request<'_>) -> bool {
        let action = request.action.to_ascii_lowercase();
        let named_action = statement
            .actions
            .iter()
            .any(|pattern| tokens(pattern, None).is_some_and(|p| matches(&p, &action)));
        let variables = if self.variables { Some(request) } else { None };
        let named_resource = statement.resources.iter().any(|pattern| {
            tokens(pattern, variables).is_some_and(|p| matches(&p, request.resource))
        });
        principal(&statement.principals, request.requester)
            && named_action != statement.not_action
            && named_resource != statement.not_resource
            && statement
                .conditions
                .iter()
                .all(|condition| holds(condition, request, variables))
    }
}

/// Whether a request goes ahead on a bucket whose owner's account is `owner`, under its
/// policy, if it has one (17 §4):
///
/// - "the root principal in a bucket owner's AWS account can perform the `GetBucketPolicy`,
///   `PutBucketPolicy`, and `DeleteBucketPolicy` API actions, even if their bucket policy
///   explicitly denies the root principal's access";
/// - otherwise a denying statement that applies refuses it, the owner's own requests included,
///   as s3-tests expects;
/// - the owner's account may do anything else: "all requests are implicitly denied with the
///   exception of the AWS account root user, which has full access";
/// - anyone else needs an allowing statement. mantle's requesters are accounts, each its own
///   root, so no identity policy stands between another account and what the bucket policy
///   grants it, as s3-tests expects of another account's root.
pub fn authorize(owner: &str, policy: Option<&Policy>, request: &Request<'_>) -> bool {
    let owners = matches!(request.requester, Requester::Account { id, .. } if id == owner);
    let own_policy = matches!(
        request.action,
        "s3:GetBucketPolicy" | "s3:PutBucketPolicy" | "s3:DeleteBucketPolicy"
    );
    if owners && own_policy {
        return true;
    }
    match policy.map(|policy| policy.verdict(request)) {
        Some(Verdict::Deny) => false,
        Some(Verdict::Allow) => true,
        Some(Verdict::None) | None => owners,
    }
}

/// Whether a statement's `Principal` names the requester, or its `NotPrincipal` does not.
fn principal(principals: &Principals, requester: Requester<'_>) -> bool {
    let named = principals.everyone
        || match requester {
            Requester::Anonymous => false,
            Requester::Account { id, canonical } => {
                principals.accounts.iter().any(|account| account == id)
                    || principals.canonical.iter().any(|user| user == canonical)
            }
        };
    named != principals.not
}

/// One piece of a pattern: a character to match itself, or a wildcard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Char(char),
    /// `*`: any run of characters.
    Run,
    /// `?`: any one character.
    One,
}

/// A pattern as tokens, with its policy variables replaced when `variables` holds the request
/// they are read from (17 §3.4): `${key}` by the key's one value, or with `${key, 'default'}`
/// the default when the key has none; `${*}`, `${?}` and `${$}` by those characters. What a
/// variable stands for matches itself, never as a wildcard. `None` for a variable with no
/// value, which "will not match any resource".
fn tokens(pattern: &str, variables: Option<&Request<'_>>) -> Option<Vec<Token>> {
    let mut out = Vec::new();
    for piece in pieces(pattern, variables)? {
        match piece {
            Piece::Literal(text) => out.extend(text.chars().map(Token::Char)),
            Piece::Pattern(text) => out.extend(text.chars().map(|c| match c {
                '*' => Token::Run,
                '?' => Token::One,
                c => Token::Char(c),
            })),
        }
    }
    Some(out)
}

/// A value with its policy variables replaced, every character taken as itself: the form an
/// equality compares. `None` for a variable with no value.
fn substitute(written: &str, variables: Option<&Request<'_>>) -> Option<String> {
    let mut out = String::new();
    for piece in pieces(written, variables)? {
        match piece {
            Piece::Literal(text) | Piece::Pattern(text) => out.push_str(&text),
        }
    }
    Some(out)
}

/// A part of a written value: text as written, or what a variable stands for.
enum Piece {
    Pattern(String),
    Literal(String),
}

fn pieces(written: &str, variables: Option<&Request<'_>>) -> Option<Vec<Piece>> {
    let Some(request) = variables else {
        return Some(vec![Piece::Pattern(written.to_owned())]);
    };
    let mut out = Vec::new();
    let mut rest = written;
    while let Some(start) = rest.find("${") {
        let (before, from) = rest.split_at(start);
        let after = from.get(2..).unwrap_or("");
        let Some(end) = after.find('}') else {
            break;
        };
        let (inside, tail) = after.split_at(end);
        out.push(Piece::Pattern(before.to_owned()));
        out.push(Piece::Literal(variable(inside, request)?));
        rest = tail.get(1..).unwrap_or("");
    }
    out.push(Piece::Pattern(rest.to_owned()));
    Some(out)
}

/// A variable's text, between `${` and `}`.
fn variable(inside: &str, request: &Request<'_>) -> Option<String> {
    match inside {
        "*" | "?" | "$" => return Some(inside.to_owned()),
        _ => {}
    }
    let (name, default) = match inside.split_once(',') {
        Some((name, default)) => {
            let default = default
                .trim_start_matches(' ')
                .strip_prefix('\'')
                .and_then(|rest| rest.strip_suffix('\''))?;
            (name, Some(default))
        }
        None => (inside, None),
    };
    let key = name.trim().to_ascii_lowercase();
    match request.values(&key) {
        Some([one]) => Some(one.clone()),
        _ => default.map(str::to_owned),
    }
}

/// Whether `text` matches the tokens: a greedy match that returns to the last `*` on a
/// mismatch, whose work is at most the product of the two lengths.
fn matches(pattern: &[Token], text: &str) -> bool {
    let text: Vec<char> = text.chars().collect();
    let (mut p, mut t) = (0usize, 0usize);
    let mut resume: Option<(usize, usize)> = None;
    while t < text.len() {
        match (pattern.get(p), text.get(t)) {
            (Some(Token::Run), _) => {
                p = p.saturating_add(1);
                resume = Some((p, t));
            }
            (Some(Token::One), Some(_)) => {
                p = p.saturating_add(1);
                t = t.saturating_add(1);
            }
            (Some(Token::Char(c)), Some(d)) if c == d => {
                p = p.saturating_add(1);
                t = t.saturating_add(1);
            }
            _ => match resume {
                Some((after_run, from)) => {
                    let from = from.saturating_add(1);
                    p = after_run;
                    t = from;
                    resume = Some((after_run, from));
                }
                None => return false,
            },
        }
    }
    pattern
        .get(p..)
        .is_some_and(|rest| rest.iter().all(|token| *token == Token::Run))
}

/// The positive operator a negated one inverts, and whether it is negated.
fn positive(operator: Operator) -> (Operator, bool) {
    use Operator as O;
    match operator {
        O::StringNotEquals => (O::StringEquals, true),
        O::StringNotEqualsIgnoreCase => (O::StringEqualsIgnoreCase, true),
        O::StringNotLike => (O::StringLike, true),
        O::NumericNotEquals => (O::NumericEquals, true),
        O::DateNotEquals => (O::DateEquals, true),
        O::NotIpAddress => (O::IpAddress, true),
        O::ArnNotEquals | O::ArnNotLike => (O::ArnLike, true),
        O::ArnEquals => (O::ArnLike, false),
        other => (other, false),
    }
}

/// Whether a condition holds for the request (17 §3.3):
///
/// - a key the request lacks: `Null` true holds; `...IfExists` holds; `ForAllValues` holds
///   and `ForAnyValue` does not; otherwise a negated operator holds and a positive one does
///   not;
/// - the policy's values are ORed for a positive operator and NORed for a negated one;
/// - `ForAllValues` asks it of every one of the request's values, and `ForAnyValue`, as a key
///   without a set operator, of any one.
fn holds(
    condition: &super::Condition,
    request: &Request<'_>,
    variables: Option<&Request<'_>>,
) -> bool {
    let present = request.values(&condition.key);
    if condition.operator == Operator::Null {
        let absent = present.is_none();
        return condition
            .values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("true") == absent);
    }
    let (operator, negated) = positive(condition.operator);
    let Some(values) = present else {
        return condition.if_exists
            || match condition.set {
                Some(Set::All) => true,
                Some(Set::Any) => false,
                None => negated,
            };
    };
    let one = |value: &String| {
        condition
            .values
            .iter()
            .any(|written| compare(operator, value, written, variables))
    };
    match (condition.set, negated) {
        (Some(Set::All), false) => values.iter().all(one),
        (Some(Set::All), true) => values.iter().all(|value| !one(value)),
        (Some(Set::Any), true) => values.iter().any(|value| !one(value)),
        (None, true) => !values.iter().any(one),
        (Some(Set::Any) | None, false) => values.iter().any(one),
    }
}

/// Whether the request's `value` meets the policy's `written` value under a positive operator.
/// Variables are replaced in string and ARN values only, as IAM allows them (17 §3.4).
fn compare(
    operator: Operator,
    value: &str,
    written: &str,
    variables: Option<&Request<'_>>,
) -> bool {
    use Operator as O;
    let replaced = || substitute(written, variables);
    let numbers = || number(value).zip(number(written));
    let dates = || date(value).zip(date(written));
    match operator {
        O::StringEquals => replaced().is_some_and(|written| value == written),
        O::StringEqualsIgnoreCase => {
            replaced().is_some_and(|written| value.to_lowercase() == written.to_lowercase())
        }
        O::StringLike => tokens(written, variables).is_some_and(|p| matches(&p, value)),
        O::ArnLike => arn(written, value, variables),
        O::NumericEquals => numbers().is_some_and(|(v, w)| v == w),
        O::NumericLessThan => numbers().is_some_and(|(v, w)| v < w),
        O::NumericLessThanEquals => numbers().is_some_and(|(v, w)| v <= w),
        O::NumericGreaterThan => numbers().is_some_and(|(v, w)| v > w),
        O::NumericGreaterThanEquals => numbers().is_some_and(|(v, w)| v >= w),
        O::DateEquals => dates().is_some_and(|(v, w)| v == w),
        O::DateLessThan => dates().is_some_and(|(v, w)| v < w),
        O::DateLessThanEquals => dates().is_some_and(|(v, w)| v <= w),
        O::DateGreaterThan => dates().is_some_and(|(v, w)| v > w),
        O::DateGreaterThanEquals => dates().is_some_and(|(v, w)| v >= w),
        O::Bool => value.eq_ignore_ascii_case(written),
        O::BinaryEquals => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(written)
                .is_ok_and(|bytes| bytes == value.as_bytes())
        }
        O::IpAddress => within(value, written),
        // The negated operators and `Null` reach here only through `positive` and `holds`.
        O::StringNotEquals
        | O::StringNotEqualsIgnoreCase
        | O::StringNotLike
        | O::NumericNotEquals
        | O::DateNotEquals
        | O::NotIpAddress
        | O::ArnEquals
        | O::ArnNotEquals
        | O::ArnNotLike
        | O::Null => false,
    }
}

/// `ArnLike`: "Each of the six colon-delimited components of the ARN is checked separately and
/// each can include multi-character match wildcards (*) or single-character match wildcards (?)"
/// (17 §3.3); the sixth, the resource, runs to the end, colons and all.
fn arn(written: &str, value: &str, variables: Option<&Request<'_>>) -> bool {
    let (Some(pattern), Some(parts)) = (split_arn(written), split_arn(value)) else {
        return false;
    };
    pattern
        .iter()
        .zip(parts.iter())
        .all(|(pattern, part)| tokens(pattern, variables).is_some_and(|p| matches(&p, part)))
}

fn split_arn(text: &str) -> Option<[&str; 6]> {
    let mut parts = text.splitn(6, ':');
    let arn = [
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    ];
    Some(arn)
}

/// Whether the address `value` lies in the range `written`.
fn within(value: &str, written: &str) -> bool {
    use std::net::IpAddr;
    let (Ok(address), Some((network, prefix))) = (value.parse::<IpAddr>(), cidr(written)) else {
        return false;
    };
    let (address, network, width) = match (address, network) {
        (IpAddr::V4(a), IpAddr::V4(n)) => {
            (u128::from(u32::from(a)), u128::from(u32::from(n)), 32u32)
        }
        (IpAddr::V6(a), IpAddr::V6(n)) => (u128::from(a), u128::from(n), 128u32),
        _ => return false,
    };
    let shift = width.saturating_sub(u32::from(prefix));
    address.checked_shr(shift).unwrap_or(0) == network.checked_shr(shift).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::parse;

    const OWNER: &str = "111111111111";
    const OTHER: Requester<'static> = Requester::Account {
        id: "222222222222",
        canonical: "c2",
    };
    const OWNING: Requester<'static> = Requester::Account {
        id: OWNER,
        canonical: "c1",
    };

    fn policy(statements: &str) -> Policy {
        parse(
            format!("{{\"Version\": \"2012-10-17\", \"Statement\": [{statements}]}}").as_bytes(),
            "bucket",
        )
        .unwrap()
    }

    fn context(pairs: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        pairs
            .iter()
            .map(|(key, values)| {
                (
                    key.to_string(),
                    values.iter().map(|value| value.to_string()).collect(),
                )
            })
            .collect()
    }

    fn may(
        policy: Option<&Policy>,
        requester: Requester<'_>,
        action: &str,
        resource: &str,
        context: &[(String, Vec<String>)],
    ) -> bool {
        let request = Request {
            requester,
            action,
            resource,
            context,
        };
        authorize(OWNER, policy, &request)
    }

    /// S3's defaults: its owner's account may, anyone else may not (17 §4).
    #[test]
    fn without_a_policy_only_the_owner_may() {
        let object = "arn:aws:s3:::bucket/k";
        assert!(may(None, OWNING, "s3:GetObject", object, &[]));
        assert!(!may(None, OTHER, "s3:GetObject", object, &[]));
        assert!(!may(
            None,
            Requester::Anonymous,
            "s3:GetObject",
            object,
            &[]
        ));
    }

    /// s3-tests' `test_bucket_policy`: `{"AWS": "*"}` lets another account and an anonymous
    /// requester list, and nothing more.
    #[test]
    fn everyone_is_every_account_and_the_anonymous_requester() {
        let policy = policy(
            r#"{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:ListBucket",
                "Resource": ["arn:aws:s3:::bucket", "arn:aws:s3:::bucket/*"]}"#,
        );
        let bucket = "arn:aws:s3:::bucket";
        for requester in [OTHER, Requester::Anonymous] {
            assert!(may(Some(&policy), requester, "s3:ListBucket", bucket, &[]));
            assert!(!may(
                Some(&policy),
                requester,
                "s3:GetObject",
                "arn:aws:s3:::bucket/k",
                &[]
            ));
        }
        assert!(may(
            Some(&policy),
            OWNING,
            "s3:PutObject",
            "arn:aws:s3:::bucket/k",
            &[]
        ));
    }

    /// A denying statement binds the owner, as s3-tests' `test_encryption_sse_c_enforced_with_bucket_policy`
    /// expects; the owner's root keeps its own policy, as S3 documents (17 §2.1).
    #[test]
    fn a_deny_binds_the_owner_but_not_over_its_own_policy() {
        let policy = policy(
            r#"{"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"Null": {"s3:x-amz-server-side-encryption-customer-algorithm": true}}},
               {"Effect": "Deny", "Principal": "*", "Action": ["s3:GetBucketPolicy",
                "s3:PutBucketPolicy", "s3:DeleteBucketPolicy"], "Resource": "arn:aws:s3:::bucket"}"#,
        );
        let object = "arn:aws:s3:::bucket/k";
        assert!(!may(Some(&policy), OWNING, "s3:PutObject", object, &[]));
        let with_sse_c = context(&[(
            "s3:x-amz-server-side-encryption-customer-algorithm",
            &["AES256"],
        )]);
        assert!(may(
            Some(&policy),
            OWNING,
            "s3:PutObject",
            object,
            &with_sse_c
        ));
        let bucket = "arn:aws:s3:::bucket";
        for action in [
            "s3:GetBucketPolicy",
            "s3:PutBucketPolicy",
            "s3:DeleteBucketPolicy",
        ] {
            assert!(may(Some(&policy), OWNING, action, bucket, &[]), "{action}");
            assert!(!may(Some(&policy), OTHER, action, bucket, &[]), "{action}");
        }
    }

    /// s3-tests' tag, copy-source, metadata-directive and ACL conditions (17 §9).
    #[test]
    fn s3_tests_conditions_hold_as_it_expects() {
        let tagged = policy(
            r#"{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"StringEquals": {"s3:ExistingObjectTag/security": "public"}}}"#,
        );
        let object = "arn:aws:s3:::bucket/publictag";
        let public = context(&[("s3:existingobjecttag/security", &["public"])]);
        let private = context(&[("s3:existingobjecttag/security", &["private"])]);
        let other_tag = context(&[("s3:existingobjecttag/security1", &["public"])]);
        assert!(may(Some(&tagged), OTHER, "s3:GetObject", object, &public));
        assert!(!may(Some(&tagged), OTHER, "s3:GetObject", object, &private));
        assert!(!may(
            Some(&tagged),
            OTHER,
            "s3:GetObject",
            object,
            &other_tag
        ));

        let copies = policy(
            r#"{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"StringLike": {"s3:x-amz-copy-source": "bucket/public/*"}}}"#,
        );
        let from = |source: &str| context(&[("s3:x-amz-copy-source", &[source])]);
        assert!(may(
            Some(&copies),
            OTHER,
            "s3:PutObject",
            object,
            &from("bucket/public/foo")
        ));
        assert!(!may(
            Some(&copies),
            OTHER,
            "s3:PutObject",
            object,
            &from("bucket/private/foo")
        ));

        let directive = policy(
            r#"{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"StringEquals": {"s3:x-amz-metadata-directive": "COPY"}}}"#,
        );
        let copy = context(&[("s3:x-amz-metadata-directive", &["COPY"])]);
        assert!(may(Some(&directive), OTHER, "s3:PutObject", object, &copy));
        assert!(!may(Some(&directive), OTHER, "s3:PutObject", object, &[]));

        let acl = policy(
            r#"{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::bucket/*"},
               {"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"StringLike": {"s3:x-amz-acl": ["public*"]}}}"#,
        );
        let public_read = context(&[("s3:x-amz-acl", &["public-read"])]);
        assert!(may(Some(&acl), OTHER, "s3:PutObject", object, &[]));
        assert!(!may(
            Some(&acl),
            OTHER,
            "s3:PutObject",
            object,
            &public_read
        ));
    }

    /// `...IfExists`, and a negated operator on a missing key (17 §3.3).
    #[test]
    fn missing_keys_are_judged_as_iam_says() {
        let referred = policy(
            r#"{"Sid": "Allow GetObject if the referer is example.com", "Effect": "Allow",
                "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"StringLikeIfExists": {"aws:Referer": "http://www.example.com/*"}}}"#,
        );
        let object = "arn:aws:s3:::bucket/k";
        let referer = |value: &str| context(&[("aws:referer", &[value])]);
        let anonymous = Requester::Anonymous;
        assert!(may(
            Some(&referred),
            anonymous,
            "s3:GetObject",
            object,
            &referer("http://www.example.com/")
        ));
        assert!(may(Some(&referred), anonymous, "s3:GetObject", object, &[]));
        assert!(!may(
            Some(&referred),
            anonymous,
            "s3:GetObject",
            object,
            &referer("http://example.com")
        ));

        let not_equals = policy(
            r#"{"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"StringNotEquals": {"aws:Referer": "http://www.example.com/"}}}"#,
        );
        assert!(!may(Some(&not_equals), OWNING, "s3:GetObject", object, &[]));
        assert!(may(
            Some(&not_equals),
            OWNING,
            "s3:GetObject",
            object,
            &referer("http://www.example.com/")
        ));
    }

    /// `ForAllValues` holds for a missing key and asks every value; `ForAnyValue` fails for one
    /// and asks any value (17 §3.3).
    #[test]
    fn set_operators_follow_iam() {
        let statement = |set: &str| {
            policy(&format!(
                r#"{{"Effect": "Allow", "Principal": "*", "Action": "s3:PutObject",
                    "Resource": "arn:aws:s3:::bucket/*",
                    "Condition": {{"{set}:StringEquals": {{"s3:RequestObjectTagKeys": ["a", "b"]}}}}}}"#
            ))
        };
        let (all, any) = (statement("ForAllValues"), statement("ForAnyValue"));
        let object = "arn:aws:s3:::bucket/k";
        let keys = |values: &[&str]| context(&[("s3:requestobjecttagkeys", values)]);
        let judge = |policy: &Policy, context: &[(String, Vec<String>)]| {
            may(Some(policy), OTHER, "s3:PutObject", object, context)
        };
        assert!(judge(&all, &keys(&["a"])));
        assert!(!judge(&all, &keys(&["a", "c"])));
        assert!(judge(&all, &[]));
        assert!(judge(&any, &keys(&["c", "a"])));
        assert!(!judge(&any, &keys(&["c"])));
        assert!(!judge(&any, &[]));
    }

    #[test]
    fn numbers_dates_addresses_and_arns_compare_by_kind() {
        let object = "arn:aws:s3:::bucket/k";
        let bucket = "arn:aws:s3:::bucket";
        let listing = policy(
            r#"{"Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::bucket",
                "Condition": {"NumericLessThanEquals": {"s3:max-keys": "10"},
                              "IpAddress": {"aws:SourceIp": ["10.0.0.0/8", "2001:db8::/64"]}}}"#,
        );
        let request =
            |keys: &str, ip: &str| context(&[("s3:max-keys", &[keys]), ("aws:sourceip", &[ip])]);
        assert!(may(
            Some(&listing),
            OTHER,
            "s3:ListBucket",
            bucket,
            &request("5", "10.1.2.3")
        ));
        assert!(!may(
            Some(&listing),
            OTHER,
            "s3:ListBucket",
            bucket,
            &request("20", "10.1.2.3")
        ));
        assert!(!may(
            Some(&listing),
            OTHER,
            "s3:ListBucket",
            bucket,
            &request("5", "11.0.0.1")
        ));
        assert!(may(
            Some(&listing),
            OTHER,
            "s3:ListBucket",
            bucket,
            &request("5", "2001:db8::1")
        ));
        assert!(!may(
            Some(&listing),
            OTHER,
            "s3:ListBucket",
            bucket,
            &request("5", "2001:db9::1")
        ));

        let timed = policy(
            r#"{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::bucket/*",
                "Condition": {"DateGreaterThan": {"aws:CurrentTime": "2019-07-16T12:00:00Z"},
                              "Bool": {"aws:SecureTransport": "true"},
                              "ArnLike": {"aws:PrincipalArn": "arn:aws:iam::*:root"}}}"#,
        );
        let mut keys = principal_keys(OTHER);
        keys.extend(context(&[
            ("aws:currenttime", &["2020-01-01T00:00:00Z"]),
            ("aws:securetransport", &["true"]),
        ]));
        assert!(may(Some(&timed), OTHER, "s3:GetObject", object, &keys));
        let mut early = principal_keys(OTHER);
        early.extend(context(&[
            ("aws:currenttime", &["2019-01-01T00:00:00Z"]),
            ("aws:securetransport", &["true"]),
        ]));
        assert!(!may(Some(&timed), OTHER, "s3:GetObject", object, &early));
        let mut anonymous = principal_keys(Requester::Anonymous);
        anonymous.extend(context(&[
            ("aws:currenttime", &["2020-01-01T00:00:00Z"]),
            ("aws:securetransport", &["true"]),
        ]));
        assert!(!may(
            Some(&timed),
            Requester::Anonymous,
            "s3:GetObject",
            object,
            &anonymous
        ));
    }

    /// `NotPrincipal`, `NotAction` and `NotResource` apply to everything but what they name.
    #[test]
    fn negated_elements_apply_to_the_rest() {
        let fence = policy(
            r#"{"Effect": "Deny", "NotPrincipal": {"AWS": "222222222222"}, "Action": "s3:*",
                "Resource": "arn:aws:s3:::bucket/*"},
               {"Effect": "Allow", "Principal": "*", "NotAction": "s3:DeleteObject",
                "NotResource": "arn:aws:s3:::bucket/private/*"}"#,
        );
        let public = "arn:aws:s3:::bucket/public/k";
        assert!(may(Some(&fence), OTHER, "s3:GetObject", public, &[]));
        assert!(!may(Some(&fence), OTHER, "s3:DeleteObject", public, &[]));
        assert!(!may(
            Some(&fence),
            OTHER,
            "s3:GetObject",
            "arn:aws:s3:::bucket/private/k",
            &[]
        ));
        assert!(!may(Some(&fence), OWNING, "s3:GetObject", public, &[]));
        assert!(!may(
            Some(&fence),
            Requester::Anonymous,
            "s3:GetObject",
            public,
            &[]
        ));
        assert!(may(
            Some(&fence),
            OWNING,
            "s3:ListBucket",
            "arn:aws:s3:::bucket",
            &[]
        ));
    }

    /// Policy variables under 2012-10-17: a key's value, a default, the escapes, and a value that
    /// matches itself only; under 2008-10-17, text (17 §3.4).
    #[test]
    fn variables_stand_for_the_request_s_values() {
        let home = policy(
            r#"{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "s3:GetObject",
                "Resource": ["arn:aws:s3:::bucket/${aws:userid}/*",
                             "arn:aws:s3:::bucket/${aws:username, 'shared'}/*",
                             "arn:aws:s3:::bucket/stars/${*}"]}"#,
        );
        let keys = principal_keys(OTHER);
        let object = |key: &str| format!("arn:aws:s3:::bucket/{key}");
        assert!(may(
            Some(&home),
            OTHER,
            "s3:GetObject",
            &object("222222222222/k"),
            &keys
        ));
        assert!(!may(
            Some(&home),
            OTHER,
            "s3:GetObject",
            &object("333333333333/k"),
            &keys
        ));
        assert!(may(
            Some(&home),
            OTHER,
            "s3:GetObject",
            &object("shared/k"),
            &keys
        ));
        assert!(may(
            Some(&home),
            OTHER,
            "s3:GetObject",
            &object("stars/*"),
            &keys
        ));
        assert!(!may(
            Some(&home),
            OTHER,
            "s3:GetObject",
            &object("stars/x"),
            &keys
        ));
        let starred = context(&[("aws:userid", &["a*"])]);
        let starred_requester = Requester::Account {
            id: "222222222222",
            canonical: "c2",
        };
        assert!(may(
            Some(&home),
            starred_requester,
            "s3:GetObject",
            &object("a*/k"),
            &starred
        ));
        assert!(!may(
            Some(&home),
            starred_requester,
            "s3:GetObject",
            &object("abc/k"),
            &starred
        ));
        let literal = parse(
            br#"{"Version": "2008-10-17", "Statement": [{"Effect": "Allow", "Principal": "*",
                "Action": "s3:GetObject", "Resource": "arn:aws:s3:::bucket/${aws:userid}/*"}]}"#,
            "bucket",
        )
        .unwrap();
        assert!(may(
            Some(&literal),
            OTHER,
            "s3:GetObject",
            &object("${aws:userid}/k"),
            &keys
        ));
        assert!(!may(
            Some(&literal),
            OTHER,
            "s3:GetObject",
            &object("222222222222/k"),
            &keys
        ));
    }

    #[test]
    fn principal_keys_are_iams() {
        let anonymous = principal_keys(Requester::Anonymous);
        assert!(anonymous.contains(&("aws:principaltype".into(), vec!["Anonymous".into()])));
        assert!(!anonymous.iter().any(|(key, _)| key == "aws:principalarn"));
        let account = principal_keys(OTHER);
        assert!(account.contains(&(
            "aws:principalarn".into(),
            vec!["arn:aws:iam::222222222222:root".into()]
        )));
    }
}
