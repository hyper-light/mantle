//! AWS Signature Version 4 on S3 requests: the canonical request, the signing key, and
//! verification of the `Authorization` header and of presigned URLs (docs/research/05 §1).
//!
//! The canonical request is rebuilt from the request's decoded parts, re-encoded with S3's
//! `UriEncode` rules, rather than from the bytes as sent: clients differ in which optional
//! characters they escape, and the signed form is always the `UriEncode` form (05 §1.2.2).
//! S3 paths are encoded once and never normalized, so `a//b` stays `a//b`.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// SHA-256 of the empty string, the hashed payload of a request with no body.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// "The signed portions (using AWS Signatures) of requests are valid within 15 minutes of
/// the timestamp in the request" (S3 developer guide, Authenticating Requests; 05 §1.6).
pub const MAX_SKEW_SECS: i64 = 15 * 60;
/// A presigned URL's `X-Amz-Expires` is between 1 and 604800 seconds, seven days
/// (S3 developer guide, Using Query Parameters; 05 §1.7).
pub const MAX_EXPIRES_SECS: i64 = 604_800;

/// A request as it arrived.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub method: &'a str,
    /// The path as sent, still percent-encoded, starting with `/`.
    pub path: &'a str,
    /// The query string as sent, without the `?`.
    pub query: &'a str,
    /// Every header as sent, names in any case.
    pub headers: &'a [(&'a str, &'a str)],
}

/// How the body is protected, as `x-amz-content-sha256` declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// The body's SHA-256 was signed; the receiver hashes the body and compares.
    Sha256([u8; 32]),
    /// The body is not signed.
    Unsigned,
    /// `aws-chunked` framing with every chunk signed, and a signed trailing checksum when
    /// `trailer` is set.
    SignedChunks { trailer: bool },
    /// `aws-chunked` framing, unsigned chunks, and a trailing checksum that must be verified.
    UnsignedChunks,
}

/// A request whose signature verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    pub access_key: String,
    pub body: Body,
    /// For signed chunks: where their chain of signatures starts.
    pub chain: Option<Chain>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("the Authorization header is malformed: {0}")]
    HeaderMalformed(&'static str),
    #[error("the presigned URL's parameters are malformed: {0}")]
    QueryMalformed(&'static str),
    #[error("the request is not signed")]
    Missing,
    #[error("the access key is not known")]
    UnknownKey,
    #[error("the request's time is more than 15 minutes from the server's")]
    TooSkewed,
    #[error("the presigned URL has expired")]
    Expired,
    #[error("the signature does not match")]
    Mismatch,
    #[error("{0}")]
    Unsupported(&'static str),
}

impl AuthError {
    /// The S3 error code and HTTP status (S3 developer guide, Error responses; 05 §1.10).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::HeaderMalformed(_) => ("AuthorizationHeaderMalformed", 400),
            Self::QueryMalformed(_) => ("AuthorizationQueryParametersError", 400),
            Self::Missing | Self::Expired => ("AccessDenied", 403),
            Self::UnknownKey => ("InvalidAccessKeyId", 403),
            Self::TooSkewed => ("RequestTimeTooSkewed", 403),
            Self::Mismatch => ("SignatureDoesNotMatch", 403),
            Self::Unsupported(_) => ("InvalidRequest", 400),
        }
    }
}

/// The chain of chunk and trailer signatures that starts at a request's seed signature
/// (S3 developer guide, Transfer Payload in Multiple Chunks; 05 §1.8–§1.9).
#[derive(Clone, PartialEq, Eq)]
pub struct Chain {
    key: [u8; 32],
    timestamp: String,
    scope: String,
    previous: [u8; 32],
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The signing key is a credential.
        f.debug_struct("Chain")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl Chain {
    /// Verifies the signature of the next chunk, whose data hashes to `sha256`.
    pub fn verify_chunk(
        &mut self,
        sha256: &[u8; 32],
        signature: &[u8; 32],
    ) -> Result<(), AuthError> {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.timestamp,
            self.scope,
            hex(&self.previous),
            EMPTY_SHA256,
            hex(sha256)
        );
        self.advance(&sts, signature)
    }

    /// Verifies the trailer's signature; `canonical` is each trailing header as
    /// `name:value\n`, in order (05 §1.9).
    pub fn verify_trailer(
        &mut self,
        canonical: &[u8],
        signature: &[u8; 32],
    ) -> Result<(), AuthError> {
        let sts = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
            self.timestamp,
            self.scope,
            hex(&self.previous),
            hex(&Sha256::digest(canonical).into())
        );
        self.advance(&sts, signature)
    }

    fn advance(&mut self, string_to_sign: &str, signature: &[u8; 32]) -> Result<(), AuthError> {
        let mac = HmacSha256::new_from_slice(&self.key).map_err(|_| AuthError::Mismatch)?;
        mac.chain_update(string_to_sign.as_bytes())
            .verify_slice(signature)
            .map_err(|_| AuthError::Mismatch)?;
        self.previous = *signature;
        Ok(())
    }

    /// Signs the next chunk as a client would: for tests and for mantle's own clients.
    pub fn sign_chunk(&mut self, sha256: &[u8; 32]) -> [u8; 32] {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.timestamp,
            self.scope,
            hex(&self.previous),
            EMPTY_SHA256,
            hex(sha256)
        );
        let signature = hmac(&self.key, sts.as_bytes());
        self.previous = signature;
        signature
    }

    /// Signs the trailer as a client would.
    pub fn sign_trailer(&mut self, canonical: &[u8]) -> [u8; 32] {
        let sts = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
            self.timestamp,
            self.scope,
            hex(&self.previous),
            hex(&Sha256::digest(canonical).into())
        );
        let signature = hmac(&self.key, sts.as_bytes());
        self.previous = signature;
        signature
    }
}

/// Verifies a request signed in its `Authorization` header or as a presigned URL. `region`
/// is the region this endpoint serves; `secret` finds an access key's secret; `now` is Unix
/// seconds.
pub fn verify(
    request: &Request<'_>,
    region: &str,
    now: i64,
    secret: impl Fn(&str) -> Option<String>,
) -> Result<Verified, AuthError> {
    if let Some(authorization) = header(request.headers, "authorization") {
        return verify_header(request, authorization, region, now, &secret);
    }
    if query_has(request.query, "X-Amz-Signature") || query_has(request.query, "X-Amz-Algorithm") {
        return verify_presigned(request, region, now, &secret);
    }
    Err(AuthError::Missing)
}

/// The parts of a signature: who, when, where, which headers, and the claimed signature.
struct Claim<'a> {
    access_key: &'a str,
    date: &'a str,
    region: &'a str,
    service: &'a str,
    signed_headers: Vec<&'a str>,
    signature: [u8; 32],
}

fn verify_header(
    request: &Request<'_>,
    authorization: &str,
    region: &str,
    now: i64,
    secret: &impl Fn(&str) -> Option<String>,
) -> Result<Verified, AuthError> {
    let malformed = AuthError::HeaderMalformed;
    let rest = match authorization.split_once(|c: char| c.is_ascii_whitespace()) {
        Some((ALGORITHM, rest)) => rest,
        Some(("AWS4-ECDSA-P256-SHA256", _)) => {
            return Err(AuthError::Unsupported(
                "Signature Version 4A is not supported",
            ));
        }
        _ if authorization.starts_with("AWS ") => {
            return Err(AuthError::Unsupported(
                "the request is using the wrong signature version; use AWS4-HMAC-SHA256",
            ));
        }
        _ => return Err(malformed("unknown algorithm")),
    };
    let (mut credential, mut signed, mut signature) = (None, None, None);
    for part in rest.split(',') {
        let part = part.trim();
        match part.split_once('=') {
            Some(("Credential", v)) => credential = Some(v),
            Some(("SignedHeaders", v)) => signed = Some(v),
            Some(("Signature", v)) => signature = Some(v),
            _ => return Err(malformed("unknown component")),
        }
    }
    let claim = claim(
        credential.ok_or(malformed("no Credential"))?,
        signed.ok_or(malformed("no SignedHeaders"))?,
        signature.ok_or(malformed("no Signature"))?,
        malformed,
    )?;
    let timestamp = header(request.headers, "x-amz-date")
        .or_else(|| header(request.headers, "date"))
        .ok_or(malformed("no x-amz-date"))?;
    let time = crate::time::parse_amz_date(timestamp)
        .ok_or(malformed("x-amz-date is not YYYYMMDDTHHMMSSZ"))?;
    check_scope(&claim, timestamp, region, malformed)?;
    if now.abs_diff(time) > MAX_SKEW_SECS.unsigned_abs() {
        return Err(AuthError::TooSkewed);
    }
    check_signed_amz_headers(
        request,
        &claim.signed_headers,
        "x-amz-content-sha256",
        malformed,
    )?;
    let payload = header(request.headers, "x-amz-content-sha256")
        .ok_or(malformed("no x-amz-content-sha256"))?;
    let body = body_of(payload).ok_or(malformed("unknown x-amz-content-sha256"))?;
    let canonical = canonical_request(request, &claim.signed_headers, payload, None)?;
    let key = check_signature(&claim, timestamp, &canonical, secret)?;
    let chain = match body {
        Body::SignedChunks { .. } => Some(Chain {
            key,
            timestamp: timestamp.to_owned(),
            scope: scope(claim.date, claim.region, claim.service),
            previous: claim.signature,
        }),
        _ => None,
    };
    Ok(Verified {
        access_key: claim.access_key.to_owned(),
        body,
        chain,
    })
}

fn verify_presigned(
    request: &Request<'_>,
    region: &str,
    now: i64,
    secret: &impl Fn(&str) -> Option<String>,
) -> Result<Verified, AuthError> {
    let malformed = AuthError::QueryMalformed;
    let params = query_pairs(request.query).ok_or(malformed("undecodable query"))?;
    let get = |name: &str| {
        params
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    };
    match get("X-Amz-Algorithm") {
        Some(ALGORITHM) => {}
        Some(_) => return Err(malformed("unknown X-Amz-Algorithm")),
        None => return Err(malformed("no X-Amz-Algorithm")),
    }
    let claim = claim(
        get("X-Amz-Credential").ok_or(malformed("no X-Amz-Credential"))?,
        get("X-Amz-SignedHeaders").ok_or(malformed("no X-Amz-SignedHeaders"))?,
        get("X-Amz-Signature").ok_or(malformed("no X-Amz-Signature"))?,
        malformed,
    )?;
    let timestamp = get("X-Amz-Date").ok_or(malformed("no X-Amz-Date"))?;
    let time = crate::time::parse_amz_date(timestamp)
        .ok_or(malformed("X-Amz-Date is not YYYYMMDDTHHMMSSZ"))?;
    check_scope(&claim, timestamp, region, malformed)?;
    let expires: i64 = get("X-Amz-Expires")
        .ok_or(malformed("no X-Amz-Expires"))?
        .parse()
        .map_err(|_| malformed("X-Amz-Expires is not a number"))?;
    if !(1..=MAX_EXPIRES_SECS).contains(&expires) {
        return Err(malformed("X-Amz-Expires must be between 1 and 604800"));
    }
    // A URL signed in the future is held to the same skew as a signed header (05 §1.10 item
    // 4: AWS does not document its tolerance).
    if time.saturating_sub(now) > MAX_SKEW_SECS {
        return Err(AuthError::TooSkewed);
    }
    if now >= time.saturating_add(expires) {
        return Err(AuthError::Expired);
    }
    check_signed_amz_headers(request, &claim.signed_headers, "", malformed)?;
    let canonical = canonical_request(
        request,
        &claim.signed_headers,
        "UNSIGNED-PAYLOAD",
        Some("X-Amz-Signature"),
    )?;
    check_signature(&claim, timestamp, &canonical, secret)?;
    Ok(Verified {
        access_key: claim.access_key.to_owned(),
        body: Body::Unsigned,
        chain: None,
    })
}

fn claim<'a>(
    credential: &'a str,
    signed: &'a str,
    signature: &'a str,
    malformed: fn(&'static str) -> AuthError,
) -> Result<Claim<'a>, AuthError> {
    // access-key/YYYYMMDD/region/service/aws4_request, split from the right.
    let mut parts = credential.rsplitn(5, '/');
    let terminator = parts.next().ok_or(malformed("short Credential"))?;
    let service = parts.next().ok_or(malformed("short Credential"))?;
    let region = parts.next().ok_or(malformed("short Credential"))?;
    let date = parts.next().ok_or(malformed("short Credential"))?;
    let access_key = parts.next().ok_or(malformed("short Credential"))?;
    if terminator != "aws4_request" || access_key.is_empty() {
        return Err(malformed("Credential does not end in aws4_request"));
    }
    let signed_headers: Vec<&str> = signed.split(';').collect();
    if signed_headers.iter().any(|h| {
        h.is_empty()
            || h.bytes()
                .any(|b| b.is_ascii_uppercase() || b.is_ascii_whitespace())
    }) {
        return Err(malformed("SignedHeaders are not lowercase names"));
    }
    if !signed_headers.contains(&"host") {
        return Err(malformed("host is not signed"));
    }
    let signature = unhex(signature).ok_or(malformed("Signature is not 64 hex digits"))?;
    Ok(Claim {
        access_key,
        date,
        region,
        service,
        signed_headers,
        signature,
    })
}

/// The scope's date must be the timestamp's date, and the scope must be this endpoint's.
fn check_scope(
    claim: &Claim<'_>,
    timestamp: &str,
    region: &str,
    malformed: fn(&'static str) -> AuthError,
) -> Result<(), AuthError> {
    if timestamp.get(..8) != Some(claim.date) {
        return Err(malformed(
            "the Credential's date does not match the request's date",
        ));
    }
    if claim.region != region {
        return Err(malformed("the Credential's region is not this endpoint's"));
    }
    if claim.service != "s3" {
        return Err(malformed("the Credential's service is not s3"));
    }
    Ok(())
}

/// S3 requires every `x-amz-*` header of a request to be signed, `x-amz-content-sha256`
/// excepted (05 §1.2.4); an unsigned one could change what the request does.
fn check_signed_amz_headers(
    request: &Request<'_>,
    signed: &[&str],
    exempt: &str,
    malformed: fn(&'static str) -> AuthError,
) -> Result<(), AuthError> {
    for (name, _) in request.headers {
        let lower = name.to_ascii_lowercase();
        if lower.starts_with("x-amz-") && lower != exempt && !signed.contains(&lower.as_str()) {
            return Err(malformed("an x-amz- header is not signed"));
        }
    }
    Ok(())
}

fn check_signature(
    claim: &Claim<'_>,
    timestamp: &str,
    canonical: &str,
    secret: &impl Fn(&str) -> Option<String>,
) -> Result<[u8; 32], AuthError> {
    let secret = secret(claim.access_key).ok_or(AuthError::UnknownKey)?;
    let key = signing_key(&secret, claim.date, claim.region, claim.service);
    let sts = string_to_sign(
        timestamp,
        &scope(claim.date, claim.region, claim.service),
        canonical,
    );
    HmacSha256::new_from_slice(&key)
        .map_err(|_| AuthError::Mismatch)?
        .chain_update(sts.as_bytes())
        .verify_slice(&claim.signature)
        .map_err(|_| AuthError::Mismatch)?;
    Ok(key)
}

/// The canonical request (05 §1.2): method, URI, query, headers, signed headers, payload.
/// `skip` names a query parameter left out, the signature of a presigned URL.
pub fn canonical_request(
    request: &Request<'_>,
    signed_headers: &[&str],
    payload: &str,
    skip: Option<&str>,
) -> Result<String, AuthError> {
    let path = percent_decode(request.path.as_bytes()).ok_or(AuthError::Mismatch)?;
    let mut uri = uri_encode(&path, false);
    if uri.is_empty() {
        uri.push('/');
    }
    let mut pairs: Vec<(String, String)> = query_pairs(request.query)
        .ok_or(AuthError::Mismatch)?
        .into_iter()
        .filter(|(n, _)| Some(n.as_str()) != skip)
        .map(|(n, v)| {
            (
                uri_encode(n.as_bytes(), true),
                uri_encode(v.as_bytes(), true),
            )
        })
        .collect();
    pairs.sort();
    let query: Vec<String> = pairs.iter().map(|(n, v)| format!("{n}={v}")).collect();
    let mut headers = String::new();
    for name in signed_headers {
        let values: Vec<String> = request
            .headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| collapse(v))
            .collect();
        if values.is_empty() {
            return Err(AuthError::Mismatch);
        }
        headers.push_str(name);
        headers.push(':');
        headers.push_str(&values.join(","));
        headers.push('\n');
    }
    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        uri,
        query.join("&"),
        headers,
        signed_headers.join(";"),
        payload
    ))
}

pub fn string_to_sign(timestamp: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "{ALGORITHM}\n{timestamp}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()).into())
    )
}

pub fn scope(date: &str, region: &str, service: &str) -> String {
    format!("{date}/{region}/{service}/aws4_request")
}

/// `SigningKey = HMAC(HMAC(HMAC(HMAC("AWS4" + secret, date), region), service), "aws4_request")`.
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let mut first = b"AWS4".to_vec();
    first.extend_from_slice(secret.as_bytes());
    let date_key = hmac(&first, date.as_bytes());
    let region_key = hmac(&date_key, region.as_bytes());
    let service_key = hmac(&region_key, service.as_bytes());
    hmac(&service_key, b"aws4_request")
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    // HMAC accepts a key of any length; the error arm is unreachable for HMAC-SHA256, and a
    // key of zeros would only produce a signature that matches nothing.
    HmacSha256::new_from_slice(key)
        .map(|m| m.chain_update(data).finalize().into_bytes().into())
        .unwrap_or([0u8; 32])
}

/// Signs `canonical` as a client would, for tests and mantle's own clients.
pub fn sign(secret: &str, timestamp: &str, date: &str, region: &str, canonical: &str) -> [u8; 32] {
    let key = signing_key(secret, date, region, "s3");
    hmac(
        &key,
        string_to_sign(timestamp, &scope(date, region, "s3"), canonical).as_bytes(),
    )
}

/// The chain a client starts with its seed signature, for tests and mantle's own clients.
pub fn client_chain(
    secret: &str,
    timestamp: &str,
    date: &str,
    region: &str,
    seed: [u8; 32],
) -> Chain {
    Chain {
        key: signing_key(secret, date, region, "s3"),
        timestamp: timestamp.to_owned(),
        scope: scope(date, region, "s3"),
        previous: seed,
    }
}

fn body_of(payload: &str) -> Option<Body> {
    Some(match payload {
        "UNSIGNED-PAYLOAD" => Body::Unsigned,
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => Body::SignedChunks { trailer: false },
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => Body::SignedChunks { trailer: true },
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => Body::UnsignedChunks,
        hex if hex.len() == 64 => Body::Sha256(unhex(hex)?),
        _ => return None,
    })
}

fn header<'a>(headers: &[(&str, &'a str)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| *v)
}

fn query_has(query: &str, name: &str) -> bool {
    query
        .split('&')
        .any(|p| p.split('=').next().is_some_and(|n| n == name))
}

/// The query's parameters, decoded; a parameter without `=` has the empty value.
fn query_pairs(query: &str) -> Option<Vec<(String, String)>> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (n, v) = p.split_once('=').unwrap_or((p, ""));
            let n = String::from_utf8(percent_decode(n.as_bytes())?).ok()?;
            let v = String::from_utf8(percent_decode(v.as_bytes())?).ok()?;
            Some((n, v))
        })
        .collect()
}

/// Trims a header value and collapses runs of spaces into one (05 §1.2.4).
fn collapse(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// S3's `UriEncode`: every byte but `A-Z a-z 0-9 - . _ ~` becomes `%XX` in uppercase hex, and
/// `/` too unless `slash` is false (05 §1.2.1).
pub fn uri_encode(bytes: &[u8], slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~');
        if keep || (b == b'/' && !slash) {
            out.push(char::from(b));
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
    out
}

/// Decodes `%XX` escapes; `+` stays `+`, as RFC 3986 reads it. `None` for a broken escape.
fn percent_decode(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = bytes.iter();
    while let Some(&b) = i.next() {
        if b == b'%' {
            let hi = hex_digit(*i.next()?)?;
            let lo = hex_digit(*i.next()?)?;
            out.push((hi << 4) | lo);
        } else {
            out.push(b);
        }
    }
    Some(out)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => b.checked_sub(b'0'),
        b'a'..=b'f' => b.checked_sub(b'a').map(|d| d.saturating_add(10)),
        b'A'..=b'F' => b.checked_sub(b'A').map(|d| d.saturating_add(10)),
        _ => None,
    }
}

pub fn hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for &b in bytes {
        out.push(char::from(
            HEX.get(usize::from(b >> 4)).copied().unwrap_or(b'0'),
        ));
        out.push(char::from(
            HEX.get(usize::from(b & 15)).copied().unwrap_or(b'0'),
        ));
    }
    out
}

/// 64 lowercase hex digits as 32 bytes.
pub fn unhex(text: &str) -> Option<[u8; 32]> {
    let bytes = text.as_bytes();
    if bytes.len() != 64 || bytes.iter().any(u8::is_ascii_uppercase) {
        return None;
    }
    let mut out = [0u8; 32];
    for (slot, pair) in out.iter_mut().zip(bytes.chunks(2)) {
        let hi = hex_digit(*pair.first()?)?;
        let lo = hex_digit(*pair.get(1)?)?;
        *slot = (hi << 4) | lo;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const WHEN: &str = "20130524T000000Z";

    fn secret(key: &str) -> Option<String> {
        (key == KEY).then(|| SECRET.to_owned())
    }

    fn now() -> i64 {
        crate::time::parse_amz_date(WHEN).unwrap()
    }

    fn auth(signed: &str, signature: &str) -> String {
        format!(
            "AWS4-HMAC-SHA256 Credential={KEY}/20130524/us-east-1/s3/aws4_request,SignedHeaders={signed},Signature={signature}"
        )
    }

    /// AWS's four header-signed examples (S3 developer guide, "Signature Calculation:
    /// Transfer Payload in a Single Chunk"; 05 §1.5): the canonical request's hash and the
    /// signature must match AWS's, and the request must verify.
    #[test]
    fn aws_header_examples_verify() {
        let get_object = [
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", WHEN),
        ];
        let put_object = [
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("x-amz-date", WHEN),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            (
                "x-amz-content-sha256",
                "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            ),
        ];
        let bucket = [
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-date", WHEN),
            ("x-amz-content-sha256", EMPTY_SHA256),
        ];
        /// Method, path, query, headers, signed headers, canonical-request hash, signature.
        type Case<'a> = (
            &'a str,
            &'a str,
            &'a str,
            &'a [(&'a str, &'a str)],
            &'a str,
            &'a str,
            &'a str,
        );
        let cases: [Case<'_>; 4] = [
            (
                "GET",
                "/test.txt",
                "",
                &get_object,
                "host;range;x-amz-content-sha256;x-amz-date",
                "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972",
                "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            ),
            (
                "PUT",
                "/test$file.text",
                "",
                &put_object,
                "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
                "9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d",
                "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
            ),
            (
                "GET",
                "/",
                "lifecycle",
                &bucket,
                "host;x-amz-content-sha256;x-amz-date",
                "9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca",
                "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
            ),
            (
                "GET",
                "/",
                "max-keys=2&prefix=J",
                &bucket,
                "host;x-amz-content-sha256;x-amz-date",
                "df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7",
                "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
            ),
        ];
        for (method, path, query, headers, signed, hash, signature) in cases {
            let request = Request {
                method,
                path,
                query,
                headers,
            };
            let signed_list: Vec<&str> = signed.split(';').collect();
            let payload = headers
                .iter()
                .find(|(n, _)| *n == "x-amz-content-sha256")
                .unwrap()
                .1;
            let canonical = canonical_request(&request, &signed_list, payload, None).unwrap();
            assert_eq!(
                hex(&Sha256::digest(canonical.as_bytes()).into()),
                hash,
                "{method} {path}?{query}"
            );
            let authorization = auth(signed, signature);
            let mut with_auth = headers.to_vec();
            with_auth.push(("Authorization", &authorization));
            let request = Request {
                headers: &with_auth,
                ..request
            };
            let verified = verify(&request, "us-east-1", now(), secret).unwrap();
            assert_eq!(verified.access_key, KEY);
        }
    }

    #[test]
    fn a_changed_request_does_not_verify() {
        let authorization = auth(
            "host;range;x-amz-content-sha256;x-amz-date",
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        );
        let headers = [
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Range", "bytes=0-10"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", WHEN),
            ("Authorization", authorization.as_str()),
        ];
        let request = Request {
            method: "GET",
            path: "/test.txt",
            query: "",
            headers: &headers,
        };
        assert_eq!(
            verify(&request, "us-east-1", now(), secret),
            Err(AuthError::Mismatch)
        );
        assert_eq!(
            verify(&request, "us-east-1", now() + MAX_SKEW_SECS + 1, secret),
            Err(AuthError::TooSkewed)
        );
        assert_eq!(
            verify(&request, "us-east-1", now(), |_| None),
            Err(AuthError::UnknownKey)
        );
        assert!(matches!(
            verify(&request, "eu-west-1", now(), secret),
            Err(AuthError::HeaderMalformed(_))
        ));
    }

    #[test]
    fn an_unsigned_amz_header_is_refused() {
        let authorization = auth(
            "host;range;x-amz-content-sha256;x-amz-date",
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        );
        let headers = [
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", WHEN),
            ("x-amz-copy-source", "otherbucket/secret"),
            ("Authorization", authorization.as_str()),
        ];
        let request = Request {
            method: "GET",
            path: "/test.txt",
            query: "",
            headers: &headers,
        };
        assert!(matches!(
            verify(&request, "us-east-1", now(), secret),
            Err(AuthError::HeaderMalformed(_))
        ));
    }

    /// AWS's presigned GET (S3 developer guide, "Using Query Parameters"; 05 §1.5, §1.7).
    #[test]
    fn aws_presigned_example_verifies_until_it_expires() {
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";
        let headers = [("Host", "examplebucket.s3.amazonaws.com")];
        let request = Request {
            method: "GET",
            path: "/test.txt",
            query,
            headers: &headers,
        };
        let canonical = canonical_request(
            &request,
            &["host"],
            "UNSIGNED-PAYLOAD",
            Some("X-Amz-Signature"),
        )
        .unwrap();
        assert_eq!(
            hex(&Sha256::digest(canonical.as_bytes()).into()),
            "3bfa292879f6447bbcda7001decf97f4a54dc650c8942174ae0a9121cf58ad04"
        );
        let verified = verify(&request, "us-east-1", now() + 3600, secret).unwrap();
        assert_eq!(verified.body, Body::Unsigned);
        assert_eq!(
            verify(&request, "us-east-1", now() + 86_400, secret),
            Err(AuthError::Expired)
        );
        let too_long = query.replace("X-Amz-Expires=86400", "X-Amz-Expires=604801");
        let request = Request {
            query: &too_long,
            ..request
        };
        assert!(matches!(
            verify(&request, "us-east-1", now(), secret),
            Err(AuthError::QueryMalformed(_))
        ));
    }

    /// AWS's chunked upload: the seed signature and the chain of three chunk signatures over
    /// 66560 bytes of `a` in chunks of 65536, 1024 and 0 bytes (05 §1.8).
    #[test]
    fn aws_chunked_example_chains() {
        let headers = [
            ("Host", "s3.amazonaws.com"),
            ("x-amz-date", WHEN),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            ("Content-Encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", "66560"),
            ("Content-Length", "66824"),
        ];
        let signed = "content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class";
        let request = Request {
            method: "PUT",
            path: "/examplebucket/chunkObject.txt",
            query: "",
            headers: &headers,
        };
        let list: Vec<&str> = signed.split(';').collect();
        let canonical =
            canonical_request(&request, &list, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", None).unwrap();
        assert_eq!(
            hex(&Sha256::digest(canonical.as_bytes()).into()),
            "cee3fed04b70f867d036f722359b0b1f2f0e5dc0efadbc082b76c4c60e316455"
        );
        let authorization = auth(
            signed,
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
        );
        let mut with_auth = headers.to_vec();
        with_auth.push(("Authorization", &authorization));
        let request = Request {
            headers: &with_auth,
            ..request
        };
        let verified = verify(&request, "us-east-1", now(), secret).unwrap();
        assert_eq!(verified.body, Body::SignedChunks { trailer: false });
        let mut chain = verified.chain.unwrap();
        for (len, signature) in [
            (
                65536,
                "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
            ),
            (
                1024,
                "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
            ),
            (
                0,
                "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
            ),
        ] {
            let data = vec![b'a'; len];
            let sha: [u8; 32] = Sha256::digest(&data).into();
            chain
                .verify_chunk(&sha, &unhex(signature).unwrap())
                .unwrap();
        }
    }

    /// AWS's chunked upload with a trailing CRC-32C: the seed, the zero chunk's signature
    /// and the trailer's signature (05 §1.5, §1.9).
    #[test]
    fn aws_trailer_example_chains() {
        let headers = [
            ("Host", "s3.amazonaws.com"),
            ("x-amz-date", WHEN),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            (
                "x-amz-content-sha256",
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            ),
            ("Content-Encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", "66560"),
            ("x-amz-trailer", "x-amz-checksum-crc32c"),
        ];
        let signed = "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer";
        let request = Request {
            method: "PUT",
            path: "/examplebucket/chunkObject.txt",
            query: "",
            headers: &headers,
        };
        let list: Vec<&str> = signed.split(';').collect();
        let canonical = canonical_request(
            &request,
            &list,
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            None,
        )
        .unwrap();
        assert_eq!(
            hex(&Sha256::digest(canonical.as_bytes()).into()),
            "44d48b8c2f70eae815a0198cc73d7a546a73a93359c070abbaa5e6c7de112559"
        );
        let seed = sign(SECRET, WHEN, "20130524", "us-east-1", &canonical);
        assert_eq!(
            hex(&seed),
            "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"
        );
        let mut chain = client_chain(SECRET, WHEN, "20130524", "us-east-1", seed);
        for len in [65536, 1024] {
            chain.sign_chunk(&Sha256::digest(vec![b'a'; len]).into());
        }
        let zero = chain.sign_chunk(&Sha256::digest([]).into());
        assert_eq!(
            hex(&zero),
            "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"
        );
        let trailer = chain.sign_trailer(b"x-amz-checksum-crc32c:sOO8/Q==\n");
        assert_eq!(
            hex(&trailer),
            "d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435"
        );
    }

    #[test]
    fn uri_encoding_follows_s3() {
        assert_eq!(uri_encode(b"test$file.text", true), "test%24file.text");
        assert_eq!(uri_encode(b"a b/c~d", false), "a%20b/c~d");
        assert_eq!(uri_encode(b"a b/c", true), "a%20b%2Fc");
        assert_eq!(percent_decode(b"a%2Fb+c"), Some(b"a/b+c".to_vec()));
        assert_eq!(percent_decode(b"%zz"), None);
    }
}
