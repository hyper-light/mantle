//! AWS Signature Version 4 on S3 requests: the canonical request, the signing key, and
//! verification of the `Authorization` header and of presigned URLs (docs/research/05 §1).
//!
//! The canonical request is rebuilt from the request's decoded parts, re-encoded with S3's
//! `UriEncode` rules, rather than from the bytes as sent: clients differ in which optional
//! characters they escape, and the signed form is always the `UriEncode` form (05 §1.2.2).
//! S3 paths are encoded once and never normalized, so `a//b` stays `a//b`.

use crate::crypto::{self, CryptoError};

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
    #[error("the signature could not be checked: {0}")]
    Internal(#[from] CryptoError),
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
            Self::Internal(_) => ("InternalError", 500),
        }
    }
}

/// The chain of chunk and trailer signatures that starts at a request's seed signature
/// (S3 developer guide, Transfer Payload in Multiple Chunks; 05 §1.8–§1.9).
///
/// Each chunk's string to sign is written into one buffer the chain keeps, made once at its
/// bound: its first lines, the algorithm's, the request's time and its scope, are the same for
/// every chunk, and the rest is at most three digests in hex.
#[derive(Clone, PartialEq, Eq)]
pub struct Chain {
    key: [u8; 32],
    /// `AWS4-HMAC-SHA256-PAYLOAD\n<timestamp>\n<scope>\n`, then the string's rest.
    string: Vec<u8>,
    /// The bytes of the lines every string to sign shares.
    lines: usize,
    previous: [u8; 32],
}

/// The algorithm line of a chunk's string to sign, and a trailer's: the same length, so either
/// is written over the other.
const PAYLOAD: &[u8; 24] = b"AWS4-HMAC-SHA256-PAYLOAD";
const TRAILER: &[u8; 24] = b"AWS4-HMAC-SHA256-TRAILER";

/// The most a chunk's string adds to the shared lines: the previous signature, the empty
/// string's hash and the chunk's, in hex, each ended.
const CHAIN_REST: usize = 3 * 64 + 2;

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The signing key is a credential.
        f.debug_struct("Chain").finish_non_exhaustive()
    }
}

impl Chain {
    fn new(key: [u8; 32], timestamp: &str, scope: Scope<'_>, previous: [u8; 32]) -> Self {
        let lines = PAYLOAD
            .len()
            .saturating_add(1)
            .saturating_add(timestamp.len())
            .saturating_add(1)
            .saturating_add(scope.len())
            .saturating_add(1);
        let mut string = Vec::with_capacity(lines.saturating_add(CHAIN_REST));
        string.extend_from_slice(PAYLOAD);
        string.push(b'\n');
        string.extend_from_slice(timestamp.as_bytes());
        string.push(b'\n');
        scope.put(&mut string);
        string.push(b'\n');
        Self {
            key,
            string,
            lines,
            previous,
        }
    }

    /// The string to sign of the next chunk, whose data hashes to `sha256`, or of the trailer
    /// whose canonical form hashes to it, and its signature.
    fn signature(&mut self, kind: &[u8; 24], sha256: &[u8; 32]) -> Result<[u8; 32], AuthError> {
        self.string.truncate(self.lines);
        if let Some(algorithm) = self.string.get_mut(..kind.len()) {
            algorithm.copy_from_slice(kind);
        }
        self.string.extend_from_slice(&hex_digits(&self.previous));
        self.string.push(b'\n');
        if kind == PAYLOAD {
            self.string.extend_from_slice(EMPTY_SHA256.as_bytes());
            self.string.push(b'\n');
        }
        self.string.extend_from_slice(&hex_digits(sha256));
        Ok(crypto::hmac_sha256(&self.key, &self.string)?)
    }

    /// Verifies the signature of the next chunk, whose data hashes to `sha256`.
    pub fn verify_chunk(
        &mut self,
        sha256: &[u8; 32],
        signature: &[u8; 32],
    ) -> Result<(), AuthError> {
        let expected = self.signature(PAYLOAD, sha256)?;
        self.advance(expected, signature)
    }

    /// Verifies the trailer's signature; `canonical` is each trailing header as
    /// `name:value\n`, in order (05 §1.9).
    pub fn verify_trailer(
        &mut self,
        canonical: &[u8],
        signature: &[u8; 32],
    ) -> Result<(), AuthError> {
        let expected = self.signature(TRAILER, &crypto::sha256(canonical)?)?;
        self.advance(expected, signature)
    }

    fn advance(&mut self, expected: [u8; 32], signature: &[u8; 32]) -> Result<(), AuthError> {
        if !crypto::equal(&expected, signature)? {
            return Err(AuthError::Mismatch);
        }
        self.previous = *signature;
        Ok(())
    }

    /// Signs the next chunk as a client would: for tests and for mantle's own clients.
    pub fn sign_chunk(&mut self, sha256: &[u8; 32]) -> Result<[u8; 32], AuthError> {
        let signature = self.signature(PAYLOAD, sha256)?;
        self.previous = signature;
        Ok(signature)
    }

    /// Signs the trailer as a client would.
    pub fn sign_trailer(&mut self, canonical: &[u8]) -> Result<[u8; 32], AuthError> {
        let signature = self.signature(TRAILER, &crypto::sha256(canonical)?)?;
        self.previous = signature;
        Ok(signature)
    }
}

/// A credential's scope, `date/region/service/aws4_request`, written where it is needed.
#[derive(Clone, Copy)]
struct Scope<'a> {
    date: &'a str,
    region: &'a str,
    service: &'a str,
}

impl Scope<'_> {
    const END: &'static str = "aws4_request";

    fn len(&self) -> usize {
        self.date
            .len()
            .saturating_add(self.region.len())
            .saturating_add(self.service.len())
            .saturating_add(Self::END.len())
            .saturating_add(3)
    }

    fn put(&self, out: &mut Vec<u8>) {
        for (part, end) in [
            (self.date, "/"),
            (self.region, "/"),
            (self.service, "/"),
            (Self::END, ""),
        ] {
            out.extend_from_slice(part.as_bytes());
            out.extend_from_slice(end.as_bytes());
        }
    }
}

/// Verifies a request signed in its `Authorization` header or as a presigned URL. `region`
/// is the region this endpoint serves; `secret` finds an access key's secret; `now` is Unix
/// seconds.
pub fn verify<S: AsRef<str>>(
    request: &Request<'_>,
    region: &str,
    now: i64,
    secret: impl Fn(&str) -> Option<S>,
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
    /// The signed headers' names, `;`-separated, each checked lowercase and not empty.
    signed_headers: &'a str,
    signature: [u8; 32],
}

impl Claim<'_> {
    fn scope(&self) -> Scope<'_> {
        Scope {
            date: self.date,
            region: self.region,
            service: self.service,
        }
    }
}

fn verify_header<S: AsRef<str>>(
    request: &Request<'_>,
    authorization: &str,
    region: &str,
    now: i64,
    secret: &impl Fn(&str) -> Option<S>,
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
        claim.signed_headers,
        "x-amz-content-sha256",
        malformed,
    )?;
    let payload = header(request.headers, "x-amz-content-sha256")
        .ok_or(malformed("no x-amz-content-sha256"))?;
    let body = body_of(payload).ok_or(malformed("unknown x-amz-content-sha256"))?;
    let canonical = canonical_hash(request, claim.signed_headers, payload, None)?;
    let key = check_signature(&claim, timestamp, &canonical, secret)?;
    let chain = match body {
        Body::SignedChunks { .. } => {
            Some(Chain::new(key, timestamp, claim.scope(), claim.signature))
        }
        _ => None,
    };
    Ok(Verified {
        access_key: claim.access_key.to_owned(),
        body,
        chain,
    })
}

fn verify_presigned<S: AsRef<str>>(
    request: &Request<'_>,
    region: &str,
    now: i64,
    secret: &impl Fn(&str) -> Option<S>,
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
    check_signed_amz_headers(request, claim.signed_headers, "", malformed)?;
    let canonical = canonical_hash(
        request,
        claim.signed_headers,
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
    let signed_headers = signed;
    if signed_headers.split(';').any(|h| {
        h.is_empty()
            || h.bytes()
                .any(|b| b.is_ascii_uppercase() || b.is_ascii_whitespace())
    }) {
        return Err(malformed("SignedHeaders are not lowercase names"));
    }
    if !signed_headers.split(';').any(|h| h == "host") {
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
    signed: &str,
    exempt: &str,
    malformed: fn(&'static str) -> AuthError,
) -> Result<(), AuthError> {
    // Names compared case-insensitively against the signed names, which are lowercase: as the
    // name lowercased would compare exactly.
    for (name, _) in request.headers {
        let amz = name
            .get(..6)
            .is_some_and(|start| start.eq_ignore_ascii_case("x-amz-"));
        if amz
            && !name.eq_ignore_ascii_case(exempt)
            && !signed.split(';').any(|s| name.eq_ignore_ascii_case(s))
        {
            return Err(malformed("an x-amz- header is not signed"));
        }
    }
    Ok(())
}

fn check_signature<S: AsRef<str>>(
    claim: &Claim<'_>,
    timestamp: &str,
    canonical: &[u8; 32],
    secret: &impl Fn(&str) -> Option<S>,
) -> Result<[u8; 32], AuthError> {
    let secret = secret(claim.access_key).ok_or(AuthError::UnknownKey)?;
    let key = signing_key(secret.as_ref(), claim.date, claim.region, claim.service)?;
    let sts = signed_string(timestamp, claim.scope(), canonical);
    let expected = crypto::hmac_sha256(&key, &sts)?;
    if !crypto::equal(&expected, &claim.signature)? {
        return Err(AuthError::Mismatch);
    }
    Ok(key)
}

/// The string to sign (05 §1.3): the algorithm, the request's time, its scope and the canonical
/// request's hash, in one buffer made at their length.
fn signed_string(timestamp: &str, scope: Scope<'_>, canonical: &[u8; 32]) -> Vec<u8> {
    let len = ALGORITHM
        .len()
        .saturating_add(timestamp.len())
        .saturating_add(scope.len())
        .saturating_add(64 + 3);
    let mut sts = Vec::with_capacity(len);
    sts.extend_from_slice(ALGORITHM.as_bytes());
    sts.push(b'\n');
    sts.extend_from_slice(timestamp.as_bytes());
    sts.push(b'\n');
    scope.put(&mut sts);
    sts.push(b'\n');
    sts.extend_from_slice(&hex_digits(canonical));
    sts
}

/// The canonical request (05 §1.2): method, URI, query, headers, signed headers, payload.
/// `skip` names a query parameter left out, the signature of a presigned URL.
pub fn canonical_request(
    request: &Request<'_>,
    signed_headers: &[&str],
    payload: &str,
    skip: Option<&str>,
) -> Result<String, AuthError> {
    let signed = signed_headers.join(";");
    let mut text = Vec::new();
    write_canonical(&mut Text(&mut text), request, &signed, payload, skip)?;
    String::from_utf8(text).map_err(|_| AuthError::Mismatch)
}

/// The canonical request's SHA-256, written into the digest as it is formed: no buffer holds
/// it. `signed` is the signed headers' names as the claim lists them, `;`-separated.
fn canonical_hash(
    request: &Request<'_>,
    signed: &str,
    payload: &str,
    skip: Option<&str>,
) -> Result<[u8; 32], AuthError> {
    let mut digest = crypto::Digest::new(&crypto::SHA256)?;
    write_canonical(&mut digest, request, signed, payload, skip)?;
    Ok(digest.finish_array()?)
}

/// Where a canonical request is written: a verifier's digest, or a client's text to sign.
trait Canonical {
    fn put(&mut self, bytes: &[u8]) -> Result<(), CryptoError>;
}

impl Canonical for crypto::Digest {
    fn put(&mut self, bytes: &[u8]) -> Result<(), CryptoError> {
        self.update(bytes)
    }
}

struct Text<'a>(&'a mut Vec<u8>);

impl Canonical for Text<'_> {
    fn put(&mut self, bytes: &[u8]) -> Result<(), CryptoError> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

fn write_canonical(
    out: &mut impl Canonical,
    request: &Request<'_>,
    signed: &str,
    payload: &str,
    skip: Option<&str>,
) -> Result<(), AuthError> {
    out.put(request.method.as_bytes())?;
    out.put(b"\n")?;
    if request.path.is_empty() {
        out.put(b"/")?;
    }
    for byte in Decoded::of(request.path) {
        put_encoded(out, byte.ok_or(AuthError::Mismatch)?, false)?;
    }
    out.put(b"\n")?;
    write_query(out, request.query, skip)?;
    out.put(b"\n")?;
    for name in signed.split(';') {
        out.put(name.as_bytes())?;
        out.put(b":")?;
        let mut values = request
            .headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| *v);
        let first = values.next().ok_or(AuthError::Mismatch)?;
        put_collapsed(out, first)?;
        for value in values {
            out.put(b",")?;
            put_collapsed(out, value)?;
        }
        out.put(b"\n")?;
    }
    out.put(b"\n")?;
    out.put(signed.as_bytes())?;
    out.put(b"\n")?;
    out.put(payload.as_bytes())?;
    Ok(())
}

/// The query's parameters, decoded and each re-encoded, sorted by name and then value, `skip`
/// left out (05 §1.2.3). A query's parameters are read where they lie: a list of where each
/// lies is the one buffer, made at their count, and only for a request that has a query.
fn write_query(out: &mut impl Canonical, query: &str, skip: Option<&str>) -> Result<(), AuthError> {
    if query.is_empty() {
        return Ok(());
    }
    let count = query.split('&').filter(|p| !p.is_empty()).count();
    let mut pairs = Vec::with_capacity(count);
    for p in query.split('&').filter(|p| !p.is_empty()) {
        let (n, v) = p.split_once('=').unwrap_or((p, ""));
        // A parameter that does not decode, or decodes to a name or value that is not UTF-8,
        // makes no canonical request.
        if !utf8(n) || !utf8(v) {
            return Err(AuthError::Mismatch);
        }
        if skip.is_some_and(|skip| {
            Decoded::of(n)
                .map(Option::unwrap_or_default)
                .eq(skip.bytes())
        }) {
            continue;
        }
        pairs.push((n, v));
    }
    pairs.sort_by(|(an, av), (bn, bv)| {
        encoded(an)
            .cmp(encoded(bn))
            .then_with(|| encoded(av).cmp(encoded(bv)))
    });
    for (i, (n, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.put(b"&")?;
        }
        for byte in Decoded::of(n) {
            put_encoded(out, byte.unwrap_or_default(), true)?;
        }
        out.put(b"=")?;
        for byte in Decoded::of(v) {
            put_encoded(out, byte.unwrap_or_default(), true)?;
        }
    }
    Ok(())
}

/// A query parameter's name or value as the canonical request writes it, once it decoded.
fn encoded(text: &str) -> impl Iterator<Item = u8> + '_ {
    Decoded::of(text).flat_map(|b| Encoded::of(b.unwrap_or_default(), true))
}

/// Whether `text` percent-decodes, and to UTF-8.
fn utf8(text: &str) -> bool {
    let mut decoded = Decoded::of(text);
    let mut buf = [0u8; 4];
    loop {
        let Some(first) = decoded.next() else {
            return true;
        };
        let Some(first) = first else {
            return false;
        };
        // A sequence's length is its first byte's (RFC 3629 §4); it is checked whole.
        let len = match first {
            0x00..=0x7F => 1,
            0xC0..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF7 => 4,
            _ => return false,
        };
        let Some(sequence) = buf.get_mut(..len) else {
            return false;
        };
        let mut bytes = sequence.iter_mut();
        if let Some(slot) = bytes.next() {
            *slot = first;
        }
        for slot in bytes {
            match decoded.next() {
                Some(Some(b)) => *slot = b,
                _ => return false,
            }
        }
        if std::str::from_utf8(sequence).is_err() {
            return false;
        }
    }
}

/// A text's bytes percent-decoded as they are read: `None` for a broken escape. `+` stays `+`,
/// as RFC 3986 reads it.
struct Decoded<'a> {
    bytes: std::slice::Iter<'a, u8>,
}

impl<'a> Decoded<'a> {
    fn of(text: &'a str) -> Self {
        Self {
            bytes: text.as_bytes().iter(),
        }
    }
}

impl Iterator for Decoded<'_> {
    type Item = Option<u8>;

    fn next(&mut self) -> Option<Option<u8>> {
        let b = *self.bytes.next()?;
        if b != b'%' {
            return Some(Some(b));
        }
        let hi = self.bytes.next().and_then(|b| hex_digit(*b));
        let lo = self.bytes.next().and_then(|b| hex_digit(*b));
        Some(hi.zip(lo).map(|(hi, lo)| (hi << 4) | lo))
    }
}

/// A byte as S3's `UriEncode` writes it: itself, or `%XX` in uppercase hex.
struct Encoded {
    bytes: [u8; 3],
    at: usize,
    len: usize,
}

impl Encoded {
    fn of(b: u8, slash: bool) -> Self {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~');
        if keep || (b == b'/' && !slash) {
            return Self {
                bytes: [b, 0, 0],
                at: 0,
                len: 1,
            };
        }
        let digit = |d: u8| HEX.get(usize::from(d)).copied().unwrap_or(b'0');
        Self {
            bytes: [b'%', digit(b >> 4), digit(b & 15)],
            at: 0,
            len: 3,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..self.len).unwrap_or_default()
    }
}

impl Iterator for Encoded {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        let b = self.as_bytes().get(self.at).copied()?;
        self.at = self.at.saturating_add(1);
        Some(b)
    }
}

fn put_encoded(out: &mut impl Canonical, b: u8, slash: bool) -> Result<(), CryptoError> {
    out.put(Encoded::of(b, slash).as_bytes())
}

/// A header value trimmed, its runs of white space each written as one space (05 §1.2.4).
fn put_collapsed(out: &mut impl Canonical, value: &str) -> Result<(), CryptoError> {
    for (i, word) in value.split_whitespace().enumerate() {
        if i > 0 {
            out.put(b" ")?;
        }
        out.put(word.as_bytes())?;
    }
    Ok(())
}

pub fn string_to_sign(
    timestamp: &str,
    scope: &str,
    canonical_request: &str,
) -> Result<String, AuthError> {
    Ok(format!(
        "{ALGORITHM}\n{timestamp}\n{scope}\n{}",
        hex(&crypto::sha256(canonical_request.as_bytes())?)
    ))
}

pub fn scope(date: &str, region: &str, service: &str) -> String {
    format!("{date}/{region}/{service}/aws4_request")
}

/// `SigningKey = HMAC(HMAC(HMAC(HMAC("AWS4" + secret, date), region), service), "aws4_request")`.
pub fn signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Result<[u8; 32], AuthError> {
    let mut first = Vec::with_capacity(secret.len().saturating_add(4));
    first.extend_from_slice(b"AWS4");
    first.extend_from_slice(secret.as_bytes());
    let date_key = crypto::hmac_sha256(&first, date.as_bytes())?;
    let region_key = crypto::hmac_sha256(&date_key, region.as_bytes())?;
    let service_key = crypto::hmac_sha256(&region_key, service.as_bytes())?;
    Ok(crypto::hmac_sha256(&service_key, b"aws4_request")?)
}

/// Signs `canonical` as a client would, for tests and mantle's own clients.
pub fn sign(
    secret: &str,
    timestamp: &str,
    date: &str,
    region: &str,
    canonical: &str,
) -> Result<[u8; 32], AuthError> {
    let key = signing_key(secret, date, region, "s3")?;
    let sts = string_to_sign(timestamp, &scope(date, region, "s3"), canonical)?;
    Ok(crypto::hmac_sha256(&key, sts.as_bytes())?)
}

/// The chain a client starts with its seed signature, for tests and mantle's own clients.
pub fn client_chain(
    secret: &str,
    timestamp: &str,
    date: &str,
    region: &str,
    seed: [u8; 32],
) -> Result<Chain, AuthError> {
    let scope = Scope {
        date,
        region,
        service: "s3",
    };
    Ok(Chain::new(
        signing_key(secret, date, region, "s3")?,
        timestamp,
        scope,
        seed,
    ))
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

/// S3's `UriEncode`: every byte but `A-Z a-z 0-9 - . _ ~` becomes `%XX` in uppercase hex, and
/// `/` too unless `slash` is false (05 §1.2.1).
pub fn uri_encode(bytes: &[u8], slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::new();
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
pub(crate) fn percent_decode(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
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

/// The value of `digits`, all hex digits and at least one: `None` otherwise or past `u64`.
/// Unlike `from_str_radix`, no leading `+` is taken, which no grammar here allows.
pub(crate) fn hex_value(digits: &[u8]) -> Option<u64> {
    if digits.is_empty() {
        return None;
    }
    digits.iter().try_fold(0u64, |value, &b| {
        value.checked_mul(16)?.checked_add(u64::from(hex_digit(b)?))
    })
}

pub fn hex(bytes: &[u8; 32]) -> String {
    String::from_utf8_lossy(&hex_digits(bytes)).into_owned()
}

/// A digest's 64 lowercase hex digits, where they are needed.
fn hex_digits(bytes: &[u8; 32]) -> [u8; 64] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 64];
    for (pair, &b) in out.as_chunks_mut::<2>().0.iter_mut().zip(bytes) {
        *pair = [
            HEX.get(usize::from(b >> 4)).copied().unwrap_or(b'0'),
            HEX.get(usize::from(b & 15)).copied().unwrap_or(b'0'),
        ];
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
    // Sixty-four bytes are exactly thirty-two pairs, so no remainder is left over.
    for (slot, &[hi, lo]) in out.iter_mut().zip(bytes.as_chunks::<2>().0) {
        *slot = (hex_digit(hi)? << 4) | hex_digit(lo)?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hex values take digits alone: no sign, which `from_str_radix` would take, nothing
    /// empty, and nothing past u64.
    #[test]
    fn hex_values_are_digits_alone() {
        assert_eq!(hex_value(b"0"), Some(0));
        assert_eq!(hex_value(b"fFfF"), Some(0xffff));
        assert_eq!(hex_value(b"ffffffffffffffff"), Some(u64::MAX));
        assert_eq!(hex_value(b"10000000000000000"), None);
        assert_eq!(hex_value(b"+1f"), None);
        assert_eq!(hex_value(b""), None);
        assert_eq!(hex_value(b"1g"), None);
    }

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
                hex(&crypto::sha256(canonical.as_bytes()).unwrap()),
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
            verify(&request, "us-east-1", now(), |_| None::<String>),
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
            hex(&crypto::sha256(canonical.as_bytes()).unwrap()),
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
            hex(&crypto::sha256(canonical.as_bytes()).unwrap()),
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
            let sha = crypto::sha256(&data).unwrap();
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
            hex(&crypto::sha256(canonical.as_bytes()).unwrap()),
            "44d48b8c2f70eae815a0198cc73d7a546a73a93359c070abbaa5e6c7de112559"
        );
        let seed = sign(SECRET, WHEN, "20130524", "us-east-1", &canonical).unwrap();
        assert_eq!(
            hex(&seed),
            "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"
        );
        let mut chain = client_chain(SECRET, WHEN, "20130524", "us-east-1", seed).unwrap();
        for len in [65536, 1024] {
            chain
                .sign_chunk(&crypto::sha256(&vec![b'a'; len]).unwrap())
                .unwrap();
        }
        let zero = chain.sign_chunk(&crypto::sha256(&[]).unwrap()).unwrap();
        assert_eq!(
            hex(&zero),
            "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"
        );
        let trailer = chain
            .sign_trailer(b"x-amz-checksum-crc32c:sOO8/Q==\n")
            .unwrap();
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
