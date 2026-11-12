//! A response's head (RFC 9112 §4, §6.3; RFC 9110 §5, §15), as mantle's own S3 client reads one
//! from the S3 listener: parsed where it lies in its caller's buffer, as spans of it, with what the
//! head decides of the response judged once: how its body ends, and whether the connection
//! persists past it.
//!
//! The grammar is held as strictly as a request's ([`crate::parse`]): one SP after the version and
//! after the status code, no line after the status line that begins with whitespace, and no
//! control octet but HTAB in a field's value, each field line read by the request's own rule. A
//! line ends at CRLF or a lone LF (RFC 9112 §2.2). A head runs at most [`MAX_HEAD`] octets, the
//! request's bound: S3 holds an object's user metadata to 2 KB (research/05 §10.2), and the
//! listener's own fields are a fixed few.

use std::ops::Range;

use memchr::memchr;

use crate::head::field;
use crate::{Framing, MAX_HEAD, Refusal, Version};

/// How a response's body ends (RFC 9112 §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ends {
    /// Its framing: none, Content-Length's octets, or the chunked coding.
    Framed(Framing),
    /// At the connection's close: neither Content-Length nor a Transfer-Encoding ending in
    /// chunked framed it (RFC 9112 §6.3, items 3 and 8).
    Close,
}

/// A response's head, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    len: usize,
    version: Version,
    status: u16,
    reason: Range<usize>,
    /// Each field line's name and value, in order, the value bare of its whitespace.
    fields: Vec<(Range<usize>, Range<usize>)>,
    ends: Ends,
    persists: bool,
}

impl Response {
    /// The octets of the head in its buffer, through the empty line that ends it: the body's
    /// first octet follows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Never: a head holds at least its status line.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn reason<'b>(&self, buf: &'b [u8]) -> &'b [u8] {
        of(buf, &self.reason)
    }

    /// Each field line's name and value, in order.
    pub fn fields<'b>(&'b self, buf: &'b [u8]) -> impl Iterator<Item = (&'b [u8], &'b [u8])> + 'b {
        self.fields
            .iter()
            .map(move |(name, value)| (of(buf, name), of(buf, value)))
    }

    /// The value of the first field named `name`, in any case.
    pub fn field<'b>(&self, buf: &'b [u8], name: &str) -> Option<&'b [u8]> {
        self.fields
            .iter()
            .find(|(n, _)| of(buf, n).eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, value)| of(buf, value))
    }

    /// How its body ends.
    pub fn ends(&self) -> Ends {
        self.ends
    }

    /// Whether the connection may carry another request once this response's body has ended:
    /// HTTP/1.1 unless `Connection: close`, HTTP/1.0 only with `keep-alive`, and never after a
    /// body that ends at the close (RFC 9112 §9.3).
    pub fn persists(&self) -> bool {
        self.persists
    }
}

/// Parses the response head that begins `buf`, the answer to a request whose method was HEAD
/// where `head` is set: the head, once the empty line that ends it is in `buf`; none while it is
/// not, unless `buf` already holds [`MAX_HEAD`] octets, past which no head may run.
pub fn parse_response(buf: &[u8], head: bool) -> Result<Option<Response>, Refusal> {
    let bounded = buf.get(..MAX_HEAD).unwrap_or(buf);
    let full = buf.len() >= MAX_HEAD;
    let more = || {
        if full {
            Err(Refusal::HeadTooLarge)
        } else {
            Ok(None)
        }
    };
    let Some((status_line, mut at)) = line(bounded, 0) else {
        return more();
    };
    let (version, status, reason) = status_line_of(bounded, status_line)?;
    // The field lines counted to the head's end first, so their list is made once, at its size.
    let (mut count, mut end) = (0usize, at);
    loop {
        let Some((found, next)) = line(bounded, end) else {
            return more();
        };
        end = next;
        if found.is_empty() {
            break;
        }
        count = count.saturating_add(1);
    }
    let mut fields = Vec::with_capacity(count);
    while let Some((found, next)) = line(bounded, at)
        && !found.is_empty()
    {
        let (colon, first, last) = field(of(bounded, &found)).map_err(Refusal::Grammar)?;
        let start = found.start;
        fields.push((
            start..start.saturating_add(colon),
            start.saturating_add(first)..start.saturating_add(last),
        ));
        at = next;
    }
    judge(bounded, end, (version, status, reason), fields, head).map(Some)
}

/// The status line's version, code and reason phrase (RFC 9112 §4): `HTTP/1.x SP 3DIGIT SP
/// reason`, the reason possibly empty.
fn status_line_of(buf: &[u8], line: Range<usize>) -> Result<(Version, u16, Range<usize>), Refusal> {
    let bytes = of(buf, &line);
    let bad = Refusal::Grammar("the status line");
    let version = match bytes.get(..8).ok_or(bad)? {
        b"HTTP/1.1" => Version::Http11,
        b"HTTP/1.0" => Version::Http10,
        [b'H', b'T', b'T', b'P', b'/', major, b'.', minor]
            if major.is_ascii_digit() && minor.is_ascii_digit() =>
        {
            return Err(Refusal::Version);
        }
        _ => return Err(bad),
    };
    if bytes.get(8) != Some(&b' ') {
        return Err(bad);
    }
    let code = bytes.get(9..12).ok_or(bad)?;
    if !code.iter().all(u8::is_ascii_digit) {
        return Err(Refusal::Grammar("the status code"));
    }
    let status = code.iter().fold(0u16, |n, d| {
        n.saturating_mul(10)
            .saturating_add(u16::from(d.saturating_sub(b'0')))
    });
    if !(100..=999).contains(&status) {
        return Err(Refusal::Grammar("the status code"));
    }
    if bytes.get(12) != Some(&b' ') {
        return Err(bad);
    }
    let reason = bytes.get(13..).unwrap_or_default();
    // HTAB, SP, VCHAR and obs-text (RFC 9112 §4).
    if !reason
        .iter()
        .all(|b| matches!(b, b'\t' | b' '..=b'~' | 0x80..=0xFF))
    {
        return Err(Refusal::Grammar("the reason phrase"));
    }
    let start = line.start.saturating_add(13).min(line.end);
    Ok((version, status, start..line.end))
}

/// What the head's fields decide of the response: how its body ends and whether its connection
/// persists (RFC 9112 §6.3, §9.3).
fn judge(
    buf: &[u8],
    len: usize,
    (version, status, reason): (Version, u16, Range<usize>),
    fields: Vec<(Range<usize>, Range<usize>)>,
    head: bool,
) -> Result<Response, Refusal> {
    let mut length: Option<u64> = None;
    let mut encoded = false;
    // The last coding listed: whether it is chunked.
    let mut chunked_last = false;
    let (mut close, mut keep_alive) = (false, false);
    for (name, value) in &fields {
        let (name, bytes) = (of(buf, name), of(buf, value));
        if name.eq_ignore_ascii_case(b"content-length") {
            // A list of one repeated value is that value; any other is unrecoverable (RFC 9112
            // §6.3 item 5).
            if bytes
                .split(|b| *b == b',')
                .any(|m| members(m).next().is_none())
            {
                return Err(Refusal::Framing("an empty Content-Length"));
            }
            for member in members(bytes) {
                let n = decimal(member)?;
                if length.is_some_and(|l| l != n) {
                    return Err(Refusal::Framing("Content-Length values that differ"));
                }
                length = Some(n);
            }
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            encoded = true;
            for member in members(bytes) {
                let coding = member
                    .get(..memchr(b';', member).unwrap_or(member.len()))
                    .unwrap_or_default();
                chunked_last = coding.trim_ascii().eq_ignore_ascii_case(b"chunked");
            }
        } else if name.eq_ignore_ascii_case(b"connection") {
            for member in members(bytes) {
                close |= member.eq_ignore_ascii_case(b"close");
                keep_alive |= member.eq_ignore_ascii_case(b"keep-alive");
            }
        }
    }
    // A response to HEAD, a 1xx, a 204 and a 304 end at their head, whatever their fields say
    // (RFC 9112 §6.3 item 1).
    let bodiless = head || (100..200).contains(&status) || status == 204 || status == 304;
    let ends = if bodiless {
        Ends::Framed(Framing::None)
    } else {
        match (encoded, length) {
            // A sender must not send both; a recipient that took either could be smuggled the
            // other's message (RFC 9112 §6.1, §6.3 item 3, §11.2).
            (true, Some(_)) => {
                return Err(Refusal::Framing("Transfer-Encoding with Content-Length"));
            }
            (true, None) if version == Version::Http10 => {
                return Err(Refusal::Framing("Transfer-Encoding on HTTP/1.0"));
            }
            (true, None) if chunked_last => Ends::Framed(Framing::Chunked),
            (true, None) => Ends::Close,
            (false, Some(n)) => Ends::Framed(Framing::Length(n)),
            (false, None) => Ends::Close,
        }
    };
    let persists = ends != Ends::Close && !close && (version == Version::Http11 || keep_alive);
    Ok(Response {
        len,
        version,
        status,
        reason,
        fields,
        ends,
        persists,
    })
}

/// The line that begins at `at`, without its end, and where the next begins; none while the
/// line has not ended in `buf` (RFC 9112 §2.2: CRLF or a lone LF).
fn line(buf: &[u8], at: usize) -> Option<(Range<usize>, usize)> {
    let rest = buf.get(at..)?;
    let lf = at.checked_add(memchr(b'\n', rest)?)?;
    let end = if lf > at && buf.get(lf.checked_sub(1)?) == Some(&b'\r') {
        lf.checked_sub(1)?
    } else {
        lf
    };
    Some((at..end, lf.checked_add(1)?))
}

/// The octets `range` spans in `buf`.
fn of<'b>(buf: &'b [u8], range: &Range<usize>) -> &'b [u8] {
    buf.get(range.clone()).unwrap_or_default()
}

/// The members of a comma-separated list, each bare of its whitespace, empty ones passed (RFC
/// 9110 §5.6.1).
fn members(value: &[u8]) -> impl Iterator<Item = &[u8]> {
    value.split(|b| *b == b',').filter_map(|member| {
        let first = member.iter().position(|b| !matches!(b, b' ' | b'\t'))?;
        let last = member.iter().rposition(|b| !matches!(b, b' ' | b'\t'))?;
        member.get(first..=last)
    })
}

/// `1*DIGIT`, refused past 64 bits (RFC 9110 §8.6).
fn decimal(digits: &[u8]) -> Result<u64, Refusal> {
    if digits.is_empty() {
        return Err(Refusal::Framing("an empty Content-Length"));
    }
    digits.iter().try_fold(0u64, |n, d| {
        if !d.is_ascii_digit() {
            return Err(Refusal::Framing("a Content-Length that is not digits"));
        }
        n.checked_mul(10)
            .and_then(|n| n.checked_add(u64::from(d.saturating_sub(b'0'))))
            .ok_or(Refusal::Framing("a Content-Length past 64 bits"))
    })
}
