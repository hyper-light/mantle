//! A request's head (RFC 9112 §2–§3, §5–§6; RFC 9110 §5): parsed where it lies in its caller's
//! buffer, as spans of it, with what the head decides of the request judged once: how its body is
//! framed, whether it expects `100 Continue`, its host, and whether the connection persists past
//! its response.
//!
//! The grammar is held strictly where leniency smuggles (research/70): one SP between the request
//! line's three parts, no whitespace in its target or before a field's colon, no line after the
//! request line that begins with whitespace (obs-fold included), and no control octet but HTAB in
//! a field's value. A line ends at CRLF or a lone LF, and empty lines before the request line are
//! passed (RFC 9112 §2.2).

use memchr::memchr;

use crate::{MAX_HEAD, Refusal, field_octet, tchar};

/// Where an element lies in the head's buffer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Span {
    start: usize,
    end: usize,
}

impl Span {
    /// The element's octets in `buf`, the buffer the head was parsed from.
    pub fn of<'b>(&self, buf: &'b [u8]) -> &'b [u8] {
        buf.get(self.start..self.end).unwrap_or_default()
    }

    fn within(&self, start: usize, end: usize) -> Self {
        Self {
            start: self.start.saturating_add(start),
            end: self.start.saturating_add(end),
        }
    }
}

/// The request's HTTP version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Http10,
    Http11,
}

/// How a request's body is framed (RFC 9112 §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// No body: neither Content-Length nor Transfer-Encoding.
    None,
    /// Content-Length's octets.
    Length(u64),
    /// The chunked transfer coding.
    Chunked,
}

/// A request's head, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    len: usize,
    method: Span,
    target: Span,
    /// The target's path and query: an origin-form target whole, an absolute-form's past its
    /// authority.
    origin: Span,
    version: Version,
    /// Each field line's name and value, in order, the value bare of its whitespace.
    fields: Vec<(Span, Span)>,
    /// The Host field's value, or an absolute-form target's authority (RFC 9112 §3.2.2).
    host: Option<Span>,
    framing: Framing,
    continues: bool,
    persists: bool,
}

impl Head {
    /// The octets of the head in its buffer, through the empty line that ends it: the request's
    /// body, or the next request, begins after them.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the head has no octets: never, since a request line is never empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The method, case-sensitive (RFC 9112 §3.1).
    pub fn method<'b>(&self, buf: &'b [u8]) -> &'b [u8] {
        self.method.of(buf)
    }

    /// The request-target as sent.
    pub fn target<'b>(&self, buf: &'b [u8]) -> &'b [u8] {
        self.target.of(buf)
    }

    /// The target's path and query, which S3 routes by: an absolute-form target's past its
    /// authority, empty where it has none.
    pub fn origin<'b>(&self, buf: &'b [u8]) -> &'b [u8] {
        self.origin.of(buf)
    }

    pub fn version(&self) -> Version {
        self.version
    }

    /// Each field line's name and value, in order.
    pub fn fields<'b>(&'b self, buf: &'b [u8]) -> impl Iterator<Item = (&'b [u8], &'b [u8])> + 'b {
        self.fields
            .iter()
            .map(move |(name, value)| (name.of(buf), value.of(buf)))
    }

    /// The first field named `name`, case-insensitively (RFC 9110 §5.1).
    pub fn field<'b>(&self, buf: &'b [u8], name: &str) -> Option<&'b [u8]> {
        self.fields
            .iter()
            .find(|(n, _)| n.of(buf).eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, value)| value.of(buf))
    }

    /// The request's host: its Host field's value, or an absolute-form target's authority.
    pub fn host<'b>(&self, buf: &'b [u8]) -> Option<&'b [u8]> {
        self.host.map(|h| h.of(buf))
    }

    pub fn framing(&self) -> Framing {
        self.framing
    }

    /// Whether the request expects `100 Continue` before its body is sent (RFC 9110 §10.1.1).
    pub fn continues(&self) -> bool {
        self.continues
    }

    /// Whether the connection persists past the response (RFC 9112 §9.3): HTTP/1.1 unless
    /// `Connection: close`, HTTP/1.0 only with `keep-alive`.
    pub fn persists(&self) -> bool {
        self.persists
    }
}

/// The line that begins at `at`: its octets without the LF that ends it or a CR before that LF,
/// and where the next begins; none until its LF is in `buf`.
fn line(buf: &[u8], at: usize) -> Option<(Span, usize)> {
    let lf = at.checked_add(memchr(b'\n', buf.get(at..)?)?)?;
    let end = if lf > at && buf.get(lf.checked_sub(1)?) == Some(&b'\r') {
        lf.checked_sub(1)?
    } else {
        lf
    };
    Some((Span { start: at, end }, lf.checked_add(1)?))
}

/// Parses the request head that begins `buf`: the head, once the empty line that ends it is in
/// `buf`; none while it is not, unless `buf` already holds [`MAX_HEAD`] octets, past which no head
/// may run.
pub fn parse(buf: &[u8]) -> Result<Option<Head>, Refusal> {
    let bounded = buf.get(..MAX_HEAD).unwrap_or(buf);
    let full = buf.len() >= MAX_HEAD;
    let mut at = 0;
    // Empty lines before the request line are passed (RFC 9112 §2.2's SHOULD).
    let request = loop {
        let Some((found, next)) = line(bounded, at) else {
            return if full {
                Err(Refusal::TargetTooLong)
            } else {
                Ok(None)
            };
        };
        at = next;
        if found.start != found.end {
            break found;
        }
    };
    let (method, target, version) = request_line(bounded, request)?;
    // The field lines counted to the head's end first, so their list is made once, at its size.
    let (mut count, mut end) = (0usize, at);
    loop {
        let Some((found, next)) = line(bounded, end) else {
            return if full {
                Err(Refusal::HeadTooLarge)
            } else {
                Ok(None)
            };
        };
        end = next;
        if found.start == found.end {
            break;
        }
        count = count.saturating_add(1);
    }
    let mut fields = Vec::with_capacity(count);
    while let Some((found, next)) = line(bounded, at)
        && found.start != found.end
    {
        fields.push(field_line(bounded, found)?);
        at = next;
    }
    judge(bounded, end, (method, target, version), fields)
}

/// The request line's method, target and version.
fn request_line(buf: &[u8], line: Span) -> Result<(Span, Span, Version), Refusal> {
    let bytes = line.of(buf);
    let bad = Refusal::Grammar("the request line");
    let sp = memchr(b' ', bytes).ok_or(bad)?;
    let (method, rest) = bytes.split_at_checked(sp).ok_or(bad)?;
    if method.is_empty() || !method.iter().all(|b| tchar(*b)) {
        return Err(Refusal::Grammar("the method"));
    }
    let rest = rest.get(1..).ok_or(bad)?;
    let sp2 = memchr(b' ', rest).ok_or(bad)?;
    let (target, version) = rest.split_at_checked(sp2).ok_or(bad)?;
    // No whitespace, control or non-ASCII octet in the target (RFC 9112 §3.2).
    if target.is_empty() || !target.iter().all(|b| b.is_ascii_graphic()) {
        return Err(Refusal::Grammar("the request-target"));
    }
    let version = match version.get(1..).ok_or(bad)? {
        b"HTTP/1.1" => Version::Http11,
        b"HTTP/1.0" => Version::Http10,
        [b'H', b'T', b'T', b'P', b'/', major, b'.', minor]
            if major.is_ascii_digit() && minor.is_ascii_digit() =>
        {
            return Err(Refusal::Version);
        }
        _ => return Err(Refusal::Grammar("the HTTP version")),
    };
    let method_span = line.within(0, sp);
    let start = sp.checked_add(1).ok_or(bad)?;
    let end = start.checked_add(sp2).ok_or(bad)?;
    Ok((method_span, line.within(start, end), version))
}

/// A field line's name and value, the value bare of its whitespace (RFC 9112 §5, §5.1).
fn field_line(buf: &[u8], line: Span) -> Result<(Span, Span), Refusal> {
    let (colon, first, last) = field(line.of(buf)).map_err(Refusal::Grammar)?;
    Ok((line.within(0, colon), line.within(first, last)))
}

/// Where a field line's colon is, and where its value begins and ends bare of its whitespace;
/// why it is not a field line (RFC 9112 §2.2, §5, §5.1, §5.2; RFC 9110 §5.5). A trailer's line is
/// judged as a header's.
pub(crate) fn field(bytes: &[u8]) -> Result<(usize, usize, usize), &'static str> {
    if matches!(bytes.first(), Some(b' ' | b'\t')) {
        // An obs-fold, or whitespace between the request line and the first field: refused, as
        // each rule lets a server.
        return Err("a field line that begins with whitespace");
    }
    let colon = memchr(b':', bytes).ok_or("a field line without a colon")?;
    let name = bytes.get(..colon).unwrap_or_default();
    if matches!(name.last(), Some(b' ' | b'\t')) {
        return Err("whitespace before a field's colon");
    }
    if name.is_empty() || !name.iter().all(|b| tchar(*b)) {
        return Err("a field name");
    }
    let start = colon.saturating_add(1);
    let value = bytes.get(start..).unwrap_or_default();
    let first = value
        .iter()
        .position(|b| !matches!(b, b' ' | b'\t'))
        .unwrap_or(value.len());
    let last = value
        .iter()
        .rposition(|b| !matches!(b, b' ' | b'\t'))
        .map_or(first, |p| p.saturating_add(1));
    let bare = value.get(first..last).unwrap_or_default();
    if !bare.iter().all(|b| field_octet(*b)) {
        return Err("a control octet in a field's value");
    }
    Ok((
        colon,
        start.saturating_add(first),
        start.saturating_add(last),
    ))
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

/// What the head's fields decide of the request: its host, its framing, its expectation and its
/// connection's persistence.
fn judge(
    buf: &[u8],
    len: usize,
    (method, target, version): (Span, Span, Version),
    fields: Vec<(Span, Span)>,
) -> Result<Option<Head>, Refusal> {
    let mut host: Option<Span> = None;
    let mut hosts = 0usize;
    let mut length: Option<u64> = None;
    // A Transfer-Encoding field present is judged as present, whatever it lists (RFC 9112 §6.3).
    let mut encoded = false;
    let mut codings = Codings::default();
    let mut continues = false;
    let (mut close, mut keep_alive) = (false, false);
    for (name, value) in &fields {
        let (name, bytes) = (name.of(buf), value.of(buf));
        if name.eq_ignore_ascii_case(b"host") {
            hosts = hosts.saturating_add(1);
            host = Some(*value);
        } else if name.eq_ignore_ascii_case(b"content-length") {
            // A list of one repeated value is that value; any other is unrecoverable (RFC 9112
            // §6.3 item 5).
            for member in value_list(bytes)? {
                let n = decimal(member)?;
                if length.is_some_and(|l| l != n) {
                    return Err(Refusal::Framing("Content-Length values that differ"));
                }
                length = Some(n);
            }
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            encoded = true;
            for member in members(bytes) {
                codings.take(member);
            }
        } else if name.eq_ignore_ascii_case(b"expect") {
            for member in members(bytes) {
                if member.eq_ignore_ascii_case(b"100-continue") {
                    continues = true;
                } else {
                    return Err(Refusal::Expectation);
                }
            }
        } else if name.eq_ignore_ascii_case(b"connection") {
            for member in members(bytes) {
                close |= member.eq_ignore_ascii_case(b"close");
                keep_alive |= member.eq_ignore_ascii_case(b"keep-alive");
            }
        }
    }
    let origin = match authority(target.of(buf)) {
        // An absolute-form target's authority is the host, the Host field ignored (RFC 9112
        // §3.2.2).
        Some((start, end)) => {
            host = Some(target.within(start, end));
            target.within(end, target.end.saturating_sub(target.start))
        }
        None => target,
    };
    if hosts > 1
        || (version == Version::Http11 && hosts == 0)
        || host.is_some_and(|h| !h.of(buf).iter().all(|b| host_octet(*b)))
    {
        return Err(Refusal::Host);
    }
    let framing = match (encoded, length) {
        (false, None) => Framing::None,
        (false, Some(n)) => Framing::Length(n),
        (true, Some(_)) => {
            return Err(Refusal::Framing("Transfer-Encoding with Content-Length"));
        }
        (true, None) if version == Version::Http10 => {
            return Err(Refusal::Framing("Transfer-Encoding on HTTP/1.0"));
        }
        (true, None) => codings.framing()?,
    };
    Ok(Some(Head {
        len,
        method,
        target,
        origin,
        version,
        fields,
        host,
        framing,
        // Ignored on HTTP/1.0, as RFC 9110 §10.1.1 requires, and without content to follow,
        // which needs no 100 (its MAY).
        continues: continues && version == Version::Http11 && framing != Framing::None,
        persists: !close && (version == Version::Http11 || keep_alive),
    }))
}

/// A Content-Length field's values: a comma-separated list, none empty.
fn value_list(value: &[u8]) -> Result<impl Iterator<Item = &[u8]>, Refusal> {
    if value.is_empty()
        || value
            .split(|b| *b == b',')
            .any(|m| members(m).next().is_none())
    {
        return Err(Refusal::Framing("an empty Content-Length"));
    }
    Ok(members(value))
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

/// A Transfer-Encoding's codings, judged as they are listed (RFC 9112 §6.1, §7.1): chunked must be
/// last and only once and takes no parameters, and no other coding may come before it, which
/// mantle does not decode (`501`).
#[derive(Default)]
struct Codings {
    chunked: usize,
    others: usize,
    /// The last coding listed: whether it is chunked, and whether it has parameters.
    last: Option<(bool, bool)>,
}

impl Codings {
    fn take(&mut self, coding: &[u8]) {
        let params = memchr(b';', coding);
        let name = coding
            .get(..params.unwrap_or(coding.len()))
            .unwrap_or_default();
        let chunked = name.trim_ascii().eq_ignore_ascii_case(b"chunked");
        if chunked {
            self.chunked = self.chunked.saturating_add(1);
        } else {
            self.others = self.others.saturating_add(1);
        }
        self.last = Some((chunked, params.is_some()));
    }

    fn framing(&self) -> Result<Framing, Refusal> {
        match self.last {
            None => Err(Refusal::Framing("an empty Transfer-Encoding")),
            Some((false, _)) => Err(Refusal::Framing(
                "a Transfer-Encoding not ending in chunked",
            )),
            Some((true, true)) => Err(Refusal::Framing("chunked with parameters")),
            Some((true, false)) if self.chunked > 1 => {
                Err(Refusal::Framing("chunked more than once"))
            }
            Some((true, false)) if self.others > 0 => Err(Refusal::Coding),
            Some((true, false)) => Ok(Framing::Chunked),
        }
    }
}

/// An absolute-form target's authority, where it lies in the target: past `scheme://` and up to
/// the path, query or end (RFC 9112 §3.2.2; RFC 3986 §3).
fn authority(target: &[u8]) -> Option<(usize, usize)> {
    let scheme = memchr(b':', target)?;
    let name = target.get(..scheme)?;
    let first = name.first()?;
    if !first.is_ascii_alphabetic()
        || !name
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
    {
        return None;
    }
    let start = scheme.checked_add(3)?;
    if target.get(scheme..start)? != b"://" {
        return None;
    }
    let rest = target.get(start..)?;
    let end = rest
        .iter()
        .position(|b| matches!(b, b'/' | b'?' | b'#'))
        .unwrap_or(rest.len());
    Some((start, start.checked_add(end)?))
}

/// Whether `b` may appear in a host and port: RFC 3986 §3.2.2's reg-name, IP literal and port
/// octets, unreserved, sub-delims, `%`, `:`, `[` and `]`.
fn host_octet(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'%'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'['
                | b']'
        )
}
