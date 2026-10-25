//! mantle's HTTP/1.1 codec for the S3 listener (docs/design/node.md §4.1; docs/research/70): it
//! reads requests and writes responses and does no I/O. Its caller reads a connection's octets
//! into buffers of its own and hands them over: a request's head is parsed where it lies
//! ([`parse`]), its body decoded as it streams, given back as spans of the caller's input
//! ([`Body`]), and a response's head and chunked framing written into the caller's buffer
//! ([`write`]).
//!
//! Nothing it holds grows with a message past a bound: a head is at most [`MAX_HEAD`], S3's 8 KB;
//! a chunked body's extensions and its trailer section are held to the same bound. Every input it
//! is given is answered by a value or a typed [`Refusal`], never a panic, and each refusal names
//! the status RFC 9112, RFC 9110 or S3 answers it with and whether the connection closes after.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

mod body;
mod head;
pub mod write;

pub use body::{Body, Step};
pub use head::{Framing, Head, Span, Version, parse};

/// A request's head at most, its request line and field lines with their line ends and the empty
/// line that ends it: S3's "request header and query parameters" bound, 8 KB (research/05 §10.2,
/// §11.2), whose `RequestHeaderSectionTooLarge` answers a head past it.
pub const MAX_HEAD: usize = 8 << 10;

/// Why a request is refused, and how it is answered. The connection closes after any refusal's
/// response: the octets after a request whose head or framing is refused cannot be told from the
/// next request's (RFC 9112 §6.3, §9.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The head runs past [`MAX_HEAD`]: S3's `400 RequestHeaderSectionTooLarge`.
    #[error("a request head past {MAX_HEAD} octets")]
    HeadTooLarge,
    /// The request line has not ended within [`MAX_HEAD`]: `414 URI Too Long`, which RFC 9112 §3
    /// requires of a target longer than the server parses.
    #[error("a request line past {MAX_HEAD} octets")]
    TargetTooLong,
    /// The octets are not an HTTP/1.1 request: `400 Bad Request`, and the connection closed
    /// (RFC 9112 §2.2).
    #[error("a request that is not HTTP/1.1's grammar: {0}")]
    Grammar(&'static str),
    /// An HTTP/1.1 request without exactly one valid Host field: `400 Bad Request` (RFC 9112
    /// §3.2).
    #[error("a request without exactly one valid Host field")]
    Host,
    /// A version other than HTTP/1.0 and HTTP/1.1: `505 HTTP Version Not Supported`.
    #[error("an HTTP version other than 1.0 and 1.1")]
    Version,
    /// A transfer coding other than chunked, chunked last: `501 Not Implemented` (RFC 9112 §6.1).
    #[error("a transfer coding other than chunked")]
    Coding,
    /// An expectation other than 100-continue: `417 Expectation Failed` (RFC 9110 §10.1.1).
    #[error("an expectation other than 100-continue")]
    Expectation,
    /// The body's length cannot be trusted: Transfer-Encoding with Content-Length, a coding not
    /// ending in chunked, Transfer-Encoding on HTTP/1.0, or a Content-Length invalid or
    /// overflowing: `400 Bad Request`, and the connection closed (RFC 9112 §6.1, §6.3).
    #[error("a request whose body's length cannot be trusted: {0}")]
    Framing(&'static str),
    /// A chunked body that is not the chunked coding's grammar, overflows a chunk's size, or holds
    /// more extensions or trailers than [`MAX_HEAD`]: `400 Bad Request`, and the connection closed.
    #[error("a chunked body that is not the coding's grammar: {0}")]
    Chunked(&'static str),
}

impl Refusal {
    /// The status the refusal is answered with.
    pub fn status(&self) -> u16 {
        match self {
            Self::TargetTooLong => 414,
            Self::Version => 505,
            Self::Coding => 501,
            Self::Expectation => 417,
            Self::HeadTooLarge
            | Self::Grammar(_)
            | Self::Host
            | Self::Framing(_)
            | Self::Chunked(_) => 400,
        }
    }
}

/// Whether `b` may appear in a token: "any VCHAR, except delimiters" (RFC 9110 §5.6.2).
pub(crate) fn tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Whether `b` may appear in a field value past its first octet: VCHAR, obs-text, SP or HTAB
/// (RFC 9110 §5.5); CR, LF, NUL and the other controls may not.
pub(crate) fn field_octet(b: u8) -> bool {
    matches!(b, b'\t' | b' '..=b'~' | 0x80..=0xFF)
}
