//! A response's head and its chunked framing, written into its caller's buffer (RFC 9112 §4, §6,
//! §7.1). A field's name is a token and its value holds no CR, LF, NUL or other control but HTAB,
//! so no value a caller passes can end the head early or begin another response (RFC 9112 §11.1;
//! RFC 9110 §5.5). A body's octets are the caller's to send between a chunk's head and its end, so
//! nothing here copies them.

use std::io::Write as _;

use crate::{Version, field_octet, tchar};

/// The end of a chunk's data.
pub const CHUNK_END: &[u8] = b"\r\n";

/// The interim response to a request that expects `100 Continue` (RFC 9110 §10.1.1).
pub const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// How a response's body is framed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Length {
    /// No length field: a 1xx, 204 or 304 response, or a HEAD's that states none.
    None,
    /// `Content-Length`: the body's octets, or a HEAD's or 304's the body would have.
    Known(u64),
    /// The chunked transfer coding, for a body whose length is not known before its head goes.
    Chunked,
}

/// Why a response's head was not written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WriteError {
    #[error("a status outside 100 to 999")]
    Status,
    #[error("a reason phrase with a control octet")]
    Reason,
    #[error("a field name that is not a token")]
    Name,
    #[error("a field value with CR, LF, NUL or another control octet")]
    Value,
    /// A length on a 1xx or 204 response, which RFC 9112 §6.1 and RFC 9110 §8.6 forbid.
    #[error("a length on a 1xx or 204 response")]
    Length,
    /// The chunked coding in a response to an HTTP/1.0 request (RFC 9112 §6.1).
    #[error("the chunked coding to an HTTP/1.0 request")]
    Chunked,
}

/// Writes a response's head: its status line, `fields` and the length's field, and the empty line
/// that ends it. `version` is the request's.
pub fn head(
    out: &mut Vec<u8>,
    version: Version,
    status: u16,
    reason: &str,
    fields: &[(&str, &[u8])],
    length: Length,
) -> Result<(), WriteError> {
    if !(100..=999).contains(&status) {
        return Err(WriteError::Status);
    }
    if !reason.bytes().all(field_octet) {
        return Err(WriteError::Reason);
    }
    let bodiless = (100..200).contains(&status) || status == 204;
    if bodiless && length != Length::None {
        return Err(WriteError::Length);
    }
    if version == Version::Http10 && length == Length::Chunked {
        return Err(WriteError::Chunked);
    }
    for (name, value) in fields {
        if name.is_empty() || !name.bytes().all(tchar) {
            return Err(WriteError::Name);
        }
        if !value.iter().all(|b| field_octet(*b)) {
            return Err(WriteError::Value);
        }
    }
    out.extend_from_slice(b"HTTP/1.1 ");
    // A Vec's writes do not fail.
    let _ = write!(out, "{status} ");
    out.extend_from_slice(reason.as_bytes());
    out.extend_from_slice(b"\r\n");
    for (name, value) in fields {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    match length {
        Length::None => {}
        Length::Known(n) => {
            let _ = write!(out, "Content-Length: {n}\r\n");
        }
        Length::Chunked => out.extend_from_slice(b"Transfer-Encoding: chunked\r\n"),
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}

/// Writes the head of a chunk of `len` octets, which the caller sends after it with
/// [`CHUNK_END`]; a chunk of none is the last, written by [`last`] instead.
pub fn chunk(out: &mut Vec<u8>, len: usize) {
    let _ = write!(out, "{len:x}\r\n");
}

/// Writes the last chunk and the trailer section, `trailers`, that ends a chunked body (RFC 9112
/// §7.1, §7.1.2).
pub fn last(out: &mut Vec<u8>, trailers: &[(&str, &[u8])]) -> Result<(), WriteError> {
    for (name, value) in trailers {
        if name.is_empty() || !name.bytes().all(tchar) {
            return Err(WriteError::Name);
        }
        if !value.iter().all(|b| field_octet(*b)) {
            return Err(WriteError::Value);
        }
    }
    out.extend_from_slice(b"0\r\n");
    for (name, value) in trailers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}
