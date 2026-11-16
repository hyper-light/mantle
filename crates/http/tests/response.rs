//! A response's head as mantle's S3 client reads it (RFC 9112 §4, §6.3, §9.3): each rule by a head
//! that keeps it or breaks it and the answer each gets, and every head the listener's own writer
//! makes read back as written.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_http::response::{Ends, parse_response};
use mantle_http::write::{self, Length};
use mantle_http::{Framing, MAX_HEAD, Refusal, Version};

fn whole(head: &[u8], is_head: bool) -> mantle_http::response::Response {
    parse_response(head, is_head)
        .unwrap()
        .expect("a whole head")
}

/// RFC 9112 §4: the status line's version, code and reason, and its fields where they lie.
#[test]
fn the_status_line_and_fields_are_read_where_they_lie() {
    let buf = b"HTTP/1.1 404 Not Found\r\nx-amz-request-id: 7\r\nContent-Length:  3 \r\n\r\nabc";
    let read = whole(buf, false);
    assert_eq!(read.version(), Version::Http11);
    assert_eq!(read.status(), 404);
    assert_eq!(read.reason(buf), b"Not Found");
    assert_eq!(read.field(buf, "X-Amz-Request-Id"), Some(&b"7"[..]));
    assert_eq!(read.field(buf, "content-length"), Some(&b"3"[..]));
    assert_eq!(read.field(buf, "etag"), None);
    assert_eq!(
        read.fields(buf).collect::<Vec<_>>(),
        [
            (&b"x-amz-request-id"[..], &b"7"[..]),
            (&b"Content-Length"[..], &b"3"[..])
        ]
    );
    assert_eq!(read.len(), buf.len() - 3, "the body follows the head");
    assert_eq!(read.ends(), Ends::Framed(Framing::Length(3)));
    assert!(read.persists());
    // An empty reason, and lone LFs.
    let buf = b"HTTP/1.0 200 \nContent-Length: 0\n\n";
    let read = whole(buf, false);
    assert_eq!((read.version(), read.status()), (Version::Http10, 200));
    assert_eq!(read.reason(buf), b"");
    assert_eq!(read.len(), buf.len());
}

/// A head not yet whole is none; one past [`MAX_HEAD`] is refused.
#[test]
fn a_head_is_whole_or_waited_for_within_its_bound() {
    let buf = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    for cut in 0..buf.len() {
        assert_eq!(parse_response(&buf[..cut], false), Ok(None), "cut at {cut}");
    }
    let mut long = b"HTTP/1.1 200 OK\r\n".to_vec();
    while long.len() < MAX_HEAD {
        long.extend_from_slice(b"x-amz-meta-a: aaaaaaaaaaaaaaaa\r\n");
    }
    assert_eq!(parse_response(&long, false), Err(Refusal::HeadTooLarge));
}

/// RFC 9112 §6.3: how each response's body ends.
#[test]
fn each_body_ends_as_its_status_method_and_fields_say() {
    let cases: [(&[u8], bool, Ends, bool); 12] = [
        // Item 1: a HEAD's response, a 1xx, a 204 and a 304 end at their head.
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n",
            true,
            Ends::Framed(Framing::None),
            true,
        ),
        (
            b"HTTP/1.1 100 Continue\r\n\r\n",
            false,
            Ends::Framed(Framing::None),
            true,
        ),
        (
            b"HTTP/1.1 204 No Content\r\nTransfer-Encoding: chunked\r\n\r\n",
            false,
            Ends::Framed(Framing::None),
            true,
        ),
        (
            b"HTTP/1.1 304 Not Modified\r\nContent-Length: 9\r\n\r\n",
            false,
            Ends::Framed(Framing::None),
            true,
        ),
        // Item 3: chunked last is chunked; another coding last ends at the close.
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
            false,
            Ends::Framed(Framing::Chunked),
            true,
        ),
        (
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\n",
            false,
            Ends::Close,
            false,
        ),
        // Item 5: a list of one repeated value.
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 5, 5\r\n\r\n",
            false,
            Ends::Framed(Framing::Length(5)),
            true,
        ),
        // Item 8: neither ends at the close.
        (b"HTTP/1.1 200 OK\r\n\r\n", false, Ends::Close, false),
        // §9.3: close, and HTTP/1.0 without keep-alive, end the connection.
        (
            b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            false,
            Ends::Framed(Framing::Length(0)),
            false,
        ),
        (
            b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n",
            false,
            Ends::Framed(Framing::Length(0)),
            false,
        ),
        (
            b"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n",
            false,
            Ends::Framed(Framing::Length(0)),
            true,
        ),
        (
            b"HTTP/1.1 503 Slow Down\r\nContent-Length: 0\r\n\r\n",
            false,
            Ends::Framed(Framing::Length(0)),
            true,
        ),
    ];
    for (buf, is_head, ends, persists) in cases {
        let read = whole(buf, is_head);
        assert_eq!(
            (read.ends(), read.persists()),
            (ends, persists),
            "{}",
            String::from_utf8_lossy(buf)
        );
    }
}

/// A head whose grammar or framing cannot be trusted is refused, never read past.
#[test]
fn a_head_that_breaks_a_rule_is_refused() {
    let cases: [(&[u8], Refusal); 11] = [
        (b"HTTP/1.1 200\r\n\r\n", Refusal::Grammar("the status line")),
        (
            b"HTTP/1.1  200 OK\r\n\r\n",
            Refusal::Grammar("the status code"),
        ),
        (
            b"HTTP/1.1 2x0 OK\r\n\r\n",
            Refusal::Grammar("the status code"),
        ),
        (
            b"HTTP/1.1 099 OK\r\n\r\n",
            Refusal::Grammar("the status code"),
        ),
        (b"HTTP/2.0 200 OK\r\n\r\n", Refusal::Version),
        (
            b"HTTX/1.1 200 OK\r\n\r\n",
            Refusal::Grammar("the status line"),
        ),
        (
            b"HTTP/1.1 200 O\x01K\r\n\r\n",
            Refusal::Grammar("the reason phrase"),
        ),
        (
            b"HTTP/1.1 200 OK\r\n folded: x\r\n\r\n",
            Refusal::Grammar("a field line that begins with whitespace"),
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 5, 6\r\n\r\n",
            Refusal::Framing("Content-Length values that differ"),
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
            Refusal::Framing("Transfer-Encoding with Content-Length"),
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 18446744073709551616\r\n\r\n",
            Refusal::Framing("a Content-Length past 64 bits"),
        ),
    ];
    for (buf, refusal) in cases {
        assert_eq!(
            parse_response(buf, false),
            Err(refusal),
            "{}",
            String::from_utf8_lossy(buf)
        );
    }
}

/// Every head the listener's writer makes (`write::head`) reads back as it was written: its
/// status, reason, fields and length.
#[test]
fn the_listeners_heads_read_back_as_written() {
    let fields: [(&str, &[u8]); 2] = [("ETag", b"\"0f\""), ("x-amz-version-id", b"v1")];
    for (status, reason, length, ends) in [
        (
            200,
            "OK",
            Length::Known(12),
            Ends::Framed(Framing::Length(12)),
        ),
        (200, "OK", Length::Chunked, Ends::Framed(Framing::Chunked)),
        (204, "No Content", Length::None, Ends::Framed(Framing::None)),
        (
            404,
            "Not Found",
            Length::Known(0),
            Ends::Framed(Framing::Length(0)),
        ),
    ] {
        let mut out = Vec::new();
        write::head(&mut out, Version::Http11, status, reason, &fields, length).unwrap();
        let read = whole(&out, false);
        assert_eq!(read.status(), status);
        assert_eq!(read.reason(&out), reason.as_bytes());
        assert_eq!(read.field(&out, "etag"), Some(&b"\"0f\""[..]));
        assert_eq!(read.field(&out, "x-amz-version-id"), Some(&b"v1"[..]));
        assert_eq!(read.ends(), ends);
        assert_eq!(read.len(), out.len());
        assert!(read.persists());
    }
}
