//! Each rule of RFC 9112 and RFC 9110 the codec keeps (docs/research/70), by a request that
//! breaks it or keeps it, and the answer each gets.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_http::write::{self, Length, WriteError};
use mantle_http::{Body, Framing, Head, MAX_HEAD, Refusal, Version, parse};

/// Fields, each its name and value.
type Fields = Vec<(Vec<u8>, Vec<u8>)>;

/// A response head's status, reason, fields, length and request version, and the error each
/// earns.
type Try<'a> = (
    u16,
    &'a str,
    &'a [(&'a str, &'a [u8])],
    Length,
    Version,
    WriteError,
);

fn head(request: &[u8]) -> Head {
    parse(request).unwrap().expect("a whole head")
}

fn refused(request: &[u8]) -> Refusal {
    parse(request).expect_err("a refusal")
}

/// The body of `input` past the head, decoded whole, and its trailers.
fn body(framing: Framing, input: &[u8]) -> Result<(Vec<u8>, usize, Fields), Refusal> {
    let mut body = Body::new(framing);
    let (mut data, mut at) = (Vec::new(), 0);
    loop {
        let step = body.decode(&input[at..])?;
        data.extend_from_slice(&input[at..][step.data.clone()]);
        at += step.consumed;
        if step.end {
            let trailers = body
                .trailers()
                .map(|(n, v)| (n.to_vec(), v.to_vec()))
                .collect();
            return Ok((data, at, trailers));
        }
        if step.consumed == 0 {
            panic!("no progress with {} octets left", input.len() - at);
        }
    }
}

#[test]
fn a_request_line_and_fields_parse_where_they_lie() {
    let request = b"PUT /b/k?x=1 HTTP/1.1\r\nHost: b.s3\r\nContent-Length: 5\r\nX-Amz-Meta-A:  v w \r\n\r\nhello";
    let h = head(request);
    assert_eq!(h.method(request), b"PUT");
    assert_eq!(h.target(request), b"/b/k?x=1");
    assert_eq!(h.origin(request), b"/b/k?x=1");
    assert_eq!(h.version(), Version::Http11);
    assert_eq!(h.host(request), Some(&b"b.s3"[..]));
    // A value is bare of the whitespace around it, not within it (RFC 9112 §5.1).
    assert_eq!(h.field(request, "x-amz-meta-a"), Some(&b"v w"[..]));
    assert_eq!(h.framing(), Framing::Length(5));
    assert!(h.persists() && !h.continues());
    assert_eq!(&request[h.len()..], b"hello");
    assert_eq!(h.fields(request).count(), 3);
}

/// RFC 9112 §6.1, §6.3 item 3: Transfer-Encoding beside Content-Length is refused, 400, and the
/// connection closed after.
#[test]
fn transfer_encoding_with_content_length_is_refused() {
    let r = refused(
        b"PUT / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n",
    );
    assert!(matches!(r, Refusal::Framing(_)));
    assert_eq!(r.status(), 400);
}

/// RFC 9112 §6.1, §6.3 item 4, §7.1: chunked must be the final coding, once and bare; another
/// coding before it is one mantle does not decode, 501.
#[test]
fn the_codings_a_transfer_encoding_lists() {
    let te = |value: &str, version: &str| {
        parse(
            format!("PUT / {version}\r\nHost: h\r\nTransfer-Encoding: {value}\r\n\r\n").as_bytes(),
        )
        .map(|h| h.unwrap().framing())
    };
    assert_eq!(te("chunked", "HTTP/1.1"), Ok(Framing::Chunked));
    assert_eq!(te("CHUNKED", "HTTP/1.1"), Ok(Framing::Chunked));
    assert_eq!(te("gzip, chunked", "HTTP/1.1"), Err(Refusal::Coding));
    assert_eq!(te("gzip, chunked", "HTTP/1.1").unwrap_err().status(), 501);
    for bad in [
        "chunked, gzip",
        "chunked, chunked",
        "chunked;q=1",
        "gzip",
        "",
    ] {
        assert!(
            matches!(te(bad, "HTTP/1.1"), Err(Refusal::Framing(_))),
            "{bad:?}"
        );
    }
    // Transfer-Encoding on HTTP/1.0 is faulty framing (§6.1).
    assert!(matches!(
        te("chunked", "HTTP/1.0"),
        Err(Refusal::Framing(_))
    ));
    // Two field lines list the codings in order.
    let two = parse(b"PUT / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n");
    assert_eq!(two, Err(Refusal::Coding));
}

/// RFC 9112 §6.3 item 5, RFC 9110 §8.6: Content-Length is `1*DIGIT`, a list of one repeated value
/// is that value, and anything else, or past 64 bits, is refused.
#[test]
fn the_values_a_content_length_takes() {
    let cl = |value: &str| {
        parse(format!("PUT / HTTP/1.1\r\nHost: h\r\nContent-Length: {value}\r\n\r\n").as_bytes())
            .map(|h| h.unwrap().framing())
    };
    assert_eq!(cl("0"), Ok(Framing::Length(0)));
    assert_eq!(cl("5, 5"), Ok(Framing::Length(5)));
    assert_eq!(cl("18446744073709551615"), Ok(Framing::Length(u64::MAX)));
    for bad in [
        "5, 6",
        "abc",
        "-1",
        "+5",
        "5 5",
        "",
        "5,",
        "18446744073709551616",
        "0x10",
    ] {
        assert!(matches!(cl(bad), Err(Refusal::Framing(_))), "{bad:?}");
    }
    let differ =
        parse(b"PUT / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n");
    assert!(matches!(differ, Err(Refusal::Framing(_))));
    // Neither field: no body (§6.3 item 7).
    assert_eq!(
        head(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n").framing(),
        Framing::None
    );
}

/// RFC 9112 §7.1: chunks of any size in either case, extensions passed unread, the trailer
/// section kept apart from the head; a size past 64 bits refused.
#[test]
fn chunk_sizes_extensions_and_trailers() {
    let input = b"4\r\nWiki\r\n5;name=value; other\r\npedia\r\nE\r\n in\r\n\r\nchunks.\r\n0;x\r\nX-Trailer: v 1\r\nChecksum:  abc\r\n\r\nNEXT";
    let (data, used, trailers) = body(Framing::Chunked, input).unwrap();
    assert_eq!(data, b"Wikipedia in\r\n\r\nchunks.");
    assert_eq!(&input[used..], b"NEXT");
    assert_eq!(
        trailers,
        vec![
            (b"X-Trailer".to_vec(), b"v 1".to_vec()),
            (b"Checksum".to_vec(), b"abc".to_vec())
        ]
    );
    // Leading zeros, upper case, and a last chunk of several zeros; lone LFs as line ends.
    let (data, _, _) = body(Framing::Chunked, b"0000A\nabcdefghij\n000\n\n").unwrap();
    assert_eq!(data, b"abcdefghij");
    for bad in [
        &b"10000000000000000\r\n"[..],
        b"\r\n",
        b"g\r\n",
        b"3\r\nabcd\r\n",
        b"3\r\nabc\rX",
        b"1;\x01\r\n",
        b"0\r\nbad trailer\r\n\r\n",
        b"0\r\n trailer: folded\r\n\r\n",
    ] {
        assert!(
            matches!(body(Framing::Chunked, bad), Err(Refusal::Chunked(_))),
            "{:?}",
            String::from_utf8_lossy(bad)
        );
    }
}

/// RFC 9112 §7.1.1, §7.1.2: chunk extensions and the trailer section are each held to the head's
/// bound.
#[test]
fn extensions_and_trailers_are_held_to_the_heads_bound() {
    let mut long = b"1;".to_vec();
    long.extend(std::iter::repeat_n(b'e', MAX_HEAD));
    long.extend_from_slice(b"\r\nx\r\n0\r\n\r\n");
    assert!(matches!(
        body(Framing::Chunked, &long),
        Err(Refusal::Chunked(_))
    ));
    let mut within = b"1;".to_vec();
    within.extend(std::iter::repeat_n(b'e', MAX_HEAD - 1));
    within.extend_from_slice(b"\r\nx\r\n0\r\n\r\n");
    assert_eq!(body(Framing::Chunked, &within).unwrap().0, b"x");
    let mut trailers = b"0\r\nT: ".to_vec();
    trailers.extend(std::iter::repeat_n(b't', MAX_HEAD));
    trailers.extend_from_slice(b"\r\n\r\n");
    assert!(matches!(
        body(Framing::Chunked, &trailers),
        Err(Refusal::Chunked(_))
    ));
}

/// The head's bound, S3's 8 KB: a head of exactly 8,192 octets is taken, one octet more is
/// refused `RequestHeaderSectionTooLarge`; a request line not ended within it, 414.
#[test]
fn a_head_of_eight_kib_is_taken_and_one_octet_more_refused() {
    let exact = |size: usize| {
        let start = b"GET / HTTP/1.1\r\nHost: h\r\nX-Pad: ".to_vec();
        let pad = size - start.len() - 4;
        let mut request = start;
        request.extend(std::iter::repeat_n(b'p', pad));
        request.extend_from_slice(b"\r\n\r\n");
        assert_eq!(request.len(), size);
        request
    };
    let at = exact(MAX_HEAD);
    assert_eq!(head(&at).len(), MAX_HEAD);
    let over = exact(MAX_HEAD + 1);
    assert_eq!(refused(&over), Refusal::HeadTooLarge);
    assert_eq!(refused(&over).status(), 400);
    // A head not yet whole, and shorter than the bound, waits for more.
    assert_eq!(parse(&at[..MAX_HEAD - 1]), Ok(None));
    let mut target = b"GET /".to_vec();
    target.extend(std::iter::repeat_n(b'a', MAX_HEAD));
    assert_eq!(refused(&target), Refusal::TargetTooLong);
    assert_eq!(refused(&target).status(), 414);
}

/// RFC 9112 §2.2, §5.1, §5.2: whitespace before a field's colon, a line after the request line
/// that begins with whitespace, a bare CR, and a control in a value are each refused, 400; a lone
/// LF ends a line; empty lines before the request line are passed.
#[test]
fn the_grammar_held_strictly_where_leniency_smuggles() {
    for bad in [
        &b"GET / HTTP/1.1\r\nHost : h\r\n\r\n"[..],
        b"GET / HTTP/1.1\r\nHost: h\r\n folded\r\n\r\n",
        b"GET / HTTP/1.1\r\n Host: h\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\rX\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x00b\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: h\r\nX: a\x7fb\r\n\r\n",
        b"GET  / HTTP/1.1\r\nHost: h\r\n\r\n",
        b"GET / x HTTP/1.1\r\nHost: h\r\n\r\n",
        b"GET /\x80 HTTP/1.1\r\nHost: h\r\n\r\n",
        b"G(T / HTTP/1.1\r\nHost: h\r\n\r\n",
        b"GET / HTTP/1.1 \r\nHost: h\r\n\r\n",
        b"GET / HTTP/1.1\r\n: v\r\nHost: h\r\n\r\n",
        b"GET / HTTP/1.1\r\nNo colon\r\nHost: h\r\n\r\n",
    ] {
        let r = refused(bad);
        assert!(
            matches!(r, Refusal::Grammar(_)),
            "{:?}: {r:?}",
            String::from_utf8_lossy(bad)
        );
        assert_eq!(r.status(), 400);
    }
    let h = head(b"\r\n\nGET / HTTP/1.1\nHost: h\n\n");
    assert_eq!(h.len(), 27);
    // obs-text in a value is opaque data (RFC 9110 §5.5).
    assert_eq!(
        head(b"GET / HTTP/1.1\r\nHost: h\r\nX: \xe9\r\n\r\n").framing(),
        Framing::None
    );
}

/// RFC 9112 §2.3, §3: a version other than 1.0 and 1.1 is 505; one not the grammar's, 400.
#[test]
fn versions() {
    assert_eq!(
        refused(b"GET / HTTP/2.0\r\nHost: h\r\n\r\n"),
        Refusal::Version
    );
    assert_eq!(refused(b"GET / HTTP/2.0\r\nHost: h\r\n\r\n").status(), 505);
    assert!(matches!(
        refused(b"GET / http/1.1\r\nHost: h\r\n\r\n"),
        Refusal::Grammar(_)
    ));
    assert!(matches!(
        refused(b"GET / HTTP/1.10\r\nHost: h\r\n\r\n"),
        Refusal::Grammar(_)
    ));
    assert_eq!(head(b"GET / HTTP/1.0\r\n\r\n").version(), Version::Http10);
}

/// RFC 9112 §3.2, §3.2.2: an HTTP/1.1 request has exactly one valid Host field; an absolute-form
/// target's authority is the host, its path the origin.
#[test]
fn hosts() {
    assert_eq!(refused(b"GET / HTTP/1.1\r\n\r\n"), Refusal::Host);
    assert_eq!(
        refused(b"GET / HTTP/1.1\r\nHost: a\r\nHost: a\r\n\r\n"),
        Refusal::Host
    );
    assert_eq!(
        refused(b"GET / HTTP/1.1\r\nHost: a b\r\n\r\n"),
        Refusal::Host
    );
    assert_eq!(
        refused(b"GET / HTTP/1.0\r\nHost: a\r\nhost: b\r\n\r\n"),
        Refusal::Host
    );
    let request = b"GET http://b.s3.example:9000/k?versions HTTP/1.1\r\nHost: ignored\r\n\r\n";
    let h = head(request);
    assert_eq!(h.host(request), Some(&b"b.s3.example:9000"[..]));
    assert_eq!(h.origin(request), b"/k?versions");
    assert_eq!(h.target(request), b"http://b.s3.example:9000/k?versions");
    let bare = b"OPTIONS * HTTP/1.1\r\nHost: h\r\n\r\n";
    assert_eq!(head(bare).origin(bare), b"*");
    // An empty Host is a valid one, the target's authority undefined (§3.2).
    assert_eq!(
        head(b"GET / HTTP/1.1\r\nHost:\r\n\r\n").framing(),
        Framing::None
    );
}

/// RFC 9110 §10.1.1: `100-continue` is surfaced before any body octet, ignored on HTTP/1.0 and
/// without content; another expectation is 417.
#[test]
fn expectations() {
    let with = |extra: &str, version: &str| {
        parse(
            format!("PUT / {version}\r\nHost: h\r\n{extra}Expect: 100-Continue\r\n\r\n").as_bytes(),
        )
        .map(|h| h.unwrap().continues())
    };
    assert_eq!(with("Content-Length: 9\r\n", "HTTP/1.1"), Ok(true));
    assert_eq!(with("Transfer-Encoding: chunked\r\n", "HTTP/1.1"), Ok(true));
    assert_eq!(with("", "HTTP/1.1"), Ok(false));
    assert_eq!(with("Content-Length: 9\r\n", "HTTP/1.0"), Ok(false));
    assert_eq!(
        refused(b"PUT / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue, x\r\n\r\n"),
        Refusal::Expectation
    );
    assert_eq!(Refusal::Expectation.status(), 417);
}

/// RFC 9112 §9.3: HTTP/1.1 persists unless `close`; HTTP/1.0 only with `keep-alive`.
#[test]
fn persistence() {
    let persists = |extra: &str, version: &str| {
        head(format!("GET / {version}\r\nHost: h\r\n{extra}\r\n").as_bytes()).persists()
    };
    assert!(persists("", "HTTP/1.1"));
    assert!(!persists("Connection: close\r\n", "HTTP/1.1"));
    assert!(!persists("Connection: keep-alive, Close\r\n", "HTTP/1.1"));
    assert!(!persists("", "HTTP/1.0"));
    assert!(persists("Connection: Keep-Alive\r\n", "HTTP/1.0"));
}

/// RFC 9112 §9.3.2: requests sent one after another are taken one at a time, each where the last
/// one's body ended, whatever octets each read brought.
#[test]
fn pipelined_requests_are_taken_one_at_a_time() {
    let stream = b"PUT /a HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabcPUT /b HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nde\r\n0\r\n\r\nGET /c HTTP/1.1\r\nHost: h\r\n\r\n";
    let mut at = 0;
    let mut seen = Vec::new();
    while at < stream.len() {
        let rest = &stream[at..];
        let h = head(rest);
        let (data, used, _) = body(h.framing(), &rest[h.len()..]).unwrap();
        seen.push((h.target(rest).to_vec(), data));
        at += h.len() + used;
    }
    assert_eq!(
        seen,
        vec![
            (b"/a".to_vec(), b"abc".to_vec()),
            (b"/b".to_vec(), b"de".to_vec()),
            (b"/c".to_vec(), Vec::new())
        ]
    );
    // A head arriving an octet at a time is none until whole.
    let one = b"GET /c HTTP/1.1\r\nHost: h\r\n\r\n";
    for end in 0..one.len() {
        assert_eq!(parse(&one[..end]), Ok(None), "{end}");
    }
    assert!(parse(one).unwrap().is_some());
}

/// A body is given out as spans of the input it lies in, however its octets arrive.
#[test]
fn a_body_decodes_the_same_whatever_its_reads() {
    let chunked = b"3\r\nabc\r\n10;e=f\r\n0123456789abcdef\r\n0\r\nT: 1\r\n\r\n";
    let (whole, _, trailers) = body(Framing::Chunked, chunked).unwrap();
    let mut decoder = Body::new(Framing::Chunked);
    let mut out = Vec::new();
    for b in chunked {
        let step = decoder.decode(std::slice::from_ref(b)).unwrap();
        assert!(step.consumed <= 1);
        out.extend_from_slice(&std::slice::from_ref(b)[step.data]);
        if step.consumed == 0 {
            panic!("a byte not taken");
        }
    }
    assert!(decoder.done());
    assert_eq!(out, whole);
    assert_eq!(trailers, vec![(b"T".to_vec(), b"1".to_vec())]);
    let mut length = Body::new(Framing::Length(5));
    let step = length.decode(b"hel").unwrap();
    assert_eq!((step.consumed, step.data, step.end), (3, 0..3, false));
    let step = length.decode(b"loNEXT").unwrap();
    assert_eq!((step.consumed, step.data, step.end), (2, 0..2, true));
    assert!(Body::new(Framing::None).done());
}

/// RFC 9112 §4, §6.1, §7.1, §11.1: a response's head, its length and its chunks, and the values
/// that would split a response refused.
#[test]
fn responses_and_their_framing() {
    let mut out = Vec::new();
    write::head(
        &mut out,
        Version::Http11,
        200,
        "OK",
        &[("ETag", b"\"abc\""), ("x-amz-request-id", b"1")],
        Length::Known(3),
    )
    .unwrap();
    assert_eq!(
        out,
        b"HTTP/1.1 200 OK\r\nETag: \"abc\"\r\nx-amz-request-id: 1\r\nContent-Length: 3\r\n\r\n"
    );
    out.clear();
    write::head(&mut out, Version::Http11, 200, "OK", &[], Length::Chunked).unwrap();
    write::chunk(&mut out, 26);
    out.extend_from_slice(b"abcdefghijklmnopqrstuvwxyz");
    out.extend_from_slice(write::CHUNK_END);
    write::last(&mut out, &[("x-amz-checksum-crc32", b"AAAAAA==")]).unwrap();
    assert_eq!(
        out,
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1a\r\nabcdefghijklmnopqrstuvwxyz\r\n0\r\nx-amz-checksum-crc32: AAAAAA==\r\n\r\n"
    );
    // The codec reads back what it writes.
    let request = head(b"PUT / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n");
    let start = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let (data, _, trailers) = body(request.framing(), &out[start..]).unwrap();
    assert_eq!(data, b"abcdefghijklmnopqrstuvwxyz");
    assert_eq!(trailers.len(), 1);
    let mut out = Vec::new();
    let tries: [Try<'_>; 6] = [
        (
            99,
            "x",
            &[],
            Length::None,
            Version::Http11,
            WriteError::Status,
        ),
        (
            200,
            "O\rK",
            &[],
            Length::None,
            Version::Http11,
            WriteError::Reason,
        ),
        (
            200,
            "OK",
            &[("A B", b"v")],
            Length::None,
            Version::Http11,
            WriteError::Name,
        ),
        (
            200,
            "OK",
            &[("A", b"v\r\nSet-Cookie: x")],
            Length::None,
            Version::Http11,
            WriteError::Value,
        ),
        (
            204,
            "No Content",
            &[],
            Length::Known(0),
            Version::Http11,
            WriteError::Length,
        ),
        (
            200,
            "OK",
            &[],
            Length::Chunked,
            Version::Http10,
            WriteError::Chunked,
        ),
    ];
    for (status, reason, fields, length, version, error) in tries {
        assert_eq!(
            write::head(&mut out, version, status, reason, fields, length),
            Err(error)
        );
    }
    assert!(out.is_empty(), "nothing written for a refused head");
    assert_eq!(write::CONTINUE, b"HTTP/1.1 100 Continue\r\n\r\n");
}
