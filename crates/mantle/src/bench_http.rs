//! `mantle bench http`: mantle's HTTP/1.1 codec on one core (`mantle_http`; node.md §4.1), as the
//! S3 listener runs it: request heads parsed, bodies decoded, and response heads written.
//!
//! The heads are those stock S3 clients send: a PutObject the AWS CLI signs, its body
//! `aws-chunked` with a trailing checksum and framed chunked; a ranged GetObject boto3 signs; and a
//! head at S3's 8 KB bound. Bodies are a mebibyte, framed by Content-Length and read 64 KiB at a
//! time as a TCP read would bring them, or chunked, in the 8 KiB chunks S3 takes as the least
//! (research/05 §1.8) and in 64 KiB ones. A response head is a ranged GetObject's with the fields
//! S3 sends. Each row is operations a second and nanoseconds each: a head's parse or write, a
//! body's read or chunk. A body's octets are given back as spans of the caller's buffer and never
//! touched, so a body costs its reads and chunk lines, not its bytes; a head's row gives its
//! octets a second as well.

use std::io::Write;
use std::time::{Duration, Instant};

use mantle_http::write::{self, Length};
use mantle_http::{Body, Framing, MAX_HEAD, Refusal, Version, parse};

use crate::display;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Http(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Http(e) => write!(f, "the codec: {e}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

impl From<Refusal> for Error {
    fn from(e: Refusal) -> Self {
        Self::Http(e.to_string())
    }
}

/// A PutObject the AWS CLI signs, its body `aws-chunked` with a trailing CRC-64/NVME and framed
/// chunked.
const PUT: &[u8] = b"PUT /bucket/some/key.bin HTTP/1.1\r\n\
Host: 127.0.0.1:9000\r\n\
Accept-Encoding: identity\r\n\
Content-Type: application/octet-stream\r\n\
User-Agent: aws-cli/2.27.0 md/awscrt#0.26.1 ua/2.1 os/macos#25.4.0 md/arch#arm64 lang/python#3.13.3\r\n\
Expect: 100-continue\r\n\
X-Amz-Date: 20261025T120000Z\r\n\
X-Amz-Content-SHA256: STREAMING-UNSIGNED-PAYLOAD-TRAILER\r\n\
X-Amz-Trailer: x-amz-checksum-crc64nvme\r\n\
X-Amz-Decoded-Content-Length: 1048576\r\n\
Content-Encoding: aws-chunked\r\n\
amz-sdk-invocation-id: 0c5dd5a3-6a8e-4c7b-9b8f-6d2a8a7a1f3c\r\n\
amz-sdk-request: attempt=1; max=3\r\n\
Authorization: AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20261025/us-east-1/s3/aws4_request, SignedHeaders=content-encoding;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer, Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\r\n\
Transfer-Encoding: chunked\r\n\
\r\n";

/// A ranged GetObject boto3 signs.
const GET: &[u8] = b"GET /bucket/some/key.bin HTTP/1.1\r\n\
Host: 127.0.0.1:9000\r\n\
Accept-Encoding: identity\r\n\
Range: bytes=0-1048575\r\n\
User-Agent: Boto3/1.40.0 md/Botocore#1.40.0 ua/2.1 os/linux#6.8.0 md/arch#x86_64 lang/python#3.12.3\r\n\
X-Amz-Date: 20261025T120000Z\r\n\
X-Amz-Content-SHA256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\r\n\
amz-sdk-invocation-id: 9b2c8a1e-0f4d-4c6b-8a7e-3d2f1e0c9b8a\r\n\
amz-sdk-request: attempt=1\r\n\
Authorization: AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20261025/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210\r\n\
\r\n";

/// A head of exactly [`MAX_HEAD`] octets, its fields 64-octet values.
fn largest() -> Vec<u8> {
    let mut head = b"GET /bucket/key HTTP/1.1\r\nHost: h\r\n".to_vec();
    let field = |n: usize| format!("X-Amz-Meta-{n:05}: {}\r\n", "v".repeat(64)).into_bytes();
    let mut n = 0usize;
    // Fields while one more and the empty line fit.
    while head.len().saturating_add(field(n).len()).saturating_add(2) <= MAX_HEAD {
        head.extend(field(n));
        n = n.saturating_add(1);
    }
    // The last field's value lengthened to fill the bound exactly.
    let pad = MAX_HEAD.saturating_sub(head.len()).saturating_sub(2);
    let at = head.len().saturating_sub(2);
    head.splice(at..at, std::iter::repeat_n(b'p', pad));
    head.extend_from_slice(b"\r\n");
    head
}

/// A mebibyte's body, chunked in `chunk`-octet chunks with an empty trailer section.
fn chunked(chunk: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut left = 1usize << 20;
    while left > 0 {
        let len = chunk.min(left);
        write::chunk(&mut out, len);
        out.extend(std::iter::repeat_n(0xA5, len));
        out.extend_from_slice(write::CHUNK_END);
        left = left.saturating_sub(len);
    }
    // A last chunk with no trailers is never refused.
    let _ = write::last(&mut out, &[]);
    out
}

pub fn http(out: &mut impl Write, step: Duration) -> Result<(), Error> {
    writeln!(
        out,
        "  {:<44} {:>10} {:>11} {:>12}",
        "operation", "a second", "each", "head bytes"
    )?;
    let largest = largest();
    for (what, head) in [
        ("parse a PutObject head", PUT),
        ("parse a ranged GetObject head", GET),
        ("parse a head of 8 KiB", largest.as_slice()),
    ] {
        let (rate, ns) = run(step, || parse(head).map(|h| h.map(|h| h.len())))?;
        // usize -> f64 rounds above 2^53, far beyond a head's 8 KiB.
        row(out, what, rate, ns, Some(rate * head.len() as f64))?;
    }
    let length = vec![0x5Au8; 1 << 20];
    let (rate, ns) = run(step, || {
        let mut body = Body::new(Framing::Length(1 << 20));
        let mut taken = 0usize;
        for read in length.chunks(64 << 10) {
            taken = taken.saturating_add(body.decode(read)?.data.len());
        }
        Ok(taken)
    })?;
    // Sixteen reads a mebibyte.
    row(
        out,
        "a Content-Length body's 64 KiB read",
        rate * 16.0,
        ns / 16.0,
        None,
    )?;
    for chunk in [8usize << 10, 64 << 10] {
        let input = chunked(chunk);
        let (rate, ns) = run(step, || {
            let mut body = Body::new(Framing::Chunked);
            let (mut at, mut taken) = (0usize, 0usize);
            while !body.done() {
                let step = body.decode(input.get(at..).unwrap_or_default())?;
                taken = taken.saturating_add(step.data.len());
                at = at.saturating_add(step.consumed);
                if step.consumed == 0 && !step.end {
                    return Err(Refusal::Chunked("a body that ended short"));
                }
            }
            Ok(taken)
        })?;
        // usize -> f64 rounds above 2^53, far beyond a mebibyte's chunks.
        let chunks = (1usize << 20).checked_div(chunk).unwrap_or(1) as f64;
        row(
            out,
            &format!("a chunked body's {} chunk", display::size(chunk)),
            rate * chunks,
            ns / chunks,
            None,
        )?;
    }
    let mut response = Vec::with_capacity(512);
    let fields: [(&str, &[u8]); 7] = [
        ("Accept-Ranges", b"bytes"),
        ("Content-Range", b"bytes 0-1048575/8388608"),
        ("Content-Type", b"application/octet-stream"),
        ("Date", b"Sun, 25 Oct 2026 12:00:00 GMT"),
        ("ETag", b"\"d41d8cd98f00b204e9800998ecf8427e-3\""),
        ("Last-Modified", b"Sun, 25 Oct 2026 11:59:00 GMT"),
        ("x-amz-request-id", b"0123456789ABCDEF"),
    ];
    let (rate, ns) = run(step, || {
        response.clear();
        write::head(
            &mut response,
            Version::Http11,
            206,
            "Partial Content",
            &fields,
            Length::Known(1 << 20),
        )
        .map_err(|_| Refusal::Grammar("a response head"))?;
        Ok(response.len())
    })?;
    row(out, "write a ranged GetObject's head", rate, ns, None)?;
    Ok(())
}

fn row(
    out: &mut impl Write,
    what: &str,
    rate: f64,
    ns: f64,
    bytes: Option<f64>,
) -> Result<(), Error> {
    writeln!(
        out,
        "  {:<44} {:>10} {:>11} {:>12}",
        what,
        display::count(rate),
        format!("{ns:.0} ns"),
        bytes.map_or_else(|| "-".to_owned(), display::rate),
    )?;
    out.flush()?;
    Ok(())
}

/// Operations a second and nanoseconds each over repeated runs of `f` for `step`; each run's
/// answer is kept from the optimizer.
fn run<T>(step: Duration, mut f: impl FnMut() -> Result<T, Refusal>) -> Result<(f64, f64), Error> {
    let started = Instant::now();
    let mut runs = 0u64;
    while runs == 0 || started.elapsed() < step {
        std::hint::black_box(f()?);
        runs = runs.saturating_add(1);
    }
    let secs = started.elapsed().as_secs_f64();
    // u64 -> f64 rounds above 2^53, far beyond any count a bounded run produces.
    let rate = runs as f64 / secs;
    Ok((rate, 1e9 / rate))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_row_is_reported_and_the_largest_head_is_the_bound() {
        assert_eq!(largest().len(), MAX_HEAD);
        assert!(parse(&largest()).unwrap().is_some());
        let mut out = Vec::new();
        http(&mut out, Duration::from_millis(1)).unwrap();
        let text = String::from_utf8(out).unwrap();
        for row in [
            "PutObject",
            "GetObject head",
            "8 KiB",
            "Content-Length",
            "8 KiB chunk",
            "64 KiB chunk",
            "write",
        ] {
            assert!(text.contains(row), "{row}: {text}");
        }
    }
}
