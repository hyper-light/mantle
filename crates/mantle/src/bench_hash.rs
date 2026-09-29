//! `mantle bench hash`: the gateway's per-byte and per-request cryptography and framing on one
//! core.
//!
//! Every byte of an upload is hashed for its checksum and, for its ETag, with MD5
//! (docs/research/05 §3, §5.1); a body of signed chunks is also hashed chunk by chunk with
//! SHA-256, and each chunk's HMAC-SHA256 signature checked (05 §1.8); a browser's upload is
//! searched for the delimiter that ends its file (docs/research/19 §5). The rate of each at each
//! buffer size bounds the upload bytes one core can accept, and the time to verify a request's
//! signature, or a form's policy, bounds the requests it can admit. The sizes run from S3's
//! smallest chunk of a chunked body to a large part.

use std::io::Write;
use std::time::{Duration, Instant};

use mantle_disk::measure::SplitMix64;
use mantle_s3::checksum::{self, Algorithm};
use mantle_s3::chunked::{self, ChunkError, Decoder};
use mantle_s3::crypto::CryptoError;
use mantle_s3::form::{self, FormError};
use mantle_s3::post::{self, PostError};
use mantle_s3::sigv4::{self, AuthError, Request};

use crate::display;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Auth(AuthError),
    Chunk(ChunkError),
    Crypto(CryptoError),
    Form(FormError),
    Post(PostError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Auth(e) => write!(f, "verifying a signature: {e}"),
            Self::Chunk(e) => write!(f, "decoding signed chunks: {e}"),
            Self::Crypto(e) => write!(f, "hashing: {e}"),
            Self::Form(e) => write!(f, "decoding a form: {e:?}"),
            Self::Post(e) => write!(f, "checking a form's policy: {e}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

impl From<AuthError> for Error {
    fn from(e: AuthError) -> Self {
        Self::Auth(e)
    }
}

impl From<ChunkError> for Error {
    fn from(e: ChunkError) -> Self {
        Self::Chunk(e)
    }
}

impl From<CryptoError> for Error {
    fn from(e: CryptoError) -> Self {
        Self::Crypto(e)
    }
}

impl From<FormError> for Error {
    fn from(e: FormError) -> Self {
        Self::Form(e)
    }
}

impl From<PostError> for Error {
    fn from(e: PostError) -> Self {
        Self::Post(e)
    }
}

/// AWS's example PUT (S3 developer guide, "Signature Calculation: Transfer Payload in a Single
/// Chunk"; 05 §1.5), whose signature verifies at `WHEN` with `SECRET`.
const KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const WHEN: &str = "20130524T000000Z";
const PUT_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd";

/// A chunked body measured is this long, or one chunk when chunks are longer.
const CHUNKED_BODY: usize = 16 << 20;

/// A form's file measured is this long, as a chunked body is.
const FORM_FILE: usize = 16 << 20;

/// A boundary as clients draw one, 32 hex digits (docs/research/19 §5.5).
const BOUNDARY: &str = "7f3a9c1e5b2d4f60a8c3e1b5d7f9a2c4";

/// AWS's SigV4 POST example (docs/research/19 §3.3): its policy's base64, whose signature
/// verifies with `SECRET` until the policy expires at 2015-12-30T12:00:00Z.
const POST_POLICY: &str = "eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLA0KICAiY29uZGl0aW9ucyI6IFsNCiAgICB7ImJ1Y2tldCI6ICJzaWd2NGV4YW1wbGVidWNrZXQifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS8iXSwNCiAgICB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LA0KICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL3NpZ3Y0ZXhhbXBsZWJ1Y2tldC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRDb250ZW50LVR5cGUiLCAiaW1hZ2UvIl0sDQogICAgeyJ4LWFtei1tZXRhLXV1aWQiOiAiMTQzNjUxMjM2NTEyNzQifSwNCiAgICB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sDQogICAgWyJzdGFydHMtd2l0aCIsICIkeC1hbXotbWV0YS10YWciLCAiIl0sDQoNCiAgICB7IngtYW16LWNyZWRlbnRpYWwiOiAiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAxNTEyMjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LA0KICAgIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwNCiAgICB7IngtYW16LWRhdGUiOiAiMjAxNTEyMjlUMDAwMDAwWiIgfQ0KICBdDQp9";
const POST_SIGNATURE: &str = "8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e";

pub fn hash(out: &mut impl Write, sizes: &[usize], step: Duration) -> Result<(), Error> {
    write!(out, "  {:<10}", "algorithm")?;
    for &size in sizes {
        write!(out, " {:>12}", display::size(size))?;
    }
    writeln!(out)?;
    let largest = sizes.iter().copied().max().unwrap_or(0);
    let mut data = vec![0u8; largest];
    SplitMix64::new(u64::try_from(largest).unwrap_or(0)).fill(&mut data);
    for algorithm in Algorithm::ALL {
        write!(out, "  {:<10}", algorithm.name())?;
        for &size in sizes {
            let buf = data.get(..size).unwrap_or(&data);
            let run = repeat(step, || {
                std::hint::black_box(checksum::checksum(algorithm, std::hint::black_box(buf))?);
                Ok(())
            })?;
            write!(out, " {:>12}", display::rate(run.per_second(size)))?;
        }
        writeln!(out)?;
        out.flush()?;
    }

    let when = mantle_s3::time::parse_amz_date(WHEN).unwrap_or(0);
    let headers = [
        ("Host", "examplebucket.s3.amazonaws.com"),
        ("Date", "Fri, 24 May 2013 00:00:00 GMT"),
        ("x-amz-date", WHEN),
        ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        (
            "x-amz-content-sha256",
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
        ),
        ("Authorization", PUT_AUTHORIZATION),
    ];
    let request = Request {
        method: "PUT",
        path: "/test$file.text",
        query: "",
        headers: &headers,
    };
    let secret = |key: &str| (key == KEY).then(|| SECRET.to_owned());
    let run = repeat(step, || {
        std::hint::black_box(sigv4::verify(&request, "us-east-1", when, secret)?);
        Ok(())
    })?;
    writeln!(
        out,
        "  a request's signature: {} verified/s, {} each",
        display::count(run.per_second(1)),
        display::nanos(run.nanos_each()),
    )?;

    let min = usize::try_from(chunked::MIN_CHUNK).unwrap_or(usize::MAX);
    for &size in sizes.iter().filter(|&&size| size >= min) {
        let Some(chunk) = data.get(..size) else {
            continue;
        };
        let count = CHUNKED_BODY.checked_div(size).unwrap_or(1).max(1);
        let body = signed_chunks(chunk, count)?;
        let len = size.saturating_mul(count);
        let declared = u64::try_from(len).unwrap_or(u64::MAX);
        let mut payload = Vec::with_capacity(len);
        let run = repeat(step, || {
            payload.clear();
            let mut decoder = Decoder::signed(chain()?, None, declared);
            decoder.feed(&body, &mut payload)?;
            decoder.finish()?;
            Ok(())
        })?;
        writeln!(
            out,
            "  signed chunks of {}: {}",
            display::size(size),
            display::rate(run.per_second(len))
        )?;
        out.flush()?;
    }

    // Random bytes hold a CR every 256 on average, each a place the delimiter could begin.
    let mut file = vec![0u8; FORM_FILE];
    SplitMix64::new(u64::try_from(FORM_FILE).unwrap_or(0)).fill(&mut file);
    let content_type = format!("multipart/form-data; boundary={BOUNDARY}");
    let body = form_body(&post_fields(), &file);
    let mut piece_out = Vec::new();
    for &size in sizes {
        let run = repeat(step, || {
            let mut decoder = form::Decoder::new(Some(&content_type))?;
            for piece in body.chunks(size.max(1)) {
                piece_out.clear();
                decoder.feed(piece, &mut piece_out)?;
            }
            decoder.finish()?;
            Ok(())
        })?;
        writeln!(
            out,
            "  a form's file in pieces of {}: {}",
            display::size(size),
            display::rate(run.per_second(FORM_FILE))
        )?;
        out.flush()?;
    }

    let mut decoder = form::Decoder::new(Some(&content_type))?;
    decoder.feed(&form_body(&post_fields(), b"jpeg"), &mut piece_out)?;
    let (form, _) = decoder.finish()?;
    let noon = mantle_s3::time::parse_iso8601("2015-12-29T12:00:00Z")
        .map_or(0, |(seconds, _)| seconds.saturating_mul(1000));
    let run = repeat(step, || {
        std::hint::black_box(post::authorize(
            &form,
            "sigv4examplebucket",
            "us-east-1",
            noon,
            secret,
        )?);
        Ok(())
    })?;
    writeln!(
        out,
        "  a form's policy: {} verified/s, {} each",
        display::count(run.per_second(1)),
        display::nanos(run.nanos_each()),
    )?;
    Ok(())
}

/// The fields of AWS's example form (docs/research/19 §3.3), which its policy covers.
fn post_fields() -> [(&'static str, &'static str); 12] {
    [
        ("key", "user/user1/${filename}"),
        ("acl", "public-read"),
        (
            "success_action_redirect",
            "http://sigv4examplebucket.s3.amazonaws.com/successful_upload.html",
        ),
        ("Content-Type", "image/jpeg"),
        ("x-amz-meta-uuid", "14365123651274"),
        ("x-amz-server-side-encryption", "AES256"),
        (
            "X-Amz-Credential",
            "AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request",
        ),
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Date", "20151229T000000Z"),
        ("x-amz-meta-tag", ""),
        ("Policy", POST_POLICY),
        ("X-Amz-Signature", POST_SIGNATURE),
    ]
}

/// A form body as a browser sends one: `fields`, then `file`, then a submit button's field.
fn form_body(fields: &[(&str, &str)], file: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(file.len().saturating_add(4096));
    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"photo.jpg\"\r\nContent-Type: image/jpeg\r\n\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(file);
    body.extend_from_slice(
        format!("\r\n--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"submit\"\r\n\r\nUpload\r\n--{BOUNDARY}--\r\n")
            .as_bytes(),
    );
    body
}

/// The chain a chunked upload's seed signature starts (05 §1.8); any seed serves, as the
/// body is signed and verified from the same one.
fn chain() -> Result<sigv4::Chain, AuthError> {
    sigv4::client_chain(SECRET, WHEN, "20130524", "us-east-1", [0x4f; 32])
}

/// `chunk` sent `count` times as signed chunks, then the final empty chunk.
fn signed_chunks(chunk: &[u8], count: usize) -> Result<Vec<u8>, Error> {
    let sha256 = |data: &[u8]| -> Result<[u8; 32], Error> {
        let digest = checksum::checksum(Algorithm::Sha256, data)?;
        Ok(digest.bytes.as_slice().try_into().unwrap_or([0; 32]))
    };
    let mut chain = chain()?;
    let mut body = Vec::new();
    let hash = sha256(chunk)?;
    for _ in 0..count {
        let signature = sigv4::hex(&chain.sign_chunk(&hash)?);
        body.extend_from_slice(
            format!("{:x};chunk-signature={signature}\r\n", chunk.len()).as_bytes(),
        );
        body.extend_from_slice(chunk);
        body.extend_from_slice(b"\r\n");
    }
    let signature = sigv4::hex(&chain.sign_chunk(&sha256(&[])?)?);
    body.extend_from_slice(format!("0;chunk-signature={signature}\r\n\r\n").as_bytes());
    Ok(body)
}

/// Repeated runs of one operation.
struct Run {
    runs: u64,
    elapsed: Duration,
}

impl Run {
    /// Units per second when each run handles `units`.
    fn per_second(&self, units: usize) -> f64 {
        // u64 -> f64 rounds above 2^53, far beyond any count a bounded run produces.
        self.runs as f64 * units as f64 / self.elapsed.as_secs_f64()
    }

    fn nanos_each(&self) -> u64 {
        let each = self
            .elapsed
            .as_nanos()
            .checked_div(u128::from(self.runs))
            .unwrap_or(0);
        u64::try_from(each).unwrap_or(u64::MAX)
    }
}

/// Runs `f` repeatedly for `step`, at least once.
fn repeat(step: Duration, mut f: impl FnMut() -> Result<(), Error>) -> Result<Run, Error> {
    let started = Instant::now();
    let mut runs = 0u64;
    while runs == 0 || started.elapsed() < step {
        f()?;
        runs = runs.saturating_add(1);
    }
    Ok(Run {
        runs,
        elapsed: started.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_algorithm_and_measure_is_reported() {
        let mut out = Vec::new();
        hash(&mut out, &[4096, 8192], Duration::from_millis(1)).unwrap();
        let text = String::from_utf8(out).unwrap();
        for algorithm in Algorithm::ALL {
            assert!(text.contains(algorithm.name()), "{text}");
        }
        assert!(text.contains("verified/s"), "{text}");
        assert!(text.contains("signed chunks of 8 KiB"), "{text}");
        assert!(!text.contains("signed chunks of 4 KiB"), "{text}");
        assert!(text.contains("a form's file in pieces of 4 KiB"), "{text}");
        assert!(text.contains("a form's policy"), "{text}");
    }
}
