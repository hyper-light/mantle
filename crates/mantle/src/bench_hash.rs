//! `mantle bench hash`: the gateway's per-byte and per-request cryptography on one core.
//!
//! Every byte of an upload is hashed for its checksum and, for its ETag, with MD5
//! (docs/research/05 §3, §5.1); a body of signed chunks is also hashed chunk by chunk with
//! SHA-256, and each chunk's HMAC-SHA256 signature checked (05 §1.8). The rate of each
//! algorithm at each buffer size bounds the upload bytes one core can accept, and the time
//! to verify a request's signature bounds the requests it can admit. The sizes run from S3's
//! smallest chunk of a chunked body to a large part.

use std::io::Write;
use std::time::{Duration, Instant};

use mantle_disk::measure::SplitMix64;
use mantle_s3::checksum::{self, Algorithm};
use mantle_s3::chunked::{self, ChunkError, Decoder};
use mantle_s3::crypto::CryptoError;
use mantle_s3::sigv4::{self, AuthError, Request};

use crate::display;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Auth(AuthError),
    Chunk(ChunkError),
    Crypto(CryptoError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Auth(e) => write!(f, "verifying a signature: {e}"),
            Self::Chunk(e) => write!(f, "decoding signed chunks: {e}"),
            Self::Crypto(e) => write!(f, "hashing: {e}"),
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

/// AWS's example PUT (S3 developer guide, "Signature Calculation: Transfer Payload in a Single
/// Chunk"; 05 §1.5), whose signature verifies at `WHEN` with `SECRET`.
const KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const WHEN: &str = "20130524T000000Z";
const PUT_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd";

/// A chunked body measured is this long, or one chunk when chunks are longer.
const CHUNKED_BODY: usize = 16 << 20;

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
    Ok(())
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
    }
}
