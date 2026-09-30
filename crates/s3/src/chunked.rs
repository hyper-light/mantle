//! Decoding `aws-chunked` bodies (docs/research/05 §1.8–§1.9).
//!
//! A body is a sequence of chunks, `<hex size>[;chunk-signature=<sig>]\r\n<data>\r\n`, ended by
//! a zero-size chunk and a trailer section, as in HTTP/1.1 chunked coding (RFC 9112 §7.1):
//! trailer fields, then an empty line. With signed chunks every chunk's signature chains from
//! the request's seed signature, and a trailing checksum is signed too; with unsigned chunks
//! the trailing checksum is the only protection of the body, and the caller must verify it.
//!
//! The decoder is fed the body as it arrives and holds at most one line of framing, whose
//! longest legal form bounds it (`MAX_LINE`).

use crate::crypto::{self, CryptoError};
use crate::sigv4::{AuthError, Chain, hex_value, unhex};

/// "The chunk size must be at least 8 KB ... except the last one" (S3 developer guide,
/// Transfer Payload in Multiple Chunks; 05 §1.8). The rule also bounds the signature checks a
/// sender can force per byte.
pub const MIN_CHUNK: u64 = 8192;

/// The longest framing line: a signed chunk header, 16 hex digits of size, the 17 bytes of
/// `;chunk-signature=` and 64 of signature. Trailer lines are shorter: the longest checksum
/// field is `x-amz-checksum-crc64nvme:` with 44 base64 characters of SHA-256, and the trailer
/// signature field 24 + 64 bytes.
pub const MAX_LINE: usize = 16 + 17 + 64;

/// The trailing checksum a body ended with, still base64 as sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trailer {
    /// Its field name, lowercase, e.g. `x-amz-checksum-crc32c`.
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChunkError {
    #[error("malformed chunk framing: {0}")]
    Framing(&'static str),
    #[error("a chunk other than the last is under 8 KiB")]
    ShortChunk,
    #[error("the body is shorter than x-amz-decoded-content-length")]
    Incomplete,
    #[error("the body is longer than x-amz-decoded-content-length")]
    TooLong,
    #[error("the trailer is missing or does not match x-amz-trailer")]
    Trailer,
    #[error(transparent)]
    Signature(#[from] AuthError),
    #[error("a chunk could not be hashed: {0}")]
    Internal(#[from] CryptoError),
}

impl ChunkError {
    /// The S3 error code and HTTP status. AWS documents no code for malformed framing or
    /// short chunks (05 §1.8, UNVERIFIED), so those use `InvalidRequest`.
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::Incomplete => ("IncompleteBody", 400),
            Self::Signature(e) => e.code(),
            Self::Framing(_) | Self::ShortChunk | Self::TooLong | Self::Trailer => {
                ("InvalidRequest", 400)
            }
            Self::Internal(_) => ("InternalError", 500),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Reading a chunk's size line.
    Header,
    /// Reading this many more data bytes of the current chunk.
    Data(u64),
    /// Reading the CRLF after a chunk's data.
    DataEnd,
    /// Reading trailer fields until an empty line.
    Trailer,
    Done,
}

pub struct Decoder {
    /// The signature chain, for signed chunks.
    chain: Option<Chain>,
    /// The trailing field `x-amz-trailer` announced, lowercase.
    expect: Option<String>,
    declared: u64,
    decoded: u64,
    state: State,
    line: Vec<u8>,
    /// The SHA-256 of the current chunk's data, for signed chunks.
    hasher: Option<crypto::Digest>,
    signature: [u8; 32],
    /// A data chunk under `MIN_CHUNK` was seen, so the next chunk must be the last.
    short: bool,
    trailer: Option<Trailer>,
    trailer_signature: Option<[u8; 32]>,
}

impl Decoder {
    /// A body of signed chunks; `trailer` is the field `x-amz-trailer` names, if any.
    pub fn signed(chain: Chain, trailer: Option<&str>, declared: u64) -> Self {
        Self::new(Some(chain), trailer, declared)
    }

    /// A body of unsigned chunks, which must end with the trailing checksum `trailer` names.
    pub fn unsigned(trailer: &str, declared: u64) -> Self {
        Self::new(None, Some(trailer), declared)
    }

    fn new(chain: Option<Chain>, trailer: Option<&str>, declared: u64) -> Self {
        Self {
            chain,
            expect: trailer.map(|t| t.trim().to_ascii_lowercase()),
            declared,
            decoded: 0,
            state: State::Header,
            line: Vec::new(),
            hasher: None,
            signature: [0; 32],
            short: false,
            trailer: None,
            trailer_signature: None,
        }
    }

    /// Consumes `input`, appending the payload bytes it carries to `out`.
    pub fn feed(&mut self, mut input: &[u8], out: &mut Vec<u8>) -> Result<(), ChunkError> {
        while let Some((&first, rest)) = input.split_first() {
            match self.state {
                State::Data(left) => {
                    let take = usize::try_from(left).unwrap_or(usize::MAX).min(input.len());
                    let (data, rest) = input
                        .split_at_checked(take)
                        .ok_or(ChunkError::Framing("a chunk's data overran its input"))?;
                    if let Some(hasher) = &mut self.hasher {
                        hasher.update(data)?;
                    }
                    out.extend_from_slice(data);
                    let taken = u64::try_from(take).unwrap_or(u64::MAX);
                    self.decoded = self.decoded.saturating_add(taken);
                    if self.decoded > self.declared {
                        return Err(ChunkError::TooLong);
                    }
                    let left = left.saturating_sub(taken);
                    self.state = if left == 0 {
                        State::DataEnd
                    } else {
                        State::Data(left)
                    };
                    input = rest;
                }
                State::Done => {
                    // Clients differ in how many line ends close a trailer (05 §1.9).
                    if first != b'\r' && first != b'\n' {
                        return Err(ChunkError::Framing("bytes after the end of the body"));
                    }
                    input = rest;
                }
                State::Header | State::DataEnd | State::Trailer => {
                    input = rest;
                    if first != b'\n' {
                        if self.line.len() >= MAX_LINE {
                            return Err(ChunkError::Framing("a framing line is too long"));
                        }
                        self.line.push(first);
                        continue;
                    }
                    let mut line = std::mem::take(&mut self.line);
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    self.line_done(&line)?;
                    line.clear();
                    self.line = line;
                }
            }
        }
        Ok(())
    }

    fn line_done(&mut self, line: &[u8]) -> Result<(), ChunkError> {
        match self.state {
            State::Header => self.header(line),
            State::DataEnd => {
                if !line.is_empty() {
                    return Err(ChunkError::Framing(
                        "a chunk's data is longer than its size",
                    ));
                }
                self.chunk_done()?;
                self.state = State::Header;
                Ok(())
            }
            State::Trailer => self.trailer_line(line),
            State::Data(_) | State::Done => Ok(()),
        }
    }

    fn header(&mut self, line: &[u8]) -> Result<(), ChunkError> {
        let text = std::str::from_utf8(line).map_err(|_| ChunkError::Framing("not text"))?;
        let (size, signature) = match (&self.chain, text.split_once(';')) {
            (Some(_), Some((size, ext))) => {
                let sig = ext
                    .strip_prefix("chunk-signature=")
                    .and_then(unhex)
                    .ok_or(ChunkError::Framing("no chunk-signature"))?;
                (size, sig)
            }
            (Some(_), None) => return Err(ChunkError::Framing("no chunk-signature")),
            (None, None) => (text, [0; 32]),
            (None, Some(_)) => {
                return Err(ChunkError::Framing("an unsigned chunk has an extension"));
            }
        };
        if size.is_empty() || size.len() > 16 {
            return Err(ChunkError::Framing(
                "a chunk size is not 1 to 16 hex digits",
            ));
        }
        // RFC 9112 §7.1: chunk-size = 1*HEXDIG, so no sign.
        let size =
            hex_value(size.as_bytes()).ok_or(ChunkError::Framing("a chunk size is not hex"))?;
        self.signature = signature;
        self.hasher = match self.chain {
            Some(_) => Some(crypto::Digest::new(&crypto::SHA256)?),
            None => None,
        };
        if size == 0 {
            self.chunk_done()?;
            self.state = State::Trailer;
            return Ok(());
        }
        if self.short {
            return Err(ChunkError::ShortChunk);
        }
        self.short = size < MIN_CHUNK;
        self.state = State::Data(size);
        Ok(())
    }

    /// Checks the signature of the chunk just read, when chunks are signed.
    fn chunk_done(&mut self) -> Result<(), ChunkError> {
        if let Some(chain) = &mut self.chain {
            let hasher = self
                .hasher
                .take()
                .ok_or(ChunkError::Framing("a chunk ended that did not begin"))?;
            chain.verify_chunk(&hasher.finish_array()?, &self.signature)?;
        }
        Ok(())
    }

    fn trailer_line(&mut self, line: &[u8]) -> Result<(), ChunkError> {
        if line.is_empty() {
            return self.trailer_done();
        }
        let text = std::str::from_utf8(line).map_err(|_| ChunkError::Framing("not text"))?;
        let (name, value) = text
            .split_once(':')
            .ok_or(ChunkError::Framing("a trailer field has no colon"))?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        if name == "x-amz-trailer-signature" {
            if self.chain.is_none() || self.trailer_signature.is_some() {
                return Err(ChunkError::Trailer);
            }
            self.trailer_signature = Some(unhex(value).ok_or(ChunkError::Trailer)?);
            return Ok(());
        }
        // "Only one trailing chunk is allowed" (05 §1.9), and it must be the one announced.
        if self.trailer.is_some() || self.expect.as_deref() != Some(name.as_str()) {
            return Err(ChunkError::Trailer);
        }
        self.trailer = Some(Trailer {
            name,
            value: value.to_owned(),
        });
        Ok(())
    }

    fn trailer_done(&mut self) -> Result<(), ChunkError> {
        match (&self.expect, &self.trailer) {
            (None, None) => {}
            (Some(_), Some(trailer)) => {
                if let Some(chain) = &mut self.chain {
                    let signature = self.trailer_signature.ok_or(ChunkError::Trailer)?;
                    let canonical = format!("{}:{}\n", trailer.name, trailer.value);
                    chain.verify_trailer(canonical.as_bytes(), &signature)?;
                }
            }
            _ => return Err(ChunkError::Trailer),
        }
        self.state = State::Done;
        Ok(())
    }

    /// Ends the body: every chunk read, its length as declared; returns the trailer, if any.
    pub fn finish(self) -> Result<Option<Trailer>, ChunkError> {
        if self.state != State::Done {
            return Err(ChunkError::Incomplete);
        }
        if self.decoded != self.declared {
            return Err(ChunkError::Incomplete);
        }
        Ok(self.trailer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigv4::{client_chain, hex};
    use proptest::prelude::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const WHEN: &str = "20130524T000000Z";

    fn chain(seed: [u8; 32]) -> Chain {
        client_chain(SECRET, WHEN, "20130524", "us-east-1", seed).unwrap()
    }

    /// Frames `data` as signed chunks of `sizes` (then the rest), with an optional trailer.
    fn encode_signed(
        data: &[u8],
        sizes: &[usize],
        trailer: Option<(&str, &str)>,
        seed: [u8; 32],
    ) -> Vec<u8> {
        let mut chain = chain(seed);
        let mut out = Vec::new();
        let mut at = 0;
        let mut pieces: Vec<&[u8]> = Vec::new();
        for &size in sizes {
            let end = (at + size).min(data.len());
            if end > at {
                pieces.push(&data[at..end]);
            }
            at = end;
        }
        if at < data.len() {
            pieces.push(&data[at..]);
        }
        for piece in pieces {
            let sig = chain.sign_chunk(&crypto::sha256(piece).unwrap()).unwrap();
            out.extend_from_slice(
                format!("{:x};chunk-signature={}\r\n", piece.len(), hex(&sig)).as_bytes(),
            );
            out.extend_from_slice(piece);
            out.extend_from_slice(b"\r\n");
        }
        let zero = chain.sign_chunk(&crypto::sha256(&[]).unwrap()).unwrap();
        out.extend_from_slice(format!("0;chunk-signature={}\r\n", hex(&zero)).as_bytes());
        if let Some((name, value)) = trailer {
            let sig = chain
                .sign_trailer(format!("{name}:{value}\n").as_bytes())
                .unwrap();
            out.extend_from_slice(
                format!(
                    "{name}:{value}\r\nx-amz-trailer-signature:{}\r\n",
                    hex(&sig)
                )
                .as_bytes(),
            );
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    fn seed() -> [u8; 32] {
        unhex("4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9").unwrap()
    }

    /// AWS's example body: 66560 bytes of `a` in chunks of 65536 and 1024, then the zero
    /// chunk, 66824 bytes in all, with AWS's chunk signatures (05 §1.8).
    #[test]
    fn aws_chunked_body_decodes() {
        let data = vec![b'a'; 66560];
        let body = encode_signed(&data, &[65536, 1024], None, seed());
        assert_eq!(body.len(), 66824);
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains(
            "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
        ));
        assert!(text.contains(
            "0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
        ));
        let mut d = Decoder::signed(chain(seed()), None, 66560);
        let mut out = Vec::new();
        for piece in body.chunks(777) {
            d.feed(piece, &mut out).unwrap();
        }
        assert_eq!(d.finish().unwrap(), None);
        assert_eq!(out, data);
    }

    /// AWS's example with a trailing CRC-32C, 66946 bytes on the wire (05 §1.9).
    #[test]
    fn aws_trailer_body_decodes() {
        let data = vec![b'a'; 66560];
        let seed =
            unhex("106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e").unwrap();
        let body = encode_signed(
            &data,
            &[65536, 1024],
            Some(("x-amz-checksum-crc32c", "sOO8/Q==")),
            seed,
        );
        assert_eq!(body.len(), 66946);
        assert!(String::from_utf8_lossy(&body).contains("x-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435"));
        let mut d = Decoder::signed(chain(seed), Some("x-amz-checksum-crc32c"), 66560);
        let mut out = Vec::new();
        d.feed(&body, &mut out).unwrap();
        assert_eq!(
            d.finish().unwrap(),
            Some(Trailer {
                name: "x-amz-checksum-crc32c".into(),
                value: "sOO8/Q==".into()
            })
        );
        assert_eq!(out, data);
    }

    #[test]
    fn a_changed_byte_fails_its_chunk_signature() {
        let data = vec![b'a'; 66560];
        let mut body = encode_signed(&data, &[65536, 1024], None, seed());
        body[1000] = b'b';
        let mut d = Decoder::signed(chain(seed()), None, 66560);
        let result = d.feed(&body, &mut Vec::new());
        assert!(matches!(
            result,
            Err(ChunkError::Signature(AuthError::Mismatch))
        ));
    }

    #[test]
    fn framing_rules_are_enforced() {
        let data = vec![7u8; 20_000];
        // A short chunk that is not the last.
        let body = encode_signed(&data, &[100, 8192], None, seed());
        let mut d = Decoder::signed(chain(seed()), None, 20_000);
        assert_eq!(d.feed(&body, &mut Vec::new()), Err(ChunkError::ShortChunk));
        // More data than declared.
        let body = encode_signed(&data, &[8192], None, seed());
        let mut d = Decoder::signed(chain(seed()), None, 10_000);
        assert_eq!(d.feed(&body, &mut Vec::new()), Err(ChunkError::TooLong));
        // Cut short.
        let mut d = Decoder::signed(chain(seed()), None, 20_000);
        d.feed(&body[..body.len() - 10], &mut Vec::new()).unwrap();
        assert_eq!(d.finish(), Err(ChunkError::Incomplete));
        // A trailer that was not announced, or is missing.
        let with = encode_signed(&data, &[8192], Some(("x-amz-checksum-sha256", "x")), seed());
        let mut d = Decoder::signed(chain(seed()), Some("x-amz-checksum-crc32c"), 20_000);
        assert_eq!(d.feed(&with, &mut Vec::new()), Err(ChunkError::Trailer));
        let mut d = Decoder::signed(chain(seed()), Some("x-amz-checksum-crc32c"), 20_000);
        assert_eq!(d.feed(&body, &mut Vec::new()), Err(ChunkError::Trailer));
        // An endless line.
        let mut d = Decoder::signed(chain(seed()), None, 20_000);
        assert_eq!(
            d.feed(&[b'1'; MAX_LINE + 1], &mut Vec::new()),
            Err(ChunkError::Framing("a framing line is too long"))
        );
    }

    /// Unsigned chunks with a trailer, in the line-end forms clients send (05 §1.9).
    #[test]
    fn unsigned_chunks_decode_with_either_trailer_ending() {
        for ending in ["\r\n\r\n", "\n\r\n\r\n", "\n\n"] {
            let body = format!(
                "2000\r\n{}\r\n5\r\nhello\r\n0\r\nx-amz-checksum-crc32:YABb/g=={ending}",
                "z".repeat(8192)
            );
            let mut d = Decoder::unsigned("x-amz-checksum-crc32", 8197);
            let mut out = Vec::new();
            d.feed(body.as_bytes(), &mut out).unwrap();
            assert_eq!(d.finish().unwrap().unwrap().value, "YABb/g==");
            assert_eq!(out.len(), 8197);
            assert_eq!(&out[8192..], b"hello");
        }
    }

    proptest! {
        #[test]
        fn any_chunking_decodes_to_the_data(
            data in proptest::collection::vec(any::<u8>(), 0..40_000),
            sizes in proptest::collection::vec(8192usize..20_000, 0..4),
            split in 1usize..5000) {
            let body = encode_signed(&data, &sizes, Some(("x-amz-checksum-crc32c", "AAAAAA==")), seed());
            let mut d = Decoder::signed(chain(seed()), Some("x-amz-checksum-crc32c"), data.len() as u64);
            let mut out = Vec::new();
            for piece in body.chunks(split) {
                d.feed(piece, &mut out).unwrap();
            }
            prop_assert!(d.finish().unwrap().is_some());
            prop_assert_eq!(out, data);
        }
    }
}
