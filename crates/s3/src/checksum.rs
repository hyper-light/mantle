//! S3's object checksums (docs/research/05 §3, §5): the ten algorithms S3 accepts, full-object
//! values combined from parts, composite values, and ETags.
//!
//! Every value is "a base64 encoding of the big-endian checksum value" (05 §3.1). Full-object
//! values exist only for the three CRCs, "because they can linearize into a full object
//! checksum": a multipart object's CRC is combined from its parts' CRCs and lengths without
//! reading the data again (05 §3.3–§3.4).

use std::hash::Hasher as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest as _, Sha256, Sha512};
use twox_hash::{XxHash3_64, XxHash3_128, XxHash64};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Algorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
    Md5,
    XxHash64,
    XxHash3,
    XxHash128,
    Sha512,
}

impl Algorithm {
    pub const ALL: [Self; 10] = [
        Self::Crc32,
        Self::Crc32c,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
        Self::Md5,
        Self::XxHash64,
        Self::XxHash3,
        Self::XxHash128,
        Self::Sha512,
    ];

    /// The name in `x-amz-checksum-algorithm` and `x-amz-sdk-checksum-algorithm`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Md5 => "MD5",
            Self::XxHash64 => "XXHASH64",
            Self::XxHash3 => "XXHASH3",
            Self::XxHash128 => "XXHASH128",
            Self::Sha512 => "SHA512",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|a| a.name().eq_ignore_ascii_case(name.trim()))
    }

    /// The header that carries a value, `x-amz-checksum-<name>`.
    pub fn header(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64Nvme => "x-amz-checksum-crc64nvme",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
            Self::Md5 => "x-amz-checksum-md5",
            Self::XxHash64 => "x-amz-checksum-xxhash64",
            Self::XxHash3 => "x-amz-checksum-xxhash3",
            Self::XxHash128 => "x-amz-checksum-xxhash128",
            Self::Sha512 => "x-amz-checksum-sha512",
        }
    }

    pub fn from_header(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|a| a.header().eq_ignore_ascii_case(name.trim()))
    }

    /// Bytes in a value.
    pub const fn width(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64Nvme | Self::XxHash64 | Self::XxHash3 => 8,
            Self::Md5 | Self::XxHash128 => 16,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    /// The element that holds a value in XML bodies (docs/research/13 §6.2).
    pub const fn element(self) -> &'static str {
        match self {
            Self::Crc32 => "ChecksumCRC32",
            Self::Crc32c => "ChecksumCRC32C",
            Self::Crc64Nvme => "ChecksumCRC64NVME",
            Self::Sha1 => "ChecksumSHA1",
            Self::Sha256 => "ChecksumSHA256",
            Self::Md5 => "ChecksumMD5",
            Self::XxHash64 => "ChecksumXXHASH64",
            Self::XxHash3 => "ChecksumXXHASH3",
            Self::XxHash128 => "ChecksumXXHASH128",
            Self::Sha512 => "ChecksumSHA512",
        }
    }

    /// Whether a multipart object may carry a full-object value: the CRCs only (05 §3.1).
    pub fn full_object(self) -> bool {
        matches!(self, Self::Crc32 | Self::Crc32c | Self::Crc64Nvme)
    }

    /// Whether a multipart object may carry a composite value: all but CRC-64/NVME, which "is
    /// always a full object checksum" (05 §3.3).
    pub fn composite(self) -> bool {
        self != Self::Crc64Nvme
    }

    /// Whether the value may arrive as a trailer: the five classic algorithms (05 §3.2).
    pub fn trailer(self) -> bool {
        matches!(
            self,
            Self::Crc32 | Self::Crc32c | Self::Crc64Nvme | Self::Sha1 | Self::Sha256
        )
    }
}

/// A value: the big-endian bytes of a digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    pub algorithm: Algorithm,
    pub bytes: Vec<u8>,
}

impl Checksum {
    pub fn to_base64(&self) -> String {
        BASE64.encode(&self.bytes)
    }

    /// A value as sent in a header or trailer; `None` unless it is base64 of the algorithm's
    /// width.
    pub fn from_base64(algorithm: Algorithm, text: &str) -> Option<Self> {
        let bytes = BASE64.decode(text.trim()).ok()?;
        if bytes.len() == algorithm.width() {
            Some(Self { algorithm, bytes })
        } else {
            None
        }
    }
}

/// Computes a value as data streams through.
#[derive(Clone)]
pub enum Hasher {
    Crc32(mantle_crc::Crc32),
    Crc32c(mantle_crc::Crc32c),
    Crc64Nvme(mantle_crc::Crc64Nvme),
    Sha1(Sha1),
    Sha256(Sha256),
    Md5(Md5),
    XxHash64(XxHash64),
    XxHash3(XxHash3_64),
    XxHash128(XxHash3_128),
    Sha512(Sha512),
}

impl Hasher {
    pub fn new(algorithm: Algorithm) -> Self {
        match algorithm {
            Algorithm::Crc32 => Self::Crc32(mantle_crc::Crc32::new()),
            Algorithm::Crc32c => Self::Crc32c(mantle_crc::Crc32c::new()),
            Algorithm::Crc64Nvme => Self::Crc64Nvme(mantle_crc::Crc64Nvme::new()),
            Algorithm::Sha1 => Self::Sha1(Sha1::new()),
            Algorithm::Sha256 => Self::Sha256(Sha256::new()),
            Algorithm::Md5 => Self::Md5(Md5::new()),
            // S3's xxHash values use the default seed, zero.
            Algorithm::XxHash64 => Self::XxHash64(XxHash64::with_seed(0)),
            Algorithm::XxHash3 => Self::XxHash3(XxHash3_64::with_seed(0)),
            Algorithm::XxHash128 => Self::XxHash128(XxHash3_128::with_seed(0)),
            Algorithm::Sha512 => Self::Sha512(Sha512::new()),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(h) => h.update(data),
            Self::Crc32c(h) => h.update(data),
            Self::Crc64Nvme(h) => h.update(data),
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
            Self::Md5(h) => h.update(data),
            Self::XxHash64(h) => h.write(data),
            Self::XxHash3(h) => h.write(data),
            Self::XxHash128(h) => h.write(data),
            Self::Sha512(h) => h.update(data),
        }
    }

    pub fn finish(self) -> Checksum {
        let (algorithm, bytes) = match self {
            Self::Crc32(h) => (Algorithm::Crc32, h.finish().to_be_bytes().to_vec()),
            Self::Crc32c(h) => (Algorithm::Crc32c, h.finish().to_be_bytes().to_vec()),
            Self::Crc64Nvme(h) => (Algorithm::Crc64Nvme, h.finish().to_be_bytes().to_vec()),
            Self::Sha1(h) => (Algorithm::Sha1, h.finalize().to_vec()),
            Self::Sha256(h) => (Algorithm::Sha256, h.finalize().to_vec()),
            Self::Md5(h) => (Algorithm::Md5, h.finalize().to_vec()),
            Self::XxHash64(h) => (Algorithm::XxHash64, h.finish().to_be_bytes().to_vec()),
            Self::XxHash3(h) => (Algorithm::XxHash3, h.finish().to_be_bytes().to_vec()),
            Self::XxHash128(h) => (Algorithm::XxHash128, h.finish_128().to_be_bytes().to_vec()),
            Self::Sha512(h) => (Algorithm::Sha512, h.finalize().to_vec()),
        };
        Checksum { algorithm, bytes }
    }
}

/// The value of `data`.
pub fn checksum(algorithm: Algorithm, data: &[u8]) -> Checksum {
    let mut h = Hasher::new(algorithm);
    h.update(data);
    h.finish()
}

/// The full-object value of an object made of parts, from each part's value and length:
/// the CRC of all the object's bytes, combined without the data (05 §3.4). `None` if the parts
/// differ in algorithm or the algorithm does not combine.
pub fn full_object(parts: &[(Checksum, u64)]) -> Option<Checksum> {
    let (first, _) = parts.first()?;
    let algorithm = first.algorithm;
    if !algorithm.full_object() || parts.iter().any(|(c, _)| c.algorithm != algorithm) {
        return None;
    }
    let bytes = match algorithm {
        Algorithm::Crc32 | Algorithm::Crc32c => {
            let combine = if algorithm == Algorithm::Crc32 {
                mantle_crc::crc32_combine
            } else {
                mantle_crc::crc32c_combine
            };
            let mut crc: Option<u32> = None;
            for (c, len) in parts {
                let part = u32::from_be_bytes(c.bytes.as_slice().try_into().ok()?);
                crc = Some(crc.map_or(part, |acc| combine(acc, part, *len)));
            }
            crc?.to_be_bytes().to_vec()
        }
        _ => {
            let mut crc: Option<u64> = None;
            for (c, len) in parts {
                let part = u64::from_be_bytes(c.bytes.as_slice().try_into().ok()?);
                crc = Some(crc.map_or(part, |acc| mantle_crc::crc64nvme_combine(acc, part, *len)));
            }
            crc?.to_be_bytes().to_vec()
        }
    };
    Some(Checksum { algorithm, bytes })
}

/// The composite value of an object made of parts, `base64(H(H1 ‖ … ‖ Hn))-n`, over the
/// parts' binary values (05 §3.4). `None` if the parts differ in algorithm or it has no
/// composite form.
pub fn composite(parts: &[Checksum]) -> Option<String> {
    let algorithm = parts.first()?.algorithm;
    if !algorithm.composite() || parts.iter().any(|c| c.algorithm != algorithm) {
        return None;
    }
    let mut h = Hasher::new(algorithm);
    for part in parts {
        h.update(&part.bytes);
    }
    Some(format!("{}-{}", h.finish().to_base64(), parts.len()))
}

/// A single-part object's ETag: the MD5 of its bytes in lowercase hex, quoted (05 §5.1).
pub fn etag(md5: &[u8; 16]) -> String {
    format!("\"{}\"", hex(md5))
}

/// A multipart object's ETag: the MD5 of its parts' binary MD5s, then `-n`, quoted (05 §4.5).
pub fn multipart_etag(parts: &[[u8; 16]]) -> String {
    let mut h = Md5::new();
    for part in parts {
        h.update(part);
    }
    let digest: [u8; 16] = h.finalize().into();
    format!("\"{}-{}\"", hex(&digest), parts.len())
}

/// A `Content-MD5` header's value: base64 of 16 bytes.
pub fn content_md5(text: &str) -> Option<[u8; 16]> {
    BASE64.decode(text.trim()).ok()?.try_into().ok()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for &b in bytes {
        out.push(char::from(
            HEX.get(usize::from(b >> 4)).copied().unwrap_or(b'0'),
        ));
        out.push(char::from(
            HEX.get(usize::from(b & 15)).copied().unwrap_or(b'0'),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn b64(algorithm: Algorithm, text: &str) -> Checksum {
        Checksum::from_base64(algorithm, text).unwrap()
    }

    /// AWS's composite SHA-256 example from its multipart-checksum tutorial (05 §3.4).
    #[test]
    fn aws_tutorial_composite() {
        let parts = [
            "QLl8R4i4+SaJlrl8ZIcutc5TbZtwt2NwB8lTXkd3GH0=",
            "xCdgs1K5Bm4jWETYw/CmGYr+m6O2DcGfpckx5NVokvE=",
            "f5wsfsa5bB+yXuwzqG1Bst91uYneqGD3CCidpb54mAo=",
        ]
        .map(|p| b64(Algorithm::Sha256, p));
        assert_eq!(
            composite(&parts).unwrap(),
            "aI8EoktCdotjU8Bq46DrPCxQCGuGcPIhJ51noWs6hvk=-3"
        );
    }

    /// s3-tests' multipart vectors: three 5 MiB parts of `A`, `B` and `C` (05 §3.4), and one
    /// part of 1024 × `A`.
    #[test]
    fn s3_tests_multipart_vectors() {
        let parts: Vec<Vec<u8>> = b"ABC".iter().map(|&b| vec![b; 5 << 20]).collect();
        let len = 5u64 << 20;
        let crc32: Vec<(Checksum, u64)> = parts
            .iter()
            .map(|p| (checksum(Algorithm::Crc32, p), len))
            .collect();
        assert_eq!(
            crc32.iter().map(|(c, _)| c.to_base64()).collect::<Vec<_>>(),
            ["JRTCyQ==", "QoZTGg==", "YAgjqw=="]
        );
        assert_eq!(full_object(&crc32).unwrap().to_base64(), "WgDhBQ==");
        let nvme: Vec<(Checksum, u64)> = parts
            .iter()
            .map(|p| (checksum(Algorithm::Crc64Nvme, p), len))
            .collect();
        assert_eq!(
            nvme.iter().map(|(c, _)| c.to_base64()).collect::<Vec<_>>(),
            ["L/E4WYn8v98=", "xW1l19VobYM=", "cK5MnNaWrW4="]
        );
        assert_eq!(full_object(&nvme).unwrap().to_base64(), "i+6LR0y3eFo=");
        let sha: Vec<Checksum> = parts
            .iter()
            .map(|p| checksum(Algorithm::Sha256, p))
            .collect();
        assert_eq!(
            composite(&sha).unwrap(),
            "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3"
        );
        let md5: Vec<[u8; 16]> = parts.iter().map(|p| Md5::digest(p).into()).collect();
        assert_eq!(
            multipart_etag(&md5),
            "\"b2add96cc9702bbf4efb0ccdfc6b7747-3\""
        );
        let one = [checksum(Algorithm::Sha256, &[b'A'; 1024])];
        assert_eq!(
            composite(&one).unwrap(),
            "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1"
        );
    }

    /// The trailing CRC-32C in AWS's chunked-upload example (05 §1.9).
    #[test]
    fn aws_trailer_crc32c() {
        assert_eq!(
            checksum(Algorithm::Crc32c, &[b'a'; 66560]).to_base64(),
            "sOO8/Q=="
        );
    }

    #[test]
    fn names_headers_and_widths_agree() {
        for a in Algorithm::ALL {
            assert_eq!(Algorithm::from_name(a.name()), Some(a));
            assert_eq!(Algorithm::from_name(&a.name().to_lowercase()), Some(a));
            assert_eq!(Algorithm::from_header(a.header()), Some(a));
            assert_eq!(checksum(a, b"x").bytes.len(), a.width());
        }
        assert!(Checksum::from_base64(Algorithm::Crc32, "AAAAAAAA").is_none());
        assert!(composite(&[checksum(Algorithm::Crc64Nvme, b"x")]).is_none());
        assert!(full_object(&[(checksum(Algorithm::Sha256, b"x"), 1)]).is_none());
        assert_eq!(
            etag(&Md5::digest(b"").into()),
            "\"d41d8cd98f00b204e9800998ecf8427e\""
        );
    }

    proptest! {
        #[test]
        fn full_object_values_equal_the_whole_objects(
            data in proptest::collection::vec(any::<u8>(), 0..20_000),
            cuts in proptest::collection::vec(any::<usize>(), 0..5)) {
            let mut cuts: Vec<usize> = cuts.iter().map(|c| c % (data.len() + 1)).collect();
            cuts.sort_unstable();
            let mut at = 0;
            let mut pieces = Vec::new();
            for cut in cuts.into_iter().chain([data.len()]) {
                pieces.push(&data[at..cut]);
                at = cut;
            }
            for a in [Algorithm::Crc32, Algorithm::Crc32c, Algorithm::Crc64Nvme] {
                let parts: Vec<(Checksum, u64)> = pieces
                    .iter()
                    .map(|p| (checksum(a, p), p.len() as u64))
                    .collect();
                prop_assert_eq!(full_object(&parts).unwrap(), checksum(a, &data));
            }
        }
    }
}
