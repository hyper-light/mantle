//! The cryptography S3 needs, from AWS-LC through aws-lc-rs (docs/design/crypto.md): digests,
//! HMAC-SHA256, and comparison in constant time.
//!
//! Every call goes through aws-lc-rs's fallible entry points, so a failure inside the library
//! is an error the caller handles, and runs behind an unwind boundary, as every dependency is
//! called (CLAUDE.md §1). A running digest allocates its state, so the failure a caller can
//! meet in practice is an allocation's; one-shot SHA-256 and HMAC allocate nothing. The
//! fallible digest entry points and the one-shot HMAC are local additions to the vendored
//! aws-lc-rs (vendor/UPSTREAM.md).

use std::panic::{AssertUnwindSafe, catch_unwind};

use aws_lc_rs::error::Unspecified;
use aws_lc_rs::{constant_time, digest, hmac};

pub use aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY as SHA1;
pub use aws_lc_rs::digest::{MD5_FOR_LEGACY_USE_ONLY as MD5, SHA256, SHA512};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the cryptographic library failed")]
pub struct CryptoError;

/// Runs `f`, turning the library's error, or a panic inside it, into `CryptoError`.
fn guarded<T>(f: impl FnOnce() -> Result<T, Unspecified>) -> Result<T, CryptoError> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(Unspecified)) | Err(_) => Err(CryptoError),
    }
}

/// A digest computed as data streams through.
pub struct Digest(digest::Context);

impl Digest {
    pub fn new(algorithm: &'static digest::Algorithm) -> Result<Self, CryptoError> {
        guarded(|| digest::Context::try_new(algorithm)).map(Self)
    }

    pub fn update(&mut self, data: &[u8]) -> Result<(), CryptoError> {
        guarded(|| self.0.try_update(data))
    }

    pub fn finish(self) -> Result<Vec<u8>, CryptoError> {
        guarded(move || self.0.try_finish()).map(|d| d.as_ref().to_vec())
    }

    /// The digest as an array of its length, `N`.
    pub fn finish_array<const N: usize>(self) -> Result<[u8; N], CryptoError> {
        let digest = guarded(move || self.0.try_finish())?;
        digest.as_ref().try_into().map_err(|_| CryptoError)
    }
}

pub fn sha256(data: &[u8]) -> Result<[u8; 32], CryptoError> {
    let digest = guarded(|| Ok(digest::digest(&SHA256, data)))?;
    digest.as_ref().try_into().map_err(|_| CryptoError)
}

/// AWS-LC's one-shot HMAC: one context keyed, used and wiped. SigV4 keys each HMAC of a
/// request's signature once, and for those the one-shot took 187 ns where keying a `Key` and
/// signing with a copy of its context took 255 ns. A chunked body signs every chunk under one
/// key, where a kept `Key` would save about 30 ns a chunk, under 1.2% of hashing S3's smallest
/// chunk (docs/measurements/2026-09-28-aws-lc-crypto.md, finding 3).
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<[u8; 32], CryptoError> {
    let mut tag = [0u8; 32];
    guarded(|| hmac::sign_once(hmac::HMAC_SHA256, key, data, &mut tag).map(|_| ()))?;
    Ok(tag)
}

/// Whether `a` and `b` are equal, compared in time that does not depend on their contents.
pub fn equal(a: &[u8], b: &[u8]) -> Result<bool, CryptoError> {
    catch_unwind(|| constant_time::verify_slices_are_equal(a, b).is_ok()).map_err(|_| CryptoError)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// FIPS 180-4's "abc" examples (NIST CSRC example values) and RFC 1321 §A.5, each whole
    /// and fed in pieces.
    #[test]
    fn digests_match_their_standards() {
        let cases: [(&'static digest::Algorithm, &str); 4] = [
            (&SHA1, "a9993e364706816aba3e25717850c26c9cd0d89d"),
            (
                &SHA256,
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                &SHA512,
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
            ),
            (&MD5, "900150983cd24fb0d6963f7d28e17f72"),
        ];
        for (algorithm, want) in cases {
            let mut d = Digest::new(algorithm).unwrap();
            d.update(b"a").unwrap();
            d.update(b"").unwrap();
            d.update(b"bc").unwrap();
            assert_eq!(hex(&d.finish().unwrap()), want);
        }
        assert_eq!(
            hex(&sha256(b"abc").unwrap()),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let mut d = Digest::new(&MD5).unwrap();
        d.update(b"message digest").unwrap();
        assert_eq!(
            d.finish_array::<16>().unwrap(),
            [
                0xf9, 0x6b, 0x69, 0x7d, 0x7c, 0xb7, 0x93, 0x8d, 0x52, 0x5a, 0x2f, 0x31, 0xaa, 0xf1,
                0x61, 0xd0
            ]
        );
        assert_eq!(
            Digest::new(&SHA256).unwrap().finish_array::<16>(),
            Err(CryptoError)
        );
    }

    /// RFC 4231 test cases 1 and 6: a short key, and a key longer than a block.
    #[test]
    fn hmac_matches_rfc_4231() {
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There").unwrap()),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )
            .unwrap()),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn equal_compares_contents_and_lengths() {
        assert_eq!(equal(b"abc", b"abc"), Ok(true));
        assert_eq!(equal(b"abc", b"abd"), Ok(false));
        assert_eq!(equal(b"abc", b"ab"), Ok(false));
    }
}
