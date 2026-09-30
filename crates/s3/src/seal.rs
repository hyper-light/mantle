//! Sealing a file's bytes at rest (docs/design/encryption.md §2–§3): a random data key per
//! file, wrapped with AES-256 key wrap under the root key or a customer's key, and the file's
//! bytes sealed in 64 KiB segments of AES-256-GCM, each at a nonce that counts its segment and
//! marks the last.
//!
//! Every call into AWS-LC runs behind the unwind boundary `crypto` keeps (docs/design/crypto.md
//! §3). Nonces are never drawn at random: one key per file makes a counter unique, and keys are
//! read from the operating system with `getrandom`, which fails with an error where AWS-LC's own
//! generator would abort.

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::key_wrap::{AES_256, AesKek, KeyWrap as _};
use zeroize::Zeroizing;

use crate::crypto::{CryptoError, guarded};

/// Plaintext bytes a segment holds, the chunk store's checksum block (encryption.md §3).
pub const SEGMENT: usize = 64 * 1024;
/// GCM's tag, the full 128 bits (SP 800-38D §5.2.1.2).
pub const TAG: usize = 16;
/// A data key wrapped with AES key wrap: the key and one semiblock (SP 800-38F §6.2).
pub const WRAPPED: usize = 32 + 8;

/// The top bit of a nonce's counter, set on a file's last segment.
const LAST: u64 = 1 << 63;

/// A segment's plaintext bytes, and its sealed bytes with the tag.
const PLAIN_SEGMENT: u64 = 64 * 1024;
const SEALED_SEGMENT: u64 = 64 * 1024 + 16;
const TAG_BYTES: u64 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SealError {
    #[error("the operating system's random source failed")]
    Random,
    /// Another key wrapped it, or its bytes changed: KW's integrity check failed.
    #[error("the data key does not unwrap under this key")]
    Unwrap,
    /// The segment's bytes, its place in its file, or its file are not the ones sealed.
    #[error("a segment does not open")]
    Open,
    #[error("a segment over 64 KiB, or a file of more segments than a nonce counts")]
    Size,
    #[error(transparent)]
    Crypto(#[from] CryptoError),
}

/// A key that wraps data keys: a root key's generation, or a customer's key. Wiped when dropped.
pub struct WrappingKey(Zeroizing<[u8; 32]>);

impl WrappingKey {
    pub fn new(bytes: &[u8; 32]) -> Self {
        Self(Zeroizing::new(*bytes))
    }

    /// A new root key from the operating system's generator.
    pub fn generate() -> Result<Self, SealError> {
        Ok(Self(random()?))
    }

    pub fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A file's data key: 256 random bits that seal one file. Wiped when dropped.
pub struct DataKey(Zeroizing<[u8; 32]>);

impl DataKey {
    pub fn generate() -> Result<Self, SealError> {
        Ok(Self(random()?))
    }

    /// The key wrapped under `with`, as the file's header keeps it (encryption.md §2).
    pub fn wrap(&self, with: &WrappingKey) -> Result<[u8; WRAPPED], SealError> {
        let mut out = [0u8; WRAPPED];
        let written = guarded(|| {
            let kek = AesKek::new(&AES_256, with.bytes())?;
            kek.wrap(self.0.as_slice(), &mut out).map(|w| w.len())
        })?;
        if written != WRAPPED {
            return Err(SealError::Crypto(CryptoError));
        }
        Ok(out)
    }

    /// The key `wrapped` holds, if `with` wrapped it. Any other key fails KW's integrity check,
    /// forged with probability 1 in 2^64 (SP 800-38F App. A.3).
    pub fn unwrap(wrapped: &[u8; WRAPPED], with: &WrappingKey) -> Result<Self, SealError> {
        let mut key = Zeroizing::new([0u8; 32]);
        let unwrapped = guarded(|| {
            let kek = AesKek::new(&AES_256, with.bytes())?;
            kek.unwrap(wrapped, key.as_mut_slice()).map(|w| w.len())
        })
        .map_err(|_| SealError::Unwrap)?;
        if unwrapped != 32 {
            return Err(SealError::Unwrap);
        }
        Ok(Self(key))
    }
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataKey")
    }
}

impl std::fmt::Debug for WrappingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WrappingKey")
    }
}

fn random() -> Result<Zeroizing<[u8; 32]>, SealError> {
    let mut bytes = Zeroizing::new([0u8; 32]);
    getrandom::fill(bytes.as_mut_slice()).map_err(|_| SealError::Random)?;
    Ok(bytes)
}

/// The segments of one file under its data key: sealed and opened in place, each at its index.
pub struct Segments {
    key: LessSafeKey,
    /// The file's ID, each segment's additional data.
    file: [u8; 16],
}

impl Segments {
    pub fn new(key: &DataKey, file: u128) -> Result<Self, SealError> {
        let key =
            guarded(|| UnboundKey::new(&AES_256_GCM, key.0.as_slice()).map(LessSafeKey::new))?;
        Ok(Self {
            key,
            file: file.to_be_bytes(),
        })
    }

    /// Seals segment `index` of the file in place: `data` holds its plaintext, at most
    /// `SEGMENT` bytes, and gains its tag. `last` marks the file's final segment.
    pub fn seal(&self, index: u64, last: bool, data: &mut Vec<u8>) -> Result<(), SealError> {
        if data.len() > SEGMENT {
            return Err(SealError::Size);
        }
        let nonce = nonce(index, last)?;
        guarded(|| {
            self.key.seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(self.file),
                data,
            )
        })?;
        Ok(())
    }

    /// Opens segment `index` in place, leaving its plaintext in `data`. A segment opens only at
    /// the index and the lastness it was sealed at, and only in its own file.
    pub fn open(&self, index: u64, last: bool, data: &mut Vec<u8>) -> Result<(), SealError> {
        let plain = data.len().checked_sub(TAG).ok_or(SealError::Open)?;
        if plain > SEGMENT {
            return Err(SealError::Size);
        }
        let nonce = nonce(index, last)?;
        guarded(|| {
            self.key
                .open_in_place(
                    Nonce::assume_unique_for_key(nonce),
                    Aad::from(self.file),
                    data.as_mut_slice(),
                )
                .map(|_| ())
        })
        .map_err(|_| SealError::Open)?;
        data.truncate(plain);
        Ok(())
    }
}

/// A segment's nonce: 32 bits of zero, SP 800-38D §8.2.1's fixed field, which one key per file
/// makes constant, then its index, the invocation field, with the top bit set on the last
/// segment, STREAM's marker of the end (encryption.md §3).
fn nonce(index: u64, last: bool) -> Result<[u8; 12], SealError> {
    if index & LAST != 0 {
        return Err(SealError::Size);
    }
    let counter = if last { index | LAST } else { index };
    let [a, b, c, d, e, f, g, h] = counter.to_be_bytes();
    Ok([0, 0, 0, 0, a, b, c, d, e, f, g, h])
}

/// Segments a file of `plain` bytes is sealed in: every 64 KiB, and one, empty, for an empty
/// file, so that its end is sealed too.
pub fn segments(plain: u64) -> u64 {
    let whole = plain / PLAIN_SEGMENT;
    let part = u64::from(!plain.is_multiple_of(PLAIN_SEGMENT));
    // A quotient by 64 KiB is far below u64::MAX, so adding one cannot overflow.
    whole.checked_add(part).unwrap_or(whole).max(1)
}

/// Bytes a file of `plain` bytes takes sealed: a tag for each segment.
pub fn sealed_len(plain: u64) -> Option<u64> {
    segments(plain)
        .checked_mul(TAG_BYTES)
        .and_then(|tags| plain.checked_add(tags))
}

/// The plaintext bytes a sealed file of `sealed` bytes holds; `None` if no file seals to that
/// length.
pub fn plain_len(sealed: u64) -> Option<u64> {
    let full = sealed.checked_div(SEALED_SEGMENT)?;
    let rest = sealed.checked_rem(SEALED_SEGMENT)?;
    let plain = match rest {
        0 if full > 0 => full.checked_mul(PLAIN_SEGMENT)?,
        0 => return None,
        rest => full
            .checked_mul(PLAIN_SEGMENT)?
            .checked_add(rest.checked_sub(TAG_BYTES)?)?,
    };
    if sealed_len(plain)? == sealed {
        Some(plain)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn unhex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    /// A byte of a test file's contents at `i`.
    fn byte(i: usize) -> u8 {
        u8::try_from(i % 251).unwrap()
    }

    /// RFC 3394 §4.6: 256 bits of key data wrapped with a 256-bit KEK.
    #[test]
    fn key_wrap_matches_rfc_3394() {
        let kek: [u8; 32] =
            unhex("000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F")
                .try_into()
                .unwrap();
        let data: [u8; 32] =
            unhex("00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F")
                .try_into()
                .unwrap();
        let key = DataKey(Zeroizing::new(data));
        let wrapped = key.wrap(&WrappingKey::new(&kek)).unwrap();
        assert_eq!(
            wrapped.to_vec(),
            unhex(
                "28C9F404C4B810F4CBCCB35CFB87F8263F5786E2D80ED326CBC7F0E71A99F43BFB988B9B7A02DD21"
            )
        );
        let back = DataKey::unwrap(&wrapped, &WrappingKey::new(&kek)).unwrap();
        assert_eq!(*back.0, data);
        // Another key, or a changed byte, fails the integrity check.
        let mut other = kek;
        other[0] ^= 1;
        assert_eq!(
            DataKey::unwrap(&wrapped, &WrappingKey::new(&other)).unwrap_err(),
            SealError::Unwrap
        );
        let mut changed = wrapped;
        changed[20] ^= 1;
        assert_eq!(
            DataKey::unwrap(&changed, &WrappingKey::new(&kek)).unwrap_err(),
            SealError::Unwrap
        );
    }

    /// GCM's test cases 13 and 14 (McGrew and Viega, "The Galois/Counter Mode of Operation"): a
    /// zero 256-bit key and nonce, no additional data, and an empty or zero block of plaintext.
    #[test]
    fn a_segment_at_nonce_zero_is_plain_aes_256_gcm() {
        let key =
            guarded(|| UnboundKey::new(&AES_256_GCM, &[0u8; 32]).map(LessSafeKey::new)).unwrap();
        let seal = |plain: &[u8]| {
            let mut data = plain.to_vec();
            key.seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce(0, false).unwrap()),
                Aad::empty(),
                &mut data,
            )
            .unwrap();
            data
        };
        assert_eq!(seal(&[]), unhex("530f8afbc74536b9a963b4f1c4cb738b"));
        assert_eq!(
            seal(&[0u8; 16]),
            unhex("cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919")
        );
    }

    fn seal_file(segments: &Segments, plain: &[u8]) -> Vec<Vec<u8>> {
        let count = self::segments(plain.len() as u64);
        (0..count)
            .map(|i| {
                let start = (usize::try_from(i).unwrap() * SEGMENT).min(plain.len());
                let end = (start + SEGMENT).min(plain.len());
                let mut data = plain[start..end].to_vec();
                segments.seal(i, i + 1 == count, &mut data).unwrap();
                data
            })
            .collect()
    }

    /// A segment opens only at its own index and lastness, in its own file, under its own key,
    /// with its bytes as sealed: a file cut short, reordered or moved fails to open (STREAM).
    #[test]
    fn a_segment_opens_only_where_it_was_sealed() {
        let key = DataKey::generate().unwrap();
        let file = Segments::new(&key, 7).unwrap();
        let plain: Vec<u8> = (0..(2 * SEGMENT + 5)).map(byte).collect();
        let sealed = seal_file(&file, &plain);
        assert_eq!(sealed.len(), 3);
        assert_eq!(
            sealed.iter().map(Vec::len).sum::<usize>() as u64,
            sealed_len(plain.len() as u64).unwrap()
        );
        let open = |segments: &Segments, index: u64, last: bool, data: &[u8]| {
            let mut data = data.to_vec();
            segments.open(index, last, &mut data).map(|()| data)
        };
        assert_eq!(
            open(&file, 1, false, &sealed[1]).unwrap(),
            &plain[SEGMENT..2 * SEGMENT]
        );
        assert_eq!(
            open(&file, 2, true, &sealed[2]).unwrap(),
            &plain[2 * SEGMENT..]
        );
        // Moved, reordered, cut short, or taken for the last.
        assert_eq!(open(&file, 0, false, &sealed[1]), Err(SealError::Open));
        assert_eq!(open(&file, 1, true, &sealed[1]), Err(SealError::Open));
        assert_eq!(open(&file, 2, false, &sealed[2]), Err(SealError::Open));
        // Another file under the same key, or another key.
        let other_file = Segments::new(&key, 8).unwrap();
        assert_eq!(
            open(&other_file, 1, false, &sealed[1]),
            Err(SealError::Open)
        );
        let other_key = Segments::new(&DataKey::generate().unwrap(), 7).unwrap();
        assert_eq!(open(&other_key, 1, false, &sealed[1]), Err(SealError::Open));
        // A changed bit anywhere, the tag's included.
        for bit in [0, 8 * 100, 8 * (sealed[0].len() - 1)] {
            let mut damaged = sealed[0].clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(open(&file, 0, false, &damaged), Err(SealError::Open));
        }
        assert_eq!(
            open(&file, 0, false, &sealed[0][..10]),
            Err(SealError::Open)
        );
        let mut long = vec![0u8; SEGMENT + 1];
        assert_eq!(file.seal(0, false, &mut long), Err(SealError::Size));
        assert_eq!(nonce(LAST, false), Err(SealError::Size));
    }

    #[test]
    fn an_empty_file_seals_its_end() {
        let file = Segments::new(&DataKey::generate().unwrap(), 1).unwrap();
        let sealed = seal_file(&file, &[]);
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].len(), TAG);
        let mut data = sealed[0].clone();
        file.open(0, true, &mut data).unwrap();
        assert!(data.is_empty());
        let mut data = sealed[0].clone();
        assert_eq!(file.open(0, false, &mut data), Err(SealError::Open));
    }

    #[test]
    fn sealed_and_plain_lengths_invert() {
        let s = SEGMENT as u64;
        for (plain, sealed) in [
            (0, 16),
            (1, 17),
            (s, s + 16),
            (s + 1, s + 33),
            (3 * s, 3 * s + 48),
        ] {
            assert_eq!(sealed_len(plain), Some(sealed), "{plain}");
            assert_eq!(plain_len(sealed), Some(plain), "{sealed}");
        }
        for impossible in [0, 1, 15, s + 16 + 5] {
            assert_eq!(plain_len(impossible), None, "{impossible}");
        }
    }

    proptest! {
        /// Any file seals and opens back, segment by segment in any order.
        #[test]
        fn any_file_opens_to_what_was_sealed(
            len in 0usize..(3 * SEGMENT + 17),
            order in proptest::collection::vec(any::<prop::sample::Index>(), 0..8),
        ) {
                        let plain: Vec<u8> = (0..len).map(|i| byte(i * 31)).collect();
            let file = Segments::new(&DataKey::generate().unwrap(), 42).unwrap();
            let sealed = seal_file(&file, &plain);
            prop_assert_eq!(sealed.iter().map(Vec::len).sum::<usize>() as u64, sealed_len(len as u64).unwrap());
            prop_assert_eq!(plain_len(sealed_len(len as u64).unwrap()), Some(len as u64));
            let count = sealed.len();
            for pick in order.iter().map(|i| i.index(count)).chain(0..count) {
                let mut data = sealed[pick].clone();
                file.open(pick as u64, pick + 1 == count, &mut data).unwrap();
                let start = (pick * SEGMENT).min(len);
                prop_assert_eq!(&data[..], &plain[start..(start + SEGMENT).min(len)]);
            }
        }
    }
}
