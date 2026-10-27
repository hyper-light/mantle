//! The transfer harness's objects (plan step 9): each object's bytes are AES-256-CTR's keystream
//! under a key the run's seed gives, its counter starting where the object's name says, so any
//! range of any object is made again at its offset and a read is checked against it with nothing
//! kept: no copy of an object exists outside the cell (research/76). AES in counter mode makes a
//! block of keystream from a block of counter alone, so the bytes at an offset need no bytes before
//! them, and AWS-LC runs it on the CPU's AES instructions.
//!
//! Every call into aws-lc-rs runs behind an unwind boundary, as every dependency is called
//! (CLAUDE.md §1).

use std::panic::{AssertUnwindSafe, catch_unwind};

use aws_lc_rs::cipher::{AES_256, EncryptingKey, EncryptionContext, UnboundCipherKey};
use aws_lc_rs::digest::{SHA256, digest};
use aws_lc_rs::iv::FixedLength;

/// A block of AES, and of the counter that makes one block of keystream (FIPS 197; SP 800-38A
/// §6.5).
const BLOCK: usize = 16;
/// `BLOCK`, for an object's offsets.
const BLOCK_BYTES: u64 = BLOCK as u64;

/// What the run's seed is hashed under to make its key, so no other use of a seed's hash is the
/// same key.
const KEY_DOMAIN: &[u8] = b"mantle bench transfer: key";

/// What an object's name is hashed under to make where its counter starts.
const COUNTER_DOMAIN: &[u8] = b"mantle bench transfer: counter";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenError {
    Crypto,
    Offset,
}

impl std::fmt::Display for GenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Crypto => "the cryptographic library failed",
            Self::Offset => "an offset past what an object can address",
        })
    }
}

/// Runs `f`, turning the library's error, or a panic inside it, into `GenError::Crypto`.
fn guarded<T>(f: impl FnOnce() -> Option<T>) -> Result<T, GenError> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Some(value)) => Ok(value),
        Ok(None) | Err(_) => Err(GenError::Crypto),
    }
}

/// The key a run's objects are made under.
pub struct Generator {
    key: EncryptingKey,
}

impl Generator {
    /// The generator of run `seed`: its key the SHA-256 of the seed under the key's domain.
    pub fn new(seed: u64) -> Result<Self, GenError> {
        let key = guarded(|| {
            let mut input = Vec::with_capacity(KEY_DOMAIN.len().saturating_add(8));
            input.extend_from_slice(KEY_DOMAIN);
            input.extend_from_slice(&seed.to_be_bytes());
            let hashed = digest(&SHA256, &input);
            let unbound = UnboundCipherKey::new(&AES_256, hashed.as_ref()).ok()?;
            EncryptingKey::ctr(unbound).ok()
        })?;
        Ok(Self { key })
    }

    /// The object named `name`: its counter starts at the first 16 bytes of the SHA-256 of the
    /// name under the counter's domain, as a big-endian number.
    pub fn object(&self, name: &str) -> Result<Object<'_>, GenError> {
        let start = guarded(|| {
            let mut input = Vec::with_capacity(COUNTER_DOMAIN.len().saturating_add(name.len()));
            input.extend_from_slice(COUNTER_DOMAIN);
            input.extend_from_slice(name.as_bytes());
            let hashed = digest(&SHA256, &input);
            let first: [u8; BLOCK] = hashed.as_ref().get(..BLOCK)?.try_into().ok()?;
            Some(u128::from_be_bytes(first))
        })?;
        Ok(Object {
            key: &self.key,
            start,
        })
    }
}

/// One object's bytes, made at any offset.
pub struct Object<'g> {
    key: &'g EncryptingKey,
    start: u128,
}

impl Object<'_> {
    /// Writes the object's bytes from `offset` over `into`, every byte of it.
    pub fn fill(&self, offset: u64, into: &mut [u8]) -> Result<(), GenError> {
        let block = u128::from(offset.checked_div(BLOCK_BYTES).ok_or(GenError::Offset)?);
        let skip = offset
            .checked_rem(BLOCK_BYTES)
            .and_then(|skip| usize::try_from(skip).ok())
            .ok_or(GenError::Offset)?;
        let counter = self.start.wrapping_add(block);
        let mut at = 0usize;
        if skip > 0 {
            // A first block entered mid-way: made whole, its tail kept.
            let mut first = [0u8; BLOCK];
            self.keystream(counter, &mut first)?;
            let take = BLOCK.saturating_sub(skip).min(into.len());
            let tail = first
                .get(skip..skip.saturating_add(take))
                .ok_or(GenError::Offset)?;
            into.get_mut(..take)
                .ok_or(GenError::Offset)?
                .copy_from_slice(tail);
            at = take;
        }
        let rest = into.get_mut(at..).ok_or(GenError::Offset)?;
        if rest.is_empty() {
            return Ok(());
        }
        let counter = if skip > 0 {
            counter.wrapping_add(1)
        } else {
            counter
        };
        rest.fill(0);
        self.keystream(counter, rest)
    }

    /// The first offset of `got`, the bytes said to be the object's from `offset`, that is not the
    /// object's, if any, made again a piece of `scratch` at a time.
    pub fn verify(
        &self,
        offset: u64,
        got: &[u8],
        scratch: &mut [u8],
    ) -> Result<Option<u64>, GenError> {
        if scratch.is_empty() {
            return Err(GenError::Offset);
        }
        let mut done = 0usize;
        while done < got.len() {
            let len = scratch.len().min(got.len().saturating_sub(done));
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| GenError::Offset)?)
                .ok_or(GenError::Offset)?;
            let made = scratch.get_mut(..len).ok_or(GenError::Offset)?;
            self.fill(at, made)?;
            let piece = got
                .get(done..done.saturating_add(len))
                .ok_or(GenError::Offset)?;
            if let Some(i) = piece.iter().zip(made.iter()).position(|(a, b)| a != b) {
                let i = u64::try_from(i).map_err(|_| GenError::Offset)?;
                return Ok(Some(at.checked_add(i).ok_or(GenError::Offset)?));
            }
            done = done.saturating_add(len);
        }
        Ok(None)
    }

    /// The keystream from counter block `counter` over `zeros`, which hold zeros: AES-CTR of
    /// zeros is its keystream.
    fn keystream(&self, counter: u128, zeros: &mut [u8]) -> Result<(), GenError> {
        guarded(|| {
            let context = EncryptionContext::Iv128(FixedLength::from(counter.to_be_bytes()));
            self.key.less_safe_encrypt(zeros, context).ok().map(|_| ())
        })
    }

    /// The object whose counter starts at `start`, for a test of the counter's carry.
    #[cfg(test)]
    fn at<'k>(key: &'k EncryptingKey, start: u128) -> Object<'k> {
        Object { key, start }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn whole(object: &Object<'_>, len: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; len];
        object.fill(0, &mut bytes).unwrap();
        bytes
    }

    /// The same seed and name make the same bytes on every platform: AES and SHA-256 are each
    /// one function, so the first bytes of run 1's object `o0` are these wherever the test runs.
    #[test]
    fn a_seed_and_a_name_make_the_same_bytes_everywhere() {
        let generator = Generator::new(1).unwrap();
        let object = generator.object("o0").unwrap();
        let bytes = whole(&object, 32);
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, GOLDEN_O0);
        let other = generator.object("o1").unwrap();
        assert_ne!(whole(&other, 32), bytes);
        let again = Generator::new(2).unwrap();
        assert_ne!(whole(&again.object("o0").unwrap(), 32), bytes);
    }

    /// Bytes made from any offset are the whole object's at that offset, entered mid-block or not.
    #[test]
    fn any_range_is_the_whole_objects_bytes_at_its_offset() {
        let generator = Generator::new(7).unwrap();
        let object = generator.object("ranges").unwrap();
        let all = whole(&object, 4096);
        for (offset, len) in [
            (0, 1),
            (1, 15),
            (15, 2),
            (16, 16),
            (17, 100),
            (100, 3996),
            (4095, 1),
        ] {
            let mut part = vec![0u8; len];
            object.fill(offset, &mut part).unwrap();
            let offset = usize::try_from(offset).unwrap();
            assert_eq!(part, &all[offset..offset + len], "{offset} {len}");
        }
    }

    /// The counter carries past its low 32 bits as one 128-bit number: blocks made one at a time
    /// from a counter that crosses 2^32 are the keystream made in one call across it.
    #[test]
    fn the_counter_carries_across_its_low_words() {
        let generator = Generator::new(3).unwrap();
        let start = 0x0123_4567_89ab_cdef_0000_0000_ffff_fffe_u128;
        let object = Object::at(&generator.key, start);
        let all = whole(&object, 4 * BLOCK);
        for block in 0..4u128 {
            let one = Object::at(&generator.key, start + block);
            let made = whole(&one, BLOCK);
            let at = usize::try_from(block).unwrap() * BLOCK;
            assert_eq!(made, &all[at..at + BLOCK], "block {block}");
        }
    }

    /// A read with one byte flipped is caught at that byte, a piece of scratch at a time.
    #[test]
    fn the_verifier_finds_a_flipped_byte() {
        let generator = Generator::new(9).unwrap();
        let object = generator.object("flip").unwrap();
        let mut got = whole(&object, 10_000);
        let mut scratch = vec![0u8; 64];
        assert_eq!(object.verify(0, &got, &mut scratch).unwrap(), None);
        got[6_789] ^= 0x10;
        assert_eq!(object.verify(0, &got, &mut scratch).unwrap(), Some(6_789));
        assert_eq!(
            object.verify(3, &got[3..], &mut scratch).unwrap(),
            Some(6_789)
        );
    }

    /// Run 1's object `o0`, its first 32 bytes, recorded once on macOS aarch64 (2026-10-27).
    const GOLDEN_O0: &str = "8ee26fc265a6752206482c5dcad445d06f432ecb84bb3f666755d0f8e64a0147";
}
