// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR ISC

// mantle: a digest's state at a block boundary, read out of a context and resumed from
// (vendor/UPSTREAM.md).

use crate::aws_lc::{MD5_CTX, SHA256_CTX, SHA512_CTX, SHA_CTX};
use crate::digest::{Algorithm, AlgorithmID, Context, MAX_CHAINING_LEN};
use crate::error::Unspecified;

/// A digest's state between two blocks: the chaining value its compression function carries and
/// the bytes it has taken. It is taken only where no input waits in the context's buffer, so these
/// two are the whole state, and [`Context::resume`] goes on from it as the context it was taken
/// from would.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct State {
    algorithm: &'static Algorithm,
    chaining: [u8; MAX_CHAINING_LEN],
    len: u64,
}

impl core::fmt::Debug for State {
    // A chaining value can be keyed material (HMAC's inner and outer states), so it is not shown.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("State")
            .field("algorithm", self.algorithm)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// How a family of algorithms keeps its state in AWS-LC.
#[derive(Clone, Copy)]
enum Family {
    /// `MD5_CTX`: four 32-bit words, little-endian as MD5's digest writes them.
    Md5,
    /// `SHA_CTX`: five 32-bit words, big-endian.
    Sha1,
    /// `SHA256_CTX`, for SHA-224 and SHA-256: eight 32-bit words, big-endian.
    Sha256,
    /// `SHA512_CTX`, for SHA-384, SHA-512 and SHA-512/256: eight 64-bit words, big-endian.
    Sha512,
}

impl Family {
    /// The family of `algorithm` and the bytes of its chaining value; `None` for SHA-3, whose
    /// sponge keeps no chaining value of this kind.
    fn of(algorithm: &Algorithm) -> Option<(Self, usize)> {
        match algorithm.id {
            AlgorithmID::MD5 => Some((Self::Md5, 16)),
            AlgorithmID::SHA1 => Some((Self::Sha1, 20)),
            AlgorithmID::SHA224 | AlgorithmID::SHA256 => Some((Self::Sha256, 32)),
            AlgorithmID::SHA384 | AlgorithmID::SHA512 | AlgorithmID::SHA512_256 => {
                Some((Self::Sha512, 64))
            }
            AlgorithmID::SHA3_256 | AlgorithmID::SHA3_384 | AlgorithmID::SHA3_512 => None,
        }
    }
}

impl State {
    /// A state from its parts, as [`State::chaining`] and [`State::len`] give them.
    ///
    /// # Errors
    /// `error::Unspecified` for an algorithm that keeps no such state (SHA-3), a chaining value of
    /// another length than the algorithm's, or a length that is not a whole number of blocks or
    /// whose count of bits does not fit 64 bits.
    pub fn new(
        algorithm: &'static Algorithm,
        chaining: &[u8],
        len: u64,
    ) -> Result<Self, Unspecified> {
        let (_, chaining_len) = Family::of(algorithm).ok_or(Unspecified)?;
        let block = u64::try_from(algorithm.block_len).map_err(|_| Unspecified)?;
        if chaining.len() != chaining_len
            || len % block != 0
            || len.checked_mul(8).is_none()
            || len > algorithm.max_input_len
        {
            return Err(Unspecified);
        }
        let mut state = Self {
            algorithm,
            chaining: [0u8; MAX_CHAINING_LEN],
            len,
        };
        state.chaining[..chaining_len].copy_from_slice(chaining);
        Ok(state)
    }

    /// The algorithm whose state it is.
    #[must_use]
    pub fn algorithm(&self) -> &'static Algorithm {
        self.algorithm
    }

    /// The chaining value: its words in the byte order the algorithm's digest writes them.
    #[must_use]
    pub fn chaining(&self) -> &[u8] {
        let len = Family::of(self.algorithm).map_or(0, |(_, len)| len);
        &self.chaining[..len]
    }

    /// The bytes digested, a whole number of blocks.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether no byte was digested.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Context {
    /// The context's state, which it holds whole only at a block boundary.
    ///
    /// # Errors
    /// `error::Unspecified` while input waits in the context's buffer, past a block boundary, or
    /// for an algorithm that keeps no such state (SHA-3).
    pub fn state(&self) -> Result<State, Unspecified> {
        let (family, chaining_len) = Family::of(self.algorithm).ok_or(Unspecified)?;
        let mut chaining = [0u8; MAX_CHAINING_LEN];
        // The context was initialized for `self.algorithm` (`DigestContext::new`), so its
        // `md_data` points to that algorithm's context, the struct `family` names.
        let bits = unsafe {
            let md_data = (*self.digest_ctx.as_ptr()).md_data;
            if md_data.is_null() {
                return Err(Unspecified);
            }
            match family {
                Family::Md5 => {
                    let ctx = &*md_data.cast::<MD5_CTX>();
                    if ctx.num != 0 {
                        return Err(Unspecified);
                    }
                    for (out, word) in chaining.chunks_exact_mut(4).zip(ctx.h.iter()) {
                        out.copy_from_slice(&word.to_le_bytes());
                    }
                    (u64::from(ctx.Nh) << 32) | u64::from(ctx.Nl)
                }
                Family::Sha1 => {
                    let ctx = &*md_data.cast::<SHA_CTX>();
                    if ctx.num != 0 {
                        return Err(Unspecified);
                    }
                    for (out, word) in chaining.chunks_exact_mut(4).zip(ctx.h.iter()) {
                        out.copy_from_slice(&word.to_be_bytes());
                    }
                    (u64::from(ctx.Nh) << 32) | u64::from(ctx.Nl)
                }
                Family::Sha256 => {
                    let ctx = &*md_data.cast::<SHA256_CTX>();
                    if ctx.num != 0 {
                        return Err(Unspecified);
                    }
                    for (out, word) in chaining.chunks_exact_mut(4).zip(ctx.h.iter()) {
                        out.copy_from_slice(&word.to_be_bytes());
                    }
                    (u64::from(ctx.Nh) << 32) | u64::from(ctx.Nl)
                }
                Family::Sha512 => {
                    let ctx = &*md_data.cast::<SHA512_CTX>();
                    // A count of bits past 64 bits is past `max_input_len`.
                    if ctx.num != 0 || ctx.Nh != 0 {
                        return Err(Unspecified);
                    }
                    for (out, word) in chaining.chunks_exact_mut(8).zip(ctx.h.iter()) {
                        out.copy_from_slice(&word.to_be_bytes());
                    }
                    ctx.Nl
                }
            }
        };
        // The context's own count and AWS-LC's must agree, in whole bytes.
        if bits % 8 != 0 || bits / 8 != self.msg_len {
            return Err(Unspecified);
        }
        State::new(self.algorithm, &chaining[..chaining_len], self.msg_len)
    }

    /// A context that goes on from `state` as the context it was taken from would.
    ///
    /// # Errors
    /// `error::Unspecified` if an aws-lc digest context cannot be initialized for the state's
    /// algorithm.
    pub fn resume(state: &State) -> Result<Self, Unspecified> {
        let (family, _) = Family::of(state.algorithm).ok_or(Unspecified)?;
        let bits = state.len.checked_mul(8).ok_or(Unspecified)?;
        let mut context = Self::try_new(state.algorithm)?;
        // `try_new` initialized the context for the state's algorithm, so its `md_data` points to
        // the struct `family` names; every field the next update and the final padding read is
        // set, and `num` says no input waits in the buffer.
        unsafe {
            let md_data = (*context.digest_ctx.as_mut_ptr()).md_data;
            if md_data.is_null() {
                return Err(Unspecified);
            }
            let low = (bits & u64::from(u32::MAX)) as u32;
            let high = (bits >> 32) as u32;
            match family {
                Family::Md5 => {
                    let ctx = &mut *md_data.cast::<MD5_CTX>();
                    for (word, bytes) in ctx.h.iter_mut().zip(state.chaining.chunks_exact(4)) {
                        *word = u32::from_le_bytes(bytes.try_into().map_err(|_| Unspecified)?);
                    }
                    ctx.Nl = low;
                    ctx.Nh = high;
                    ctx.num = 0;
                }
                Family::Sha1 => {
                    let ctx = &mut *md_data.cast::<SHA_CTX>();
                    for (word, bytes) in ctx.h.iter_mut().zip(state.chaining.chunks_exact(4)) {
                        *word = u32::from_be_bytes(bytes.try_into().map_err(|_| Unspecified)?);
                    }
                    ctx.Nl = low;
                    ctx.Nh = high;
                    ctx.num = 0;
                }
                Family::Sha256 => {
                    let ctx = &mut *md_data.cast::<SHA256_CTX>();
                    for (word, bytes) in ctx.h.iter_mut().zip(state.chaining.chunks_exact(4)) {
                        *word = u32::from_be_bytes(bytes.try_into().map_err(|_| Unspecified)?);
                    }
                    ctx.Nl = low;
                    ctx.Nh = high;
                    ctx.num = 0;
                }
                Family::Sha512 => {
                    let ctx = &mut *md_data.cast::<SHA512_CTX>();
                    for (word, bytes) in ctx.h.iter_mut().zip(state.chaining.chunks_exact(8)) {
                        *word = u64::from_be_bytes(bytes.try_into().map_err(|_| Unspecified)?);
                    }
                    ctx.Nl = bits;
                    ctx.Nh = 0;
                    ctx.num = 0;
                }
            }
        }
        context.msg_len = state.len;
        context.max_input_reached = state.len == state.algorithm.max_input_len;
        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    use crate::digest::{self, Context, State};
    use alloc::vec::Vec;

    const RESUMABLE: [&digest::Algorithm; 7] = [
        &digest::MD5_FOR_LEGACY_USE_ONLY,
        &digest::SHA1_FOR_LEGACY_USE_ONLY,
        &digest::SHA224,
        &digest::SHA256,
        &digest::SHA384,
        &digest::SHA512,
        &digest::SHA512_256,
    ];

    /// Bytes from a linear congruential generator (Knuth's MMIX constants), so each input is the
    /// same on every run.
    fn generated(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed;
        (0..len)
            .map(|_| {
                x = x
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (x >> 56) as u8
            })
            .collect()
    }

    #[test]
    fn a_digest_resumed_at_every_block_boundary_equals_the_one_shot_digest() {
        for algorithm in RESUMABLE {
            let block = algorithm.block_len();
            for (seed, len) in [0, 1, block - 1, block, block + 1, 5 * block + 3, 9 * block]
                .into_iter()
                .enumerate()
            {
                let input = generated(len, seed as u64);
                let want = digest::digest(algorithm, &input);
                for at in (0..=len).step_by(block) {
                    let mut first = Context::try_new(algorithm).unwrap();
                    first.try_update(&input[..at]).unwrap();
                    let state = first.state().unwrap();
                    assert_eq!(state.len(), at as u64);
                    // Through its parts, as a record keeps them.
                    let kept = State::new(algorithm, state.chaining(), state.len()).unwrap();
                    assert_eq!(kept, state);
                    let mut rest = Context::resume(&kept).unwrap();
                    rest.try_update(&input[at..]).unwrap();
                    assert_eq!(rest.try_finish().unwrap().as_ref(), want.as_ref());
                    // The context it was taken from goes on unchanged.
                    first.try_update(&input[at..]).unwrap();
                    assert_eq!(first.try_finish().unwrap().as_ref(), want.as_ref());
                }
            }
        }
    }

    #[test]
    fn a_state_mid_block_or_of_sha3_is_refused() {
        for algorithm in RESUMABLE {
            let block = algorithm.block_len();
            let mut ctx = Context::try_new(algorithm).unwrap();
            ctx.try_update(&generated(block + 1, 7)).unwrap();
            assert!(ctx.state().is_err());
            let chaining_len = Context::try_new(algorithm)
                .unwrap()
                .state()
                .unwrap()
                .chaining()
                .len();
            let zeros = [0u8; digest::MAX_CHAINING_LEN];
            assert!(State::new(algorithm, &zeros[..chaining_len], 1).is_err());
            assert!(State::new(algorithm, &zeros[..chaining_len - 1], 0).is_err());
            assert!(State::new(algorithm, &zeros[..chaining_len], u64::MAX - 63).is_err());
        }
        let ctx = Context::try_new(&digest::SHA3_256).unwrap();
        assert!(ctx.state().is_err());
        assert!(State::new(&digest::SHA3_256, &[0u8; 32], 0).is_err());
    }

    #[test]
    fn a_fresh_state_is_the_algorithms_initial_value() {
        // RFC 1321 §3.3: word A 01 23 45 67, B 89 ab cd ef, C fe dc ba 98, D 76 54 32 10, low
        // byte first.
        let md5 = Context::try_new(&digest::MD5_FOR_LEGACY_USE_ONLY)
            .unwrap()
            .state()
            .unwrap();
        assert_eq!(
            crate::hex::encode(md5.chaining()),
            "0123456789abcdeffedcba9876543210"
        );
        // FIPS 180-4 §5.3.3: SHA-256's H(0) begins 6a09e667.
        let sha256 = Context::try_new(&digest::SHA256).unwrap().state().unwrap();
        assert!(crate::hex::encode(sha256.chaining()).starts_with("6a09e667bb67ae85"));
        assert!(sha256.is_empty());
    }
}
