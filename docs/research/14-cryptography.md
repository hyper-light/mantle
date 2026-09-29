# 14 — Cryptography: AWS-LC, its entropy, and the costs of its interfaces

Research note for mantle's cryptography (`crates/s3/src/crypto.rs`, `vendor/`). It covers:

- where AWS-LC's random bytes come from, and what CPU jitter entropy costs;
- how the CPU's random-number instructions fail, and what AWS-LC did when they did;
- how the operating systems' generators fail;
- where AWS-LC aborts the process, and where aws-lc-rs panics;
- what HMAC costs in AWS-LC, and how SigV4 derives its keys;
- how aws-lc-rs is built for other targets;
- how TLS 1.3 protects a record, and what sealing one from pieces requires;
- what JSON Web Encryption's algorithms need from a cryptographic library.

Compiled 2026-09-28 and 2026-09-29 from the vendored AWS-LC and aws-lc-rs sources, the GitHub
issues and pull requests named below as served those days, the Linux kernel source, Intel's
and Microsoft's documentation, and the Linux and macOS manual pages. This is research input;
the decision record is docs/design/crypto.md.

---

## 1. AWS-LC's entropy sources

Source: [ENT] `crypto/fipsmodule/rand/entropy/entropy_sources.c`, AWS-LC `02561621` as
vendored.

AWS-LC's DRBG draws from one of two configurations. The default:

> "Tree-DRBG entropy source configuration. - Tree DRBG with Jitter Entropy as root for
> seeding. - OS as personalization string source. - If run-time is on an x86_64 or Arm64 CPU
> and it supports rdrand or rndr respectively, use it as a source for prediction resistance.
> Otherwise, no source."

And the one used when jitter entropy is built out (`DISABLE_CPU_JITTER_ENTROPY`), or when
the file system presents SysGenID, the Linux interface that reports a VM resumed from a
snapshot, which AWS-LC calls a "uniqueness breaking event" [UBE]:

> "Out-out CPU Jitter configurations. CPU source required for rule-of-two. - OS as seed source
> source. - Uses rdrand or rndr, if supported, for personalization string. Otherwise falls back
> to OS source."

On a CPU without a hardware generator the second source is the operating system again:
"Fall back to seed source because a second source must always be present." The seed read is
`CRYPTO_sysrand` [VMUBE]. An AWS-LC designer on the two sources: "CPU Jitter is one of
(possibly) 3 different sources. We always use two sources at a minimum" [LC899].

## 2. CPU jitter entropy and its cost

**What AWS-LC changed.** "Starting with AWS-LC v1.60 our entropy source for both FIPS and
non-FIPS builds switched to CPU jitter. We believe this is more secure, but it can add a few
milliseconds of latency when (and only when) a new process is forked" [LC899, 2025-09-29].
rustls reported the change as a 26,000% regression in full-handshake cost, measured in
instructions [LC899].

**Why it cannot be made faster in place.** jitterentropy refuses optimisation:

> "#ifdef \_\_OPTIMIZE\_\_ #error "The CPU Jitter random number generator must not be compiled
> with optimizations. See documentation. Use the compiler switch -O0 for compiling
> jitterentropy.c."" [JENT]

AWS-LC's maintainers confirm it is "required by the logic" [LC899]. AWS-LC's CMake build
states: "At the moment, CPU Jitter source code needs to disable all optimizations"
[JENT-CMAKE].

**The switch.** From aws-lc-sys 0.32.3, "if you have `AWS_LC_SYS_NO_JITTER_ENTROPY=1` set in
the build environment it will disable the build (and use) of CPU Jitter Entropy. This should
eliminate the additional latency that you see when a process starts" [LC899, 2025-10-14]. The
aws-lc-rs user guide: "Use of jitter entropy has a one-time-per-process latency cost,
typically around 50ms, for the collection of entropy. This flag may be used to eliminate this
latency" [LCRS-GUIDE]. On AWS Lambda a user traced "a cold start latency regression (~1
second) on a small Lambda, that was mostly mitigated by this flag" [LC899, 2026-01-30]. The
Rust Lambda runtime now documents the setting [LAMBDA1172].

**Measured here.** 17.6 ms before a new process's first random bytes on an Apple M5 Max:
4,000 timed samples of about 4.5 µs, 73% of them in SHA-3
(docs/measurements/2026-09-28-aws-lc-first-random.md).

## 3. The CPU's random-number instructions, and how they fail

**RNDR (Arm).** "Reads of RNDR set PSTATE.NZCV to 0b0000 on success, and set PSTATE.NZCV to
0b0100 otherwise" [ARCHRANDOM]. AWS-LC's header on the register: "if a random number cannot
be returned 'in a reasonable period of time', PSTATE.NZCV is set to 0b0100 and the returned
value is 0. ... The Arm architecture does not specify a failure probability. Therefore, use
the same attempt bound as for rdrand" [ENT-H, citing the Arm register description].

**RDRAND (Intel).** "It is recommended that applications attempt 10 retries in a tight loop in
the unlikely event that the RDRAND instruction does not return a random number. This number is
based on a binomial probability argument: given the design margins of the DRNG, the odds of
ten failures in a row are astronomically small and would in fact be an indication of a larger
CPU issue" [DRNG §5.2.1]. "CF is the sole indicator of the success or failure of the RDRAND
instruction" [DRNG §5.2].

**Failures in the wild.** On aarch64 with `FEAT_RNG`, "a single failed `RNDR` read aborts the
whole process" [LC3453]. `CRYPTO_rndr_multiple8` "bails on the first failure", while "the
rdrand path retries 10 times" [LC3453]. A service mesh on Google Axion (Neoverse V2) nodes
"crashloops with exit 133 (SIGTRAP), sometimes for hours until whatever node condition makes
RNDR flaky clears up"; "Graviton3/4 expose `FEAT_RNG` too" [LC3453]. rustls handshakes hit it
"~3–4%, host-clustered" [LCRS1233].

**AWS-LC's fix.** aws/aws-lc#3475, merged 2026-09-15 as `8d931575b042`: "Adds a bounded retry
to rndr register reads", with the reason that "applications require a restart, but compute
recover on their own; hence it's not 'stuck' and retrying should be effective" [LC3475]. The
pull request's own note: "Can't induce an rndr register read failure atm. So, logic is not
currently exercised through a test" [LC3475]; the commit adds a test with an injected
generator. After ten failed attempts the method still fails.

## 4. Where AWS-LC aborts

Source: [RAND] `crypto/fipsmodule/rand/rand.c` as vendored.

A method that fails aborts the process, whichever input it supplies:

```c
if (entropy_source->methods->get_prediction_resistance(
  entropy_source, pred_resistance) != 1) {
  abort();
}
...
// If the seed source is missing it is impossible to source any entropy.
if (entropy_source->methods->get_seed(entropy_source, seed) != 1) {
  abort();
}
...
if(entropy_source->methods->get_extra_entropy(
    entropy_source, extra_entropy) != 1) {
  abort();
}
```

## 5. The operating systems' generators

**AWS-LC's reads.** On Linux `CRYPTO_sysrand` calls `getrandom`, retries `EINTR`, falls back to
`/dev/urandom` on `ENOSYS`, and otherwise calls `perror` and `abort` [URANDOM]. On macOS it
calls `getentropy` in pieces of at most 256 bytes and aborts if a call fails [GETENTROPY-C]. On
Windows it calls `ProcessPrng` and aborts if `bcryptprimitives.dll` or the function is
missing, or the call returns false [WINDOWS-C].

**Linux, getrandom(2).** "If the urandom source has not yet been initialized, then getrandom()
will block, unless GRND_NONBLOCK is specified in flags." "If the urandom source has been
initialized, reads of up to 256 bytes will always return as many bytes as requested and will
not be interrupted by signals." Its errors are `EAGAIN` (only with `GRND_NONBLOCK`), `EFAULT`,
`EINTR`, `EINVAL` (an invalid flag) and `ENOSYS` [GETRANDOM].

**macOS, getentropy(2).** "getentropy() will succeed unless: [EINVAL] The buf parameter points
to an invalid address. [EIO] Too many bytes requested, or some other fatal error occurred"
[GETENTROPY-MAC].

**Windows, ProcessPrng.** "Retrieves a specified number of random bytes from the user-mode
per-processor random number generator." Return value: "Always returns TRUE." Windows 8 and
later [PROCESSPRNG].

## 6. Panics in aws-lc-rs 1.18.1

Source: [LCRS] `src/digest.rs`, `src/hmac.rs` as vendored.

`digest::Context::new` unwraps the context's construction; `update` and `finish` call
`try_update(..).expect("digest update failed")` and `try_finish(..).expect("EVP_DigestFinal
failed")`. `hmac::Key::new` calls `try_new(..).expect("Unable to create HmacContext")`;
`Context::update` and `sign` expect `HMAC_Update` and `HMAC_Final` to succeed; `Clone` for a
key's context expects `HMAC_CTX_copy_ex` to. The `try_` forms are private. `sign_to_buffer`
and `verify` return `Result` but reach the same expects. Upstream `main` is unchanged in this
(2026-09-29). A digest context allocates its state in `EVP_DigestInit_ex`, whose failure is an
allocation's [DIGEST-C].

## 7. HMAC in AWS-LC, and SigV4's keys

**The context.** AWS-LC's `HMAC_CTX` holds three unions of every digest's state, the largest
being `uint8_t sha3[400]`: `md_ctx`, `i_ctx` and `o_ctx` [HMAC-H]. `HMAC_CTX_init` sets all of
it to zero, `HMAC_CTX_cleanup` wipes all of it with `OPENSSL_cleanse`, and `HMAC_Init_ex` and
`HMAC_Final` copy whole unions between the three [HMAC-C]. The one-shot `HMAC` keys, uses and
wipes one context [HMAC-C]. aws-lc-rs signs with a `Key` by copying its context
(`HMAC_CTX_copy_ex`, a `memcpy` of the struct) [LCRS; HMAC-C].

**The construction.** HMAC is `H(K XOR opad, H(K XOR ipad, text))` over the hash's block size,
with a key longer than a block first hashed [RFC2104 §2]. RFC 4231 gives HMAC-SHA-256 test
vectors, among them test case 1 (key `0x0b` × 20, "Hi There") and test case 6 (key `0xaa` ×
131) [RFC4231 §4.2, §4.7].

**SigV4's signing key.** `DateKey = HMAC-SHA256("AWS4"+SecretAccessKey, YYYYMMDD)`, then the
region, the service and `"aws4_request"` in turn; the signature is the HMAC of the string to
sign under that key [SIGV4]. The key depends only on the secret, the date, the region and the
service.

## 8. Building aws-lc-sys for other targets

aws-lc-sys compiles AWS-LC's C and assembly in its build script for the target being built.
Its Windows requirements: `x86_64-pc-windows-msvc` needs "C/C++ Compiler & \*NASM", where
"NASM is recommended on x86-64 but can be avoided using prebuilt NASM objects";
`aarch64-pc-windows-msvc` needs "C/C++ Compiler (clang-cl)" [LCRS-WIN]. On cross-compiling
for the MSVC ABI: "plain `clang` (for example `CC=clang`, or cross-compiling with
`cargo-xwin`) runs in GNU driver mode ... Both driver modes are supported, and both are
exercised in CI" [LCRS-WIN]. Upstream CI builds from macOS and Linux hosts with
`cargo zigbuild` for Linux targets and `cargo xwin build` for both MSVC targets [LCRS-CI].
Without the `prebuilt-nasm` feature, prebuilt objects are used only if
`AWS_LC_SYS_PREBUILT_NASM` asks for them and NASM is absent [LCRS-BUILDER].

**GCC 15.** aws/aws-lc-rs#935 reports `-Werror=unterminated-string-initialization` failures in
`aws-lc-fips-sys` 0.13.9's self-test (`static const uint8_t kAESKey[16] = "BoringCrypto
Key";`) [LCRS935]. The report is against the FIPS crate.

## 9. TLS 1.3 records

**The record.** A record's plaintext is `TLSInnerPlaintext`: the content, then its
`ContentType type`, then `uint8 zeros[length_of_padding]`. It is sealed as
`AEAD-Encrypt(write_key, nonce, additional_data, plaintext)`, where `additional_data` is the
record header: `TLSCiphertext.opaque_type || TLSCiphertext.legacy_record_version ||
TLSCiphertext.length` [RFC8446 §5.2].

**The nonce.** "A 64-bit sequence number is maintained separately for reading and writing
records. The appropriate sequence number is incremented by one after reading or writing each
record. Each sequence number is set to zero at the beginning of a connection and whenever the
key is changed ... If a TLS implementation would need to wrap a sequence number, it MUST either
rekey (Section 4.6.3) or terminate the connection." The nonce is the sequence number "encoded
in network byte order and padded to the left with zeros to iv_length", "XORed with either the
static client_write_iv or server_write_iv" [RFC8446 §5.3].

**Limits.** "For AES-GCM, up to 2^24.5 full-size records (about 24 million) may be encrypted on
a given connection while keeping a safety margin of approximately 2^-57 for Authenticated
Encryption (AE) security" [RFC8446 §5.5].

**AWS-LC's TLS 1.3 AEAD.** `aead_aes_gcm_tls13_seal_scatter` takes the mask from the first nonce
it seals, assuming sequence number 0, and then refuses a nonce whose counter is `UINT64_MAX`
or below the next one allowed: `if (given_counter == UINT64_MAX || given_counter <
gcm_ctx->min_next_nonce)` [E-AES]. Its input is one contiguous buffer and an extra tail
[LCRS].

**Sealing from pieces.** aws/aws-lc-rs#1241 asks for "one AES-GCM invocation over the logical
concatenation of those slices, using one nonce and producing one authentication tag". Its
acceptance criteria: byte-identical output to the existing TLS AEAD for AES-128-GCM and
AES-256-GCM; one nonce and one tag, "including unaligned and empty slices"; "strictly
increasing sequence numbers, no sequence wrap, and no key reuse after partially executed
encryption fails"; "safe output bounds even when an iterator yields a different length than
declared"; "preservation of existing output prefixes; no exposure of uninitialized bytes";
no change to FIPS behavior [LCRS1241]. AWS-LC's incremental GCM finishes a partial block left
between updates one byte at a time (`CRYPTO_gcm128_encrypt_ctr32`) [GCM-C].

**A trace to test against.** RFC 8448 §3 traces a TLS 1.3 handshake with every secret. The
server's first encrypted record carries EncryptedExtensions (40 octets), Certificate (445),
CertificateVerify (136) and Finished (36), each traced on its own, under the key and IV of
"{server} derive write traffic keys for handshake data". It gives the complete record, 679
octets, beginning `17 03 03 02 a2` [RFC8448 §3].

## 10. JSON Web Encryption's algorithms

**The scope asked of aws-lc-rs.** aws/aws-lc-rs#617 asks for JWE generation and validation. An
aws-lc-rs maintainer's reply: "While support for JOSE's high-level operations may be out of
scope for our library, I see value in ensuring that our library provides whatever
cryptographic operations are required for its implementation" [LCRS617].

**The algorithms.** RFC 7518 registers JWE's key-management algorithms (§4.1): RSA1_5,
RSA-OAEP and RSA-OAEP-256; A128KW, A192KW and A256KW (AES Key Wrap, RFC 3394); dir; ECDH-ES
and ECDH-ES with each AES Key Wrap; A128GCMKW, A192GCMKW and A256GCMKW; and PBES2 with
HS256+A128KW, HS384+A192KW and HS512+A256KW. ECDH-ES derives its key with "the Concat KDF, as
defined in Section 5.8.1 of [NIST.800-56A], where the Digest Method is SHA-256" (§4.6.2). Its
content-encryption algorithms (§5.1) are A128CBC-HS256, A192CBC-HS384 and A256CBC-HS512, and
A128GCM, A192GCM and A256GCM [RFC7518].

**AES_CBC_HMAC_SHA2 (§5.2.2.1).** "MAC_KEY consists of the initial MAC_KEY_LEN octets of K, in
order. ENC_KEY consists of the final ENC_KEY_LEN octets of K, in order." "The IV used is a
128-bit value generated randomly or pseudorandomly." "The plaintext is CBC encrypted using
PKCS #7 padding using ENC_KEY as the key and the IV." "The octet string AL is equal to the
number of bits in the Additional Authenticated Data A expressed as a 64-bit unsigned big-endian
integer." The tag is HMAC over "the Additional Authenticated Data A, the Initialization Vector
IV, the ciphertext E ..., and the octet string AL", keyed with MAC_KEY, of which "the first
T_LEN octets of M are used as T." Decryption (§5.2.2.2) checks the HMAC first: "If those
values are identical, then A and E are considered valid, and processing is continued.
Otherwise, all of the data used in the MAC validation are discarded, and the authenticated
decryption operation returns an indication that it failed." Appendix B gives test cases for
all three [RFC7518].

**AES Key Wrap with a 192-bit key.** RFC 3394 §4.2 and §4.4 give 192-bit-KEK test vectors
[RFC3394]; RFC 5649 §6 gives two padded key-wrap examples under a 192-bit KEK [RFC5649].
aws-lc-rs wraps with AWS-LC's `AES_wrap_key` under a key set by `AES_set_encrypt_key` with
the KEK's length in bits, and offers AES_128 and AES_256 KEKs [LCRS].

**A worked example.** RFC 7516 Appendix A.3 encrypts "Live long and prosper." with A128KW and
A128CBC-HS256 and gives every intermediate value and the compact serialization. "Since both the
AES Key Wrap and AES GCM computations are deterministic, the resulting JWE value will be the
same for all encryptions performed using these inputs" [RFC7516 §A.3.8].

## Sources

- [ENT] AWS-LC `crypto/fipsmodule/rand/entropy/entropy_sources.c`, commit 02561621ffa4cf17c0c4f70bc11a82df36b42ae9, as vendored in `vendor/aws-lc-sys/aws-lc/`.
- [ENT-H] AWS-LC `crypto/fipsmodule/rand/entropy/internal.h` at 8d931575b042 (aws/aws-lc#3475).
- [VMUBE] AWS-LC `crypto/rand_extra/vm_ube_fallback.c`.
- [UBE] AWS-LC `crypto/ube/vm_ube_detect.h` and `crypto/ube/internal.h`.
- [RAND] AWS-LC `crypto/fipsmodule/rand/rand.c`, `rand_maybe_get_ctr_drbg_pred_resistance` and `rand_get_ctr_drbg_seed_entropy`.
- [URANDOM] AWS-LC `crypto/rand_extra/urandom.c`.
- [GETENTROPY-C] AWS-LC `crypto/rand_extra/getentropy.c`.
- [WINDOWS-C] AWS-LC `crypto/rand_extra/windows.c`.
- [DIGEST-C] AWS-LC `crypto/fipsmodule/digest/digest.c`, `EVP_DigestInit_ex`.
- [HMAC-H] AWS-LC `include/openssl/hmac.h`, `union md_ctx_union` and `struct hmac_ctx_st`.
- [HMAC-C] AWS-LC `crypto/fipsmodule/hmac/hmac.c`.
- [JENT] AWS-LC `third_party/jitterentropy/jitterentropy-library/src/jitterentropy-base.c`, lines 46–47.
- [JENT-CMAKE] AWS-LC `third_party/jitterentropy/CMakeLists.txt`.
- [LCRS] aws-lc-rs 1.18.1 `aws-lc-rs/src/digest.rs`, `src/digest/digest_ctx.rs`, `src/hmac.rs`; https://github.com/aws/aws-lc-rs/blob/main/aws-lc-rs/src/digest.rs and `hmac.rs`, fetched 2026-09-29.
- [LCRS-GUIDE] aws-lc-rs user guide, "Entropy Configuration", https://aws.github.io/aws-lc-rs/resources.html#entropy-configuration (book/src/resources.md at v1.18.1).
- [LCRS-WIN] aws-lc-rs user guide, "Windows Requirements", book/src/requirements/windows.md at v1.18.1.
- [LCRS-CI] aws-lc-rs `.github/workflows/zig.yml` and `cross.yml` at v1.18.1.
- [LCRS-BUILDER] aws-lc-sys 0.45.0 `builder/main.rs`, `use_prebuilt_nasm`.
- [LC899] aws/aws-lc-rs#899, "Large performance regression 1.14.0 -> 1.14.1", https://github.com/aws/aws-lc-rs/issues/899, with comments by justsmth, torben-hansen and jlizen.
- [LC3453] aws/aws-lc#3453, "transient RNDR failure on aarch64 aborts the process (no retry, unlike rdrand)", https://github.com/aws/aws-lc/issues/3453.
- [LC3475] aws/aws-lc#3475, "Retry failed rndr register read a bounded number of times", https://github.com/aws/aws-lc/pull/3475.
- [LCRS1233] aws/aws-lc-rs#1233, "Process aborts on aarch64 when a transient RNDR read fails (no retry)", https://github.com/aws/aws-lc-rs/issues/1233.
- [LCRS935] aws/aws-lc-rs#935, "Fails to build with newer versions of GCC due to '-Werror=unterminated-string-initialization'", https://github.com/aws/aws-lc-rs/issues/935.
- [LAMBDA1172] aws/aws-lambda-rust-runtime#1172, "Document AWS-LC jitter entropy cold-start cost", https://github.com/aws/aws-lambda-rust-runtime/pull/1172.
- [ARCHRANDOM] Linux `arch/arm64/include/asm/archrandom.h`, `__arm64_rndr`, https://github.com/torvalds/linux/blob/master/arch/arm64/include/asm/archrandom.h, fetched 2026-09-29.
- [DRNG] Intel Digital Random Number Generator (DRNG) Software Implementation Guide, §5.2 and §5.2.1, https://www.intel.com/content/www/us/en/developer/articles/guide/intel-digital-random-number-generator-drng-software-implementation-guide.html.
- [GETRANDOM] getrandom(2), Linux man-pages, https://man7.org/linux/man-pages/man2/getrandom.2.html.
- [GETENTROPY-MAC] getentropy(2), macOS 26.4 System Calls Manual.
- [PROCESSPRNG] "ProcessPrng function", Microsoft Learn, https://learn.microsoft.com/en-us/windows/win32/seccng/processprng.
- [RFC2104] H. Krawczyk, M. Bellare, R. Canetti, "HMAC: Keyed-Hashing for Message Authentication", RFC 2104, February 1997.
- [RFC4231] M. Nystrom, "Identifiers and Test Vectors for HMAC-SHA-224, HMAC-SHA-256, HMAC-SHA-384, and HMAC-SHA-512", RFC 4231, December 2005.
- [E-AES] AWS-LC `crypto/fipsmodule/cipher/e_aes.c`, `aead_aes_gcm_tls13_seal_scatter`.
- [GCM-C] AWS-LC `crypto/fipsmodule/modes/gcm.c`, `CRYPTO_gcm128_encrypt_ctr32`.
- [LCRS617] aws/aws-lc-rs#617, "Support JWE generation and validation", https://github.com/aws/aws-lc-rs/issues/617, with the reply of 2024-11-26.
- [RFC3394] J. Schaad, R. Housley, "Advanced Encryption Standard (AES) Key Wrap Algorithm", RFC 3394, September 2002, §4.2, §4.4.
- [RFC5649] R. Housley, M. Dworkin, "Advanced Encryption Standard (AES) Key Wrap with Padding Algorithm", RFC 5649, September 2009, §6.
- [RFC7516] M. Jones, J. Hildebrand, "JSON Web Encryption (JWE)", RFC 7516, May 2015, Appendix A.3.
- [RFC7518] M. Jones, "JSON Web Algorithms (JWA)", RFC 7518, May 2015, §4.1, §4.6.2, §5.1, §5.2, Appendix B.
- [LCRS1241] aws/aws-lc-rs#1241, "Support TLS 1.3 AES-GCM sealing from multiple borrowed input slices", https://github.com/aws/aws-lc-rs/issues/1241.
- [RFC8446] E. Rescorla, "The Transport Layer Security (TLS) Protocol Version 1.3", RFC 8446, August 2018, §5.2, §5.3, §5.5.
- [RFC8448] M. Thomson, "Example Handshake Traces for TLS 1.3", RFC 8448, January 2019, §3.
- [SIGV4] "Create a signed AWS API request", AWS IAM User Guide, https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html, "Derive a signing key".
