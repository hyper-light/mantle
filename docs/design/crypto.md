# Cryptography: AWS-LC, vendored

Status: design, 2026-09-29. Sources: docs/research/14 (cited as "14 §x"); the measurements
docs/measurements/2026-09-28-aws-lc-first-random.md and 2026-09-28-aws-lc-crypto.md;
vendor/UPSTREAM.md, which lists every local change.

## 1. The library

mantle's cryptography is AWS-LC, through its Rust binding aws-lc-rs, both vendored under
`vendor/` and patched in for crates.io's copies. The S3 layer's SHA-1, SHA-256, SHA-512, MD5
and HMAC-SHA256 moved to it from the RustCrypto crates. The transport's TLS 1.3 and QUIC will
use it through the same binding. One library means one implementation to follow upstream
and one set of fixes to carry.

Measured against the RustCrypto crates in one process, bulk hashing is as fast or faster on
this machine: SHA-256 and SHA-512 run within 2%, SHA-1 7% faster and MD5 2–5% faster. HMAC
of a short message costs 1.6× as much, 189 ns against 117 ns, for the reason in §5
(2026-09-28-aws-lc-crypto.md). Every target now needs a C toolchain for AWS-LC, including to
type-check (§7).

## 2. Where random bytes come from

**Jitter entropy is built out.** Upstream AWS-LC seeds its DRBG from CPU jitter entropy (14
§1). It costs every new process 17.6 ms before its first random bytes here, and upstream
states about 50 ms (14 §2). The cost is inherent to the source. jitterentropy must be
compiled without optimisation. Its oversampling rate fixes how many timed samples it needs,
and the hashing and memory walk inside each sample are what it measures
(2026-09-28-aws-lc-first-random.md, finding 3). mantle runs many short processes: its
command line, its test binaries, and servers restarted during repair. So the vendored
aws-lc-sys builds jitter entropy in only when `AWS_LC_SYS_NO_JITTER_ENTROPY=0` asks for it.

Without it, AWS-LC's DRBG seeds from the operating system's generator, the source Linux's
`getrandom`, macOS's `getentropy` and Windows's `ProcessPrng` serve. It takes its second input
from RDRAND or RNDR where the CPU has them, and from the operating system otherwise (14 §1).
Two sources still feed every seed.

**A hardware generator that keeps failing gives way to the operating system.** RNDR and
RDRAND may fail a read, and AWS-LC aborted the process on the first RNDR failure. On Google
Axion hosts that kept a service restarting, sometimes for hours, and the same abort was
reported on Graviton (14 §3). Upstream now retries 10
times, Intel's recommendation for RDRAND, and aborts after that (14 §3, §4). The vendored
copy carries that fix. It also carries one further change: when all ten attempts fail, the
bytes come from the operating system and the method succeeds.

The hardware generator supplies only a second input, mixed into a DRBG seeded elsewhere. On
a CPU without one, AWS-LC already takes that input from the operating system. A failed read
now takes the same path instead of ending the process. Tests drive the retry and the fallback
with fake generators that fail on demand, since a real one cannot be made to fail
(vendor/UPSTREAM.md).

**What still aborts.** AWS-LC aborts if the operating system's generator fails (14 §4, §5).
On Linux that is `getrandom` failing with an error other than `EINTR` or `ENOSYS`. After
initialization its only other errors are a bad buffer or flag. On macOS it is `getentropy`
failing on a valid request of at most 256 bytes, "some other fatal error". On Windows
`ProcessPrng` "always returns TRUE". Each is the platform failing, not the library.

**mantle's own identifiers use getrandom.** Volume IDs and log nonces are read with the
`getrandom` crate. It reads the same operating-system source but returns a typed error where
AWS-LC aborts, which is what CLAUDE.md §1 requires of mantle's code.

## 3. No panics

aws-lc-rs's digest and HMAC functions panic when AWS-LC reports a failure. The fallible
forms are private, and even the functions that return `Result` reach the panics (14 §6). A
running digest can fail in practice only by failing to allocate its state.
`mantle_s3::crypto` closes the paths at their cause and adds an unwind boundary as well, as
CLAUDE.md §1 asks of every dependency:

- digests go through `digest::Context::try_new`, `try_update` and `try_finish`, public in
  the vendored copy;
- one-shot SHA-256 goes through `digest::digest`, which calls AWS-LC's `SHA256` and allocates
  nothing;
- HMAC goes through `hmac::sign_once`, added to the vendored copy over AWS-LC's one-shot
  `HMAC`, which allocates nothing;
- every call runs inside `catch_unwind`, and a failure of any kind is `CryptoError`. SigV4
  reports it as `InternalError` (500) rather than as a signature mismatch.

## 4. Comparing signatures

A request's signature is compared with the expected one in constant time
(`constant_time::verify_slices_are_equal`, AWS-LC's `CRYPTO_memcmp`). An internal failure is
kept apart from a mismatch: the HMAC is computed first, and only its result is compared.

## 5. HMAC's cost

AWS-LC's `HMAC_CTX` holds three unions sized for SHA-3, 1,224 bytes, and each HMAC
initializes, copies and wipes them (14 §7). aws-lc-rs adds a copy of the whole context per
signature. For a 170-byte string to sign, the binding's keyed path took 255 ns and AWS-LC's
one-shot 187 ns. An HMAC over AWS-LC's 112-byte `SHA256_CTX` would take 117 ns, which is
RustCrypto's time (2026-09-28-aws-lc-crypto.md, findings 2 and 3).

mantle uses the one-shot. The keys that derive a request's signing key, and the signing key
for its signature, each sign once. A chunked body signs every chunk under one key, and a key
kept for the body would save about 30 ns a chunk (155 against 184 ns), under 1.2% of hashing
S3's smallest chunk; it would also need aws-lc-rs's key copy, which panics on failure, made
fallible. The remaining gap is in AWS-LC's FIPS-module HMAC, whose one-shot takes 170 ns
called from C. Closing it would change how the module copies and wipes key material, for
about 50 ns per HMAC, and mantle does not carry that change.

The larger lever is the gateway's. SigV4's signing key depends only on the secret, the date,
the region and the service (14 §7), so a gateway can derive it once per access key per day.
That removes four of the five HMACs from each request. Such a cache is keyed by a
client-chosen value and holds key material, so it needs a bound, pruning and wiping
(CLAUDE.md §2). It belongs with the gateway's design.

## 6. Vendoring

aws-lc-sys 0.45.0 and aws-lc-rs 1.18.1 are their crates.io packages plus the changes in
vendor/UPSTREAM.md, which also records checksums, upstream commits, and how to update. In
brief: jitter entropy off by default; a system AWS-LC used only on request; the RNDR retry
backported, with the operating-system fallback; the fallible digest entry points and the
one-shot HMAC; MD5; `Clone` for `LessSafeKey` (aws/aws-lc-rs#1165); and upstream's test
data, so both crates' suites run. `cargo test --manifest-path vendor/Cargo.toml --workspace
--locked` is a gate, run on every CI target.

x86_64 Windows builds AWS-LC's NASM sources rather than linking the prebuilt objects the
package ships, so the build contains nothing that was not built from source. CI installs
NASM on that runner.

## 7. Building for every target

aws-lc-sys compiles C and assembly for the target even to type-check, so
`scripts/check-targets.sh` needs a C toolchain per target. The host's own serves its
targets, including both macOS architectures on a Mac. Linux targets from another host go
through cargo-zigbuild, and Windows targets through cargo-xwin. These are the two drivers
aws-lc-rs's own CI cross-builds with (14 §8). x86_64 Windows also needs NASM. The script names
any tool it cannot find. CI builds each target natively on its runner.

GCC 15's `-Werror=unterminated-string-initialization`, aws/aws-lc-rs#935, does not arise in
this copy. aws-lc-sys builds with GCC 15.3.0 under both its builders, including CMake with
`-Werror`, with no such diagnostic (vendor/UPSTREAM.md).

## 8. Open

- aws/aws-lc-rs#1241: TLS 1.3 AES-GCM sealing from several borrowed slices, for the
  transport's record layer.
- aws/aws-lc-rs#617: JWE generation and validation, pending a use in mantle that settles its
  scope.
