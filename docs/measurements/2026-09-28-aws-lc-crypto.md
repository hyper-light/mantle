# The gateway's hashing and HMAC: RustCrypto against AWS-LC

**Question.** mantle's S3 code hashed with the RustCrypto crates (sha1 0.11.0, sha2 0.11.0,
md-5 0.11.0, hmac 0.13.0) and now uses AWS-LC through the vendored aws-lc-rs 1.18.1
(docs/design/crypto.md). What does each operation on the gateway's path cost with each?

**Method.** One binary links both libraries. The AWS-LC side calls `mantle_s3::crypto`, as the
gateway does, so its figures include the unwind boundary and the fallible entry points. The
build is a release build with mantle's profile (thin LTO, one codegen unit, overflow checks),
run on macOS 26.4.1 on an Apple M5 Max. Each case alternates the two libraries for 15 rounds of
150 ms, and which runs first swaps each round, so both see the same machine state. The table
gives the median round.

The comparison has to be interleaved. `mantle bench hash` run once on each build gave rows the
change does not touch, CRC-32 and xxHash, up to 30% apart. Separate sessions measure the
machine as much as the library.

| Operation | RustCrypto | AWS-LC | AWS-LC ÷ RustCrypto |
|---|---|---|---|
| SHA-1, 8 KiB / 64 KiB / 1 MiB | 3.03 / 3.06 / 2.96 GB/s | 3.19 / 3.27 / 3.16 GB/s | 1.05 / 1.07 / 1.07 |
| SHA-256, 8 KiB / 64 KiB / 1 MiB | 3.26 / 3.28 / 3.28 GB/s | 3.21 / 3.27 / 3.28 GB/s | 0.98 / 1.00 / 1.00 |
| SHA-512, 8 KiB / 64 KiB / 1 MiB | 1.84 / 1.86 / 1.85 GB/s | 1.81 / 1.85 / 1.85 GB/s | 0.98 / 1.00 / 1.00 |
| MD5, 8 KiB / 64 KiB / 1 MiB | 0.91 / 0.89 / 0.91 GB/s | 0.93 / 0.93 / 0.94 GB/s | 1.02 / 1.05 / 1.03 |
| SHA-256 of a 400-byte canonical request | 128 ns | 130 ns | 0.99 |
| HMAC-SHA256 of a 170-byte string to sign | 117 ns | 189 ns | 0.62 |
| SigV4 signing key, four chained HMACs | 367 ns | 588 ns | 0.63 |

## Findings

**1. Bulk hashing is as fast or faster.** Both libraries use the CPU's SHA-1 and SHA-2
instructions here. SHA-256 and SHA-512 run within 2% of each other, and AWS-LC runs SHA-1 7%
faster and MD5 2–5% faster. MD5 runs at about 0.93 GB/s, a third of SHA-256's rate. Every byte
of an upload without a client-supplied `Content-MD5` pays it, for the ETag.

**2. HMAC on short messages costs 1.6× as much.** A SigV4 signature verification computes five
HMACs and one SHA-256, about 0.25 µs more with AWS-LC than with RustCrypto. The cost is in
AWS-LC's HMAC itself. Its `HMAC_CTX` holds three 400-byte unions sized for SHA-3, 1,224 bytes,
and every HMAC initializes, copies and wipes them. Called from C, AWS-LC's one-shot `HMAC`
takes 170 ns for this message. An HMAC over AWS-LC's own 112-byte `SHA256_CTX` takes 117 ns,
which is RustCrypto's time. The difference is that context handling. Avoiding it would mean
changing the copying and wiping inside AWS-LC's FIPS-module HMAC, and mantle does not carry
that change. Caching each access key's signing key per day, which SigV4's scoping allows,
would remove four of the five HMACs from every request.

**3. The binding's keyed HMAC copies its context; the one-shot does not.** aws-lc-rs signs
with a `Key` by copying the key's whole context. Measured together in one run, keying a `Key`
and signing with a copy took 255 ns; AWS-LC's one-shot `HMAC`, which the vendored aws-lc-rs
exposes as `hmac::sign_once`, took 187 ns. `sample(1)` on the keyed path puts 46% of samples
in `memmove` and `memset`, 8% in `OPENSSL_cleanse`, and 21% in the SHA-256 compression
function. A key kept across signatures, as a chunked body's signing key could be, signs in
155 ns against the one-shot's 184 ns, measured together in a later run. That is 30 ns a
chunk, under 1.2% of hashing an 8 KiB chunk at 3.1 GB/s. `mantle_s3::crypto::hmac_sha256` is
the one-shot for every HMAC.

## Baseline: `mantle bench hash`

The benchmark the gateway's hashing is held to (`crates/mantle/src/bench_hash.rs`), run three
times with `--seconds 1` on the final build, same machine. The second of the three runs:

| Algorithm | 8 KiB | 64 KiB | 1 MiB | 8 MiB |
|---|---|---|---|---|
| CRC32 | 54.7 GB/s | 93.4 GB/s | 101 GB/s | 98.7 GB/s |
| CRC32C | 52.3 GB/s | 92.9 GB/s | 101 GB/s | 97.2 GB/s |
| CRC64NVME | 57.2 GB/s | 74.6 GB/s | 74.2 GB/s | 73.2 GB/s |
| SHA1 | 3.09 GB/s | 3.17 GB/s | 3.18 GB/s | 3.18 GB/s |
| SHA256 | 3.08 GB/s | 3.17 GB/s | 3.18 GB/s | 3.17 GB/s |
| MD5 | 895 MB/s | 905 MB/s | 907 MB/s | 907 MB/s |
| XXHASH64 | 23.5 GB/s | 25.2 GB/s | 26.0 GB/s | 26.2 GB/s |
| XXHASH3 | 34.2 GB/s | 42.8 GB/s | 45.4 GB/s | 45.3 GB/s |
| XXHASH128 | 34.3 GB/s | 44.0 GB/s | 45.2 GB/s | 44.8 GB/s |
| SHA512 | 1.74 GB/s | 1.81 GB/s | 1.82 GB/s | 1.82 GB/s |

Verifying AWS's example PUT signature took 2.45 µs (2.45–2.65 µs over the three runs), about
400K requests a second on one core. Decoding a body of signed chunks, SHA-256 of each chunk
and its HMAC-SHA256 signature checked, ran at 3.05 GB/s with 64 KiB chunks (3.00–3.10 GB/s)
and 2.44 GB/s with 8 KiB chunks, S3's smallest.
