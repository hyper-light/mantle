# Decoding a browser's upload and checking its policy

**Question.** A browser uploads with POST Object: a `multipart/form-data` body whose file is
found by searching for the delimiter that ends it, and a policy whose signature and conditions
authorize it (docs/research/19; docs/design/s3-protocol.md §12). What does each cost on one
core, next to the hashing every upload already pays?

**Method.** `mantle bench hash --seconds 1 --sizes 4K,64K,1M,8M`, release build with mantle's
profile (thin LTO, one codegen unit, overflow checks), macOS 26.4.1 on an Apple M5 Max, run
three times. The form is AWS's example form (19 §3.3) with a 16 MiB file of SplitMix64 bytes,
which hold a CR every 256 bytes on average, each a place the delimiter could begin. The
decoder is fed the body in pieces of each size and its output buffer is cleared after each
piece, as a gateway passes each piece on. The policy check is `mantle_s3::post::authorize` on
AWS's example form at 2015-12-29T12:00:00Z: the signing key, the signature over the policy,
the policy's base64 and JSON, its eleven conditions, and every field's coverage.

| Measure | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| A form's file in pieces of 4 KiB | 28.8 GB/s | 28.8 GB/s | 28.6 GB/s |
| 64 KiB | 35.5 GB/s | 35.5 GB/s | 35.3 GB/s |
| 1 MiB | 30.7 GB/s | 30.8 GB/s | 30.8 GB/s |
| 8 MiB | 30.6 GB/s | 30.2 GB/s | 30.4 GB/s |
| A form's policy | 3.00 µs | 2.99 µs | 3.00 µs |
| SHA-256, 1 MiB | 3.28 GB/s | 3.29 GB/s | 3.26 GB/s |
| MD5, 1 MiB | 942 MB/s | 936 MB/s | 937 MB/s |
| A request's signature, AWS's example PUT | 2.42 µs | 2.40 µs | 2.42 µs |

## Findings

**1. The decoder is not what limits a browser's upload.** It passes a file at 29–36 GB/s:
memchr's SIMD search over the Two-Way algorithm finds the delimiter, and the file's bytes are
copied out once. That is about ten times SHA-256's rate and over thirty times MD5's, which
every upload without a `Content-MD5` pays for its ETag. A form's upload costs, per byte, what
a PUT's does.

**2. Writing a condition's text only when it fails halved the policy check.** The first
version wrote each condition's text for S3's error, `["eq", "$acl", "public-read"]`, as it
read the policy, cloned each condition's strings out of the JSON document, and lowercased
every field's name to check its coverage. It took 6.69, 6.63 and 6.51 µs in three runs on
the same machine. Borrowing the conditions from the document, writing the text only for the
condition that fails, and comparing names without copying them brought it to 3.0 µs, against
2.4 µs for a header signature's four chained HMACs, canonical request and string to sign. A
core checks about 330,000 policies a second.

## Baseline

The runs above, on the final build, are the baseline `mantle bench hash` holds form decoding
and the policy check to.
