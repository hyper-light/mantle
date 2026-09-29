# Sealing a TLS 1.3 record from its pieces

**Question.** A TLS 1.3 record's plaintext often lies in pieces: a payload in several buffers,
then the inner content type. The vendored aws-lc-rs seals them where they lie with
`Tls13VectoredSealingKey` (aws/aws-lc-rs#1241; vendor/UPSTREAM.md). The alternative gathers
them into one buffer and seals that with `TlsRecordSealingKey`. Which is faster, and at what
record size?

**Method.** One binary, release build, macOS 26.4.1 on an Apple M5 Max. Each case appends one
record to a reused `Vec` at a time, with increasing sequence numbers. It alternates the two
ways for 15 rounds of 150 ms, swapping which runs first each round, and the table gives the
median round's plaintext rate. The contiguous way copies each piece into the vector, then
seals in place (AWS-LC's TLS 1.3 AEAD, one call). The vectored way seals the pieces into the
vector's spare capacity (AWS-LC's incremental AES-GCM, one update per piece).

| Record | Pieces, then the type byte | AES-128-GCM: gather + seal | vectored | ratio | AES-256-GCM ratio |
|---|---|---|---|---|---|
| 16 KiB | 4 KiB, 12 KiB | 10.56 GB/s | 11.73 GB/s | 1.11 | 1.10 |
| 16 KiB | 1, 1, 1, 5, 4 and 4 KiB | 10.52 GB/s | 11.40 GB/s | 1.08 | 1.08 |
| 1 KiB | 512 B, 512 B | 6.06 GB/s | 6.49 GB/s | 1.07 | 1.05 |
| 1 KiB | 5 B, 1,019 B | 6.04 GB/s | 6.07 GB/s | 1.00 | |
| 512 B | 5 B, 507 B | 4.06 GB/s | 4.02 GB/s | 0.99 | |
| 256 B | 5 B, 251 B | 2.53 GB/s | 2.26 GB/s | 0.90 | 0.89 |

The first three rows and the AES-256-GCM column come from one run, the 5-byte-header rows
from a second; the 256-byte row's AES-128-GCM ratio was 0.87 in the first.

## Findings

**1. Full records seal 8–11% faster from their pieces.** A 16 KiB record, the largest TLS 1.3
allows and the size bulk transfers send, skips the pass that gathers the plaintext.

**2. Below about 1 KiB, gathering is as fast or faster.** The incremental interface does a
fixed amount of work per record: it starts the record, adds the additional data, makes one
call per piece, then finishes and reads the tag. It also completes a block that a piece left
partial one byte at a time (`CRYPTO_gcm128_encrypt_ctr32`). With 5-byte headers the
crossover is near 1 KiB, and a 256-byte record seals at 87–90% of the gathered rate. The two
keys cannot share a traffic key, because AWS-LC's TLS 1.3 AEAD takes its nonce mask from the
first record it seals. So a connection uses the vectored key where its records are large and
fragmented.
