# Sealing data at rest

**Question.** mantle seals every stored byte with AES-256-GCM in 64 KiB segments, each file
under its own data key wrapped with AES-256 key wrap (docs/design/encryption.md). What does
sealing and opening cost per byte on one core, and a file's key per file, next to the hashing
every upload already pays?

**Method.** `mantle bench hash --seconds 1 --sizes 64K`, release build with mantle's profile
(thin LTO, one codegen unit, overflow checks), macOS 26.4.1 on an Apple M5 Max, run three
times. The file is 16 MiB of SplitMix64 bytes, sealed as 256 segments through
`mantle_s3::seal::Segments` and opened back segment by segment, each segment copied into its
own buffer as a gateway would hand it on. A file's key is `DataKey::generate` from the
operating system, `wrap` under a root key and `unwrap` again.

| Measure | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Sealing 64 KiB segments | 8.40 GB/s | 7.89 GB/s | 8.15 GB/s |
| Opening 64 KiB segments | 8.82 GB/s | 8.37 GB/s | 8.33 GB/s |
| A file's data key: made, wrapped and unwrapped | 1.75 µs | 1.76 µs | 1.76 µs |
| SHA-256, 64 KiB, same runs | 3.22 GB/s | 3.24 GB/s | 3.23 GB/s |
| MD5, 64 KiB, same runs | 925 MB/s | 930 MB/s | 927 MB/s |

## Findings

**1. Sealing costs two fifths of the SHA-256 and a ninth of the MD5 each byte already pays.**
AES-256-GCM runs at about 8 GB/s on this core, using the CPU's AES and polynomial-multiply
instructions through AWS-LC. An upload without a `Content-MD5` computes MD5 for its ETag at
0.93 GB/s, so sealing adds about a tenth to its per-byte cost; a read opens at about 8.4 GB/s.

**2. A file's key costs about 1.8 µs.** Reading 32 bytes from the operating system, wrapping
them and unwrapping them once is under a millisecond for a thousand files, and a part of an
upload pays it once.

## Baseline

The runs above are the baseline `mantle bench hash` holds sealing, opening and a file's key
to.
