# mantle's HTTP/1.1 codec on one core

**Question.** What does mantle's HTTP/1.1 codec (`crates/http`, node.md §4.1) cost as the S3
listener will run it: a request head parsed, a body read or a chunk decoded, a response head
written? The baseline later work on the S3 door is judged against.

**Method.** `mantle bench http` (`crates/mantle/src/bench_http.rs`), a release build, each row
repeated for half a second on one thread, three rounds a machine. The heads are what stock clients
send: a PutObject the AWS CLI signs (864 octets, 14 fields, its body `aws-chunked` with a trailing
checksum, framed chunked), a ranged GetObject boto3 signs (643 octets, 9 fields), and a head of
exactly S3's 8 KB bound (8,192 octets). A body is a mebibyte, framed by Content-Length and given
64 KiB at a time as TCP reads bring it, or chunked in S3's least chunk, 8 KiB (research/05 §1.8),
and in 64 KiB chunks. A response head is a ranged GetObject's, its seven fields S3's.

**Measured.** Nanoseconds each, the median of three rounds; every round of each machine is below.

| operation | the box | this Mac |
|---|---|---|
| parse a PutObject head (864 octets) | 1,086 | 620 |
| parse a ranged GetObject head (643 octets) | 792 | 417 |
| parse a head of 8 KiB | 8,590 | 4,900 |
| a Content-Length body's 64 KiB read | 6 | 3 |
| a chunked body's 8 KiB chunk | 18 | 11 |
| a chunked body's 64 KiB chunk | 22 | 12 |
| write a ranged GetObject's head | 335 | 207 |

The box: adacachy, an AMD Ryzen 9 9950X3D2 (16 cores, 32 threads), 2026-10-25 12:22, its load
averages 44.9, 61.9 and 51.0 from other users' work; the release binary built there from the
step's tree (md5 `f58159b378df466f304cbefccf0ec7fd`).

| round | PutObject | GetObject | 8 KiB head | 64 KiB read | 8 KiB chunk | 64 KiB chunk | response head |
|---|---|---|---|---|---|---|---|
| 1 | 1,086 | 813 | 8,414 | 6 | 18 | 24 | 335 |
| 2 | 1,142 | 792 | 9,203 | 7 | 18 | 22 | 386 |
| 3 | 1,075 | 673 | 8,590 | 6 | 18 | 22 | 334 |

This Mac: an Apple M5 Max (18 cores, 128 GiB), 2026-10-25 13:33, its load averages 8.0, 19.9 and
27.0 (a gate's test suites and the system's own indexing running); the release binary built from
the step's tree (md5 `7c9322b10a1c83b415922b816db5d901`).

| round | PutObject | GetObject | 8 KiB head | 64 KiB read | 8 KiB chunk | 64 KiB chunk | response head |
|---|---|---|---|---|---|---|---|
| 1 | 641 | 431 | 4,995 | 3 | 11 | 13 | 206 |
| 2 | 619 | 413 | 4,872 | 3 | 10 | 12 | 207 |
| 3 | 620 | 417 | 4,900 | 3 | 11 | 12 | 210 |

**What it shows.** A head costs about 1.0 to 1.3 ns an octet on the box and 0.6 to 0.7 on this
Mac, whatever its fields: its line ends are found twice (once to count its fields, so their list is
made once at its size), its colons once, and its octets judged once. A body's octets are never
touched: a Content-Length read is the arithmetic of its span, and a chunk costs its size line and
the line end after it, so a mebibyte chunked in S3's least chunks costs 128 chunk lines, about
2.3 µs on the box and 1.4 µs on this Mac, and its octets nothing. A response head is written into
the caller's buffer with no allocation of its own (`tests/allocations.rs`).

**Generated and mutated requests.** Each of `tests/properties.rs`'s three properties ran 200,000
cases (`PROPTEST_CASES=200000`, release, this Mac, 125 s): every request written from a model read
back as the model in any pieces, every mutated or arbitrary input read as `httparse` reads it or
refused with a typed error, none past its bounds.
