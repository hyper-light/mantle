# The client's two doors

Status: built 2026-11-12. How `mantle`'s object commands ask the cell through either of its two
APIs, the native protocol over QUIC and S3's HTTP API, and how each says the same. The facts it
rests on are research/86's.

## 1. Choosing the door

Every object command (`mb`, `rb`, `ls`, `ls --versions`, `cp`, `rm`, `head`, `multipart
create|put|copy|parts|uploads|complete|abort`) asks through the door its operator names: `--door
native` or `--door s3`, else `MANTLE_DOOR`, else the native door, as the AWS CLI ranks a
command-line option over the environment over its files (research/86 [1]). The native door is the
default because every client directory `mantle init client` makes holds its certificate. A command
never moves to the other door when one fails: a door's failure is the command's. The account's
principals and keys (`mantle principal`, `mantle key`) are mantle's own requests, which S3's API
does not carry (IAM is another service's): they go through the native door whichever door the
object commands take.

Through either door a command writes the same lines: `made s3://B`, a listing's `TIME SIZE KEY` and
`PRE PREFIX`, a version's `TIME SIZE ID latest|- object|marker KEY`, `head`'s `size`, `etag`,
`modified`, `version`, metadata and `checksum`, a removal's `removed` or `failed` line, a part's
`NUMBER ETAG`. `head` says an object's metadata as S3 serves it (research/89 §1.1, §1.3): each
content header, `content-type binary/octet-stream` where the object keeps none, then each
`x-amz-meta-` entry, a value past US-ASCII as S3 encodes it, since the S3 door reads only what is
served; and no `version` for the null version, which S3 leaves unsaid (research/89 §2.2). The one
difference is a HEAD's error: S3 answers it with no body, so its code cannot be read (research/86
[5], [6]), and the S3 door says the status, `404`, where the native door says `NoSuchKey`.

## 2. The client's directory

`mantle init client NAME --cell OUT` writes, beside the native door's `config` and `identity/`
(node.md §1.5, §3.6), `s3/`: the cell's S3 profile as AWS's shared files (research/86 [2]), the
access key in `credentials` (owner-only), `config` naming the region, the endpoint (`endpoint_url`)
and, for an https endpoint, `ca_bundle`, the client's own copy of the endpoint's authority in
`authority.pem`. The S3 door reads these (`crates/mantle/src/profile.rs`); the AWS CLI takes the
same files. A cell whose founders had no S3 port and name init could state writes no
`endpoint_url`, and the S3 door refuses to guess one.

## 3. The S3 door

`crates/mantle/src/door.rs`. Each command is S3's own operation (research/86 [6]):

| Command | Operation |
|---|---|
| `mb` | CreateBucket, `LocationConstraint` for a region but us-east-1 |
| `rb` | DeleteBucket |
| `ls` | ListBuckets, paged by `continuation-token`, 10,000 a page |
| `ls s3://B[/P]` | ListObjectsV2, `delimiter=/`, paged by `NextContinuationToken`, 1,000 a page |
| `ls --versions s3://B[/P]` | ListObjectVersions, paged by `NextKeyMarker`, `NextVersionIdMarker`, 1,000 a page |
| `cp FILE s3://` | PutObject below 8 MiB; past it a multipart upload (§3.2); its metadata as headers |
| `cp - s3://` | PutObject where the input ends within 8 MiB; past it a multipart upload |
| `cp s3:// FILE` and `-` | GetObject, of a version with `--version-id`, `x-amz-checksum-mode: ENABLED` |
| `cp s3:// s3://` | CopyObject (§3.5); past 5 GiB a multipart upload of UploadPartCopy parts |
| `rm` | DeleteObject, of a version with `--version-id`; several of a bucket, DeleteObjects of 1,000 with `Content-MD5` |
| `head` | HeadObject, of a version with `--version-id`, `x-amz-checksum-mode: ENABLED` |
| `multipart create` | CreateMultipartUpload, CRC-64/NVME as one full-object value, its metadata as headers |
| `multipart put` | UploadPart, from a file or `--length` bytes of standard input |
| `multipart copy` | UploadPartCopy, of a version with `--version-id`, `x-amz-copy-source-range` with `--range` |
| `multipart parts` | ListParts, paged by `NextPartNumberMarker`, 1,000 a page |
| `multipart uploads` | ListMultipartUploads, paged by `NextKeyMarker`, `NextUploadIdMarker` |
| `multipart complete` | CompleteMultipartUpload, its 200 read for an embedded `Error` |
| `multipart abort` | AbortMultipartUpload |

### 3.1 Exchanges

A request's head is written by mantle's codec (`mantle_http::write::request`, the lean-doors
branch's own, unchanged), signed with SigV4 by mantle-s3's primitives over every header it sends
(research/05 §1.2). A response's head is parsed where it lies (`mantle_http::response`, RFC 9112
§4, §6.3, its body's end judged once) and its body decoded as it streams by the codec's decoder.
One connection is kept from request to request while the listener keeps it: plain on a loopback
endpoint, TLS elsewhere, its handshake verified by the endpoint's authority alone (node.md §4.1),
dialed at each address the endpoint's name resolves to in turn until one answers (research/86 [9]),
on one hyper-rt shard, as the native door's client runs. Every wait on the socket is a stock S3
client's patience, 60 s without a byte (research/77 D2). Every try of the socket, plain or under
TLS, is a charge of the client's turn on its shard, as the listener's are (node.md, "a
connection's turn by time"; hyper-raft docs/runtime.md §3.14): past the shard's quantum, with
anything else wanting the shard, the client yields at that await and goes on at its next poll.

An idempotent request whose connection the listener let go before any octet of its answer is asked
once more on a new connection; one that is not, CreateMultipartUpload and CompleteMultipartUpload,
is sent only on a new connection, so a connection the listener let go as quiet never takes it; a
`503` is asked again within three attempts after botocore's
full-jitter backoff, from the native client's retry quota (research/86 [4], [7]). A body read once,
a part streamed from standard input with `--length`, is never sent twice.

A listing whose page names the continuation it was asked from is refused, so every listing ends.
Every error is typed by its S3 code (`crates/mantle/src/s3_code.rs`: the 80 codes of S3's list and
the 14 its listener adds, research/86 [3]); a code past them is said as it came.

### 3.2 A copy into the cell

As the AWS CLI's (research/73 [10]): an object below 8 MiB is one PutObject; a larger one is a
multipart upload in 8 MiB parts, or of a file as many more as 10,000 parts need, at most S3's 5 TiB.
Standard input is read a part at a time, each part held until it is acknowledged, so a copy of it
holds one part and carries at most 10,000 parts, 80 GiB. Every body is `aws-chunked` with a
trailing CRC-64/NVME (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`, research/05 §1.8–§1.9): the source is
read once, and the listener holds each body to its checksum.

A multipart copy is kept in the S3 door's journal, `s3/journal/`, the native door's format
(src/journal.rs) apart from its own, before its first part is sent. Run again after a kill, the
copy finds its upload, lists its parts, reads the source's bytes of each held part from the first
and keeps those whose MD5, the part's ETag, they are, sending none of them again; it says `resuming
upload ID at N bytes, K parts` and then `part K committed, N bytes durable` for each part after. A
held part that is not the source's bytes now ends the copy in `BadDigest` and aborts the upload; a
held part of another length is sent again. The completion's full-object CRC-64/NVME must be the
parts' own combined, else the copy is `Corrupt`.

### 3.3 A copy out of the cell

One GetObject, streamed to the file or to standard output, its bytes counted to their length and
held to the object's full-object CRC-64/NVME where it keeps one. A file is written beside its place,
flushed, renamed over it and its directory flushed (CLAUDE.md §6), as the native door writes one.

### 3.4 Bounds

The connection's buffer is one piece (`mantle_client::PIECE`); a head at most 8 KB; a response's
document at most a ListObjectVersions page's at its widest, 1,000 versions each of the longest key
escaped and a 1,024-octet ID (S3's bound on a version ID), 8.2 MB, within which a DeleteResult of
1,000 results fits; a part from standard input at most 8 MiB; the journal at most 1,000 open
uploads, as the native door's.

### 3.5 A copy within the cell

As the native door's (research/89 §3–§4): the source is headed first, and an object of one copy's
5 GiB or less is one CopyObject, a larger one a multipart upload of UploadPartCopy parts of 5 GiB,
S3's largest part. Every copy names its source's version where the bucket keeps one, and is held to
the ETag headed (`x-amz-copy-source-if-match`), so a source replaced meanwhile fails it `412`. Its
metadata is the source's under `COPY`, the command's under `REPLACE`, which naming any makes the
default; a multipart copy carries the source's metadata as headed, as s3transfer carries a managed
copy's (research/89 §3.7). A copy's `200` whose document is an `Error` is the copy's failure.

## 4. How it is proven

`scripts/release.py`'s archive check runs every command above through both doors against a cell of
three, as a user takes the archive (docs/design/release.md §2), the S3 door under TLS to its first
founder's endpoint by the name `localhost`, and its plainer commands again over plain HTTP to a
loopback founder's: each object put through one door is
read back through the other, to standard output and to a file, byte for byte; each door lists the
other's keys, buckets and uploads and heads its objects line for line as the other does; a copy
from standard input is killed once it committed a part and resumed through each door. The codec's
response reader is held to RFC 9112 by `crates/http/tests/response.rs`, and every document the
listener writes reads back through the S3 door's readers (`door.rs`'s tests).
