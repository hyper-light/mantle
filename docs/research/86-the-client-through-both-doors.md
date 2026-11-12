# 86. The client through both doors: choosing the door, and S3's HTTP API as mantle's own client asks it

Status: research note, 2026-11-12. For the S3 door of mantle's object commands
(docs/design/doors.md, `crates/mantle/src/door.rs`, `crates/mantle/src/profile.rs`,
`crates/mantle/src/s3_code.rs`, `crates/http/src/response.rs`). Sources [1] to [8] were read on
2026-11-12: [1] to [3] and [6] are AWS's documentation, [4], [5] and [8] the IETF's.

## The question

The owner asked that the binary's client work through both of the cell's APIs (2026-11-12): every
object command, `mb`, `rb`, `ls`, `cp` both ways with standard input and output and a resumed copy,
`rm`, `head` and every multipart step, over the native QUIC protocol as before and over S3's HTTP
API, SigV4-signed, against the cell's own S3 listener. Six things need settling: how the operator
chooses the door; where the client's credentials and endpoint come from; what of HTTP/1.1 a client
must read that the listener's codec does not yet; which of S3's operations each command is, and how
each pages; how an S3 error is typed; and what a client may ask again.

## Sources

[1] AWS Command Line Interface User Guide for Version 2, "Configuring settings for the AWS CLI",
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html: "Certain locations take
precedence over others. The AWS CLI credentials and configuration settings take precedence in the
following order: Command line options – Overrides settings in any other location, such as the
--region, --output, and --profile parameters. Environment variables – You can store values in your
system's environment variables." Configuration and credentials files follow them.

[2] AWS SDKs and Tools Reference Guide, "The shared config and credentials files", its format,
https://docs.aws.amazon.com/sdkref/latest/guide/file-format.html: sections `[profile foo]` in the
config file and `[foo]` in the credentials file, each of `setting = value` lines; "Some settings
have their own nested group of subsettings, such as the s3 setting and subsettings in the following
example. Associate the subsettings with the group by indenting them by one or more spaces."

[3] Amazon S3 API Reference, "Error", https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html:
the list of error codes S3 answers with, each a `Code:` with its description and status, 80 distinct
codes (`AccessDenied` to `UserKeyMustBeSpecified`; `InvalidRequest` listed nine times, for nine
causes). mantle's listener answers 14 more where S3 names a code the list does not
(`AccessControlListNotSupported`, `AccessForbidden`, `AuthorizationQueryParametersError`,
`BadRequest`, `HttpVersionNotSupported`, `IllegalLocationConstraintException`,
`InsufficientStorage`, `InvalidBucketAclWithObjectOwnership`, `InvalidBucketOwnerAWSAccountID`,
`InvalidTag`, `MalformedPolicy`, `MaxPostPreDataLengthExceeded`, `RequestHeaderSectionTooLarge`,
`XAmzContentSHA256Mismatch`), read from `crates/node/src/listener` and `crates/s3/src` at
`5c39edbf`.

[4] RFC 9112, "HTTP/1.1": §4, the status line, "status-line = HTTP-version SP status-code SP
[ reason-phrase ]"; §6.3, a response's body length: a response to HEAD and any 1xx, 204 or 304 ends
at its head (item 1); "If a Transfer-Encoding header field is present in a response and the chunked
transfer coding is not the final encoding, the message body length is determined by reading the
connection until it is closed by the server" (item 3); a Content-Length list of differing values is
"an unrecoverable error" (item 5); "Otherwise, this is a response message without a declared
message body length, so the message body length is determined by the number of octets received
prior to the server closing the connection" (item 8); §9.3, persistence; §9.3.1, "A user agent
MUST NOT automatically retry a request with a non-idempotent method unless it has some means to
know that the request semantics are actually idempotent", and a client may retry an idempotent
request after a connection closes before any response.

[5] RFC 9110, "HTTP Semantics": §9.2.2, GET, HEAD, PUT and DELETE are idempotent, POST is not;
§9.3.2, "The server SHOULD NOT send content in the response to a HEAD request."

[6] Amazon S3 API Reference, each operation's page:
- "ListBuckets", https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html:
  `GET /?bucket-region=...&continuation-token=...&max-buckets=...&prefix=...`; `max-buckets`
  "Valid Range: Minimum value of 1. Maximum value of 10000"; a `ContinuationToken` in the result
  "can [be used] for pagination of the list results".
- "ListObjectsV2", https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html:
  "NextContinuationToken is sent when isTruncated is true, which means there are more keys in the
  bucket that can be listed. The next list requests to Amazon S3 can be continued with this
  NextContinuationToken."
- "ListParts", https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html:
  `part-number-marker` "Specifies the part after which listing should begin"; `NextPartNumberMarker`
  "When a list is truncated, this element specifies the last part in the list, as well as the value
  to use for the part-number-marker request parameter in a subsequent request."
- "ListMultipartUploads",
  https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html: `NextKeyMarker`
  and `NextUploadIdMarker` "When a list is truncated, this element specifies the value that should
  be used for the key-marker [upload-id-marker] request parameter in a subsequent request."
- "CompleteMultipartUpload",
  https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html: "a 200 OK
  response can contain either a success or an error. The error response might be embedded in the
  200 OK response. If you call this API operation directly, make sure to design your application
  to parse the contents of the response and handle it appropriately."
- "HeadObject", https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html: "if the HEAD
  request generates an error, it returns a generic code, such as 400 Bad Request, 403 Forbidden,
  404 Not Found, 405 Method Not Allowed, 412 Precondition Failed, or 304 Not Modified. It's not
  possible to retrieve the exact exception of these error codes."

[7] mantle's own notes: research/05 §1.2 (SigV4's canonical request), §1.8–§1.9 (`aws-chunked`
and its trailing checksum, `STREAMING-UNSIGNED-PAYLOAD-TRAILER`), §4.1 (S3's part bounds: 5 MiB
to 5 GiB, 10,000 parts), §10.2 (an 8 KB head, 2 KB of user metadata); research/73 [5] and D4 (the
SDKs' standard retry: three attempts, botocore's full-jitter backoff over `min(2^(n-1) s, 20 s)`
and its retry quota) and [10] (the AWS CLI's 8 MiB `multipart_threshold` and
`multipart_chunksize`); research/77 D2 (a stock client's patience, 60 s without a byte); and
node.md §4.1 (plain HTTP on a loopback endpoint, TLS elsewhere under the endpoint's P-256
authority).

[8] RFC 7468, "Textual Encodings of PKIX, PKCS, and CMS Structures": §2, the encapsulation
boundaries and the base64 between them, white space allowed; §5, the `CERTIFICATE` label.

## What the sources settle

**The door is chosen as the AWS CLI's settings are.** A command-line option overrides the
environment, which overrides what files say [1]. So `--door native|s3`, else `MANTLE_DOOR`, else
the native door, the one every client's directory has; never a door chosen for the operator, and
never a move to the other door when one fails. `--client` and `MANTLE_CLIENT` already rank so.

**The S3 door's key, region and endpoint are the profile `mantle init` writes**, read as AWS's
tools read it [2]: `aws_access_key_id` and `aws_secret_access_key` of `[mantle]` in `credentials`,
`region` and `endpoint_url` of `[profile mantle]` in `config`, its indented `s3 =` subsettings
passed, and the endpoint's authority from the PEM beside them [8]. `mantle init client` gives each
client directory its own copy under `s3/`, `ca_bundle` naming the client's copy, so one directory
is a client of both doors and a profile the AWS CLI takes too.

**A response is read by the codec's own rules, as a request is.** The status line is RFC 9112
§4's, one SP each side of a three-digit code [4]; field lines are read by the request's rule; a
HEAD's, a 1xx's, a 204's and a 304's body is none, chunked last is chunked, another coding last or
neither field reads to the close, and Transfer-Encoding beside Content-Length, or differing
Content-Lengths, are refused rather than guessed [4]. A head is held to the request's 8 KB, which
S3's 2 KB of user metadata and the listener's fixed fields fit [7].

**Each command is one of S3's operations, and pages as S3 says** [6]: ListBuckets by
`continuation-token` at `max-buckets` 10,000; ListObjectsV2 by `NextContinuationToken` while
`IsTruncated`; ListParts by `NextPartNumberMarker`; ListMultipartUploads by `NextKeyMarker` and
`NextUploadIdMarker`; a completion's 200 read for an embedded `Error` [6]. A document is held to a
bound: a ListObjectsV2 page of 1,000 keys of 1,024 octets, escaped at worst to six octets a
character, with each key's own elements, under 7.2 MB.

**An error is typed by S3's own list** [3]: each of its 80 codes and the 14 the listener adds is a
variant; a code past them is said as it came, typed as unlisted. A HEAD's error has no body [5],
and S3 names it by its status alone [6]: the S3 door says `404`, where the native door, whose
replies carry their status, says `NoSuchKey`.

**What is asked again.** Only an idempotent request whose connection closed before any octet of
its answer is asked again, once, on a new connection [4, 5]; a `503` is asked again within the
SDKs' three attempts after their full-jitter backoff, paid from their retry quota [7], the native
client's own rule. A body read once is never sent twice.

**A copy goes as the AWS CLI's does** [7]: below 8 MiB one PutObject, past it a multipart upload in
8 MiB parts or as many more as 10,000 parts of a file need; standard input in 8 MiB parts, each
held until acknowledged, so at most 80 GiB of it, as the CLI's. Every body is `aws-chunked` with a
trailing CRC-64/NVME [7], one pass over the source. A resumed copy keeps a held part only where
its MD5, the part's ETag, is the source's bytes now, and the parts' CRC-64/NVMEs, combined, are
held to the full-object value the completion answers, the native door's end-to-end check through
S3's own operations.

## Open

A ranged GetObject that resumes a read cut mid-body is not yet asked; such a copy ends with a typed
error and is run again. The listener's codec and the client's response reader keep their own copies
of the line, list and decimal readers, which share a module once the lean-doors branch's head reader
lands.
