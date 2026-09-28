# S3 API semantics: ground truth for mantle

**Scope:** the exact S3 wire semantics that a Rust S3-compatible object store (mantle) must reproduce for client compatibility.
**Compiled:** 2026-09-28.
**Status:** research reference. Decisions belong in `docs/design/`.

## How to read this document

**Sources.** Only two kinds of source were used:

- Official AWS documentation on `docs.aws.amazon.com`: the S3 API Reference, the S3 User Guide, the new S3 Developer Guide, the IAM SigV4 reference, and the AWS SDKs & Tools reference.
- The **ceph/s3-tests** source, pinned to commit [`5522d1c351f75bc00ae0f64f742f3f095f5939d9`](https://github.com/ceph/s3-tests/tree/5522d1c351f75bc00ae0f64f742f3f095f5939d9) (master, 2026-05-27).

No blogs. Where AWS docs are silent, the behavior asserted by s3-tests is given with the test name, a line link, and the test's pytest markers.

**Method.**

- Every AWS page was fetched as its official Markdown rendition (`<page>.md`, which docs.aws.amazon.com now serves beside each `<page>.html`). This gives exact text.
- Quotations in "double quotes" or `>` blocks are **verbatim**.
- Values marked **verified** were recomputed locally with `shasum`, `md5`, `base64` and Python `hashlib`/`zlib`. This covers the SigV4 chunk and trailer hashes, the multipart ETag, and the composite and full-object checksums.

**Legend.**

- **UNVERIFIED**: the claim is not stated in the AWS docs consulted, nor asserted by s3-tests. It is included only as a flagged note.
- **Our analysis**: an implication derived from cited facts, not a quotation.
- **Contradiction**: two official sources disagree. Both are cited.

**Docs relocation (important for future lookups).** As of this writing, the S3 **authentication (SigV4), common-headers and error-code pages are no longer in the S3 API Reference**. URLs like `https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html` and `.../API/ErrorResponses.html` now 302-redirect to the API index. The pages live in the **Amazon S3 Developer Guide** at `https://docs.aws.amazon.com/AmazonS3/latest/developerguide/<page>.html`; see the [API Reference Welcome](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) note. Many AWS pages and search engines still link the dead URLs.

## Recent changes that matter (2024–2026)

| Date | Change | Source |
|---|---|---|
| 2024-08-20 | Conditional writes (`If-None-Match: *`) for PutObject / CompleteMultipartUpload | [Doc history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html) |
| 2024-09-30 | Bucket quota increases auto-approved up to 1,000 | Doc history |
| 2024-11-25 | `If-Match` conditional writes; `s3:if-match` / `s3:if-none-match` policy keys | Doc history |
| 2024-12-01 | CRC64NVME algorithm, full-object checksums for multipart ("improved checksum integrity features") | Doc history |
| Dec 2024 | AWS SDKs/CLI compute CRC checksums **by default** (`WHEN_SUPPORTED`), usually as aws-chunked trailers | [SDK data integrity](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html) |
| 2025-09-16 | Conditional deletes (`If-Match`) for general purpose buckets | Doc history |
| 2025-10-01 | Email-grantee ACLs discontinued (HTTP 405) | [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) |
| 2025-12-02 | Max object size raised from 5 TB to "50 TB" (actually 48.8 TiB = 10,000 × 5 GiB); single GET capped at 5 TB | Doc history, [qfacts](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html) |
| date UNVERIFIED | New checksum algorithms MD5, XXHASH64, XXHASH3, XXHASH128, SHA512 (10 total) | [Checking object integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html) |
| date UNVERIFIED | Default bucket quota 10,000 per account; paginated ListBuckets | [Bucket quotas](https://docs.aws.amazon.com/AmazonS3/latest/userguide/BucketRestrictions.html) |
| date UNVERIFIED | "Account regional namespace" buckets (`...-<acct>-<region>-an`, header `x-amz-bucket-namespace: account-regional`) | [Bucket naming](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html) |
| date UNVERIFIED (2026) | SigV4, common-headers and error pages moved to the new S3 Developer Guide | [API Welcome](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) |

## Contents

1. Signature Version 4 for S3: canonical request, signing, presigned URLs, aws-chunked, trailers
2. Conditional requests: writes, deletes, reads
3. Checksums
4. Multipart upload rules
5. ETag rules and Content-MD5
6. Listing: ListObjectsV2, ListObjects, ListObjectVersions
7. Versioning semantics
8. DeleteObjects
9. CopyObject
10. Object keys, user metadata, bucket names, bucket quota
11. Error responses
12. Range GET and partNumber GET
13. Consistency model
14. Virtual-hosted vs path-style addressing
15. Running ceph/s3-tests against a custom endpoint
16. Implications for mantle


## 1. Signature Version 4 (SigV4) for S3

> **Where the docs live now.** AWS has moved the S3 authentication, common-header and error pages out of the *S3 API Reference*. The move date is **UNVERIFIED**; it was observed on 2026-09-28. Their old `/AmazonS3/latest/API/sig-v4-*.html`, `/API/sigv4-*.html`, `/API/RESTCommon*.html` and `/API/ErrorResponses.html` URLs now return `302` to the API Reference index. The pages are now in the new **Amazon S3 Developer Guide** at `https://docs.aws.amazon.com/AmazonS3/latest/developerguide/<page>.html`. The API Reference Welcome page says so: "For information about using the Amazon S3 API—including authentication, signing requests, code examples, and error handling—see the Amazon S3 Developer Guide." ([API Welcome](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)). Many AWS pages, and search results, still link to the old `/API/` URLs. All citations below use the new locations.

Primary sources for this section:

- [Authenticating Requests (AWS Signature Version 4)](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-authenticating-requests.html) ("S3-Auth")
- [Using the Authorization Header](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-auth-using-authorization-header.html) ("S3-AuthHeader")
- [Signature Calculation: Transfer Payload in a Single Chunk](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html) ("S3-SingleChunk")
- [Transfer Payload in Multiple Chunks](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html) ("S3-Chunked")
- [Including Trailing Headers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html) ("S3-Trailers")
- [Using Query Parameters](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html) ("S3-Presign")
- [Common request headers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTCommonRequestHeaders.html)
- IAM's generic SigV4 reference: [Create a signed request](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html) ("IAM-Create"), [Request elements](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-signing-elements.html), [Authentication methods](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html), [Troubleshoot](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-troubleshooting.html)
- The user guide's [trailing-checksum section](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums) ("UG-Trailing")

### 1.1 Authentication methods

- **Authorization header.** "All of the Amazon S3 REST operations (except for browser-based uploads using POST requests) require this header" ([S3-Auth](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-authenticating-requests.html#auth-methods-intro)).
- **Query string (presigned URL).** Valid "for up to seven days" (same page).
- **Browser POST uploads.** A POST policy is signed ([sigv4-UsingHTTPPOST](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-UsingHTTPPOST.html)). This is out of scope here.
- **SigV2.** "AWS Regions created before January 30, 2014 will continue to support the previous protocol, Signature Version 2. Any new Regions after January 30, 2014 will support only Signature Version 4" ([S3-Auth](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-authenticating-requests.html)). The error table lists `InvalidRequest` (400) for "The request is using the wrong signature version. Use `AWS4-HMAC-SHA256` (Signature Version 4)" ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)).
- **SigV4a.** Uses algorithm `AWS4-ECDSA-P256-SHA256`. The credential scope has no Region. A `X-Amz-Region-Set` header or query parameter carries the Regions ([IAM-Create](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html)). "When you supply requests to Multi-Region Access Points, SDKs and the CLI automatically switch to using Signature Version 4A without additional configuration" (same page). A single-endpoint S3 clone normally does not receive SigV4a.

### 1.2 Canonical request

The format is the same on every page ([S3-SingleChunk, Task 1](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html#canonical-request)):

```
<HTTPMethod>\n
<CanonicalURI>\n
<CanonicalQueryString>\n
<CanonicalHeaders>\n
<SignedHeaders>\n
<HashedPayload>
```

#### 1.2.1 `UriEncode()` rules (verbatim, S3-SingleChunk function table)

> - URI encode every byte except the unreserved characters: 'A'-'Z', 'a'-'z', '0'-'9', '-', '.', '_', and '~'.
> - The space character is a reserved character and must be encoded as "%20" (and not as "+").
> - Each URI encoded byte is formed by a '%' and the two-digit hexadecimal value of the byte.
> - Letters in the hexadecimal value must be uppercase, for example "%1A".
> - Encode the forward slash character, '/', everywhere except in the object key name. For example, if the object key name is `photos/Jan/sample.jpg`, the forward slash in the key name is not encoded.

AWS also warns: "The standard UriEncode functions provided by your development platform may not work because of differences in implementation and related ambiguity in the underlying RFCs. We recommend that you write your own custom UriEncode function".

#### 1.2.2 CanonicalURI (S3 rules)

- "the URI-encoded version of the absolute path component of the URI—everything starting with the "/" that follows the domain name and up to the end of the string or to the question mark character" ([S3-SingleChunk](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html#canonical-request)). IAM-Create adds: "If the absolute path is empty, use a forward slash character (`/`)."
- **No normalization.** "You do not normalize URI paths for requests to Amazon S3. For example, you may have a bucket with an object named "my-object//example//photo.user". Normalizing the path changes the object name in the request to "my-object/example/photo.user". This is an incorrect path for that object." (S3-SingleChunk).
- **No double encoding.** For S3 the path is encoded **once**. AWS's worked example shows the key `test$file.text` canonicalized as `/test%24file.text` (single `%24`) (S3-SingleChunk, "Example: PUT Object").
  - Generic SigV4 for other AWS services double-encodes path segments, and older IAM text named S3 as the exception. Neither statement appears in the current IAM page (IAM-Create), so treat them as background (**UNVERIFIED** in current docs).
  - The authoritative evidence is the S3 examples plus the `UriEncode` definition, which encodes every byte once and keeps `/` in the key.
- **Addressing.** With path-style the bucket name is part of CanonicalURI: `/examplebucket/chunkObject.txt` ([S3-Chunked example](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html#example-signature-calculations-streaming)). With virtual-hosted style it is not: `/test.txt` with `host:examplebucket.s3.amazonaws.com` (S3-SingleChunk examples).
- **Implication (our analysis, not AWS text).** A server should canonicalize from the *decoded* key bytes, re-encoding with the rules above and leaving `/` unencoded. It should not reuse the raw request-target. Clients differ in which optional characters they percent-escape (for example `$`, `~`, `!`), and the signed form is always the `UriEncode` form. Never collapse `//`, `/./` or `/../` in keys.

#### 1.2.3 CanonicalQueryString

- "You URI-encode name and values individually. You must also sort the parameters in the canonical query string alphabetically by key name. The sorting occurs after encoding." (S3-SingleChunk).
- Subresources with no value: "the corresponding query parameter value will be an empty string ("")", so `?acl` becomes `acl=`. AWS's `GET ?lifecycle` example canonicalizes to the line `lifecycle=`.
- No query string: "set the canonical query string to an empty string (""). You will still need to include the "\n"."
- In the worked example `?max-keys=2&prefix=J` becomes `max-keys=2&prefix=J`.
- Duplicate parameter names and their tie-break order are **UNVERIFIED**. AWS pages only say "sort ... by key name". Sorting by encoded name and then by encoded value is the common SDK behavior but has no source here.
- Treatment of `+` in incoming query strings is also **UNVERIFIED**. `UriEncode` requires a space to be `%20`, never `+`, when clients build the canonical string. A server that decodes `+` as space before re-encoding could mis-verify a literal `+` sent unencoded. AWS docs do not address this.

#### 1.2.4 CanonicalHeaders and SignedHeaders

- Format: `Lowercase(<HeaderName>)+":"+Trim(<value>)+"\n"`, with names sorted alphabetically (S3-SingleChunk). IAM-Create adds value rules: "trim any leading or trailing spaces", "convert sequential spaces to a single space", "separate the values for a multi-value header using commas".
- Headers that **must** be signed, per the S3 page: the HTTP `host` header; `Content-MD5` if present in the request; "Any `x-amz-*` headers that you plan to include in your request", for example `x-amz-security-token` with temporary credentials (S3-SingleChunk).
  - The IAM page says instead "If the `Content-Type` header is present in the request, you must add it" and does not mention Content-MD5. It also says "You must include the host header (HTTP/1.1) or the :authority header (HTTP/2), and any `x-amz-*` headers in the signature. You can optionally include other standard headers" ([IAM-Create](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html#create-canonical-request)). **Contradiction:** the S3 page requires `content-md5`, the IAM page requires `content-type`. A server should verify exactly the headers listed in `SignedHeaders`, and may additionally require `host` plus all present `x-amz-*`.
  - The HTTP status/code AWS returns when a present `x-amz-*` header is not signed is **UNVERIFIED**; no AWS page in scope states it.
- `x-amz-content-sha256` **need not be signed**. "For the purpose of calculating an authorization signature, the host header and all `x-amz-*` headers, excluding `x-amz-content-sha256`, are required ... Signing the `x-amz-content-sha256` header is optional because S3 will use its value when calculating the received request payload hash." (S3-SingleChunk).
- Do not sign hop-by-hop headers: "`connection`, `x-amzn-trace-id`, `user-agent`, `keep-alive`, `transfer-encoding`, `TE`, `trailer`, `upgrade`, `proxy-authorization`, and `proxy-authenticate`" (IAM-Create).
- SignedHeaders: "an alphabetically sorted, semicolon-separated list of lowercase request header names. The request headers in the list are the same headers that you included in the `CanonicalHeaders` string." (S3-SingleChunk).

#### 1.2.5 HashedPayload and `x-amz-content-sha256`

- `Hex(SHA256Hash(<payload>))`. For no payload use `Hex(SHA256Hash(""))` = `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` (S3-SingleChunk).
- "The `x-amz-content-sha256` header is required for all AWS Signature Version 4 requests" (S3-SingleChunk; IAM-Create: "required for Amazon S3 AWS requests"). The last line of the canonical request is the *same value* as the header: a hex digest, or a literal constant such as `UNSIGNED-PAYLOAD` or `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`.
  - IAM-Create shows the formula `Hex(SHA256Hash("UNSIGNED-PAYLOAD"))` in a note. That conflicts with every S3 worked example, where the literal string is used. Follow the S3 pages: the literal string, not its hash.
- Allowed values, verbatim table from [S3-AuthHeader](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-auth-using-authorization-header.html#sigv4-auth-header-overview):

| `x-amz-content-sha256` value | Meaning (AWS wording) |
|---|---|
| *Actual payload checksum value* (64 lowercase hex) | "the actual checksum of your object and is only possible when you are uploading the data in a single chunk" |
| `UNSIGNED-PAYLOAD` | "uploading the object as a single unsigned chunk" |
| `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | "sending an unsigned payload over multiple chunks. In this case you also have a trailing header after the chunk is uploaded" |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | "payload over multiple chunks, and the chunks are signed using `AWS4-HMAC-SHA256`" |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` | same, "In addition, the digest for the chunks is included as a trailing header" |
| `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD` | chunks signed with `AWS4-ECDSA-P256-SHA256` (SigV4a) |
| `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER` | SigV4a chunks plus trailing header |

- The error for a body whose SHA-256 does not match a hex `x-amz-content-sha256` is **UNVERIFIED**. The commonly observed code `XAmzContentSHA256Mismatch` does **not** appear in the current [error code list](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList). The error for a *missing* `x-amz-content-sha256` is also undocumented (**UNVERIFIED**).
- Policy key `s3:x-amz-content-sha256` lets a bucket policy deny `UNSIGNED-PAYLOAD` ([SigV4 policy keys](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/bucket-policy-s3-sigv4-conditions.html)).

### 1.3 String to sign, signing key, signature

- String to sign ([S3-SingleChunk, Task 2](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html#request-string)):

  ```
  "AWS4-HMAC-SHA256" + "\n" +
  timeStampISO8601Format + "\n" +          // e.g. 20130524T000000Z
  <Scope> + "\n" +                          // YYYYMMDD/<region>/s3/aws4_request
  Hex(SHA256Hash(<CanonicalRequest>))
  ```

  IAM-Create: "Do not end this string with a newline character." "The Region code, service code, and termination string must use lowercase characters."
- Scope: `date.Format(<YYYYMMDD>) + "/" + <region> + "/" + <service> + "/aws4_request"`. "For Amazon S3, the service string is `s3`." "`Scope` must use the same date that you use to compute the signing key". "The signature is valid for seven days after the specified date." (S3-SingleChunk).
  - S3 Object Lambda signs with service `s3-object-lambda` ([UG-Trailing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums)).
- Signing key ([S3-SingleChunk, Task 3](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html#signing-key)):

  ```
  DateKey              = HMAC-SHA256("AWS4"+"<SecretAccessKey>", "<YYYYMMDD>")
  DateRegionKey        = HMAC-SHA256(<DateKey>, "<aws-region>")
  DateRegionServiceKey = HMAC-SHA256(<DateRegionKey>, "<aws-service>")
  SigningKey           = HMAC-SHA256(<DateRegionServiceKey>, "aws4_request")
  signature            = Hex(HMAC-SHA256(SigningKey, StringToSign))   // 64 lowercase hex chars
  ```

- IAM troubleshooting error strings a server may mirror ([IAM troubleshoot](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-troubleshooting.html)):
  - "Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date from HTTP"
  - "Signature not yet current: {date} is still later than {date}"
  - "Signature expired: {date} is now earlier than {date}"

### 1.4 `Authorization` header format

```
Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41
```

- `Credential` = `<access-key-id>/<YYYYMMDD>/<region>/s3/aws4_request`.
- `SignedHeaders` = lowercase names joined by `;`.
- `Signature` = "The 256-bit signature expressed as 64 lowercase hexadecimal characters" ([S3-AuthHeader](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-auth-using-authorization-header.html)).
- IAM-Create: "There is no comma between the algorithm and `Credential`. However, the other elements must be separated by commas."
- AWS's overview example puts a space after the commas (`..._request, SignedHeaders=...`), while the worked examples use bare commas. **Parsers must accept optional whitespace after the commas.**
- Malformed header returns `AuthorizationHeaderMalformed` (400). Bad query auth returns `AuthorizationQueryParametersError` (400) ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)).

### 1.5 AWS test vectors (copy for unit tests)

The credentials are `AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`, region `us-east-1`, time `20130524T000000Z`.

| Example (source) | Canonical-request hash | Signature |
|---|---|---|
| GET `/test.txt` with `Range: bytes=0-9`, virtual host `examplebucket.s3.amazonaws.com` ([S3-SingleChunk](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html#example-signature-GET-object)) | `7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972` | `f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41` |
| PUT `test$file.text`, body "Welcome to Amazon S3.", signs `date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class` | `9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d` | `98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd` |
| GET `?lifecycle` | `9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca` | `fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543` |
| GET `?max-keys=2&prefix=J` | `df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7` | `34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7` |
| Presigned GET `/test.txt`, `X-Amz-Expires=86400` ([S3-Presign](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html#query-string-auth-v4-signing-example)) | `3bfa292879f6447bbcda7001decf97f4a54dc650c8942174ae0a9121cf58ad04` | `aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404` |
| Chunked PUT seed ([S3-Chunked](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html#example-signature-calculations-streaming)) | `cee3fed04b70f867d036f722359b0b1f2f0e5dc0efadbc082b76c4c60e316455` | seed `4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9` |
| Chunked PUT with trailer seed ([S3-Trailers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html#example-signature-calculations-trailing-header)) | `44d48b8c2f70eae815a0198cc73d7a546a73a93359c070abbaa5e6c7de112559` | seed `106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e` |

The PUT example's canonical request shows the `Date` header (RFC 1123 form) signed alongside `x-amz-date`.

### 1.6 Dates, `x-amz-date` vs `Date`, and clock skew

- **Format.** "The time stamp must be in UTC and use the following ISO 8601 format: *YYYYMMDD*T*HHMMSS*Z ... Do not include milliseconds" ([IAM elements](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-signing-elements.html#date)).
- **Precedence.** "The request date can be specified by using either the HTTP `Date` or the `x-amz-date` header. If both headers are present, `x-amz-date` takes precedence." ([S3-AuthHeader](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-auth-using-authorization-header.html)). IAM: "If we can't find an `x-amz-date` header, then we look for a `date` header."
- **`Date` used for signing.** [Common request headers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTCommonRequestHeaders.html): "If you are using the `Date` header for signing, then it must be in the ISO 8601 basic `YYYYMMDD'T'HHMMSS'Z'` format. If `Date` is specified but is not in ISO 8601 basic format, then you must also include the `x-amz-date` header." "Note that when `x-amz-date` is present, it always overrides the value of the `Date` header." A non-signing `Date` may be any RFC 2616 §3.3 format.
- **Allowed skew.** "The signed portions (using AWS Signatures) of requests are valid within **15 minutes** of the timestamp in the request." ([S3-Auth](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-authenticating-requests.html)).
  - **Contradiction:** IAM's generic page says "In most cases, a request must reach AWS within five minutes of the time stamp in the request" ([IAM SigV4](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html#why-requests-are-signed)). For S3 the S3-specific 15-minute statement governs.
  - The error table does not tie a number to the code: `RequestTimeTooSkewed` is **403 Forbidden**, "The difference between the request time and the server's time is too large." ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList); same in the [Error type](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html)).
- **s3-tests (SigV2 only; no SigV4 date tests exist at this commit).** All marked `auth_aws2`. mantle can skip them if it drops SigV2.
  - `x-amz-date: Tue, 07 Jul 2010 ...` returns 403 `RequestTimeTooSkewed`: [test_object_create_bad_date_before_today_aws2](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L473).
  - Year 9999 returns 403 `RequestTimeTooSkewed`: [test_object_create_bad_date_after_end_aws2](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L491).
  - Year 1950 (before epoch) returns 403 `AccessDenied`: [test_object_create_bad_date_before_epoch_aws2](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L482).
  - `x-amz-date: Bad Date` or empty returns 403 `AccessDenied`: [test_object_create_bad_date_invalid_aws2](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L444) and [..._empty_aws2](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L453).
  - Bucket variants are at [L553](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L553) to [L572](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L572).
- **s3-tests (`auth_common`, SigV4 client).**
  - Both `Date` and `X-Amz-Date` present succeeds: [test_object_create_date_and_amz_date](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L266), marked `fails_on_rgw`.
  - Empty `Authorization` returns 403: [test_object_create_bad_authorization_empty](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L258), `fails_on_rgw`.
  - On bucket creation, empty or missing `Authorization` returns 403 `AccessDenied`: [test_bucket_create_bad_authorization_empty](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L369), `fails_on_rgw`.
  - The helpers inject headers in botocore's `before-call` hook, so the injected headers are signed ([helpers](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L36)).

### 1.7 Presigned URLs (query-string auth)

Parameters ([S3-Presign table](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html)):

| Parameter | Rule |
|---|---|
| `X-Amz-Algorithm` | `AWS4-HMAC-SHA256` |
| `X-Amz-Credential` | `<key-id>/<YYYYMMDD>/<region>/s3/aws4_request`; "In practice, it should be encoded as `%2F`" |
| `X-Amz-Date` | ISO 8601 `yyyyMMddTHHmmssZ` (UTC) |
| `X-Amz-Expires` | "This value is an integer. The minimum value you can set is 1, and the maximum is 604800 (seven days)." |
| `X-Amz-SignedHeaders` | must include `host` and "Any `x-amz-*` headers that you plan to add to the request" |
| `X-Amz-Signature` | 64 lowercase hex |
| `X-Amz-Security-Token` | "For S3, you must include the `X-Amz-Security-Token` query parameter in the URL if using credentials sourced from the STS service." |

Canonical-request differences for presigned URLs (S3-Presign):

- HashedPayload is the constant `UNSIGNED-PAYLOAD`.
- "The **Canonical Query String** must include all the query parameters from the preceding table except for `X-Amz-Signature`."
- "If you add a signed header that is also a signed query parameter, and they differ in value, you will receive an `InvalidRequest` error as the input is conflicting."

Worked canonical request (note the encoded `%2F` in the credential, and the sort order):

```
GET
/test.txt
X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host
host:examplebucket.s3.amazonaws.com

host
UNSIGNED-PAYLOAD
```

Expiry semantics:

- "Amazon S3 checks the expiration date and time of a signed URL at the time of the HTTP request ... the download continues even if the expiration time passes during the download" ([UG presigned](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html#PresignedUrl-Expiration)).
- A URL signed with temporary credentials "expires when the credential expires" (same page).
- Policy key `s3:signatureAge` is in milliseconds ([SigV4 policy keys](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/bucket-policy-s3-sigv4-conditions.html)).
- The exact AWS error code/message for an expired presigned URL is **UNVERIFIED** in AWS docs. It is commonly `AccessDenied` "Request has expired", but that text is not documented.

s3-tests behavior:

- Expires 100000 s (under 7 days) returns 200: [test_object_raw_get_x_amz_expires_not_expired](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3507). The helper also asserts an `OPTIONS` on the presigned URL returns **400** ([L3495](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3495)).
- Expires 0 returns **403**: [..._out_range_zero](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3513).
- Expires 609901 (over 604800) returns **403**: [..._out_max_range](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3523).
- Expires -7 returns **403**: [..._out_positive_range](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3533).
- Expired presigned PUT returns 403: [test_object_raw_put_authenticated_expired](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3632).
- None of these tests carry markers.

### 1.8 `aws-chunked` payload signing (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`)

**Required headers** ([S3-Chunked](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html)):

- `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`.
- `Content-Encoding: aws-chunked`. Other codings may be combined, for example `aws-chunked,gzip`. "Amazon S3 stores the resulting object without the `aws-chunked` value in the `content-encoding` header. If `aws-chunked` is the only value ... S3 considers the `content-encoding` header empty and does not return this header".
  - **Contradiction:** [UG-Trailing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums-headers) says for trailing-checksum uploads "While this header isn't required, including this header can minimize HTTP proxy issues". Detect aws-chunked framing from the `STREAMING-*` value of `x-amz-content-sha256`, not from `Content-Encoding`.
- `x-amz-decoded-content-length`: "the length, in bytes, of the data to be chunked, without counting any metadata". "For all requests, you must include the `x-amz-decoded-content-length` header".
- `Content-Length`: the full encoded body including chunk metadata, **or** HTTP `Transfer-Encoding`. "If you include the `Transfer-Encoding` header and specify any value other than `identity`, you must omit the `Content-Length` header." A server may therefore see HTTP/1.1 `Transfer-Encoding: chunked` *wrapping* an `aws-chunked` body.
- s3-tests pins the stored `Content-Encoding` behavior: `gzip, aws-chunked` becomes `gzip`; `aws-chunked, gzip` becomes `gzip`; `aws-chunked` gives no header; `aws-chunked, aws-chunked` gives no header ([test_object_content_encoding_aws_chunked](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3543), no markers).

**Chunk size.** "The chunk size must be at least 8 KB ... This chunk size applies to all chunks except the last one. The last chunk you send can be smaller than 8 KB." UG-Trailing: "at least 8,192 bytes (or 8 KiB) ... There is no explicit maximum chunk size". The error AWS returns for undersized chunks is **UNVERIFIED** (no code documented).

**Seed signature.** The seed is the normal SigV4 signature over the headers, with HashedPayload = the literal `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`. This is the `Signature=` in `Authorization`.

**Chunk framing** ([S3-Chunked, "Defining the Chunk Body"](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html#sigv4-chunked-body-definition)):

```
string(IntHexBase(chunk-size)) + ";chunk-signature=" + signature + \r\n + chunk-data + \r\n
```

**Chunk string to sign.** It uses the algorithm literal `AWS4-HMAC-SHA256-PAYLOAD`. The last three lines are "`previous-signature`, `hash("")`, and `hash(current-chunk-data)`". The first chunk uses the seed signature as the previous signature:

```
AWS4-HMAC-SHA256-PAYLOAD
20130524T000000Z
20130524/us-east-1/s3/aws4_request
<previous-signature>
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
<hex(sha256(chunk-data))>
```

**Final chunk.** "Send the final additional chunk, which is the same as the other chunks in the construction, but it has zero data bytes."

**AWS example.** 66560 bytes of `a`, sent as chunks of 65536, 1024 and 0 bytes, with `Content-Length: 66824`:

- Chunk 1: `10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648`
- Chunk 2: `400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497`
- Final chunk: `0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9`
- Content-Length arithmetic (our check). Chunk 1 is 86 header bytes + 2 CRLF + 65536 data + 2 CRLF = 65626. Chunk 2 is 84 + 2 + 1024 + 2 = 1112. The final chunk is 82 + 2 + 0 + 2 = 86. Total = **66824**, which matches AWS.
- So the terminating chunk is exactly `0;chunk-signature=<sig>\r\n\r\n`.
- We verified locally that `sha256` of 65536 × `a` = `bf718b6f...9c5a` and of 1024 × `a` = `2edc9868...2e4a`, as in AWS's strings to sign.

### 1.9 Trailing headers (`...-TRAILER` variants) and `x-amz-trailer`

**Request headers.** You must set `x-amz-content-sha256` to a trailer variant and `x-amz-trailer` to the trailing header name(s) ([S3-Trailers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html)). S3-AuthHeader says "specify the trailing header names as a string in a comma-separated list". UG-Trailing says "Only one trailing chunk is allowed".

Supported `x-amz-trailer` values ([UG-Trailing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums-headers)):

- `x-amz-checksum-crc32`
- `x-amz-checksum-crc32c`
- `x-amz-checksum-crc64nvme`
- `x-amz-checksum-sha1`
- `x-amz-checksum-sha256`

"The header name field ... must match the value passed into the `x-amz-trailer` request header ... [otherwise] the request fails." The value is "a base64 encoding of the big-endian checksum value". Trailing checksums are supported "for `PutObject` and `UploadPart` requests".

**Order.** Data chunks, then the 0-byte chunk (signed normally), then one trailer chunk.

**Trailer string to sign** ([S3-Trailers example step 4](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html#example-signature-calculations-trailing-header)):

```
AWS4-HMAC-SHA256-TRAILER
20130524T000000Z
20130524/us-east-1/s3/aws4_request
<signature of the 0-byte chunk>
hex(sha256("x-amz-checksum-crc32c:sOO8/Q==\n"))
```

- AWS: "The hash is calculated as follows with no whitespace: The trailing checksum header name, A colon (`:`), The base64-encoded trailing checksum value, A newline character (`\n`)."
- We verified that `sha256("x-amz-checksum-crc32c:sOO8/Q==\n")` = `1e376db7e1a34a8ef1c4bcee131a2d60a1cb62503747488624e10995f448d774`, which is exactly AWS's value.
- The trailer signature in the example is `d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435`.

**Signed trailer bytes on the wire (AWS example).** The AWS example request has `Content-Length: 66946`:

```
0;chunk-signature=2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992\r\n
x-amz-checksum-crc32c:sOO8/Q==\r\n
x-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n
\r\n
```

- Our arithmetic gives 65626 + 1112 + 84 (zero chunk without the extra CRLF) + 32 + 90 + 2 = **66946**, which matches AWS. The layout "zero chunk + CRLF, trailers without the final CRLF" also sums to 66946, so the byte count alone cannot disambiguate.
- The layout above is the RFC 9112 chunked grammar: last-chunk line, trailer fields, final CRLF. It matches UG-Trailing's rule "Every chunked upload must end with a final CRLF".
- The AWS example also shows each chunk signature: chunk 1 `b474d886...5aa2`, chunk 2 `1c1344b1...e5c7`, zero chunk `2ca2aba2...f992`.

**Unsigned streaming (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`)** ([UG-Trailing, chunk formats](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums-chunks)):

- Body chunks are `<hex-size>\r\n<bytes>\r\n` with no `;chunk-signature=`.
- The completion chunk is `0\r\n`.
- Trailer: `x-amz-checksum-<alg>:<base64>\r\n\r\n`. AWS also shows `...<base64>\n\r\n\r\n` and notes "The usage of the linefeed `\n` at the end of the checksum value might vary across clients."
- The response echoes `x-amz-checksum-crc32: YABb/g==`.
- **Be lenient:** accept an optional bare `\n` before the CRLF, and accept LF-only line ends in trailers.
- In unsigned mode the signature covers only the headers, not the body. Body integrity comes solely from the trailing checksum, so it **must** be verified.

**Documentation defects to be aware of.**

- In S3-Trailers the seed canonical request signs `content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer`. The `Authorization` header printed right after lists `SignedHeaders=content-encoding;content-length;host;...;x-amz-storage-class` (with `content-length`, without `x-amz-trailer`). The two lists are inconsistent. A server must trust the `SignedHeaders` actually sent.
- UG-Trailing's signed example writes `2000;chunk-signature=...` for the first chunk and then just `2000;{chunk-signature}` for the next. The developer guide's `;chunk-signature=<hex>` form is normative.

### 1.10 Checklist for a SigV4 verifier (derived from the above)

1. Parse `Authorization`, tolerating `, ` after commas, or the `X-Amz-*` query parameters. Reject an unknown algorithm with `AuthorizationHeaderMalformed` or `AuthorizationQueryParametersError` (400).
2. Look up the access key. If unknown, return `InvalidAccessKeyId` (403) ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)).
3. Get the timestamp from `x-amz-date`, falling back to `Date` in ISO 8601 basic form. The credential-scope date must equal its `YYYYMMDD`.
4. Enforce ±15 minutes for header auth, returning `RequestTimeTooSkewed` (403). For presigned URLs, enforce now < `X-Amz-Date` + `X-Amz-Expires`, with `1 ≤ X-Amz-Expires ≤ 604800` (403 per s3-tests). How much tolerance AWS allows for a future-dated `X-Amz-Date` is **UNVERIFIED**; applying the same 15-minute skew is a reasonable choice.
5. Rebuild the canonical request from decoded components: single-encoded path, sorted encoded query without `X-Amz-Signature`, the listed headers (lowercased, trimmed, inner spaces collapsed), and the payload token taken from `x-amz-content-sha256`, or `UNSIGNED-PAYLOAD` for presigned URLs.
6. Compare signatures in constant time. On mismatch return `SignatureDoesNotMatch` (403).
7. If the body is a hex digest, hash it while streaming and fail at the end (code **UNVERIFIED**, see 1.2.5). If it is `STREAMING-*`, run the chunk decoder with signature chaining and trailer verification. Enforce `x-amz-decoded-content-length` against the decoded byte count (error code **UNVERIFIED**; `IncompleteBody` 400 is documented for "You did not provide the number of bytes specified by the Content-Length HTTP header").

## 2. Conditional requests

Primary sources:

- [Conditional requests overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)
- [Conditional reads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-reads.html)
- [Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Enforce conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
- [Conditional deletes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)
- [Enforce conditional deletes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-delete-enforce.html)
- API pages: [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html), [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html), [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html), [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html), [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html), [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html), [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)

### 2.1 Timeline (from the [S3 document history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html))

| Date | Change |
|---|---|
| August 20, 2024 | "Amazon S3 supports using conditional writes for `PutObject` and `CompleteMultipartUpload`" (`If-None-Match`) |
| November 25, 2024 | "New HTTP header for conditional writes to check if the object has changed" (`If-Match`), plus bucket-policy keys `s3:if-match` / `s3:if-none-match` |
| September 16, 2025 | "Amazon S3 now supports conditional deletes in general purpose buckets" (`If-Match` on DeleteObject/DeleteObjects) |
| no document-history row found | CopyObject destination `If-Match`/`If-None-Match`. It is documented on the [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html#AmazonS3-CopyObject-request-header-IfMatch) and [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) pages; the launch date is **UNVERIFIED** |

### 2.2 Conditional writes: `PutObject`, `CompleteMultipartUpload`, `CopyObject` (destination)

"To use conditional writes, you must use AWS Signature Version 4 to sign the request." ([conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html))

**`If-None-Match: *`** (create-only). Rules:

- PutObject: "Uploads the object only if the object key name does not already exist in the bucket specified. Otherwise, Amazon S3 returns a `412 Precondition Failed` error. If a conflicting operation occurs during the upload S3 returns a `409 ConditionalRequestConflict` response. On a 409 failure you should retry the upload. Expects the '*' (asterisk) character." ([PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-IfNoneMatch)).
- CompleteMultipartUpload: identical wording, except "On a 409 failure you should re-initiate the multipart upload with `CreateMultipartUpload` and re-upload each part." ([CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#AmazonS3-CompleteMultipartUpload-request-header-IfNoneMatch)).
- CopyObject: "Copies the object only if the object key name at the destination does not already exist ... Otherwise ... `412 Precondition Failed` ... `409 ConditionalRequestConflict` ... retry" ([CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html#AmazonS3-CopyObject-request-header-IfNoneMatch)).
- Versioned buckets: "The HTTP `If-None-Match` header only applies to the current version of an object in a version bucket." "For buckets with versioning enabled, if there's no current object version with the same name, or if the current object version is a delete marker, the write operation succeeds." ([conditional writes, behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response)).
- Permission: `s3:PutObject` only.

**`If-Match: <etag>`** (compare-and-swap). Rules:

- PutObject: "Uploads the object only if the ETag (entity tag) value provided during the WRITE operation matches the ETag of the object in S3. If the ETag values do not match, the operation returns a `412 Precondition Failed` error. If a conflicting operation occurs during the upload S3 returns a `409 ConditionalRequestConflict` response. On a 409 failure you should fetch the object's ETag and retry the upload. Expects the ETag value as a string." ([PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-IfMatch)).
- CompleteMultipartUpload: on 409 "fetch the object's ETag, re-initiate the multipart upload with `CreateMultipartUpload`, and re-upload each part."
- Missing object: "If there's no current object version with the same name, or if the current object version is a delete marker, the operation fails with a `404 Not Found` error." "You will receive a `404 Not Found` response if a concurrent delete request to an object succeeds before a conditional write operation on that object completes" ([conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response)).
- Permission: `s3:PutObject` **and** `s3:GetObject`.

**Concurrency semantics (verbatim, same page):**

- "If multiple conditional writes or copies occur for the same object name, the first write operation to finish succeeds. Amazon S3 then fails subsequent writes with a `412 Precondition Failed` response."
- "You can also receive a `409 Conflict` response in the case of concurrent requests if a delete request to an object succeeds before a conditional write operation on that object completes."
- Multipart: "Conditional writes do not consider any in-progress multipart uploads requests since those are not yet fully written objects ... During the multipart upload, Client 2 is able to successfully write the same object with the conditional write operation. Subsequently, when Client 1 tries to complete the multipart upload using a conditional write the upload fails." Both headers then give `412`.
- "Concurrent deletes during multipart uploads ... will result in a `409 Conflict` response for an `If-None-Match` header and a `404 Not Found` response for an `If-Match` header."
- **Implication:** the precondition is evaluated atomically at **commit time**: the end of the PutObject body, or CompleteMultipartUpload. It is not evaluated when the request arrives or at CreateMultipartUpload. mantle needs a linearizable compare-and-set on the key's "current version" pointer at commit.

**Error codes** ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)):

- `PreconditionFailed` 412: "At least one of the preconditions that you specified did not hold."
- `ConditionalRequestConflict` 409: "A conflicting operation occurred. If using PutObject you can retry the request. If using multipart upload you should initiate another CreateMultipartUpload request and re-upload each part."
- `OperationAborted` 409: "A conflicting conditional operation is currently in progress against this resource. Try again."
- For the `404` case the docs say only "404 Not Found". s3-tests asserts the code `NoSuchKey` (below).

**Bucket-policy enforcement** ([enforce page](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)):

- Condition keys `s3:if-none-match` and `s3:if-match`, typically with a `Null` operator.
- "For multipart uploads you must specify the `s3:ObjectCreationOperation` condition key to exempt the `CreateMultipartUpload`, `UploadPart`, and `UploadPartCopy` operations, as these APIs don't accept conditional headers."
- **Contradiction:** the same page says "`CopyObject` requests without an `If-None-Match` or `If-Match` HTTP header fail with a `403 Access Denied` error. `CopyObject` requests made with those HTTP headers fail with a `501 Not Implemented` response". That conflicts with the CopyObject API page, which now documents destination `If-Match`/`If-None-Match`. The enforce page looks stale.

**Values other than `*` / an ETag.**

- AWS documents only `If-None-Match: *` and `If-Match: <etag>` for writes. `If-Match: *` is documented only for deletes.
- s3-tests (marker `conditional_write`, not `fails_on_aws`) expects RFC-7232-style generality:
  - [test_put_object_if_match](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19470) (`conditional_write`, `fails_on_dbstore`) asserts:
    - `IfNoneMatch='*'` on an existing key gives `(412, PreconditionFailed)`.
    - `IfNoneMatch=<current etag>` gives 412.
    - `IfNoneMatch='badetag'` gives 200.
    - After delete, `IfMatch='*'` and `IfMatch='badetag'` give `(404, NoSuchKey)`.
    - When the key exists, `IfMatch='*'` gives 200 and `IfMatch='badetag'` gives 412.
  - The same matrix runs through CompleteMultipartUpload in [test_multipart_put_object_if_match](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19532).
  - The versioned-bucket variants use the *current* version only and treat a delete marker as "no object", giving `(404, NoSuchKey)` for `If-Match`: [L19567](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19567), [L19623](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19623), [L19681](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19681).
  - Whether AWS accepts `If-None-Match: <etag>` or `If-Match: *` on PutObject is **UNVERIFIED**.
  - Implementing the RFC superset is safe and matches s3-tests.
- Older RGW-specific tests are all marked `fails_on_aws`: [test_put_object_ifmatch_good](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3119), [test_put_object_ifmatch_nonexisted_failed](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3180) (404 `NoSuchKey`), and the `ifnonmatch_*` tests around [L3196](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3196).
  - [test_put_object_ifmatch_good](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3119) sends the ETag **without quotes**. AWS's CLI example (`--if-match "6805f2cf..."`) also passes an unquoted value.
  - **Compare ETags after stripping surrounding double quotes.** Weak `W/` ETags are unaddressed (**UNVERIFIED**).
- Commit-time evaluation is also exercised by `_test_atomic_dual_conditional_write`: a PUT with `If-Match: <etag A>` fails 412 because another write landed while its body streamed ([test_atomic_dual_conditional_write_1mb](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7448), marked `fails_on_aws` and `fails_on_rgw`).

### 2.3 Conditional deletes

- "You can perform conditional deletes using the `DeleteObject` or `DeleteObjects` API operations in S3 general purpose and directory buckets ... use the `HTTP If-Match` header with the precondition value `*` to check if object exists or the `If-Match` header with your provided `ETag`" ([conditional deletes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)). "Conditional delete evaluations only apply to the current version of the object."
- DeleteObject `If-Match`: "Deletes the object if the ETag ... matches ... If the ETag values do not match, the operation returns a `412 Precondition Failed` error. Expects the ETag value as a string. `If-Match` does accept a string value of an '*' (asterisk) character to denote a match of any ETag." ([DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html#AmazonS3-DeleteObject-request-header-IfMatch)).
- Success gives `204 No Content`, mismatch gives `412`. "If the latest version of the object is a delete marker, the object doesn't exist and the `DeleteObject` API will fail and return a `412 Precondition Failed` response" (for `If-Match: *`).
- Concurrency: "You can also receive a `409 Conflict` error response in the case of concurrent requests if a `DELETE` or `PUT` request to an object succeeds before a conditional delete operation on that object completes. You will receive a `404 Not Found` response if a concurrent delete request to an object succeeds before a conditional write operation".
- DeleteObjects: put the value in the per-object `<ETag>` element ([ObjectIdentifier](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectIdentifier.html)). Passes are reported under `<Deleted>` and failures under `<Error>`. "If the object doesn't exist when evaluating either of the preconditions, S3 rejects the request and returns a `Not Found` error response." (AWS does not say whether this is per-key or whole-request: **ambiguous**.)
- Directory buckets only: `x-amz-if-match-last-modified-time` and `x-amz-if-match-size` (and `LastModifiedTime`/`Size` in `ObjectIdentifier`). For these, "If the `Timestamp` matches or if the object doesn't exist, the operation returns a `204 Success (No Content)` response."
- Permissions: ETag form needs `s3:DeleteObject` and `s3:GetObject`; `*` needs `s3:DeleteObject` only. Policies can require the header with `"Null": {"s3:if-match": "false"}`; without the header the request is denied with 403 ([enforce deletes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-delete-enforce.html)).
- **Divergence from s3-tests.** The `test_delete_object*_if_match*` tests (e.g. [L19709](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19709), [L19734](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19734)) are all marked `fails_on_aws # only supported for directory buckets`. That marker is stale since 2025-09-16.
  - They encode RGW behavior that conflicts with AWS general-purpose docs: `If-Match: *` or `If-Match: badetag` on a **missing** key returns **204** ("-ENOENT doesn't raise error in delete op"), and `If-Match: *` when the current version is a delete marker succeeds.
  - AWS says 412 for `*` against a delete marker, and "Not Found" for a missing object in DeleteObjects.
  - Follow AWS and deselect these tests.

### 2.4 Conditional reads (GetObject, HeadObject) and precedence

**Headers** ([GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#AmazonS3-GetObject-request-header-IfMatch)):

- `If-Match`: return only if the ETag matches, else `412 Precondition Failed`.
- `If-Modified-Since`: else `304 Not Modified`.
- `If-None-Match`: return only if the ETag is different, else `304 Not Modified`.
- `If-Unmodified-Since`: else `412 Precondition Failed`.

**The only two documented combinations** (verbatim; HeadObject has the same text):

> If both of the `If-Match` and `If-Unmodified-Since` headers are present in the request as follows: `If-Match` condition evaluates to `true`, and; `If-Unmodified-Since` condition evaluates to `false`; then, S3 returns `200 OK` and the data requested.

> If both of the `If-None-Match` and `If-Modified-Since` headers are present in the request as follows: `If-None-Match` condition evaluates to `false`, and; `If-Modified-Since` condition evaluates to `true`; then, S3 returns `304 Not Modified` status code.

Both pages cite [RFC 7232](https://tools.ietf.org/html/rfc7232). Its §6 evaluation order yields exactly these outcomes, and is the recommended total order for mantle:

1. If `If-Match` is present and false, return 412.
2. Else if `If-Unmodified-Since` is present, `If-Match` is absent, and the object was modified after the date, return 412.
3. If `If-None-Match` is present and matches, return 304 for GET/HEAD.
4. Else if `If-Modified-Since` is present, `If-None-Match` is absent, and the object was not modified since the date, return 304.

Other combinations (for example `If-Match` false + `If-None-Match` false) are not documented by AWS. They are **UNVERIFIED** beyond the RFC.

**HEAD errors.** "if the `HEAD` request generates an error, it returns a generic code, such as `400 Bad Request`, `403 Forbidden`, `404 Not Found`, `405 Method Not Allowed`, `412 Precondition Failed`, or `304 Not Modified`. It's not possible to retrieve the exact exception" (no body) ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)).

**404 vs 403.** "If the object that you request doesn't exist ... If you have the `s3:ListBucket` permission on the bucket, Amazon S3 returns an HTTP status code `404 Not Found` error. If you don't have the `s3:ListBucket` permission, Amazon S3 returns an HTTP status code `403 Access Denied` error." (GetObject/HeadObject).

**s3-tests** (all unmarked unless noted):

- [test_get_object_ifmatch_failed](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3034): 412 `PreconditionFailed`.
- [test_get_object_ifnonematch_good](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3044): 304, and the **304 response must carry the `ETag` header**.
- [test_get_object_ifmodifiedsince_failed](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3075) (`fails_on_dbstore`): 304 with `ETag`. An `If-Modified-Since` one second after `LastModified` gives 304. HTTP dates have 1-second resolution, so truncate stored mtimes to whole seconds before comparing (our analysis).
- [test_get_object_ifunmodifiedsince_good](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3098) (`fails_on_dbstore`): a 1994 date gives 412.
- [test_get_object_ifunmodifiedsince_failed](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3108): a 2100 date gives 200.

**CopyObject source conditions** (`x-amz-copy-source-if-*`) are covered in §9. Per [conditional reads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-reads.html), CopyObject supports both the source conditions and destination `If-Match`/`If-None-Match`.


## 3. Checksums

Primary sources:

- [Checking object integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html) ("UG-Integrity")
- [... for data uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html) ("UG-Upload")
- [AWS SDKs: Data Integrity Protections for Amazon S3](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html) ("SDK-Integrity")
- [Checksum type](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Checksum.html)
- The multipart-checksum [tutorial](https://docs.aws.amazon.com/AmazonS3/latest/userguide/tutorial-s3-mpu-additional-checksums.html)

### 3.1 Supported algorithms (current list, larger than the classic five)

UG-Integrity and UG-Upload now list **ten** algorithms. "The `CRC64NVME` checksum algorithm is the default checksum algorithm used for checksum calculations."

| Algorithm id | Request/response header | Width (per [Checksum](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Checksum.html)) | Multipart FULL_OBJECT | Multipart COMPOSITE |
|---|---|---|---|---|
| `CRC64NVME` | `x-amz-checksum-crc64nvme` | 64-bit | Yes | **No** |
| `CRC32` | `x-amz-checksum-crc32` | 32-bit | Yes | Yes |
| `CRC32C` | `x-amz-checksum-crc32c` | 32-bit | Yes | Yes |
| `SHA1` | `x-amz-checksum-sha1` | 160-bit | No | Yes |
| `SHA256` | `x-amz-checksum-sha256` | 256-bit | No | Yes |
| `MD5` | `x-amz-checksum-md5` | 128-bit | No | Yes |
| `XXHASH64` | `x-amz-checksum-xxhash64` | 64-bit | No | Yes |
| `XXHASH3` | `x-amz-checksum-xxhash3` | 64-bit | No | Yes |
| `XXHASH128` | `x-amz-checksum-xxhash128` | 128-bit | No | Yes |
| `SHA512` | `x-amz-checksum-sha512` | 512-bit | No | Yes |

- The multipart columns are verbatim from UG-Upload, [Multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#MultipartUploads-Checksums).
- All values are **base64 of the big-endian digest** ("a base64 encoding of the big-endian checksum value", UG-Upload trailer section).
- The launch date of MD5/XXHASH*/SHA512 is **UNVERIFIED**: no row in the [document history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html) mentions them. CRC64NVME and "improved checksum integrity features" date from **December 1, 2024** (same page).
- "AWS SDKs do not automatically calculate MD5 checksums." "The legacy `Content-MD5` header remains available for single part uploads using SSE-S3 encryption." (UG-Integrity).
- UG-Upload also says: "The `content-MD5` header is only available using the S3 ETag for objects uploaded in a single part upload (`PUT` operation) that uses the SSE-S3 encryption."

### 3.2 Request headers and rules

- `x-amz-checksum-<alg>: <base64>` on PutObject, UploadPart and CompleteMultipartUpload (whole-object value). S3 "independently calculates a checksum ... and validates it with the provided value before storing the object and checksum value" (UG-Integrity).
- A mismatch returns **`BadDigest`** (400), "The Content-MD5 or checksum value that you specified did not match what the server received". `InvalidDigest` (400) is "The Content-MD5 or checksum value that you specified is not valid" ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)).
  - s3-tests sends the literal `'bad'` as `ChecksumSHA256` and expects **`400 BadDigest`**, not InvalidDigest ([test_object_checksum_sha256](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L14961), `checksum`).
- `x-amz-sdk-checksum-algorithm` ([PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-ChecksumAlgorithm)):
  - "When you send this header, there must be a corresponding `x-amz-checksum-algorithm` or `x-amz-trailer` header sent. Otherwise, Amazon S3 fails the request with the HTTP status code `400 Bad Request`."
  - "If the individual checksum value you provide through `x-amz-checksum-algorithm` doesn't match the checksum algorithm you set through `x-amz-sdk-checksum-algorithm`, Amazon S3 fails the request with a `BadDigest` error."
  - "If you provide an individual checksum, Amazon S3 ignores any provided `ChecksumAlgorithm` parameter" ([UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html#AmazonS3-UploadPart-request-header-ChecksumAlgorithm)).
  - UG-Upload: "When you use the REST API, don't use the `x-amz-sdk-checksum-algorithm` parameter".
- Trailing checksums: `x-amz-trailer: x-amz-checksum-<alg>` with aws-chunked framing (see §1.9). Supported for PutObject and UploadPart. The allowed trailer names cover only the five classic algorithms (crc32, crc32c, crc64nvme, sha1, sha256) ([UG-Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums-headers)).
- The default if none is supplied: "If you don't specify a checksum algorithm and the SDK also doesn't calculate a checksum for you, then S3 automatically chooses the CRC-64/NVME (`CRC64NVME`) checksum algorithm." "if objects are uploaded without a checksum, S3 automatically attaches the recommended full object CRC-64/NVME" (UG-Upload). [Checksum.ChecksumCRC64NVME](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Checksum.html) is "present ... if the object was uploaded without a checksum (and Amazon S3 added the default checksum, `CRC64NVME`...)".
- Operations whose request bodies need integrity:
  - DeleteObjects: "The Content-MD5 request header is required for all Multi-Object Delete requests" (general purpose). Directory buckets accept "Content-MD5 ... or a additional checksum request header" ([DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)).
  - Several bucket/object config PUTs say Content-MD5 "must be used" and also accept `x-amz-sdk-checksum-algorithm`: [PutBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html), [PutBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html), [PutBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html), [PutObjectAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html), [PutBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html), [PutBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketWebsite.html), [PutBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html), [PutBucketRequestPayment](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketRequestPayment.html).
  - [PutBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html) has no Content-MD5 header at all. Its own example request sends `x-amz-sdk-checksum-algorithm: CRC32` + `x-amz-checksum-crc32: UCqxTw==`.
  - PutObject with Object Lock retention: "The `Content-MD5` or `x-amz-sdk-checksum-algorithm` header is required".
  - **Trap:** SDKs with default integrity protection send a CRC checksum header, not Content-MD5 (SDK-Integrity; the Lifecycle example above). The server must accept *either* `Content-MD5` *or* any valid `x-amz-checksum-*` (or trailer) wherever MD5 is "required". Whether AWS general-purpose DeleteObjects accepts a checksum header without Content-MD5 is not stated in AWS docs (**UNVERIFIED**), but s3-tests runs unpinned `boto3`/`botocore` ([requirements.txt](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/requirements.txt)) and calls `delete_objects` without MD5.

### 3.3 Checksum type: `FULL_OBJECT` vs `COMPOSITE`

- Definitions (UG-Upload):
  - "**Full object checksums:** A full object checksum is calculated based on all of the content of a multipart upload, covering all data from the first byte of the first part to the last byte of the last part."
  - "**Composite checksums:** A composite checksum is calculated based on the individual checksums of each part in a multipart upload ... aggregates the part-level checksums (from the first part to the last)".
  - "Objects that you upload using `PutObject` use the full object checksum type".
- Headers:
  - `x-amz-checksum-type` (`COMPOSITE | FULL_OBJECT`) on [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html#AmazonS3-CreateMultipartUpload-request-header-ChecksumType) together with `x-amz-checksum-algorithm`.
  - On [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#AmazonS3-CompleteMultipartUpload-request-header-ChecksumType): "If the checksum type doesn't match the checksum type that was specified for the object during the `CreateMultipartUpload` request, it'll result in a `BadDigest` error."
  - Returned as a response header on PutObject ("For `PutObject` uploads, the checksum type is always `FULL_OBJECT`"), GetObject, HeadObject and CompleteMultipartUpload.
- "The `CRC64NVME` checksum is always a full object checksum" (CompleteMultipartUpload).
- Full-object is available only for CRC32/CRC32C/CRC64NVME, "because they can linearize into a full object checksum" (UG-Upload).
- `x-amz-mp-object-size` on CompleteMultipartUpload: "If there's a mismatch between the specified object size value and the actual object size value, it results in an `HTTP 400 InvalidRequest` error."
- Algorithm declaration at CreateMultipartUpload (UG-Upload, verbatim):
  - "When using multipart uploads with the new checksum algorithms (MD5, XXHash3, XXHash64, XXHash128, SHA-512), you must specify the checksum algorithm in the `CreateMultipartUpload` request ... [otherwise] the request will fail with an `InvalidRequest` error."
  - "For existing checksum algorithms (CRC32, CRC32C, SHA-1, SHA-256), if the algorithm is not specified in `CreateMultipartUpload`, any checksum header provided in `CompleteMultipartUpload` is currently accepted but not validated or stored with the object."
- UploadPart: the per-part algorithm "must be the same for all parts and it match the checksum value supplied in the `CreateMultipartUpload` request" ([UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html#AmazonS3-UploadPart-request-header-ChecksumAlgorithm)).
- **Quirk:** "If you use a multipart upload with **Checksums** for composite (or part-level) checksums, the multipart upload part numbers must be consecutive and begin with 1. If you try to complete a multipart upload request with nonconsecutive part numbers, Amazon S3 generates an `HTTP 500 Internal Server` error." (UG-Upload). mantle should return a 400 instead of copying a 500. This deviation is harmless.

### 3.4 How the composite and full-object values are computed (verified)

**Composite.** The value is `base64( H( H(part1) || H(part2) || ... || H(partN) ) ) + "-" + N`, where `H` is the chosen algorithm and each `H(part)` is the **binary** digest.

- AWS's tutorial decodes the three base64 part SHA-256 values, concatenates the bytes, and runs `sha256sum` on the result, then compares with `ChecksumSHA256 = "aI8EoktCdotjU8Bq46DrPCxQCGuGcPIhJ51noWs6hvk=-3"` ([tutorial step 9](https://docs.aws.amazon.com/AmazonS3/latest/userguide/tutorial-s3-mpu-additional-checksums.html#verify-object-integrity-sha256-step9)). The Java sample in UG-Upload computes the same "checksum of checksums".
- We recomputed AWS's example locally: the parts `QLl8R4i4+SaJlrl8ZIcutc5TbZtwt2NwB8lTXkd3GH0=`, `xCdgs1K5Bm4jWETYw/CmGYr+m6O2DcGfpckx5NVokvE=`, `f5wsfsa5bB+yXuwzqG1Bst91uYneqGD3CCidpb54mAo=` give sha256 `688f04a2...86f9`, whose base64 is `aI8EoktCdotjU8Bq46DrPCxQCGuGcPIhJ51noWs6hvk=`. This matches exactly.
- The **`-N` suffix appears on the value returned for composite objects** (CompleteMultipartUpload and HeadObject output in the tutorial). s3-tests expects it on the CompleteMultipartUpload request *and* response: [test_multipart_checksum_sha256](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15006) uses `'Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1'` for one 1024×`A` part. We verified this equals `base64(sha256(sha256(1024×'A')))` plus `-1`.

**Full object (CRC).** The value is the CRC of the entire object bytes, with **no `-N` suffix**. s3-tests vectors, all verified locally with `zlib.crc32`, use three parts of 5 MiB of `A`, `B` and `C`:

- CRC32 parts `JRTCyQ==`, `QoZTGg==`, `YAgjqw==` give the full object `WgDhBQ==` ([test_multipart_use_cksum_helper_crc32](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15228)).
- CRC64NVME parts `L/E4WYn8v98=`, `xW1l19VobYM=`, `cK5MnNaWrW4=` give `i+6LR0y3eFo=` ([..._crc64nvme](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15204)).
- The SHA256 COMPOSITE value for the same parts is `uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3` ([..._sha256](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15180)). The multipart ETag is `b2add96cc9702bbf4efb0ccdfc6b7747-3` ([test_multipart_reupload_checksum_and_etag](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15071)). Both were recomputed locally and match.
- A server must be able to **combine part CRCs** (CRC "linearization", i.e. `crc_combine`) to produce FULL_OBJECT values without re-reading data.

`multipart_checksum_3parts_helper` ([L15117](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15117)) also asserts:

- `CreateMultipartUpload` echoes `ChecksumAlgorithm`.
- CompleteMultipartUpload returns `ChecksumType` and the combined value.
- `HeadObject` **without** `x-amz-checksum-mode` returns no checksum; with `ChecksumMode=ENABLED` it returns value and type.
- `GetObjectAttributes(Checksum)` returns type and value.
- `GetObject(PartNumber=n)` returns **that part's** checksum with the object's `ChecksumType`.

### 3.5 Retrieving checksums

- `x-amz-checksum-mode: ENABLED` on GetObject/HeadObject: "To retrieve the checksum, this mode must be enabled." ([GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#AmazonS3-GetObject-request-header-ChecksumMode)).
- The response carries `x-amz-checksum-<alg>` plus `x-amz-checksum-type`.
- "For completed uploads, you can get an individual part's checksum by using the `GetObject` or `HeadObject` operations and specifying a part number or byte range that aligns with a single part." In-progress uploads expose part checksums via ListParts (UG-Upload).
- The behavior for ranged GETs that do not align to a part is not stated (**UNVERIFIED**). The safe choice is to omit checksum headers.
- CopyObject recomputes: "With a copy command, the checksum of the object is a direct checksum of the full object. If the object was originally uploaded using a multipart upload, the checksum value changes even though the data doesn't." "If the source object doesn't have a specified checksum algorithm or checksum value, Amazon S3 uses the CRC-64/NVME algorithm" for the destination (UG-Upload).

### 3.6 SDK default integrity protections (late 2024 onward)

- SDK-Integrity: "Previously, these checks were opt-in. Now, we've enabled these checks by default, using CRC-based algorithms such as CRC32 or CRC64NVME." "If your application uses a version prior to December 2024 of the SDK or tool, Amazon S3 still computes a CRC64NVME checksum on new objects".
- Settings, both defaulting to `WHEN_SUPPORTED`:

| Setting (config file / env var / JVM property) | Default | Values |
|---|---|---|
| `request_checksum_calculation` / `AWS_REQUEST_CHECKSUM_CALCULATION` / `aws.requestChecksumCalculation` | `WHEN_SUPPORTED` | `WHEN_SUPPORTED`: "Checksum validation is performed on all request payloads when supported by the API operation"; `WHEN_REQUIRED` |
| `response_checksum_validation` / `AWS_RESPONSE_CHECKSUM_VALIDATION` / `aws.responseChecksumValidation` | `WHEN_SUPPORTED` | `WHEN_SUPPORTED`; `WHEN_REQUIRED`: "only when ... the caller has explicitly enabled checksum ... `ChecksumMode` parameter is set to enabled" |

- Default algorithm by client (SDK-Integrity table):

| Client | Default algorithm |
|---|---|
| AWS CLI v2, SDK for C++ | **CRC64NVME** |
| Boto3 (and CLI v1), SDK for Rust, Java 2.x, Go v2, JavaScript v3, Kotlin, .NET, PHP, Ruby, Swift | **CRC32** |

  Boto3 supports CRC32C/CRC64NVME/xxHash/SHA512 only "Via CRT". The SDK for Rust supports CRC64NVME, CRC32, CRC32C, SHA1 and SHA256.
- On the wire, s3-tests shows botocore switching `put_object` to `STREAMING-UNSIGNED-PAYLOAD-TRAILER` unless `request_checksum_calculation='when_required'` is set. See the comment in [test_object_create_bad_contentlength_negative](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L212) (`auth_common`, `fails_on_mod_proxy_fcgi`).
- The consequence for a server: **current default clients compute CRC32 (boto3, Rust) or CRC64NVME (CLI v2) on every upload and validate response checksums when present.** For botocore, s3-tests shows the upload is sent as an unsigned aws-chunked body with a trailing checksum.
  - For the Rust SDK and CLI v2, whether the checksum travels as a trailer or a header is **UNVERIFIED** here. Support both.

## 4. Multipart upload rules

Primary sources:

- [Multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html) ("qfacts")
- [Multipart upload overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) ("MPU-UG")
- API pages: [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html), [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html), [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html), [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html), [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html), [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html), [ListMultipartUploads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)

### 4.1 Limits (current, verified September 2026)

The [qfacts](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html) table, verbatim:

| Item | Specification |
|---|---|
| Maximum object size | **48.8 TiB** |
| Maximum number of parts per upload | 10,000 |
| Part numbers | 1 to 10,000 (inclusive) |
| Part size | 5 MiB to 5 GiB. There is no minimum size limit on the last part of your multipart upload. |
| Maximum number of parts returned for a list parts request | 1000 |
| Maximum number of multipart uploads returned in a list multipart uploads request | 1000 |

- **The object-size limit changed.** On **December 2, 2025**, "Amazon S3 has increased the maximum size of an object you can store in an S3 bucket from 5 TB to 50 TB. For uploading these larger objects, you must implement the Multipart Upload REST API ... and when downloading these objects, use concurrent `GetObject` requests, as single GET requests are limited to 5 TB." ([document history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html)).
- The per-part (5 GiB) and part-count (10,000) limits did **not** change. The new ceiling is simply 10,000 × 5 GiB: "While Amazon S3 documentation commonly references 50 TB object size limit, the actual maximum object size is 53.7 TB (48.8 TiB). This limit is determined by multipart upload constraints: 10,000 maximum parts x 5 GiB per part = 50,000 GiB (53.7 TB)" ([Objects overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingObjects.html)).
- The General Reference quota table says "Object size: 48.828125 Terabytes", "Maximum part size: 5 Gigabytes", and "Parts: 10,000" ([S3 quotas](https://docs.aws.amazon.com/general/latest/gr/s3.html)).
- Single PUT limit: "With a single `PUT` operation, you can upload a single object up to 5 GB in size" ([Uploading objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html)).
- Single GET limit: "Single GET requests are limited to 5 TB, and you will receive a `405 - Method Not Allowed` error for GET requests beyond 5 TB" ([Downloading objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html)). This is the `EntityTooLarge`/405 variant in the error list: "Your proposed download exceeds the maximum allowed size."
- Oversized upload: `EntityTooLarge` 400, "Your proposed upload exceeds the maximum allowed object size" ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)).

### 4.2 CreateMultipartUpload

- `POST /{Key}?uploads`. The response is `InitiateMultipartUploadResult{Bucket, Key, UploadId}`. It also returns headers `x-amz-checksum-algorithm`, `x-amz-checksum-type`, and, with lifecycle rules, `x-amz-abort-date` / `x-amz-abort-rule-id` ([CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)).
- "If you want to provide metadata describing the object being uploaded, you must provide it in the request to initiate the multipart upload. Anonymous users cannot initiate multipart uploads." ([MPU-UG](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#mpu-process)).
- Checksum algorithm and type are declared here; see §3.3.

### 4.3 UploadPart / UploadPartCopy

**UploadPart** (`PUT /{Key}?partNumber=N&uploadId=ID`):

- "Part numbers can be any number from 1 to 10,000, inclusive ... If you upload a new part using the same part number that was used with a previous part, the previously uploaded part is overwritten." ([UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)).
- "The part number that you choose doesn't need to be in a consecutive sequence (for example, it can be 1, 5, and 14)" (MPU-UG).
  - Exception: with additional checksums, part numbers "must use consecutive part numbers and begin with 1", else **HTTP 500** (MPU-UG, [#mpuchecksums](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#mpuchecksums)).
- Integrity: "specify the `Content-MD5` header ... If the upload request is signed with Signature Version 4, then AWS S3 uses the `x-amz-content-sha256` header as a checksum instead of `Content-MD5`."
- Special error: `NoSuchUpload` 404.
- The error code for part numbers outside 1..10000 is **UNVERIFIED**; no AWS page in scope names it.

**UploadPartCopy** (`PUT ...?partNumber&uploadId` with `x-amz-copy-source`):

- `x-amz-copy-source-range`: "The range value must use the form bytes=first-last, where the first and last are the zero-based byte offsets to copy ... You can copy a range only if the source object is greater than 5 MB." ([UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html#AmazonS3-UploadPartCopy-request-header-CopySourceRange)).
- Copy-source conditions have the same documented pairs as CopyObject (§9.4).
- Special errors: `NoSuchUpload` 404; `InvalidRequest` 400, "The specified copy source is not supported as a byte-range copy source."
- Delete-marker sources: "If the current version is a delete marker and you don't specify a versionId in the `x-amz-copy-source` request header, Amazon S3 returns a `404 Not Found` error ... If you specify versionId in the `x-amz-copy-source` and the versionId is a delete marker, Amazon S3 returns an HTTP `400 Bad Request` error".
- Response: `CopyPartResult{ETag, LastModified, Checksum*}`, plus header `x-amz-copy-source-version-id`.
- s3-tests:
  - A range past the end of a 5-byte source returns `InvalidRange` with status 400 **or** 416: [test_multipart_copy_invalid_range](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6113) (`copy`).
  - Malformed ranges (`0-2`, `bytes=0`, `bytes=hello-world`, `bytes=0-bar`, `bytes=hello-`, `bytes=0-2,3-5`) return 400 `InvalidArgument`: [test_multipart_copy_improper_range](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6135) (`copy`, `fails_on_rgw`).
  - Special source keys `' '`, `'_'`, `'__'`, `'?versionId'` must round-trip: [test_multipart_copy_special_names](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6192).
  - The copy source must be URL-decoded exactly once. Copying raw key `anyfilename%.txt` must **fail** even though `anyfilename%25.txt` exists: [test_upload_part_copy_percent_encoded_key](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19344).

### 4.4 CompleteMultipartUpload

**Request** (`POST /{Key}?uploadId=ID`):

- The body is `<CompleteMultipartUpload><Part><PartNumber/><ETag/>[<Checksum*/>]</Part>...</CompleteMultipartUpload>`.
- "Amazon S3 concatenates all the parts in ascending order by part number ... The CompleteMultipartUpload API operation concatenates the parts that you provide in the list" ([CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)).
- Parts not listed are discarded. "After a successful *complete* request, the parts no longer exist" (MPU-UG).
- "You can't use `Content-Type: application/x-www-form-urlencoded` for the CompleteMultipartUpload requests."
- Optional headers: `If-Match`/`If-None-Match` (§2), `x-amz-checksum-*`, `x-amz-checksum-type`, `x-amz-mp-object-size` (§3.3).

**Special errors** (verbatim, same page):

| Code | HTTP | Description |
|---|---|---|
| `EntityTooSmall` | 400 | "Your proposed upload is smaller than the minimum allowed object size. Each part must be at least 5 MB in size, except the last part." |
| `InvalidPart` | 400 | "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified ETag might not have matched the uploaded part's ETag." |
| `InvalidPartOrder` | 400 | "The list of parts was not in ascending order. The parts list must be specified in order by part number." |
| `NoSuchUpload` | 404 | "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed." |

**The 200-with-error trap** (verbatim):

> The processing of a CompleteMultipartUpload request could take several minutes to finalize. After Amazon S3 begins processing the request, it sends an HTTP response header that specifies a `200 OK` response. While processing is in progress, Amazon S3 periodically sends white space characters to keep the connection from timing out. A request could fail after the initial `200 OK` response has been sent. This means that a `200 OK` response can contain either a success or an error.

- The documented example body is `<Error><Code>InternalError</Code>...</Error>` with `HTTP/1.1 200 OK`.
- The same applies to CopyObject (§9.5). No such statement exists for UploadPartCopy; that is **UNVERIFIED** for UploadPartCopy.
- mantle may choose never to emit a 200-then-error. SDKs handle both forms.

**Concurrency** (MPU-UG, [#distributedmpupload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#distributedmpupload)):

- "When the buckets have S3 Versioning enabled, completing a multipart upload always creates a new version. When you initiate multiple multipart uploads that use the same object key in a versioning-enabled bucket, the current version of the object is determined by which upload started most recently (`createdDate`)."
- Example: an upload started at 11:00 AM "becomes the current version, even if the first upload is completed after the second one". For unversioned buckets, "any other request received between the time when the multipart upload is initiated and when it completes, the other request might take precedence."
- **Implication:** in versioned buckets the version order of MPU objects is by *initiation* time, not completion time.

**s3-tests:**

- Parts of 10 KiB (non-last) return 400 `EntityTooSmall`: [test_multipart_upload_size_too_small](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6411).
- Referencing part number 9999 (never uploaded) in the Complete list returns 400 `InvalidPart`: [test_multipart_upload_missing_part](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6587).
- A wrong ETag returns 400 `InvalidPart`: [test_multipart_upload_incorrect_etag](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6606).
- Complete with **no body** returns 400 `MalformedXML`: [test_multipart_upload_empty](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5988).
- An unknown upload ID returns 404 `NoSuchUpload`: [test_multipart_upload_complete_without_create](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6001) (`fails_on_dbstore`).
- A 1-byte single-part upload is valid (the only part is also the last part): [test_multipart_upload_small](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6020).
  - That same test **calls CompleteMultipartUpload a second time with the same parts and expects success**. [test_multipart_reupload_checksum_and_etag](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15071) asserts the retried complete returns the *same* ETag and checksum.
  - This idempotent re-complete conflicts with AWS's `NoSuchUpload` wording ("might have been ... completed"). Implement idempotent re-completion for an identical part list; SDK retries depend on it.
- s3-tests always sends part ETags **without quotes** (`.strip('"')`), while AWS's sample request uses quoted ETags. Accept both.
- Re-uploading the same part number overwrites it: [test_multipart_upload_resend_part](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6331).
- No s3-tests test exercises `InvalidPartOrder`.

### 4.5 ETag of a multipart object (documented and verified)

- AWS now states the algorithm explicitly: "Amazon S3 calculates the MD5 digest of each individual part as it is uploaded ... Amazon S3 concatenates the bytes for the MD5 digests together and then calculates the MD5 digest of these concatenated values. During the final ETag creation step, Amazon S3 adds a dash with the total number of parts to the end." ([UG-Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#ChecksumTypes-Uploads)).
- The tutorial works an example: part MD5s `e611...5e69`, `63d2...5854`, `95b8...d310` are hex-decoded and concatenated, then `md5sum` gives `f453c6dccca969c457efdf9b1361e291`, and the ETag is `"f453c6dccca969c457efdf9b1361e291-3"` ([tutorial step 8](https://docs.aws.amazon.com/AmazonS3/latest/userguide/tutorial-s3-mpu-additional-checksums.html#verify-object-integrity-step8)). **We recomputed this locally and it matches.**
- s3-tests vector: three 5 MiB parts of `A`/`B`/`C` give `b2add96cc9702bbf4efb0ccdfc6b7747-3` (recomputed locally; matches) ([L15071](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L15071)).
- `N` counts the parts actually included in the Complete list.
- partNumber GET/HEAD returns the **whole-object** multipart ETag, not the part's ETag ([test_multipart_get_part](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6626)).

### 4.6 ListParts pagination

- `GET /{Key}?uploadId=ID&max-parts=&part-number-marker=`. "The `ListParts` request returns a maximum of 1,000 uploaded parts. The limit of 1,000 parts is also the default value ... If your multipart upload consists of more than 1,000 parts, the response returns an `IsTruncated` field with the value of `true`, and a `NextPartNumberMarker` element. To list remaining uploaded parts, in subsequent `ListParts` requests, include the `part-number-marker` query string parameter and set its value to the `NextPartNumberMarker`" ([ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)).
- `part-number-marker`: "Only parts with higher part numbers will be listed."
- The response `ListPartsResult` contains Bucket, Key, UploadId, PartNumberMarker, NextPartNumberMarker, MaxParts, IsTruncated, Part*, Initiator, Owner, StorageClass, ChecksumAlgorithm and ChecksumType. Each `Part` has Checksum*, ETag, LastModified, PartNumber and Size.
- "the returned list of parts doesn't include parts that haven't finished uploading" (MPU-UG).
- "Do not use the result of this listing when sending a *complete multipart upload* request" (MPU-UG).

### 4.7 ListMultipartUploads pagination and ordering

- `GET /?uploads&prefix=&delimiter=&key-marker=&upload-id-marker=&max-uploads=&encoding-type=url` ([ListMultipartUploads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)).
- `max-uploads` is "from 1 to 1,000". The default is 1,000. When truncated the response has `IsTruncated=true`, `NextKeyMarker` and `NextUploadIdMarker`.
- Marker semantics: "If `upload-id-marker` is not specified, only the keys lexicographically greater than the specified `key-marker` will be included in the list. If `upload-id-marker` is specified, any multipart uploads for a key equal to the `key-marker` might also be included, provided those multipart uploads have upload IDs lexicographically greater than the specified `upload-id-marker`." "If key-marker is not specified, the upload-id-marker parameter is ignored."
- Ordering: "Multipart uploads are initially sorted in ascending order based on their object keys ... For uploads that share the same object key, they are further sorted in ascending order based on the upload initiation time."
  - **Note the tension:** paging by `upload-id-marker` is lexicographic, but the order within a key is by initiation time. Upload IDs should therefore sort lexicographically in initiation order, for example with a time-prefixed ID.
- Delimiter: `CommonPrefixes` roll-up as in ListObjects. "`CommonPrefixes` is filtered out from results if it is not lexicographically greater than the key-marker."
- `encoding-type=url` encodes `Delimiter`, `KeyMarker`, `Prefix`, `NextKeyMarker` and `Key`.

### 4.8 AbortMultipartUpload

- `DELETE /{Key}?uploadId=ID` returns **204**. `NoSuchUpload` is 404 ([AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)).
- "After a multipart upload is aborted, no additional parts can be uploaded using that upload ID. The storage consumed by any previously uploaded parts will be freed. However, if any part uploads are currently in progress, those part uploads might or might not succeed. As a result, it might be necessary to abort a given multipart upload multiple times in order to completely free all storage consumed by all parts."
- Directory buckets only: `x-amz-if-match-initiated-time`, which returns 412 on mismatch and 204 "if the multipart upload doesn't exist".
- s3-tests: an abort with a bogus ID returns 404 `NoSuchUpload` ([test_abort_multipart_upload_not_found](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6499)). After an abort no object is listed ([test_abort_multipart_upload](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6487)).
- Lifecycle `AbortIncompleteMultipartUpload` is covered in [mpu-abort-incomplete-mpu-lifecycle-config](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html). It is optional for mantle v1; exclude the `lifecycle` marker.


## 5. ETag rules and Content-MD5

### 5.1 ETag for single-part objects

[Object.ETag](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html), verbatim:

> The entity tag is a hash of the object. The ETag reflects changes only to the contents of an object, not its metadata ...
> - Objects created by the PUT Object, POST Object, or Copy operation, or through the AWS Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that are an MD5 digest of their object data.
> - Objects created by the PUT Object, POST Object, or Copy operation ... and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5 digest of their object data.
> - If an object is created by either the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest, regardless of the method of encryption.

- The same rules appear in [Common response headers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTCommonResponseHeaders.html) and [UG-Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#checking-object-integrity-etag-and-md5).
- Directory buckets: "The ETag for the object in a directory bucket isn't the MD5 digest of the object" ([PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-response-header-ETag)).
- **Format:** lowercase hex MD5 wrapped in double quotes, both in the header and in XML (`<ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>` in the CompleteMultipartUpload sample; `ETag: "..."` in headers).
  - s3-tests: `put_object(Body='bar')` returns ETag `'"37b51d194a7513e45b56f6524f2d51f2"'` (MD5 of "bar", quoted) ([test_object_write_check_etag](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1803)).
  - Listings also return quoted ETags in XML, for example `<ETag>"fba9dede5f27731c9771645a39863328"</ETag>` in the [ListObjectsV2 examples](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_Examples) Literal `"` and the `&quot;` entity are equivalent XML; which one AWS emits on the wire is **UNVERIFIED**.
- A copy of a multipart object via CopyObject becomes single-part: "If the source object is an object that was uploaded by using a multipart upload, the object copy will be a single part object" ([CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html#AmazonS3-CopyObject-request-header-CopySource)). Its ETag is therefore MD5 for SSE-S3/plaintext. This follows from the rules above; it is not stated verbatim.
- "The ETag reflects changes only to the contents of an object, not its metadata". A metadata-only self-copy keeps the ETag, given MD5-of-content.

### 5.2 Content-MD5

- "The base64 encoded 128-bit MD5 digest of the message (without the headers) according to RFC 1864" ([Common request headers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTCommonRequestHeaders.html); [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-ContentMD5)).
- "After uploading the object, Amazon S3 calculates the MD5 digest of the object and compares it to the value that you provided. The request succeeds only if the two digests match." ([UG-Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#checking-object-integrity-md5)).
- Errors ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)):
  - `BadDigest` 400: "The Content-MD5 or checksum value that you specified did not match what the server received."
  - `InvalidDigest` 400: "The Content-MD5 or checksum value that you specified is not valid."
  - "if you send a Content-MD5 header with a REST PUT request that doesn't match the digest calculated on the server, you receive a `BadDigest` error. The error response also includes as detail elements the digest that the server calculated, and the digest that you told the server to expect." The element names are **UNVERIFIED**; they are not given.
- s3-tests (`auth_common`):
  - `Content-MD5: YWJyYWNhZGFicmE=` (valid base64 but only 11 bytes) returns 400 `InvalidDigest`: [test_object_create_bad_md5_invalid_short](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L158).
  - A well-formed but wrong MD5 returns 400 `BadDigest`: [test_object_create_bad_md5_bad](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L165).
  - An empty value returns 400 `InvalidDigest`: [..._empty](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_headers.py#L172).
  - The rule to implement: decode base64. If decoding fails or the result is not 16 bytes, return `InvalidDigest`. If it is 16 bytes and differs, return `BadDigest`.
- SSE and MD5: "The legacy `Content-MD5` header remains available for single part uploads using SSE-S3 encryption" ([UG-Integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)). Directory buckets do not support MD5 at all.


## 6. Listing: ListObjectsV2, ListObjects (V1), ListObjectVersions

Primary sources:

- [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
- [ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)
- [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
- [Listing object keys programmatically](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)
- [Object key sort order](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-sort-order)

### 6.1 Ordering

- "List results are always returned in UTF-8 binary order." ([ListingKeysUsingAPIs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)).
- "Amazon S3 sorts object keys, including prefixes, lexicographically by their UTF-8 encoded byte values." The worked example orders `Apple/` (0x41) < `apple/` (0x61) < `éclair/` (0xC3 0xA9) < `中 文/` ([object-keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-sort-order)).
- ListObjectsV2 says the same for general purpose buckets: "returns objects in lexicographical order based on their key names". Directory buckets are unordered.
- Implementation rule: compare raw UTF-8 byte strings (memcmp). Do not use locale or UTF-16 collation. CommonPrefixes interleave with keys in this same order.

### 6.2 ListObjectsV2 (`GET /?list-type=2`)

Request parameters ([ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestParameters)):

| Parameter | AWS semantics (verbatim where quoted) |
|---|---|
| `prefix` | "Limits the response to keys that begin with the specified prefix." |
| `delimiter` | "A delimiter is a character that you use to group keys." "`CommonPrefixes` is filtered out from results if it is not lexicographically greater than the `StartAfter` value." |
| `max-keys` | "By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more." |
| `continuation-token` | "`ContinuationToken` is obfuscated and is not a real key." |
| `start-after` | "Amazon S3 starts listing after this specified key. StartAfter can be any key in the bucket." |
| `fetch-owner` | "The owner field is not present in `ListObjectsV2` by default. If you want to return the owner field with each key in the result, then set the `FetchOwner` field to `true`." |
| `encoding-type` | `url`: "non-ASCII characters that are used in an object's key name will be percent-encoded according to UTF-8 code values. For example, the object `test_file(3).png` will appear as `test_file%283%29.png`." |
| `x-amz-optional-object-attributes` | `RestoreStatus` |

Response (`ListBucketResult`):

- Elements: `IsTruncated`, `Contents*` (ChecksumAlgorithm*, ChecksumType, ETag, Key, LastModified, Owner?, RestoreStatus?, Size, StorageClass), `Name`, `Prefix`, `Delimiter`, `MaxKeys`, `CommonPrefixes*{Prefix}`, `EncodingType`, `KeyCount`, `ContinuationToken`, `NextContinuationToken`, `StartAfter`.
- **CommonPrefixes counting:** "All of the keys that roll up into a common prefix count as a single return when calculating the number of returns." For Delimiter: "These rolled-up keys are not returned elsewhere in the response. Each rolled-up result counts as only one return against the `MaxKeys` value."
- **KeyCount:** "the number of keys returned with this request. `KeyCount` will always be less than or equal to the `MaxKeys` field."
  - s3-tests pins that KeyCount **includes CommonPrefixes**: `assert response['KeyCount'] == len(prefixes) + len(keys)` ([test_bucket_listv2_delimiter_basic](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L221), `list_objects_v2`).
- **NextContinuationToken:** "sent when `isTruncated` is true ... obfuscated and is not a real key".
- **encoding-type=url** encodes these elements: "`Delimiter, Prefix, Key,` and `StartAfter`".
  - s3-tests shows `CommonPrefixes/Prefix` values are encoded too, and `/` is not encoded. With keys `foo+1/bar`, `quux ab/thud`, `asdf+b`, the expected output is keys `['asdf%2Bb']` and prefixes `['foo%2B1/', 'foo/', 'quux%20ab/']` ([test_bucket_listv2_encoding_basic](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L237), and the V1 twin [L250](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L250)).
  - Use a `UriEncode`-like encoder that keeps `/` literal and encodes space as `%20` and `+` as `%2B`. Whether AWS encodes other unreserved characters such as `~` is **UNVERIFIED**.
- Errors: `NoSuchBucket` 404.

s3-tests behaviors (marker `list_objects_v2` unless noted):

- `MaxKeys=0` gives `IsTruncated == False` and no keys, even though objects exist: [test_bucket_listv2_maxkeys_zero](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1042).
- Default `MaxKeys` is echoed as `1000`: [test_bucket_listv2_maxkeys_none](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1065).
- `ContinuationToken=''` is treated as absent but echoed back as `''`: [test_bucket_listv2_continuationtoken_empty](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1278).
- With both `StartAfter` and `ContinuationToken`, the token wins, and `StartAfter` is still echoed: [test_bucket_listv2_both_continuationtoken_startafter](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1307).
- `StartAfter='\x0a'` is echoed verbatim: [test_bucket_listv2_startafter_unreadable](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1335).
- A `StartAfter` value not present in the list works: [L1357](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1357).
- `Owner` appears only with `FetchOwner=True`: [L622](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L622), [L632](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L632).
- `Delimiter=''` means "no delimiter", and the `Delimiter` element is **omitted** from the response: [test_bucket_listv2_delimiter_empty](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L578).
- Paging with delimiter `/` and `MaxKeys=1` over `asdf, boo/bar, boo/baz/xyzzy, cquux/...` yields pages `[asdf]`, `[boo/]`, `[cquux/]`: [test_bucket_listv2_delimiter_prefix](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L335).
  - The continuation after a CommonPrefix must skip every key under that prefix. This follows from the documented "filtered out ... if it is not lexicographically greater than" rule.
- A key equal to `prefix` that ends with the delimiter (`asdf/` with prefix `asdf/`) is returned as a **key**, not a CommonPrefix: [L358](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L358).
- Delimiters other than `/` must work (`a`, `%`, space, `.`, and unreadable `\x0a`): [L383](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L383) through [L550](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L550). All s3-tests delimiters are single characters. AWS calls the delimiter "a character" but defines roll-up by "the first occurrence of the delimiter". Multi-character delimiters are **UNVERIFIED**; string matching is a safe superset.
- 999 keys under `0/` plus `1999`, `1999#`, `1999+`, `2000` must yield one CommonPrefix `0/` plus the 4 keys in one default page, so roll-ups do not consume max-keys: [test_bucket_list_delimiter_not_skip_special](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L682) (`fails_on_dbstore`).
- The `test_bucket_list[v2]_unordered` tests ([L1132](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1132), [L1186](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1186)) are RGW extensions, marked `fails_on_aws`.

### 6.3 ListObjects (V1) (`GET /`)

- `marker`: "Amazon S3 starts listing after this specified key. Marker can be any key in the bucket."
- Response `Marker`: "Marker is included in the response if it was sent with the request." **But** s3-tests expects `<Marker>` to be present, as `''`, even when not sent ([test_bucket_list_marker_none](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1257), unmarked). Always emitting `<Marker></Marker>` satisfies both.
- **NextMarker** (verbatim): "When the response is truncated ... you can use the key name in this field as the `marker` parameter in the subsequent request ... **This element is returned only if you have the `delimiter` request parameter specified.** If the response does not include the `NextMarker` element and it is truncated, you can use the value of the last `Key` element in the response as the `marker` parameter in the subsequent request" ([ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html#AmazonS3-ListObjects-response-NextMarker)).
- s3-tests: with a delimiter, `NextMarker` is the last item returned, which may be a **CommonPrefix** such as `'boo/'`. It is absent when not truncated ([test_bucket_list_delimiter_prefix](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L312), `fails_on_dbstore`).
- V1 returns `Owner` by default. s3-tests compares `Owner.ID`/`DisplayName`, `ETag`, `Size` and `LastModified` with HEAD/ACL data ([test_bucket_list_return_data](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1400)).
- Listing `LastModified` must equal HEAD's `Last-Modified` at whole-second precision. The s3-tests helper `_compare_dates` zeroes the listing value's sub-seconds before comparing.
- `max-keys=blah` returns 400 `InvalidArgument` ([test_bucket_list_maxkeys_invalid](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1239)).
- V1 `encoding-type=url`: AWS does not list which elements are encoded (**UNVERIFIED** beyond s3-tests, which shows Key and CommonPrefix encoding, [L250](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L250)). Mirror V2 and also encode `Marker`/`NextMarker`. That V1 part is **UNVERIFIED**.
- Anonymous listing: 403 `AccessDenied` unless the bucket allows public read ([L1485](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1485)).

### 6.4 ListObjectVersions (`GET /?versions`)

- Parameters: `key-marker` ("Specifies the key to start with when listing objects in a bucket"), `version-id-marker` ("Specifies the object version you want to start listing from"), `max-keys` (default 1000), `prefix`, `delimiter`, `encoding-type`.
- Response `ListVersionsResult`: `IsTruncated`, `KeyMarker`, `VersionIdMarker`, `NextKeyMarker`, `NextVersionIdMarker`, then `Version*` (ETag, IsLatest, Key, LastModified, Owner, Size, StorageClass, VersionId, checksum fields, RestoreStatus) interleaved with `DeleteMarker*` (IsLatest, Key, LastModified, Owner, VersionId), then Name, Prefix, Delimiter, MaxKeys, CommonPrefixes, EncodingType.
- `encoding-type=url` encodes "`KeyMarker, NextKeyMarker, Prefix, Key`, and `Delimiter`".
- **Marker semantics are internally inconsistent in AWS text.** NextKeyMarker "specifies the first key not returned", but KeyMarker "Marks the last key returned in a truncated response".
  - Clients just echo `NextKeyMarker`/`NextVersionIdMarker` back, so any self-consistent resume position works.
  - The exact AWS semantics (exclusive vs inclusive) are **UNVERIFIED**. No s3-tests test covers version-listing pagination.
- Order: by key (UTF-8 binary), and **within a key newest version first**. s3-tests relies on this: "obj versions in versions come out created last to first" ([comment in check_obj_versions](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7677)). AWS docs do not state the within-key order explicitly (**UNVERIFIED** in AWS text).
- Permission name: "you must have permission to perform the `s3:ListBucketVersions` action. Be aware of the name difference."
- Unversioned buckets: "Amazon S3 returns the object listing with a version ID of `null`" ([list-obj-version-enabled-bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/list-obj-version-enabled-bucket.html)).
- ListObjects/V2 do **not** return keys whose current version is a delete marker ([DeleteMarker](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html)).
- In-progress MPUs: "For general purpose buckets, `ListObjectsV2` doesn't return prefixes that are related only to in-progress multipart uploads."
- `test_bucket_list_return_data_versioning` ([L1432](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1432)) cross-checks VersionId, ETag, Size and Owner against HEAD.


## 7. Versioning semantics

Primary sources:

- [S3 Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
- [How S3 Versioning works](https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html)
- [Working with delete markers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html)
- [Managing delete markers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManagingDelMarkers.html)
- [Adding objects to versioning-suspended buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html)
- [Deleting objects from versioning-suspended buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectsfromVersioningSuspendedBuckets.html)
- API pages: [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html), [PutBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html)

### 7.1 States and version IDs

- Three states: "Unversioned (the default)", "Versioning-enabled", "Versioning-suspended". "After you version-enable a bucket, it can never return to an unversioned state. But you can *suspend* versioning."
- Unversioned GetBucketVersioning: "If the versioning state has never been set on a bucket, it has no versioning state; a GetBucketVersioning request does not return a versioning state value." The body is an empty `<VersioningConfiguration/>`, and `MfaDelete` appears only if it was ever configured.
- `PutBucketVersioning` body: `<VersioningConfiguration><Status>Enabled|Suspended</Status>[<MfaDelete>Enabled|Disabled</MfaDelete>]</VersioningConfiguration>`. It needs Content-MD5 or a checksum (§3.2).
- s3-tests toggles Suspended to Enabled to Suspended freely: [test_versioning_bucket_create_suspend](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7654).
- Version IDs: "Each object has a version ID, whether or not S3 Versioning is enabled. If S3 Versioning is not enabled, Amazon S3 sets the value of the version ID to `null`." "Version IDs are Unicode, UTF-8 encoded, URL-ready, opaque strings that are no more than 1,024 bytes long." Example: `3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo` ([versioning-workflows](https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html#version-ids)).
  - The AWS example contains `+`. That conflicts with "URL-ready", so clients must percent-encode it in `?versionId=`. Choosing an alphabet without `+`, `/` or `=` avoids the issue.
- Pre-existing objects keep version ID `null`. Versioning takes effect for future writes only.
- Eventual consistency of the config: "it might take up to 15 minutes for the change to fully propagate" ([manage-versioning-examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/manage-versioning-examples.html)). mantle can make this instant.

### 7.2 Writes

- Enabled: every PUT/POST/CopyObject/CompleteMultipartUpload creates a new version. `x-amz-version-id` is returned ([AddingObjectstoVersioningEnabledBuckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersioningEnabledBuckets.html)).
- Multipart ordering: the current version is decided by upload **initiation** time (§4.4).
- Suspended: "Amazon S3 automatically adds a `null` version ID to every subsequent object stored ... If a null version is already in the bucket and you add another object with the same key, the added object overwrites the original null version." Versioned (non-null) versions are kept. The new `null` version becomes current ([AddingObjectstoVersionSuspendedBuckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html)).
  - s3-tests: after Enabled then Suspended, overwriting a pre-versioning object leaves exactly one `Versions` entry, the null one ([test_versioning_obj_plain_null_version_overwrite_suspended](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7854)).

### 7.3 Deletes and delete markers

- Simple DELETE (no versionId), from [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html):
  - Unversioned: permanent delete.
  - Enabled: "inserts a delete marker, which becomes the current version".
  - Suspended: "removes the object that has a null `versionId`, if there is one, and inserts a delete marker that becomes the current version ... If there isn't an object with a null `versionId` ... Amazon S3 does not remove the object and only inserts a delete marker." In suspended mode the marker's version ID is `null` ([DeletingObjectsfromVersioningSuspendedBuckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectsfromVersioningSuspendedBuckets.html)).
- Stacking: "If you use a `DeleteObject` request where the current version is a delete marker (without specifying the version ID of the delete marker), Amazon S3 does not delete the delete marker, but instead `PUTs` another delete marker." ([ManagingDelMarkers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManagingDelMarkers.html)). s3-tests: 3 deletes give 3 DeleteMarkers ([test_versioning_stack_delete_merkers](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7786), `fails_on_dbstore`).
- DELETE with `versionId` permanently deletes that version. "If the object deleted is a delete marker, Amazon S3 sets the response header `x-amz-delete-marker` to true."
  - Deleting a delete marker "undeletes" the object. The response is "204 NoContent / x-amz-version-id: versionID / x-amz-delete-marker: true".
  - "To delete a delete marker with a `NULL` version ID, you must pass the `NULL` as the version ID". s3-tests uses `VersionId='null'` to delete the pre-versioning object ([L7801](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7801)).
- A simple DELETE on a versioned bucket returns `x-amz-delete-marker: true` and `x-amz-version-id: <marker id>`. s3-tests reads `response['DeleteMarker'] == True` and `response['VersionId']` ([test_versioning_obj_create_read_remove_head](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7751)).
- MFA Delete: `x-amz-mfa` is required for versioned deletes when enabled, and "Requests that include `x-amz-mfa` must use HTTPS." It is optional for mantle.

### 7.4 Reading a delete marker (verbatim, [DeleteMarker](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html))

> When you get an object without specifying a `versionId` in your request, if its current version is a delete marker, Amazon S3 responds with the following:
> - A 404 (Not Found) error
> - A response header, `x-amz-delete-marker: true`
>
> When you get an object by specifying a `versionId` in your request, if the specified version is a delete marker, Amazon S3 responds with the following:
> - A 405 (Method Not Allowed) error
> - A response header, `x-amz-delete-marker: true`
> - A response header, `Last-Modified: timestamp` (only when using the HeadObject or GetObject API operations)
>
> The `x-amz-delete-marker: true` response header tells you that the object accessed was a delete marker. This response header never returns `false` ...

- Error codes: the 404 body code is `NoSuchKey` (s3-tests [L7801](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7801) after a null-version delete). The 405 code is `MethodNotAllowed` per the error table. That pairing for this case is not stated verbatim (**UNVERIFIED**).
- **Contradiction:** s3-tests [test_delete_marker_nonversioned](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19403) (`delete_marker`, `fails_on_dbstore`) expects a HEAD 404 on an **unversioned** bucket to carry `x-amz-delete-marker: false`. AWS says the header "never returns `false`". Deselect that test or accept the deviation.
- Versioned and suspended buckets return `true` on HEAD 404 after a simple delete ([L19415](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19415), [L19428](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19428)).
- Copy source that is a delete marker: 404 if implicit, 400 if a delete-marker versionId is named (UploadPartCopy text, §4.3).


## 8. DeleteObjects (Multi-Object Delete)

Source: [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html), [Deleting multiple objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-multiple-objects.html), [Delete](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Delete.html), [ObjectIdentifier](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectIdentifier.html), [DeletedObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletedObject.html), [Error](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html).

### 8.1 Request

```
POST /?delete HTTP/1.1
Host: {Bucket}.s3.amazonaws.com
x-amz-mfa / x-amz-bypass-governance-retention / x-amz-sdk-checksum-algorithm / Content-MD5 ...
<?xml version="1.0" encoding="UTF-8"?>
<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Object>
      <ETag>string</ETag>              <!-- conditional delete (§2.3) -->
      <Key>string</Key>
      <LastModifiedTime>timestamp</LastModifiedTime>   <!-- directory buckets only -->
      <Size>long</Size>                <!-- directory buckets only -->
      <VersionId>string</VersionId>
   </Object>
   ...
   <Quiet>boolean</Quiet>
</Delete>
```

- **Limit:** "The request can contain a list of up to 1,000 keys".
  - s3-tests: 1001 keys returns HTTP 400 ([test_multi_object_delete_key_limit](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1761), [..v2](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1778)). Only the status is asserted. The AWS error code for more than 1000 keys is **UNVERIFIED**; `MalformedXML` is the likely candidate but is not documented for this case.
- **Integrity header:**
  - General purpose: "The Content-MD5 request header is required for all Multi-Object Delete requests."
  - Directory: "The Content-MD5 request header or a additional checksum request header (including `x-amz-checksum-crc32`, `x-amz-checksum-crc32c`, `x-amz-checksum-sha1`, or `x-amz-checksum-sha256`) is required".
  - Modern SDKs send a CRC `x-amz-checksum-*` rather than Content-MD5 (§3.6). **Accept either.**
  - The AWS error code when neither is present is **UNVERIFIED** in AWS docs.
- **Quiet:** "Element to enable quiet mode for the request. When you add this element, you must set its value to `true`." "In quiet mode the response includes only keys where the delete operation encountered an error. For a successful deletion in a quiet mode, the operation does not return any information about the delete in the response body."
- **MFA Delete:** "If you do not provide one, the entire request will fail, even if there are non-versioned objects".
- Keys with XML-special characters: "Replacement must be made for object keys containing special characters (such as carriage returns) when using XML requests" ([ObjectIdentifier](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectIdentifier.html); see §10).

### 8.2 Response

```
HTTP/1.1 200
<DeleteResult>
   <Deleted>
      <DeleteMarker>boolean</DeleteMarker>
      <DeleteMarkerVersionId>string</DeleteMarkerVersionId>
      <Key>string</Key>
      <VersionId>string</VersionId>
   </Deleted> ...
   <Error>
      <Code>string</Code><Key>string</Key><Message>string</Message><VersionId>string</VersionId>
   </Error> ...
</DeleteResult>
```

- Per-key semantics: "For each key, Amazon S3 performs a delete operation and returns the result of that delete, success or failure, in the response. **If the object specified in the request isn't found, Amazon S3 confirms the deletion by returning the result as deleted.**"
- `DeleteMarker`: "Indicates whether the specified object version that was permanently deleted was (true) or was not (false) a delete marker before deletion. In a simple DELETE, this header indicates whether (true) or not (false) the current version of the object is a delete marker."
- `DeleteMarkerVersionId`: "The version ID of the delete marker created as a result of the DELETE operation. If you delete a specific object version, the value returned by this header is the version ID of the object version deleted." ([DeletedObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletedObject.html)).
- The HTTP status is 200 whenever the request itself is valid. Per-key failures appear only as `<Error>` elements.
- s3-tests: deleting 3 existing keys returns 3 `Deleted` and no `Errors`. Deleting the same 3 keys again (now missing) **also** returns 3 `Deleted` ([test_multi_object_delete](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1697)).
- Conditional per-key failures: `<Error><Code>PreconditionFailed</Code>...` ([test_delete_objects_if_match](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L19933), marked `fails_on_aws` (stale) and `conditional_write`).


## 9. CopyObject

Source: [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html), [Copying, moving, and renaming objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html), [CopyObjectResult](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObjectResult.html).

### 9.1 Size limit and request shape

- `PUT /{DestKey}` with `x-amz-copy-source`. "You create a copy of your object up to 5 GB in size in a single atomic action using this API. However, to copy an object greater than 5 GB, you must use the multipart upload Upload Part - Copy (UploadPartCopy) API." Also: "The source object can be up to 5 GB."
- The error table lists `InvalidRequest` (400) for "CopyObject request made on objects larger than 5GB in size" ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)).
- "All headers with the `x-amz-` prefix, including `x-amz-copy-source`, must be signed."

### 9.2 `x-amz-copy-source` syntax and encoding

- "specify the name of the source bucket and the key of the source object, separated by a slash (/). For example ... use `awsexamplebucket/reports/january.pdf`. **The value must be URL-encoded.**"
- Access-point form: "the URL encoding of `arn:aws:s3:us-west-2:123456789012:accesspoint/my-access-point/object/reports/january.pdf`".
- Versions: "append `?versionId=<version-id>` to the value (for example, `awsexamplebucket/reports/january.pdf?versionId=QUpfdndhfd8438MNFDN93jdnJFkdmqnh893`)". AWS examples also show a leading slash (`/awsexamplebucket/...` in [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html#AmazonS3-UploadPartCopy-request-header-CopySource)), so **accept an optional leading `/`**.
- Parse as follows: split off `?versionId=` **before** percent-decoding the bucket/key part. Decode exactly once. A key literally named `?versionId` arrives as `%3FversionId` (s3-tests special names; §4.3).
- "If the current version is a delete marker, Amazon S3 behaves as if the object was deleted." For UploadPartCopy AWS gives the codes: 404 without versionId, **400** if the versionId names a delete marker.
- Versioned destination: the new version ID is returned in `x-amz-version-id`. The source version is returned in `x-amz-copy-source-version-id`. For unversioned or suspended destinations, `x-amz-version-id` "is always null".
- s3-tests: a missing source bucket returns 404, and a missing source key returns 404 ([test_object_copy_bucket_not_found](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5721), [..._key_not_found](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5731)). Keys `foo?bar` and `bar&foo` copy with a VersionId ([test_object_copy_versioned_url_encoding](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5807)).

### 9.3 Metadata and tagging directives

- `x-amz-metadata-directive: COPY | REPLACE`. "If this header isn't specified, `COPY` is the default behavior."
  - With `REPLACE`, the request's metadata (including Content-Type and `x-amz-meta-*`) replaces the source's. With `COPY`, the source metadata including Content-Type is preserved.
  - s3-tests: [test_object_copy_retaining_metadata](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5680), [test_object_copy_replacing_metadata](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5700) (`copy`, `fails_on_dbstore`), and [test_object_copy_verify_contenttype](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5558).
  - "`x-amz-website-redirect-location` is unique to each object and is not copied when using the `x-amz-metadata-directive` header."
- `x-amz-tagging-directive: COPY | REPLACE`, default `COPY`.
- (New) `x-amz-object-annotation-directive: COPY | EXCLUDE` exists for S3 object annotations. This is out of scope for mantle.

### 9.4 Copy-source conditions

These are `x-amz-copy-source-if-match`, `-if-none-match`, `-if-modified-since` and `-if-unmodified-since`. The documented combinations (verbatim, CopyObject):

- "If both the `x-amz-copy-source-if-match` and `x-amz-copy-source-if-unmodified-since` headers are present in the request and evaluate as follows, Amazon S3 returns `200 OK` and copies the data: `x-amz-copy-source-if-match` condition evaluates to true; `x-amz-copy-source-if-unmodified-since` condition evaluates to false".
- "If both the `x-amz-copy-source-if-none-match` and `x-amz-copy-source-if-modified-since` headers are present ... [none-match] false ... [modified-since] true ... Amazon S3 returns the `412 Precondition Failed` response code".
- **Note:** for copies, a failed `if-none-match`/`if-modified-since` gives **412, not 304**. This differs from GET.
- s3-tests (`copy`):
  - Matching `CopySourceIfMatch` copies: [test_copy_object_ifmatch_good](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L14028).
  - A mismatch returns 412 `PreconditionFailed`: [..._ifmatch_failed](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L14041) (`fails_on_rgw`).
  - `CopySourceIfNoneMatch=<current etag>` returns 412 `PreconditionFailed`: [..._ifnonematch_good](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L14054) (`fails_on_rgw`).
- Destination `If-Match`/`If-None-Match` are conditional *writes* (§2.2).

### 9.5 Response and the 200-with-error behavior

- Body: `CopyObjectResult{ETag, LastModified, ChecksumType, Checksum*}`. Headers: `x-amz-version-id`, `x-amz-copy-source-version-id`, SSE headers, `x-amz-expiration`.
- Verbatim: "A `200 OK` response can contain either a success or an error. If the error occurs before the copy action starts, you receive a standard Amazon S3 error. If the error occurs during the copy operation, the error response is embedded in the `200 OK` response ... You always need to read the entire response body to check if the copy succeeds." Also: "When the request is an HTTP 1.1 request, the response is chunk encoded."
- Checksums: the destination gets a full-object checksum. It uses the source algorithm, or CRC64NVME if the source has none, unless the request sets `x-amz-checksum-algorithm` (§3.5).

### 9.6 Copying an object onto itself

- AWS docs endorse same-key copies as *the* way to change metadata: "The only way to modify object metadata is to make a copy of the object and set the metadata. To do so, in the copy operation, set the same object as the source and target." ([copy-object](https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html)).
- The rejection of a no-change self-copy is **not** documented in current AWS pages; its message text is **UNVERIFIED** in AWS docs.
- s3-tests pins it: a self-copy without changes returns **400 `InvalidRequest`** ([test_object_copy_to_itself](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5576), `copy`). A self-copy with `MetadataDirective='REPLACE'` and new metadata succeeds ([test_object_copy_to_itself_with_metadata](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5590), `copy`, `fails_on_dbstore`).
- Other s3-tests cases:
  - A zero-byte copy works: [test_object_copy_zero_size](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5514).
  - A 16 MiB single-request copy works: [test_object_copy_16m](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L5529).
  - These use the boto3 managed `client.copy`, which switches to UploadPartCopy for large objects.

## 10. Object keys, user metadata, bucket names, bucket quota

### 10.1 Object keys ([Naming Amazon S3 objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html))

- "The object key name consists of a sequence of Unicode characters encoded in UTF-8, with a maximum length of 1,024 bytes". The limit includes prefixes and delimiters. Keys are case-sensitive.
- Error `KeyTooLongError` 400, "Your key is too long." ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)). No s3-tests test covers it.
- The API minimum is 1 (`Length Constraints: Minimum length of 1` on GetObject Key).
- **Relative path segments** (AWS enforces): "Object keys that contain relative path elements (for example, `../`) are valid if, when parsed left-to-right, the cumulative count of relative path segments never exceeds the number of non-relative path elements encountered ... `videos/2014/../../video1.wmv` is valid. `videos/../../video1.wmv` isn't valid." The error code for invalid ones is **UNVERIFIED**.
- The key `"soap"` isn't supported for virtual-hosted-style requests. Path-style must be used for it.
- Guidance lists (advisory, not enforced):
  - Safe characters: `0-9 a-z A-Z ! - _ . * ' ( )`.
  - "Might require special handling": `& $ @ = ; / : + , ?`, space, 0x00–0x1F and 0x7F.
  - "Avoid": backslash, `{`, `^`, `}`, `%`, backtick, `]`, `"`, `>`, `[`, `~`, `<`, `#`, `|`, and bytes 128–255.
- XML: "carriage returns and other special characters must be replaced with their equivalent XML entity code" in XML requests (for example DeleteObjects keys with `&#13;`). Use `encoding-type=url` for keys containing characters invalid in XML 1.0 (listing docs).
- s3-tests uses keys with `?`, `&`, `%`, space and `#`. See §4.3/§9.2, and [test_bucket_list_delimiter_percentage](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L443).

### 10.2 User-defined metadata ([Working with object metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata))

- REST names "must begin with `x-amz-meta-`". "Amazon S3 stores user-defined metadata keys in lowercase." "Amazon S3 combines headers that have the same name (ignoring case) into a comma-delimited list."
- Size, verbatim: "The `PUT` request header is limited to 8 KB in size. Within the `PUT` request header, the user-defined metadata is limited to 2 KB in size. The size of user-defined metadata is measured by taking the sum of the number of bytes in the UTF-8 encoding of each key and value."
  - It is unstated whether the `x-amz-meta-` prefix counts (**UNVERIFIED**).
  - Error `MetadataTooLarge` 400, "Your metadata headers exceed the maximum allowed metadata size". No s3-tests test covers it.
  - HeadObject also notes "Request headers are limited to 8 KB in size" ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)). The error table has `RequestHeaderSectionTooLarge` (400).
- Non-ASCII: "Values of such headers are character decoded as per RFC 2047 before storing and encoded as per RFC 2047 to make them mail-safe before returning." Example: `x-amz-meta-nonascii: ÄMÄZÕÑ S3` is returned as `=?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?=`.
  - "If some metadata contains unprintable characters, it is not returned. Instead, the `x-amz-missing-meta` header is returned with a value of the number of unprintable metadata entries."
- s3-tests: empty metadata values round-trip ([L1877](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1877)). A PUT without metadata **replaces** (clears) previous metadata ([test_object_metadata_replaced_on_put](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1922)).
- System metadata:
  - User-modifiable: Cache-Control, Content-Disposition, Content-Encoding, Content-Type, x-amz-storage-class, x-amz-website-redirect-location, x-amz-tagging, and similar.
  - Not modifiable: Date, Content-Length, Last-Modified, ETag, x-amz-version-id, x-amz-delete-marker.
  - "**Last-Modified** ... For multipart uploads, the object creation date is the date of initiation of the multipart upload." ([System-defined metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#SysMetadata)).

### 10.3 General purpose bucket naming ([bucketnamingrules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html#general-purpose-bucket-names))

Verbatim rules:

- 3–63 characters.
- Only lowercase letters, numbers, `.` and `-`.
- Must begin and end with a letter or number.
- No two adjacent periods.
- Not formatted as an IP address.
- Must not start with `xn--`, `sthree-` or `amzn-s3-demo-`.
- Must not end with `-s3alias`, `--ol-s3`, `.mrap`, `--x-s3` or `--table-s3`.
- "Bucket names can only end with the suffix `-an` when you are creating buckets in your account regional namespace".
- Transfer Acceleration buckets can't contain `.`.
- Legacy: "Before March 1, 2018, buckets created in the US East (N. Virginia) Region could have names that were up to 255 characters long and included uppercase letters and underscores."

Other points:

- **New (launch date UNVERIFIED): account regional namespace.** Names of the form `<prefix>-<12-digit-account>-<region>-an`, created with `CreateBucket` plus header `x-amz-bucket-namespace: account-regional`. Only the owning account can create them.
- Virtual-host TLS caveat: "the SSL wildcard certificate matches only buckets that do not contain dots" ([VirtualHosting](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html)).
- Error: `InvalidBucketName` 400.
- s3-tests, all expecting 400 `InvalidBucketName`:
  - Short names `a` and `aa`: [L3682](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3682), [L3685](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3685) (unmarked).
  - `foo_bar`, `foo-`, `foo..bar`, `foo.-bar`, `foo-.bar`: [L3784](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3784) to [L3831](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3831). These are marked `fails_on_aws` with the comment "Should now pass on AWS even though it has 'fails_on_aws' attr."
  - The IP-address name `192.168.5.123` ([L3778](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3778)) is `fails_on_aws`.
- Bucket create conflicts ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)):
  - `BucketAlreadyExists` 409 when another account owns the name.
  - `BucketAlreadyOwnedByYou` 409 "in all AWS Regions except in ... us-east-1 ... if you re-create an existing bucket that you already own in us-east-1, Amazon S3 returns 200 OK and resets the bucket access control lists (ACLs)".
  - s3-tests accepts either success or 409 `BucketAlreadyOwnedByYou` for self-recreate ([test_bucket_create_exists](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3837)). It expects 409 `BucketAlreadyExists` for another user ([L3867](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3867)).
  - **But** it also expects `BucketAlreadyExists` when the *same* owner re-creates with an `ACL` argument ([test_bucket_recreate_overwrite_acl](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3882), [..._new_acl](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3893), `fails_on_dbstore`). That is RGW-specific; AWS docs don't support it.
- `BucketNotEmpty` 409 on DeleteBucket of a non-empty bucket ([test_bucket_delete_nonempty](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1537)).

### 10.4 Bucket quota and ListBuckets

- "By default, you can create up to 10,000 general purpose buckets per AWS account" ([BucketRestrictions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/BucketRestrictions.html)). The General Reference also lists "General purpose buckets: 10,000", adjustable ([S3 quotas](https://docs.aws.amazon.com/general/latest/gr/s3.html)).
- History: 100 by default since 2015 ([document history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html), August 4, 2015). Automatic approval of increases up to 1,000 came on September 30, 2024.
  - The raise to 10,000 default is stated on the current pages, but no document-history row gives its date (**UNVERIFIED** date).
  - The maximum raisable quota (commonly cited as 1,000,000) does not appear in the pages consulted (**UNVERIFIED**).
- "There is no max bucket size or limit to the number of objects that you can store in a bucket."
- `TooManyBuckets` 400.
- ListBuckets is paginated: `GET /?max-buckets=&continuation-token=&prefix=&bucket-region=`. `max-buckets` ranges 1–10000. "If you specify the `bucket-region`, `prefix`, or `continuation-token` query parameters without using `max-buckets` ... Amazon S3 applies a default page size of 10,000 and provides a continuation token" ([ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html)).
- The response has `Buckets/Bucket{BucketArn, BucketRegion, CreationDate, Name}`, `Owner`, `ContinuationToken` and `Prefix`. "Unpaginated `ListBuckets` requests are only supported for AWS accounts set to the default general purpose bucket quota of 10,000."


## 11. Error responses

Primary sources:

- [Error responses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html) (developer guide; formerly `/API/ErrorResponses.html`)
- [Error type](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html)
- [Error best practices](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorBestPractices.html)

### 11.1 Wire format

"When an error occurs, the header information contains the following: Content-Type: application/xml; An appropriate 3xx, 4xx, or 5xx HTTP status code" ([REST error responses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#RESTErrorResponses)):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>
```

- Elements: `Code` ("meant to be read and understood by programs"), `Message` (English, for humans), `Resource` ("The bucket or object that is involved in the error"), `RequestId`.
- AWS's own full examples also include **`<HostId>`**, which mirrors the `x-amz-id-2` header ([CompleteMultipartUpload examples](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#API_CompleteMultipartUpload_Examples)).
- "Many error responses contain additional structured data ... if you send a Content-MD5 header with a REST PUT request that doesn't match the digest calculated on the server, you receive a `BadDigest` error. The error response also includes as detail elements the digest that the server calculated, and the digest that you told the server to expect."
- The extra-element names for BadDigest and for SignatureDoesNotMatch (commonly `StringToSign`, `CanonicalRequest`) are **UNVERIFIED** in AWS docs.
- Clients should key off `Code`: "use the Amazon S3 error code instead of the HTTP status code as it contains the most information about the error" ([best practices](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorBestPractices.html#UsingErrorsIsolate)).
- **HEAD responses carry no body**, so only the status is visible ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html), [HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html)).
- **304** carries no body. s3-tests sees `Message == 'Not Modified'` (botocore's synthesized reason) and requires the `ETag` header (§2.4).
- Common response headers on every response include `x-amz-request-id`, `x-amz-id-2`, `Date` and `Server: AmazonS3` ([Common response headers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTCommonResponseHeaders.html)).
- **200-with-error**: CompleteMultipartUpload and CopyObject may send `200 OK` and then an `<Error>` body (§4.4, §9.5). ListObjectsV2 and ListObjectVersions also warn: "A `200 OK` response can contain valid or invalid XML."
- Retry guidance: "If Amazon S3 returns an InternalError response, retry the request." SlowDown: "Reducing your request rate will decrease or eliminate errors of this type."

### 11.2 Error codes

The table is quoted from the developer-guide [List of error codes](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList) unless noted.

| Code | HTTP | AWS description (verbatim or near-verbatim) |
|---|---|---|
| `AccessDenied` | 403 | "Access Denied" |
| `AuthorizationHeaderMalformed` | 400 | "The authorization header that you provided is not valid." |
| `AuthorizationQueryParametersError` | 400 | "The authorization query parameters that you provided are not valid." |
| `BadDigest` | 400 | "The Content-MD5 or checksum value that you specified did not match what the server received." |
| `BucketAlreadyExists` | 409 | "The requested bucket name is not available. The bucket namespace is shared by all users of the system." |
| `BucketAlreadyOwnedByYou` | 409 (all Regions except us-east-1) | "...Amazon S3 returns this error in all AWS Regions except in ... (us-east-1). For legacy compatibility, if you re-create an existing bucket that you already own in us-east-1, Amazon S3 returns 200 OK and resets the bucket access control lists (ACLs)." |
| `BucketNotEmpty` | 409 | "The bucket that you tried to delete is not empty." |
| `ConditionalRequestConflict` | 409 | "A conflicting operation occurred. If using PutObject you can retry the request. If using multipart upload you should initiate another CreateMultipartUpload request and re-upload each part." |
| `EntityTooSmall` | 400 | "Your proposed upload is smaller than the minimum allowed object size." |
| `EntityTooLarge` | 400 (upload) / **405** (download) | Two entries: "Your proposed upload exceeds the maximum allowed object size" (400) and "Your proposed download exceeds the maximum allowed size" (405 Method Not Allowed) |
| `ExpiredToken` | 400 | "The provided token has expired." |
| `IncompleteBody` | 400 | "You did not provide the number of bytes specified by the Content-Length HTTP header." |
| `InternalError` | 500 | "An internal error occurred. Try again." |
| `InvalidAccessKeyId` | 403 | "The AWS access key ID that you provided does not exist in our records." |
| `InvalidArgument` | 400 | Reasons include "The specified argument was not valid", "The request was missing a required header", "The specified argument was incomplete or in the wrong format", "The specified argument must have a length greater than or equal to 3" |
| `InvalidBucketName` | 400 | "The specified bucket is not valid." |
| `InvalidDigest` | 400 | "The Content-MD5 or checksum value that you specified is not valid." |
| `InvalidObjectState` | 403 | "The operation is not valid for the current state of the object." |
| `InvalidPart` | 400 | "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag." |
| `InvalidPartOrder` | 400 | "The list of parts was not in ascending order. The parts list must be specified in order by part number." |
| `InvalidRange` | **416** | "The requested range cannot be satisfied." |
| `InvalidRequest` | 400 | Many reasons, including "The request is using the wrong signature version", "Conflicting values provided in HTTP headers and query parameters", "CopyObject request made on objects larger than 5GB in size", and unpaginated ListBuckets above a 10,000 quota |
| `InvalidSignature` | 400 | "The request signature that the server calculated does not match the signature that you provided." (distinct from `SignatureDoesNotMatch`; usage context not described) |
| `InvalidToken` | 400 | "The provided token is malformed or otherwise not valid." |
| `InvalidURI` | 400 | "The specified URI couldn't be parsed." |
| `KeyTooLongError` | 400 | "Your key is too long." |
| `MalformedXML` | 400 | "The XML that you provided was not well formed or did not validate against our published schema." |
| `MaxMessageLengthExceeded` | 400 | "Your request was too large." |
| `MetadataTooLarge` | 400 | "Your metadata headers exceed the maximum allowed metadata size." |
| `MethodNotAllowed` | 405 | "The specified method is not allowed against this resource." |
| `MissingContentLength` | 411 | "You must provide the Content-Length HTTP header." |
| `MissingRequestBodyError` | 400 | "You sent an empty XML document as a request." |
| `MissingSecurityHeader` | 400 | "Your request is missing a required header." |
| `NoSuchBucket` | 404 | "The specified bucket does not exist." |
| `NoSuchKey` | 404 | "The specified key does not exist." |
| `NoSuchUpload` | 404 | "The specified multipart upload does not exist. The upload ID might not be valid, or the multipart upload might have been aborted or completed." |
| `NoSuchVersion` | 404 | "The version ID specified in the request does not match an existing version." |
| `NotImplemented` | 501 | "A header that you provided implies functionality that is not implemented." |
| `NotModified` | 304 | "The resource was not changed." |
| `OperationAborted` | 409 | "A conflicting conditional operation is currently in progress against this resource. Try again." |
| `PermanentRedirect` | 301 | "The bucket that you are attempting to access must be addressed using the specified endpoint." |
| `PreconditionFailed` | 412 | "At least one of the preconditions that you specified did not hold." |
| `RequestHeaderSectionTooLarge` | 400 | "The request header and query parameters used to make the request exceed the maximum allowed size." |
| `RequestTimeout` | 400 | "Your socket connection to the server was not read from or written to within the timeout period." |
| `RequestTimeTooSkewed` | 403 | "The difference between the request time and the server's time is too large." |
| `ServiceUnavailable` | 503 | "Service is unable to handle request." |
| `SignatureDoesNotMatch` | 403 | "The request signature that the server calculated does not match the signature that you provided. Check your AWS secret access key and signing method." |
| `SlowDown` | 503 "Slow Down" | "Please reduce your request rate." |
| `TemporaryRedirect` | 307 | "You are being redirected to the bucket while the Domain Name System (DNS) server is being updated." |
| `TooManyBuckets` | 400 | "You have attempted to create more buckets than are allowed for an account." |
| `UnexpectedContent` | 400 | "This request contains unsupported content." |
| `UnsupportedSignature` | 400 | "The provided request is signed with an unsupported STS Token version or the signature version is not supported." |

**Discrepancies between the two AWS lists.** Compare the developer-guide list with the [API `Error` type](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Error.html) list:

- `AllAccessDisabled` is 400 with a copy-pasted description in the developer guide, versus 403 "All access to this Amazon S3 resource has been disabled" in the Error type.
- `RequestIsNotMultiPartContent` is 412 in the developer guide versus 400 in the Error type.
- `TemporaryRedirect` is "307 Temporary Redirect" versus "307 Moved Temporarily". The status code is the same.

**Codes not documented** in either list, though widely seen from S3 (**UNVERIFIED**):

- `XAmzContentSHA256Mismatch` (payload hash mismatch)
- The code for >1000 keys in DeleteObjects (s3-tests only asserts 400)
- The code for an invalid part number in UploadPart

**s3-tests expectations for common error paths** (status, code):

- GET missing key: 404 `NoSuchKey`.
- CompleteMultipartUpload with an unknown upload ID: 404 `NoSuchUpload`.
- Delete a non-empty bucket: 409 `BucketNotEmpty` ([L1537](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L1537)).
- Bad bucket names: 400 `InvalidBucketName`.
- Unsatisfiable range: 416 `InvalidRange`.
- Precondition failures: 412 `PreconditionFailed`.
- Anonymous write to a private bucket: 403 `AccessDenied` ([test_object_anon_put](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3572)).
- Missing Content-Length: 411 `MissingContentLength` (`fails_on_rgw`).
- `max-keys=blah`: 400 `InvalidArgument`.
- Empty CompleteMultipartUpload body: 400 `MalformedXML`.


## 12. Range GET and partNumber GET

Primary sources:

- [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#AmazonS3-GetObject-request-header-Range)
- [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#AmazonS3-HeadObject-request-header-Range)
- [Downloading objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html#download-objects-parts)
- [RFC 9110 §14 Range](https://www.rfc-editor.org/rfc/rfc9110.html#name-range), referenced by AWS

### 12.1 Range

- "Downloads the specified byte range of an object. For more information about the HTTP Range header, see RFC 9110 ... **Amazon S3 doesn't support retrieving multiple ranges of data per `GET` request.**"
- The response for a satisfiable range is `206 Partial Content` with `Content-Range: bytes first-last/complete-length`. The AWS sample shows `Content-Range: bytes 0-9/443` and `Accept-Ranges: bytes`.
- s3-tests (all `fails_on_dbstore` except where noted):
  - `bytes=4-7` on 11 bytes gives 206, `content-range: bytes 4-7/11` ([test_ranged_request_response_code](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7564)).
  - `bytes=4-` gives `bytes 4-10/11` ([L7597](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7597)).
  - Suffix `bytes=-7` gives `bytes 4-10/11` ([L7612](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7612)).
  - An 8 MiB object with `bytes=3145728-5242880` gives `bytes 3145728-5242880/8388608` ([L7582](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7582)).
- **Unsatisfiable:** `InvalidRange` 416, "The requested range cannot be satisfied." ([ErrorResponses](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html#ErrorCodeList)).
  - s3-tests: `bytes=40-50` on an 11-byte object gives 416 `InvalidRange` ([test_ranged_request_invalid_range](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7626), unmarked).
  - `bytes=40-50` on a **0-byte** object gives 416 `InvalidRange` ([test_ranged_request_empty_object](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L7640), unmarked).
  - `Content-Range: bytes */<size>` on 416 is RFC 9110 §15.5.17 behavior. It is **not** stated in AWS docs or asserted by s3-tests (**UNVERIFIED** for AWS). Sending it is harmless.
- HEAD with Range: "If the Range is satisfiable, only the `ContentLength` is affected in the response. If the Range is not satisfiable, S3 returns a `416 - Requested Range Not Satisfiable` error." ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#AmazonS3-HeadObject-request-header-Range)).
- Multi-range (`bytes=0-1,3-4`) and syntactically invalid Range headers: AWS docs say only "doesn't support". Whether S3 ignores the header (200 full body) or errors is **UNVERIFIED**. For UploadPartCopy's `x-amz-copy-source-range`, s3-tests expects 400 `InvalidArgument` for malformed or multi ranges (§4.3).
- Single GETs are capped at 5 TB. Beyond that the response is `405 Method Not Allowed` ([Downloading objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html)).
- `GetObject` range responses and checksums: see §3.5.

### 12.2 partNumber

- "Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified." HEAD: "Useful querying about the size of the part and the number of parts in this object."
- `x-amz-mp-parts-count`: "The count of parts this object has. This value is only returned if you specify `partNumber` in your request and the object was uploaded as a multipart upload."
- s3-tests [test_multipart_get_part](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6626) (`fails_on_dbstore`):
  - `PartNumber=1` **before** completion gives 404 `NoSuchKey`.
  - After completion, HEAD and GET with `PartNumber=n` return `PartsCount == 4`, `ETag ==` the whole-object multipart ETag, and `ContentLength ==` that part's size.
  - An out-of-range `PartNumber=5` gives **400 `InvalidPart`**.
  - The upload in that test re-sent part 3, so re-uploaded parts serve the latest bytes.
- Non-multipart objects ([test_non_multipart_get_part](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L6765)): `PartNumber=1` returns the whole object with the normal ETag. `PartNumber=2` gives 400 `InvalidPart`.
- The partNumber response status code is not stated in AWS docs. HTTP semantics imply 206 with `Content-Range`, but that is **UNVERIFIED** for AWS, and s3-tests does not assert the status.
- Combining `partNumber` with `Range`: AWS docs don't say (**UNVERIFIED**). A 400 is the conservative choice.

## 13. Consistency model

Source: [Amazon S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel). Strong consistency launched on **December 1, 2020** per the [document history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html).

> Amazon S3 provides strong read-after-write consistency for PUT and DELETE requests of objects in your Amazon S3 bucket in all AWS Regions. This behavior applies to both writes to new objects as well as PUT requests that overwrite existing objects and DELETE requests. In addition, read operations on Amazon S3 Select, Amazon S3 access controls lists (ACLs), Amazon S3 Object Tags, and object metadata (for example, the HEAD object) are strongly consistent.

> Updates to a single key are atomic. For example, if you make a PUT request to an existing key from one thread and perform a GET request on the same key from a second thread concurrently, you will get either the old data or the new data, but never partial or corrupt data.

> ... Any read (GET or LIST request) that is initiated following the receipt of a successful PUT response will return the data written by the PUT request.

The documented examples:

- "A process writes a new object to Amazon S3 and immediately lists keys within its bucket. The new object appears in the list."
- A replace followed by an immediate read returns the new data.
- A delete followed by an immediate read returns nothing.
- A delete followed by an immediate list does not show the object.

Concurrent writers and bucket configuration:

- Concurrent writers: "Amazon S3 does not support object locking for concurrent writers. If two PUT requests are simultaneously made to the same key, the request with the latest timestamp wins." "Amazon S3 internally uses last-writer-wins semantics to determine which write takes precedence." ([Concurrent applications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ApplicationConcurrency)).
  - Conditional writes (§2) are now the documented exception: they give compare-and-swap on a single key.
- "Updates are key-based. There is no way to make atomic updates across keys."
- **Bucket configurations are eventually consistent:** "If you delete a bucket and immediately list all buckets, the deleted bucket might still appear in the list." Versioning enablement may take up to 15 minutes to propagate.

**What this means for mantle's distributed design** (our analysis):

- A successful PUT/DELETE/CompleteMultipartUpload response must be issued only after the new current version is visible to every subsequent GET, HEAD, *and LIST* from any node.
- The listing index must therefore be updated synchronously and linearizably with the object's version pointer. An async index is not enough.
- Single-key atomicity means readers never see partial objects. Publish by atomically swapping a version pointer after the data is durable.


## 14. Virtual-hosted-style vs path-style addressing

Source: [Virtual hosting of general purpose buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html).

- Formats:
  - Virtual-hosted: `https://bucket-name.s3.region-code.amazonaws.com/key-name`.
  - Path-style: `https://s3.region-code.amazonaws.com/bucket-name/key-name`.
- "Currently, Amazon S3 supports both virtual-hosted–style and path-style URL access in all AWS Regions. However, path-style URLs will be discontinued in the future." The deprecation was delayed: "Update (September 23, 2020) ... we have decided to delay the deprecation of path-style URLs."
- **Host-header interpretation** (verbatim rules, [#VirtualHostingSpecifyBucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#VirtualHostingSpecifyBucket)):
  1. "If the `Host` header is omitted or its value is `s3.region-code.amazonaws.com`, the bucket for the request will be the first slash-delimited component of the Request-URI, and the key for the request will be the rest of the Request-URI ... Omitting the `Host` header is valid only for HTTP 1.0 requests."
  2. "Otherwise, if the value of the `Host` header ends in `.s3.region-code.amazonaws.com`, the bucket name is the leading component of the `Host` header's value up to `.s3.region-code.amazonaws.com`. The key for the request is the Request-URI."
  3. "Otherwise, the bucket for the request is the lowercase value of the `Host` header, and the key for the request is the Request-URI." This is the CNAME case: a CNAME `images.example.com → images.example.com.s3.us-west-2.amazonaws.com` serves bucket `images.example.com`.
- TLS: "the SSL wildcard certificate matches only buckets that do not contain dots". Dotted bucket names therefore need path-style (or custom certificate handling) over HTTPS.
- Legacy endpoints, per "Backward compatibility":
  - The dash form `s3-region`.
  - The global endpoint `bucket.s3.amazonaws.com`, which defaults to us-east-1.
  - For buckets in other Regions: a "307 Temporary Redirect" for Regions launched before March 20, 2019, and "HTTP 400 Bad Request" for later Regions.
  - Path-style against the wrong Regional endpoint gives "HTTP 301 Permanent Redirect".
- Key `"soap"` isn't supported with virtual-hosted-style ([object-keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)).
- Region plumbing:
  - CreateBucket: "If you don't specify a Region, the bucket is created in the US East (N. Virginia) Region (us-east-1) by default." Requests to `s3.amazonaws.com` must be signed with `us-east-1` ([CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)).
  - GetBucketLocation: "Buckets in Region `us-east-1` have a LocationConstraint of `null`" ([GetBucketLocation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html)).
  - HeadBucket returns `x-amz-bucket-region`, even on its 301/403 examples ([HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html)).
- SigV4 interplay: the CanonicalURI includes `/bucket` only for path-style (§1.2.2). The `host` header is always signed, so a proxy that rewrites `Host` breaks signatures.
- Directory buckets are virtual-hosted only (various API pages). This is irrelevant to mantle.
- s3-tests does not force an addressing style. It builds `endpoint_url = "%s://%s:%d" % (proto, host, port)` and boto3 decides ([configure()](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py#L177)).
  - The `fails_with_subdomain` marker exists but is unused in the test files at this commit.
  - Which style botocore picks for a custom `endpoint_url` (for example `localhost` vs an IP) is **UNVERIFIED** here, since it is not documented in the allowed sources.
  - mantle should accept **both** styles, with a configurable base domain for virtual-host parsing, plus the CNAME rule.


## 15. Running ceph/s3-tests against a custom endpoint

Sources:

- [README.rst](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/README.rst)
- [s3tests.conf.SAMPLE](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests.conf.SAMPLE)
- [tox.ini](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/tox.ini)
- [pytest.ini](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/pytest.ini)
- [requirements.txt](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/requirements.txt)
- [s3tests/functional/\_\_init\_\_.py](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py)

All of these are at commit `5522d1c351f75bc00ae0f64f742f3f095f5939d9` (master, 2026-05-27).

### 15.1 Layout and invocation

- The suite is pytest-based. `tox.ini`: `envlist = py`; `deps = -rrequirements.txt`; `passenv = S3TEST_CONF S3_USE_SIGV4`; `commands = pytest {posargs}`.
- `requirements.txt` is **unpinned**: `boto3 >=1.0.0`, `botocore`, `PyYAML`, `munch`, `gevent`, `isodate`, `requests`, `pytz`, `httplib2`, `lxml`, `pytest`, `tox`. The newest botocore is installed, **with default CRC32 request checksums and aws-chunked trailers** (§3.6).
- README invocations:

```
S3TEST_CONF=your.conf tox                                          # everything
S3TEST_CONF=your.conf tox -- s3tests/functional/test_s3.py          # one file
S3TEST_CONF=your.conf tox -- s3tests/functional/test_s3.py::test_bucket_list_empty
S3TEST_CONF=aws.conf  tox -- -m 'not fails_on_aws'                  # marker filter
```

- Test files: `test_s3.py` (760 tests), `test_headers.py` (48), `test_iam.py`, `test_sts.py`, `test_sns.py`, `test_s3select.py`, `test_s3control.py`. The README text about Boto2 and `s3test_boto3` directories is stale; only `s3tests/functional/` exists.
- **Per-test fixture cost:** an `autouse` fixture runs `setup()`/`teardown()` around *every* test. It calls `nuke_prefixed_buckets` for the **main, alt and tenant** clients ([L308](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py#L308), [L346](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py#L346)). That cleanup uses:
  - `ListBuckets`, filtered client-side by the prefix.
  - `ListObjectVersions` with `MaxKeys=128`, paginating via `KeyMarker=NextKeyMarker` and `VersionIdMarker=NextVersionIdMarker`. **If `IsTruncated` is true, both Next markers must be present** or botocore rejects the `None` value ([list_versions](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py#L84)).
  - `DeleteObjects` with `Quiet: True` and `x-amz-bypass-governance-retention`.
  - `GetObjectRetention` (only on AccessDenied errors).
  - `DeleteBucket`.
  - **These operations, for three users, must work before any test can pass.**

### 15.2 Configuration file (`S3TEST_CONF`)

`configure()` ([L177](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py#L177)) enforces the following:

- `[DEFAULT]` must exist. It provides `host` and `port` (required) and `is_secure` (required). `ssl_verify` is optional and defaults to False. The endpoint is `http(s)://host:port`.
- `[s3 main]`, `[s3 alt]` and `[s3 tenant]` sections are **required** (explicit `RuntimeError` checks).
  - Each needs `access_key`, `secret_key`, `display_name`, `user_id` and `email`.
  - `[s3 tenant]` additionally needs `tenant`.
  - `account_id` is optional.
  - `[s3 main]` optional keys: `api_name` (LocationConstraint for bucket-location tests), `kms_keyid`/`kms_keyid2`, `storage_classes`, `lc_debug_interval`, `rgw_restore_*`.
- **`[iam]`, `[iam root]` and `[iam alt root]` are effectively required too.** They are read with `cfg.get()` without fallback, so a missing section raises `NoSectionError` during configuration. Include them with dummy credentials even if no IAM tests run.
- `[fixtures]` is optional: `bucket prefix` (default `test-{random}-`, padded to at most 30 chars), `iam name prefix`, `iam path prefix`.
- `[s3 cloud]` and `[webidentity]` are only for cloud-transition and STS web-identity tests.
- **Identity expectations:**
  - `display_name` and `user_id` must match what mantle returns in `Owner`/ACL grants. Listing tests compare `Owner.ID`/`DisplayName` against `GetObjectAcl` (§6.3).
  - `alt` must be a distinct account, for `BucketAlreadyExists` and ACL/permission tests.
  - `tenant` is an RGW multi-tenancy concept (`tenant$user`). 11 test names contain `tenant`; exclude them with `-k 'not tenant'` if mantle has no tenancy.

Minimal `mantle.conf` sketch (values are placeholders):

```ini
[DEFAULT]
host = 127.0.0.1
port = 9000
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = mantle-{random}-

[s3 main]
display_name = main
user_id = main-user-id
email = main@example.com
access_key = MAINACCESSKEY0000000
secret_key = mainsecretkeymainsecretkeymainsecretkey0
api_name = us-east-1

[s3 alt]
display_name = alt
user_id = alt-user-id
email = alt@example.com
access_key = ALTACCESSKEY00000000
secret_key = altsecretkeyaltsecretkeyaltsecretkey0000

[s3 tenant]
display_name = tenant
user_id = tenant-user-id
email = tenant@example.com
access_key = TENANTACCESSKEY00000
secret_key = tenantsecretkeytenantsecretkeytenant0000
tenant = t1

[iam]
email = iam@example.com
user_id = iam-user-id
access_key = IAMACCESSKEY00000000
secret_key = iamsecretkeyiamsecretkeyiamsecretkey0000
display_name = iam

[iam root]
access_key = IAMROOTKEY0000000000
secret_key = iamrootsecretiamrootsecretiamrootsecret0
user_id = iam-root
email = iamroot@example.com

[iam alt root]
access_key = IAMALTROOTKEY0000000
secret_key = iamaltrootsecretiamaltrootsecretiamalt00
user_id = iam-alt-root
email = iamaltroot@example.com
```

`api_name` sets the LocationConstraint used by `test_bucket_get_location`. An empty value skips that test ([L3852](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L3852)).

### 15.3 Markers and deselection

`pytest.ini` registers these markers:

- **Feature and area markers:** `abac_test`, `appendobject`, `auth_aws2`, `auth_aws4`, `auth_common`, `bucket_policy`, `bucket_encryption`, `bucket_logging`, `bucket_logging_cleanup`, `conditional_write`, `checksum`, `cloud_transition`, `cloud_restore`, `target_by_bucket`, `copy`, `encryption`, `lifecycle`, `lifecycle_expiration`, `lifecycle_transition`, `list_objects_v2`, `object_lock`, `object_ownership`, `s3control`, `s3select`, `s3website`, `s3website_routing_rules`, `s3website_redirect_location`, `sns`, `sse_s3`, `storage_class`, `tagging`, `versioning`, `delete_marker`.
- **IAM, STS and token markers:** `group`, `group_policy`, `iam_account`, `iam_cross_account`, `iam_role`, `iam_tenant`, `iam_user`, `role_policy`, `session_policy`, `test_of_sts`, `token_claims_trust_policy_test`, `token_principal_tag_role_policy_test`, `token_request_tag_trust_policy_test`, `token_resource_tags_test`, `token_role_tags_test`, `token_tag_keys_test`, `user_policy`, `webidentity_test`.
- **Backend-specific failure markers:** `fails_on_aws`, `fails_on_dbstore`, `fails_on_dho`, `fails_on_mod_proxy_fcgi`, `fails_on_rgw`, `fails_on_s3`, `fails_with_subdomain`, `fails_without_logging_rollover`.

Marker usage in `test_s3.py` at this commit, counted by us:

| Marker | Count |
|---|---|
| `fails_on_aws` | 200 |
| `fails_on_dbstore` | 237 |
| `fails_on_rgw` | 16 |
| `bucket_logging` | 118 |
| `encryption` | 67 |
| `list_objects_v2` | 52 |
| `lifecycle` | 48 |
| `copy` | 33 |
| `bucket_policy` | 31 |
| `conditional_write` | 25 |
| `tagging` | 25 |
| `sse_s3` | 21 |
| `bucket_encryption` | 12 |
| `checksum` | 11 |
| `object_ownership` | 8 |
| `delete_marker` | 4 |

`test_headers.py` has `auth_common` 27, `auth_aws2` 21 and `fails_on_rgw` 20.

**Several registered markers are unused** in `test_s3.py`: `object_lock`, `versioning`, `storage_class`, `s3select`, `s3website`. For example, the 39 `test_object_lock_*` tests carry no `object_lock` marker. Area exclusion therefore needs **`-k` name filters** as well as `-m`.

Recommended staged invocation for mantle (our recommendation):

```
# Stage 1: core object/bucket semantics, AWS-faithful behavior only
S3TEST_CONF=mantle.conf tox -- s3tests/functional/test_s3.py s3tests/functional/test_headers.py \
  -m 'not fails_on_aws and not auth_aws2 and not bucket_logging and not bucket_logging_cleanup
      and not fails_without_logging_rollover and not lifecycle and not lifecycle_expiration
      and not lifecycle_transition and not cloud_transition and not cloud_restore and not target_by_bucket
      and not encryption and not sse_s3 and not bucket_encryption and not bucket_policy
      and not object_ownership and not tagging and not iam_account and not iam_user and not appendobject' \
  -k 'not object_lock and not post_object and not cors and not acl and not policy and not website
      and not torrent and not tenant and not restore and not storage_class and not public_access
      and not block_public and not usage'
```

- Rationale for `not fails_on_aws`: those 200 tests encode RGW behavior that AWS does not have. Examples: RFC-style `If-None-Match: <etag>` on PUT; the `?usage` and `unordered` listing extensions; and the stale conditional-delete tests (§2.3).
- **Keep** `fails_on_rgw` and `fails_on_dbstore` tests. They generally encode AWS behavior that RGW or its DB backend misses.
- Then re-enable areas as features land: `checksum`, `conditional_write`, `copy` and `list_objects_v2` are already in stage 1; add `tagging`, `bucket_policy`, `encryption`, `lifecycle` and so on later.
- Tests whose AWS-vs-s3-tests expectations conflict, to review individually:
  - `test_delete_marker_nonversioned`, which expects `x-amz-delete-marker: false` (§7.4).
  - `test_bucket_recreate_*_acl`, which expects `BucketAlreadyExists` for the same owner (§10.3).
  - Idempotent re-complete of a multipart upload (§4.4), which s3-tests requires and SDK retries rely on.
  - `test_bucket_list_marker_none`, which expects `<Marker>` even when not sent (§6.3).
- `S3_USE_SIGV4` is passed through by tox but is not read by the Python code at this commit. The default clients already use `signature_version='s3v4'` ([get_client](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py#L429)). `get_v2_client` uses SigV2 (`signature_version='s3'`) and is only used by `auth_aws2` tests and a few `_v2` presign tests.
- Anonymous tests use `signature_version=UNSIGNED` clients ([get_unauthenticated_client](https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/__init__.py#L594)). mantle needs ACL-based public-read and public-read-write for those. Without ACLs, deselect with `-k 'not anon'` (or accept failures).
- Region: s3-tests does not pass `region_name` to S3 clients. The signing Region therefore comes from the runner's AWS config or environment (`AWS_DEFAULT_REGION`); botocore's fallback is **UNVERIFIED** here. Set `AWS_DEFAULT_REGION` explicitly in CI, and have mantle accept that Region in the credential scope.

## 16. Implications for mantle

This section is our analysis, built on the facts cited above. Client-specific internals of aws-cli, boto3, aws-sdk-rust, rclone and s3fs are outside the allowed sources except where AWS docs state them, for example the SDK checksum defaults. Claims that depend on undocumented client internals are marked **UNVERIFIED**.

### 16.1 Prioritized API surface

**P0: nothing works without these.** They are needed by every listed client and by the s3-tests per-test fixture.

1. **SigV4 header auth and presigned-URL auth.** This covers §1.
   - Single-encoded, non-normalized path.
   - Sorted, encoded query.
   - Trimmed and collapsed headers.
   - Tolerance for `, ` in `Authorization`.
   - `x-amz-date` over `Date`.
   - ±15 min skew returning `RequestTimeTooSkewed` 403.
   - `X-Amz-Expires` from 1 to 604800.
   - Every `x-amz-content-sha256` token.
   - Use the AWS test vectors in §1.5 as unit tests.
2. **aws-chunked decoding in all three modes, with trailers.** This covers §1.8–§1.9 and §3.6.
   - The modes are `STREAMING-UNSIGNED-PAYLOAD-TRAILER`, `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`.
   - Current default SDK and CLI clients send the unsigned-trailer form with a CRC trailer. s3-tests' own comment shows botocore doing this for `put_object`.
   - This is the #1 compatibility trap for S3 clones built from pre-2024 knowledge.
   - SigV4a (`ECDSA`) streaming can wait. SDKs switch to it only for Multi-Region Access Points.
3. **Checksums CRC32, CRC32C, CRC64NVME, SHA1 and SHA256.** This covers §3.
   - Accept them in headers or trailers, validate them, store them, and return them only when `x-amz-checksum-mode: ENABLED` is sent.
   - Support full-object CRC combining for multipart, and composite `-N` values.
   - The default algorithms are **CRC32** for boto3, aws-sdk-rust and most SDKs, and **CRC64NVME for AWS CLI v2**.
   - MD5, XXHASH* and SHA512 can come later. Reject them explicitly rather than silently ignoring them.
4. **Bucket CRUD and discovery.**
   - `CreateBucket`: LocationConstraint, `BucketAlreadyExists` / `BucketAlreadyOwnedByYou`, naming rules.
   - `HeadBucket` with `x-amz-bucket-region`, `DeleteBucket` with `BucketNotEmpty`, `GetBucketLocation` (null for us-east-1).
   - `ListBuckets`, accepting the `max-buckets`, `continuation-token`, `prefix` and `bucket-region` parameters.
5. **Object CRUD.**
   - `PutObject`: Content-MD5 or checksum validation, metadata, and quoted MD5 ETag.
   - `GetObject`: Range, partNumber, conditionals, `response-*` overrides.
   - `HeadObject` with no error bodies.
   - `DeleteObject`: 204. For a missing key, AWS docs imply 204 without stating it normatively; the [delete-objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-objects.html) sample prints "was deleted or does not exist" (**UNVERIFIED** as a rule).
   - `DeleteObjects`: accept Content-MD5 **or** `x-amz-checksum-*`; report missing keys as `Deleted`; Quiet mode; at most 1000 keys.
   - `CopyObject`: `x-amz-copy-source` decoded once, `?versionId`, directives, `x-amz-copy-source-if-*`, the self-copy rule.
6. **Listing.** Implement `ListObjectsV2` and `ListObjects` V1. Both are needed; V1 keeps `Marker`/`NextMarker` semantics.
   - Byte-order sorting, `delimiter` (s3-tests only uses single characters; matching it as a string is a safe superset), CommonPrefixes counted in `KeyCount`/`MaxKeys`, and `encoding-type=url`.
   - Strongly consistent with writes (§13).
   - **UNVERIFIED but high-impact:** botocore is widely reported to add `encoding-type=url` to list calls automatically and to URL-decode the returned keys. If mantle ignores `encoding-type`, keys containing `%` or `+` may be corrupted client-side. Honor it exactly.
7. **Multipart.**
   - `CreateMultipartUpload`, `UploadPart` (overwrite on re-upload), `UploadPartCopy` (`x-amz-copy-source-range`), `CompleteMultipartUpload` (validation errors, idempotent retry, MD5-of-MD5s ETag), `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`.
   - AWS docs note that the CLI and SDK transfer managers switch to multipart for objects "larger than approximately 8 MB" ([CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)). `aws s3 cp` of any sizable file needs this path.
8. **`ListObjectVersions` returning `null` versions, even before versioning ships.** The s3-tests autouse fixture paginates it (with `NextKeyMarker` and `NextVersionIdMarker`) and deletes by `VersionId` before and after *every* test, for three users (§15.1).
9. **Error XML** with the exact codes and statuses in §11, `x-amz-request-id` and `x-amz-id-2` headers, and no bodies on HEAD or 304.

**P1: needed for real workloads and most of s3-tests stage 1.**

- **Versioning:** `Put`/`GetBucketVersioning`, delete markers (404 or 405 plus `x-amz-delete-marker: true`), `null` version handling in suspended mode, and version ordering by MPU initiation time (§7, §4.4).
- **Conditional writes and deletes:** `If-None-Match: *` and `If-Match` on Put, Complete and Copy, evaluated atomically at commit, with 412, 409 `ConditionalRequestConflict` and 404 `NoSuchKey`. Also `If-Match` on `DeleteObject`/`DeleteObjects` (§2).
- **Minimal ACL surface:** `GetBucketAcl`/`GetObjectAcl` returning the owner, and acceptance of canned ACL headers. s3-tests listing tests compare `Owner` with `GetObjectAcl`, and the anonymous tests need `public-read`/`public-read-write`.
- **`GetObjectAttributes`** (`Checksum`, `ObjectParts`, `ObjectSize`, `ETag`).
- **`Expect: 100-continue` handling in the HTTP layer.** It is documented as a common request header, and AWS recommends it for PUTs ([RESTRedirect](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTRedirect.html)).

**P2 and later.** Object tagging, bucket policy (including the `s3:if-match`/`s3:if-none-match` keys), CORS (browser clients), lifecycle (`AbortIncompleteMultipartUpload` first), SSE headers, Object Lock, browser POST uploads, SigV2 (only if legacy clients matter), and SigV4a.

**Client coverage summary.**

- aws-cli v2 needs P0 including CRC64NVME, plus multipart above about 8 MB.
- boto3 and aws-sdk-rust need P0 with CRC32 trailers.
- rclone and s3fs are served by P0 V1 and V2 listing, both addressing styles, and multipart.
- Their exact default flags (path-style, listing version, chunked signing) are **UNVERIFIED** here. Support every variant rather than guess.

### 16.2 Subtle traps checklist

1. **Default integrity protections changed the wire format (Dec 2024+).** Expect bodies that are:
   - `Content-Encoding: aws-chunked` (sometimes absent),
   - `x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER`,
   - `x-amz-trailer: x-amz-checksum-crc32` (or `-crc64nvme`),
   - with `x-amz-decoded-content-length`, possibly wrapped in HTTP `Transfer-Encoding: chunked`.

   Trailer lines may end in `\r\n` or `\n\r\n`. Strip `aws-chunked` from the stored `Content-Encoding`. AWS requires chunks of at least 8 KiB except the last; mantle may accept smaller ones.
2. **Content-MD5 is no longer what SDKs send** for DeleteObjects, PutBucketVersioning and other "MD5 required" operations. They send `x-amz-sdk-checksum-algorithm` plus `x-amz-checksum-crc32`. Accept any valid checksum in place of MD5 (§3.2).
3. **Response checksums are validated by clients.** Return `x-amz-checksum-*` only if the value is exactly right for the bytes served:
   - the whole object,
   - the part checksum for `partNumber` GETs,
   - omitted for arbitrary ranges.

   Composite values carry `-N`; FULL_OBJECT values do not. Include `x-amz-checksum-type`.
4. **Canonical URI:** never normalize `//`, `.` or `..`. Encode once. Rebuild from the decoded key using `UriEncode` with `/` kept. A literal `+` in paths and queries is a classic mismatch source (§1.2.3).
5. **Signed header set:** trust the client's `SignedHeaders`. AWS's own docs disagree about which headers must be signed (`content-md5` vs `content-type`), and the trailer example is internally inconsistent (§1.2.4, §1.9).
6. **ETags are quoted everywhere.** Compare `If-Match`/`If-None-Match` and CompleteMultipartUpload part ETags with quotes stripped. The multipart ETag is `md5(concat(binary part md5s))-N`, verified. A single-request CopyObject of a multipart source yields a single-part ETag.
7. **CompleteMultipartUpload:**
   - Reject non-ascending part lists (`InvalidPartOrder`), unknown or mismatched parts (`InvalidPart`) and small non-last parts (`EntityTooSmall`).
   - An empty body gives `MalformedXML`.
   - It must be **idempotent on retry** (s3-tests).
   - Prefer real HTTP error statuses over AWS's 200-then-`<Error>` streaming. SDKs handle both.
   - Object `Last-Modified` is the **initiation** time. In versioned buckets, the newest *initiated* upload wins.
8. **Conditional writes are commit-time compare-and-swap on the current version.** In-progress MPUs are ignored until Complete. A delete marker counts as "absent". `If-Match` on an absent key gives 404 `NoSuchKey`. A concurrent-delete race gives 409 `ConditionalRequestConflict`, and MPU callers must restart.
9. **Listing details that clients and s3-tests check:**
   - `KeyCount` includes CommonPrefixes.
   - `MaxKeys=0` gives an empty, non-truncated result.
   - `<Marker>` is always echoed in V1.
   - `NextMarker` appears only with a delimiter and may be a CommonPrefix.
   - Continuation after a CommonPrefix must skip its whole subtree.
   - An empty `delimiter` means none, and the `Delimiter` element is omitted.
   - Truncated `ListObjectVersions` must include both Next markers.
10. **Delete semantics:**
    - `DeleteObject` on a missing key returns 204 (implied, **UNVERIFIED**; see P0 item 5).
    - `DeleteObjects` reports missing keys as `Deleted`.
    - A simple delete on a versioned bucket stacks delete markers.
    - `versionId=null` addresses the null version.
    - GET of a delete-marker version returns 405, not 404.
11. **Existence leak rule:** a missing key returns 404 `NoSuchKey` only if the caller has `s3:ListBucket`, and 403 otherwise (GetObject/HeadObject docs). This matters once policies exist.
12. **Metadata:**
    - `x-amz-meta-*` keys are lowercased.
    - The limit is 2 KB, counted as UTF-8 bytes of keys plus values, returning `MetadataTooLarge`.
    - The request header total is 8 KB.
    - Non-ASCII values use RFC 2047 encoding on output.
    - A PUT without metadata clears the old metadata.
13. **Consistency:** read-after-write for GET, HEAD **and LIST** is required. Only bucket configuration may lag (§13).
14. **Limits to encode as constants:**
    - Key at most 1024 bytes.
    - Part size 5 MiB to 5 GiB (the last part is unbounded below).
    - Parts 1..10000; object at most 48.8 TiB.
    - Single PUT or Copy at most 5 GB; single GET at most 5 TB.
    - DeleteObjects at most 1000 keys.
    - List pages at most 1000.
    - Presign at most 604800 s.
    - Clock skew 15 min.
15. **s3-tests harness prerequisites:**
    - Three users (main, alt, tenant) plus `[iam]`, `[iam root]` and `[iam alt root]` config sections.
    - Unpinned boto3, so the newest checksum behavior applies.
    - `-m` markers alone do not exclude object-lock, ACL, CORS or POST tests; use `-k` too (§15.3).
    - Treat `fails_on_aws` tests as RGW-specific and exclude them. Keep `fails_on_rgw`.
