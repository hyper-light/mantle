# 16 — CORS: ground truth for a bucket's cross-origin rules

Research note for mantle's CORS support in the S3 protocol layer (`crates/s3`). It covers:

- S3's bucket CORS operations and the `CORSConfiguration` document;
- how S3 evaluates a preflight `OPTIONS` request and an actual request against the rules;
- the CORS protocol as the WHATWG Fetch standard defines it, which is what browsers enforce;
- botocore's model and serializer, and ceph s3-tests' CORS tests;
- S3's observed answers, from recordings made against it.

Compiled 2026-09-29 with curl from public pages. The AWS pages are the `.md` renditions as
served that day. Labels:

- **Secondary** marks behaviour observed in S3 by others and recorded in their tests or
  issues, not stated by AWS.
- **DERIVED** marks an inference of ours.
- **UNVERIFIED** marks what no fetched source states.

Pins: botocore develop at `358f8eec8c76201bb1a7a35644abcbc9036de7ed` (release 1.43.104, as
note 13); ceph s3-tests at `5522d1c351f75bc00ae0f64f742f3f095f5939d9` (as note 05);
LocalStack main at `8b9a79f05846835cf4dff63ab7eefdde9df83783`, its final state before the
repository was archived; WHATWG Fetch at commit `357bd98924d94b81fbe8608192a2ee1f123b82f4`
(21 September 2026).

---

## 1. The operations

### 1.1 PutBucketCors ([API_PutBucketCors])

- `PUT /?cors`. "Sets the `cors` configuration for your bucket. If the configuration exists,
  Amazon S3 replaces it." It answers 200 "with an empty HTTP body".
- "The `cors` subresource is an XML document in which you configure rules that identify
  origins and the HTTP methods that can be executed on your bucket. The document is limited
  to 64 KB in size."
- Evaluation, verbatim: "When Amazon S3 receives a cross-origin request (or a pre-flight
  OPTIONS request) against a bucket, it evaluates the `cors` configuration on the bucket and
  uses the first `CORSRule` rule that matches the incoming browser request to enable a
  cross-origin request. For a rule to match, the following conditions must be met:
  - The request's `Origin` header must match `AllowedOrigin` elements.
  - The request method (for example, GET, PUT, HEAD, and so on) or the
    `Access-Control-Request-Method` header in case of a pre-flight `OPTIONS` request must be
    one of the `AllowedMethod` elements.
  - Every header specified in the `Access-Control-Request-Headers` request header of a
    pre-flight request must match an `AllowedHeader` element."
- Content-MD5: "This header must be used as a message integrity check to verify that the
  request body was not corrupted in transit." The current page gives it no "Required" line;
  the 2019 page gave "Required: Yes" ([wb-put]). botocore marks the operation
  `requestChecksumRequired` and sends `x-amz-checksum-crc32` with
  `x-amz-sdk-checksum-algorithm: CRC32` and no Content-MD5 (§5). With neither header, S3
  answered 400 `InvalidRequest` "Missing required header for this request: Content-MD5" in
  2013 and 2021 (secondary: [aws-cli-229], [amazonka-610]). LocalStack's recordings of
  2025, made with botocore 1.37 and 1.39, which send CRC32 alone, succeeded (secondary).

### 1.2 GetBucketCors and DeleteBucketCors ([API_GetBucketCors], [API_DeleteBucketCors])

- GetBucketCors, `GET /?cors`, answers the document. Its Response Syntax root has no
  namespace. Its samples are not well-formed: `<MaxAgeSeconds>3000</MaxAgeSec>` breaks XML's
  element type match, and its Content-MD5 is neither the body's MD5 nor canonical base64.
- DeleteBucketCors, `DELETE /?cors`: "Deletes the `cors` configuration information set for
  the bucket." It answers 204. It needs `s3:PutBucketCORS`; there is no delete permission.
- The error table: `NoSuchCORSConfiguration`, "The specified bucket does not have a CORS
  configuration.", 404 ([ErrorResponses]). The wire message is "The CORS configuration does
  not exist", with a `BucketName` element before `RequestId` (secondary: [vgw-1842],
  captured 2026-02-11; LocalStack `test_get_cors`).
- DeleteBucketCors on a bucket without a configuration answers 204 (secondary: LocalStack
  `test_delete_cors`).

### 1.3 The document ([API_CORSConfiguration], [API_CORSRule])

```
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <CORSRule>
      <AllowedHeader>string</AllowedHeader> ...
      <AllowedMethod>string</AllowedMethod> ...
      <AllowedOrigin>string</AllowedOrigin> ...
      <ExposeHeader>string</ExposeHeader> ...
      <ID>string</ID>
      <MaxAgeSeconds>integer</MaxAgeSeconds>
   </CORSRule> ...
</CORSConfiguration>
```

- `CORSRule`: "You can add up to 100 rules to the configuration." Required.
- `AllowedMethod`, required: "Valid values are `GET`, `PUT`, `HEAD`, `POST`, and `DELETE`."
- `AllowedOrigin`, required: "One or more origins you want customers to be able to access
  the bucket from."
- `AllowedHeader`: "Headers that are specified in the `Access-Control-Request-Headers`
  header. These headers are allowed in a preflight OPTIONS request. In response to any
  preflight OPTIONS request, Amazon S3 returns any requested headers that are allowed."
- `ExposeHeader`: "One or more headers in the response that you want customers to be able to
  access from their applications (for example, from a JavaScript `XMLHttpRequest` object)."
- `ID`: "The value cannot be longer than 255 characters."
- `MaxAgeSeconds`: "The time in seconds that your browser is to cache the preflight response
  for the specified resource." An integer.
- The 2019 page adds: "Each CORSRule must identify at least one origin and one method", "A
  CORSRule can have at most one MaxAgeSeconds element", and of both `AllowedOrigin` and
  `AllowedHeader`, "This can contain at most one * wild character" ([wb-put]).
- The samples write the root without a namespace, `AllowedOrigin` first.

## 2. How S3 evaluates a request (user guide)

- "When Amazon S3 receives a preflight request from a browser, it evaluates the CORS
  configuration for the bucket and uses the first `CORSRule` rule that matches the incoming
  browser request to enable a cross-origin request" ([ug-cors]).
- Origins: "The origin string can contain only one `*` wildcard character, such as
  `http://*.example.com`. You can optionally specify `*` as the origin to enable all the
  origins to send cross-origin requests" ([ug-elements]).
- Headers: "Each header name in the `Access-Control-Request-Headers` header must match a
  corresponding entry in the element. Amazon S3 will send only the allowed headers in a
  response that were requested." "Each AllowedHeaders string in your configuration can
  contain at most one * wildcard character. For example, `<AllowedHeader>x-amz-*</AllowedHeader>`
  will enable all Amazon-specific headers."
- MaxAgeSeconds: "the time in seconds that your browser can cache the response for a
  preflight request as identified by the resource, the HTTP method, and the origin."
- Scheme, host and port: "The scheme, host, and port values in the Origin request header
  must match the **AllowedOrigins** elements in the CORSRule. For example, suppose you set the
  CORSRule to allow the origin http://www.example.com. When you do this,
  https://www.example.com and http://www.example.com:80 origins in your request don't match
  the allowed origin in your configuration" ([repost], AWS-authored).
- "The ACLs and policies continue to apply when you enable CORS on your bucket."
- No AWS page says whether matching is case-sensitive.
- "When sending a preflight request, if any of the CORS request headers are not allowed,
  none of the response CORS headers are returned" ([ug-testing]).
- "If you make a cross-origin request to an Amazon S3 bucket that you haven't configured with
  a CORS rule, then the web server doesn't return the CORS header." "If the header [Origin]
  is missing, Amazon S3 doesn't treat the request as a cross-origin request, and doesn't send
  CORS response headers in the response" ([repost]).
- Troubleshooting: a request to a bucket without CORS gets "403 Forbidden CORS Response: CORS
  is not enabled for this bucket", and one no rule matches "403 Forbidden CORS Response: This
  CORS request is not allowed." ([ug-troubleshooting]). The page says a disallowed method or
  header in "a CORS request" is a 403; S3 refuses only preflights (§7).

**The response headers**, from [ug-testing]'s preflight sample (a rule for
`http://www.example1.com`, methods GET, PUT, POST, DELETE, `AllowedHeaders ["Authorization"]`,
`ExposeHeaders ["x-amz-meta-custom-header"]`):

```
HTTP/1.1 200 OK
Access-Control-Allow-Origin: http://www.example1.com
Access-Control-Allow-Methods: GET, PUT, POST, DELETE
Access-Control-Allow-Headers: Authorization
Access-Control-Expose-Headers: x-amz-meta-custom-header
Access-Control-Allow-Credentials: true
Vary: Origin, Access-Control-Request-Headers, Access-Control-Request-Method
Server: AmazonS3
Content-Length: 0
```

No prose describes `Vary` or `Access-Control-Allow-Credentials`.

## 3. The OPTIONS object operation ([OPTIONS])

The API reference page is gone; its text is now the developer guide's "Appendix: OPTIONS
object", identical to the 2019 page ([wb-options]).

- "A browser can send this preflight request to Amazon S3 to determine if it can send an
  actual request with the specific origin, HTTP method, and headers." "If `cors` is not
  enabled on the bucket, then Amazon S3 returns a `403 Forbidden` response."
- Request headers: `Origin`, required; `Access-Control-Request-Method`, required, "what HTTP
  method will be used in the actual request"; `Access-Control-Request-Headers`, optional, "A
  comma-delimited list of HTTP headers that will be sent in the actual request."
- Response headers:
  - `Access-Control-Allow-Origin`: "The origin you sent in your request. If the origin in
    your request is not allowed, Amazon S3 will not include this header in the response."
  - `Access-Control-Max-Age`: "How long, in seconds, the results of the preflight request
    can be cached."
  - `Access-Control-Allow-Methods`: "The HTTP method that was sent in the original request."
    Every recorded response lists the matched rule's methods instead (§7).
  - `Access-Control-Allow-Headers`: "If any of the requested headers is not allowed, Amazon
    S3 will not include that header in the response, nor will the response contain any of
    the headers with the `Access-Control` prefix."
  - `Access-Control-Expose-Headers`: "This header provides the JavaScript client with access
    to these headers in the response to the actual request."
- "This operation does not introduce any specific request parameters, but it may contain any
  request parameters that are required by the actual request." No page documents the errors
  for a missing header.

## 4. The CORS protocol: WHATWG Fetch ([Fetch])

**Requests** (§3.3.2).

- "A CORS request is an HTTP request that includes an `Origin` header. It cannot be reliably
  identified as participating in the CORS protocol as the `Origin` header is also included
  for all requests whose method is neither `GET` nor `HEAD`."
- "A CORS-preflight request is a CORS request that checks to see if the CORS protocol is
  understood. It uses `OPTIONS` as method and includes the following header:
  `Access-Control-Request-Method` Indicates which method a future CORS request to the same
  resource might use. A CORS-preflight request can also include the following header:
  `Access-Control-Request-Headers` Indicates which headers a future CORS request to the same
  resource might use."
- `Origin` (§3.2): `serialized-origin = serialized-scheme "://" serialized-host [ ":"
  serialized-port ]`, `origin-or-null = serialized-origin / %s"null" ; case-sensitive`.
  "scheme and domains serializations are all lower case ASCII, without percent encoding."

**Responses** (§3.3.3–§3.3.4).

```
Access-Control-Request-Method    = method
Access-Control-Request-Headers   = 1#field-name
wildcard                         = "*"
Access-Control-Allow-Origin      = origin-or-null / wildcard
Access-Control-Allow-Credentials = %s"true" ; case-sensitive
Access-Control-Expose-Headers    = #field-name
Access-Control-Max-Age           = delta-seconds
Access-Control-Allow-Methods     = #method
Access-Control-Allow-Headers     = #field-name
```

- `Access-Control-Allow-Origin`: "Indicates whether the response can be shared, via
  returning the literal value of the `Origin` request header (which can be `null`) or `*` in
  a response."
- `Access-Control-Allow-Credentials`: "For a CORS-preflight request, request's credentials
  mode is always "`same-origin`", i.e., it excludes credentials, but for any subsequent CORS
  requests it might not be. Support therefore needs to be indicated as part of the HTTP
  response to the CORS-preflight request as well."
- `Access-Control-Max-Age`: "the number of seconds (5 by default)".
- "For `Access-Control-Expose-Headers`, `Access-Control-Allow-Methods`, and
  `Access-Control-Allow-Headers` response headers, the value `*` counts as a wildcard for
  requests without credentials."
- "A successful HTTP response to a CORS-preflight request is similar, except it is
  restricted to an ok status, e.g., 200 or 204." "If server developers wish to denote this
  explicitly, the 403 status can be used, coupled with omitting the relevant headers."
- With credentials: "If credentials mode is "include", then Access-Control-Allow-Origin
  cannot be *."; "true is (byte) case-sensitive"; "A serialized origin has no trailing
  slash." (§3.3.5's table).

**The CORS check** (§4.10), verbatim:

1. "Let origin be the result of getting `Access-Control-Allow-Origin` from response's header
   list."
2. "If origin is null, then return failure."
3. "If request's credentials mode is not "`include`" and origin is `*`, then return success."
4. "If the result of byte-serializing a request origin with request is not origin, then
   return failure."
5. "If request's credentials mode is not "`include`", then return success."
6. "Let credentials be the result of getting `Access-Control-Allow-Credentials` from
   response's header list."
7. "If credentials is `true`, then return success."
8. "Return failure."

**How a browser writes a preflight** (§4.8). It appends `Access-Control-Request-Method` with
the request's method, and, if any are unsafe, `Access-Control-Request-Headers` with "the
items in headers separated from each other by `,`" — "This intentionally does not use
combine, as 0x20 following 0x2C is not the way this was implemented". The names are "the
result of convert header names to a sorted-lowercase set". So the header holds lowercase,
byte-sorted, distinct names joined by `,` without spaces (DERIVED). It then checks the
response: each unsafe name must be "a byte-case-insensitive match for an item in
headerNames", and `Authorization` is never covered by `*`. "If max-age is failure or null,
then set max-age to 5."

- "A CORS-safelisted method is a method that is `GET`, `HEAD`, or `POST`." Methods matching
  `DELETE`, `GET`, `HEAD`, `OPTIONS`, `POST` or `PUT` case-insensitively are uppercased
  (§2.2).
- `Origin`, `Access-Control-Request-Method` and `Access-Control-Request-Headers` are
  forbidden request-headers: scripts cannot set them.
- An actual request: "If request's response tainting is "cors" and a CORS check for request
  and response returns failure, then return a network error."

**Caching** ("CORS protocol and HTTP caches"). "If CORS protocol requirements are more
complicated than setting `Access-Control-Allow-Origin` to `*` or a static origin, `Vary` is
to be used." A cache that stored a response without CORS headers may otherwise serve it to a
later CORS request. RFC 9110 §12.5.5: caches "MUST NOT use this response to satisfy a later
request unless the later request has the same values for the listed header fields"
([RFC9110]).

**Lists** (RFC 9110 §5.6.1): elements are "separated by a single comma (",") and optional
whitespace", and "A recipient MUST parse and ignore a reasonable number of empty list
elements".

## 5. botocore ([botocore])

- `PutBucketCors`: `PUT /{Bucket}?cors`, `"httpChecksum":{"requestAlgorithmMember":
  "ChecksumAlgorithm","requestChecksumRequired":true}`. `GetBucketCors`: `GET`.
  `DeleteBucketCors`: `DELETE`, `"responseCode":204`.
- `PutBucketCorsRequest` requires `Bucket` and `CORSConfiguration`, the payload, in S3's
  namespace. `CORSConfiguration` requires `CORSRules`, flattened as `CORSRule`. `CORSRule`
  requires `AllowedMethods` and `AllowedOrigins`; its members, in model order, are `ID`,
  `AllowedHeaders`, `AllowedMethods`, `AllowedOrigins`, `ExposeHeaders`, `MaxAgeSeconds`,
  each list flattened. `MaxAgeSeconds` is an integer. The model has no length, count or
  enumeration constraint on any CORS field.
- The serializer writes a structure's members in the caller's dict order, and no XML
  declaration. An empty `CORSRules` writes `<CORSConfiguration xmlns="..." />` (DERIVED, by
  running the same ElementTree calls).
- `GetBucketCorsOutput` reads `CORSRule` children under any root, namespace stripped.

## 6. ceph s3-tests ([s3-tests])

`_cors_request_and_check` sends an unsigned request and asserts the status and the exact
`access-control-allow-origin` and `access-control-allow-methods`, `None` for absent. No CORS
test is `fails_on_aws`; the four SigV2 presigned variants are `fails_on_rgw`.

- `test_set_cors`: a rule of methods `GET, PUT` and origins `*.get, *.put`. GetBucketCors
  before a PUT is 404; after it, both lists come back in order; after DeleteBucketCors, 404.
- `test_cors_origin_response`: rules `[GET] *suffix`, `[GET] start*end`, `[GET] prefix*`,
  `[PUT] *.put`, on a public-read bucket; `obj_url` names a key that does not exist.
  - GET on the bucket: Origin `foo.suffix`, `startend`, `start1end`, `start12end`, `prefix`
    and `prefix.suffix` answer 200 with the origin echoed and `GET`; `foo.bar`,
    `foo.suffix.get`, `0start12end` and `bla.prefix` answer 200 without CORS headers.
  - GET on `obj_url` with Origin `foo.suffix`: 404, with `foo.suffix` and `GET`.
  - PUT on `obj_url` with Origin `foo.suffix` and `Access-Control-Request-Method: GET`: 403
    with `foo.suffix` and `GET`; with that header `PUT` or `DELETE`, or absent: 403 without
    CORS headers; with Origin `foo.put`: 403 with `foo.put` and `PUT`.
  - OPTIONS on the bucket with no headers, or with only an Origin: 400.
  - OPTIONS with `Access-Control-Request-Method: GET`: 200 with the origin and `GET` for the
    origins that matched GET above; 403 for `foo.bar`, `foo.suffix.get`, `0start12end`,
    `bla.prefix` and `foo.put`. With `PUT`, `foo.put` is 200 with `PUT`.
  - So `*` matches the empty string, a pattern is anchored at both ends, and an origin need
    not be a URL (DERIVED).
- `test_cors_origin_wildcard`: a rule `[GET] *`; GET with Origin `example.origin` answers
  `Access-Control-Allow-Origin: *`.
- `test_cors_header_option`: a rule `[GET] *` with `ExposeHeaders x-amz-meta-header1` and no
  `AllowedHeaders`; OPTIONS with `Access-Control-Request-Headers: x-amz-meta-header2` is 403.
- The presigned tests: OPTIONS on a presigned GET or PUT URL with no headers is 400; after a
  rule `[method] example`, OPTIONS with `Origin: example` is 200. `test_object_raw_get_x_amz_expires_not_expired`
  also sends OPTIONS without headers to a presigned URL and expects 400. So OPTIONS is
  answered before, and without, a query-string signature check (DERIVED).
- Three tests sleep 3 s after PutBucketCors.

## 7. S3's observed answers (secondary)

**LocalStack's AWS-validated tests** (`tests/aws/services/s3/test_s3_cors.py` with its
`.snapshot.json`; each marked `@markers.aws.validated`, recorded against S3 in us-east-1 on
the dates given). The snapshots sort keys, so they do not keep element order.

- `test_cors_http_options_no_config` (2023-07-31), no configuration:
  - OPTIONS with no headers: 400 `BadRequest` "Insufficient information. Origin request
    header needed."
  - OPTIONS with `Origin` and `Access-Control-Request-Method: PUT`: 403 `AccessForbidden`
    "CORSResponse: CORS is not enabled for this bucket.", with `Method` `PUT` and
    `ResourceType` `BUCKET`, although the URL names an object.
  - OPTIONS with `Origin` alone: the same, with `Method` `OPTIONS`.
- `test_cors_http_get_no_config`: GET with or without `Origin` is 200 without CORS headers.
- `test_cors_http_options_non_existent_bucket`: no headers, 400 as above; `Origin` alone, 403
  `AccessForbidden` "CORSResponse: Bucket not found".
- `test_cors_match_origins` (2023-07-31), a rule `{origins [https://localhost:4200], methods
  [GET, PUT], MaxAgeSeconds 3000, AllowedHeaders [*]}`:
  - OPTIONS without `Origin`: 400 `BadRequest`.
  - OPTIONS from that origin with `Access-Control-Request-Method: PUT`: 200, empty body, with
    `Access-Control-Allow-Credentials: true`, `Access-Control-Allow-Methods: GET, PUT`,
    `Access-Control-Allow-Origin: https://localhost:4200`, `Access-Control-Max-Age: 3000`
    and `Vary: Origin, Access-Control-Request-Headers, Access-Control-Request-Method`; no
    `Access-Control-Allow-Headers`, since none was requested.
  - GET from that origin: 200 with the same five headers.
  - From `http://localhost:4200`: OPTIONS is 403 `AccessForbidden` "CORSResponse: This CORS
    request is not allowed. This is usually because the evalution of Origin, request method
    / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by
    the resource's CORS spec.", with `Method` `PUT` and `ResourceType` `OBJECT`; GET is 200
    with no CORS headers and no `Vary`.
  - With origins `[*]`, from `http://random:1234`: OPTIONS and GET answer
    `Access-Control-Allow-Origin: *`, the methods, max age and `Vary`, and no
    `Access-Control-Allow-Credentials`.
- `test_cors_options_match_partial_origin` (2024-02-29): origins `[http://*.origin.com]`
  match `http://test.origin.com`, echoed. `test_cors_options_fails_partial_origin`
  (2024-03-01): `http://test.origin.com/`, with a trailing slash, is 403.
- `test_cors_match_methods` (2025-03-17), methods `[GET]`:
  - OPTIONS with `GET`: 200 with `Access-Control-Allow-Methods: GET`.
  - GET with `Origin` and `Access-Control-Request-Method: PUT`: 200 without CORS headers or
    `Vary`. The request-method header decides the match on an actual request.
  - PUT with `Origin`, PUT not allowed: 200, the object stored, without CORS headers.
- `test_cors_match_headers` (2025-07-07):
  - `AllowedHeaders [*]`: requested `x-amz-request-payer` is answered
    `Access-Control-Allow-Headers: x-amz-request-payer`; two requested names are answered in
    the order requested, joined by `, `.
  - `AllowedHeaders [x-amz-expected-bucket-owner, x-amz-server-side-encryption-customer-algorithm,
    x-AMZ-server-SIDE-encryption]`: GetBucketCors gives them back as stored, cases kept.
    Requested `x-amz-request-payer` is 403. Requested `x-AMZ-expected-BUCKET-owner,
    x-amz-server-side-encryption` is 200 with `Access-Control-Allow-Headers:
    x-amz-expected-bucket-owner, x-amz-server-side-encryption`: names match without regard to
    case and come back lowercase. Without the space after the comma, the answer is the same.
  - GET with `Origin` and `Access-Control-Request-Headers: x-amz-request-payer`, not allowed:
    200 without CORS headers. GET with `Origin` and the real header `x-amz-request-payer`: 200
    with CORS headers. A real request's headers are not checked.
- `test_cors_expose_headers` (2023-07-31): origins `[*]`, `ExposeHeaders [x-amz-id-2,
  x-amz-request-id, x-amz-request-payer]`; a preflight from `localhost:4566`, which is not a
  serialized origin, is 200 with `Access-Control-Expose-Headers: x-amz-id-2,
  x-amz-request-id, x-amz-request-payer`.
- `test_get_cors`: without a configuration, 404 `NoSuchCORSConfiguration` "The CORS
  configuration does not exist" with a `BucketName`; with one, only the members set come
  back.
- `test_put_cors`: origins come back in the order sent and unnormalized, among them
  `http://test.com:80`.
- `test_put_cors_default_values`: a rule of `[*]` and `[GET]` answers a preflight with
  `Access-Control-Allow-Methods`, `Access-Control-Allow-Origin: *` and `Vary` only; with
  `Access-Control-Request-Headers`, it is 403. A rule without `AllowedHeader` allows no
  requested header.
- `test_put_cors_invalid_rules`: a method `MYMETHOD` is 400 `InvalidRequest` "Found
  unsupported HTTP method in CORS config. Unsupported method is MYMETHOD"; no rules is 400
  `MalformedXML` "The XML you provided was not well-formed or did not validate against our
  published schema".
- `test_put_cors_empty_origin`: `AllowedOrigin` `""` is accepted and given back.
- `test_delete_cors`: 204 with or without a configuration.

**Issue trackers.**

- [vgw-1863] (2026-02-17): an empty `<CORSRule/>`, or one without `AllowedOrigin` or
  `AllowedMethod`, is `MalformedXML`; an empty `<AllowedMethod/>` is `InvalidRequest` "Found
  unsupported HTTP method in CORS config. Unsupported method is ".
- [vgw-1870] (2026-02-18): `<AllowedOrigin>*example*.com</AllowedOrigin>` is 400
  `InvalidRequest` "AllowedOrigin "*example*.com" can not have more than one wildcard."
- [vgw-1893] (2026-02-24): a GET from an allowed origin carried
  `Access-Control-Allow-Origin`, `Access-Control-Allow-Methods: GET, PUT`,
  `Access-Control-Allow-Credentials: true` and `Vary`, and no `Access-Control-Expose-Headers`:
  "S3 exposes this header [ETag] if asked to, but not by default."
- [vgw-2083] (2026-04-22): the error document's element order is `Code`, `Message`, `Method`,
  `ResourceType`, `RequestId`, `HostId`, as the capture in [repost] (2021) also shows.
- [aws-sdk-js-3667] (2021): `AllowedMethod` `OPTIONS` is "Found unsupported HTTP method in
  CORS config. Unsupported method is OPTIONS".
- [amplify-3267] (2023): `Method` in the 403 is the requested method.
- [xinu-8] (2013): a PUT refused 403 `SignatureDoesNotMatch` still carried the CORS headers.
- [boto3-2972] (2021): `ExposeHeaders ["GET", "PUT"]` was accepted: values are not checked as
  header names.
- [librelio-53] (2014): an `ExposeHeader` of `*` was refused: "We currently do not support
  wildcard for ExposeHeader." Old and without its code.

## 8. Discrepancies

1. `Access-Control-Allow-Methods`: the OPTIONS page says the method requested; every
   recorded answer lists the matched rule's methods, joined by `, `.
2. `Access-Control-Allow-Headers`: [ug-testing]'s 2024 sample answers `Authorization` as
   requested; LocalStack's 2025 recording answers lowercase names.
3. The 403 messages: the user guide writes "CORS Response: ..."; the wire says
   `CORSResponse: ...` with code `AccessForbidden`, which S3's error table does not list.
4. `NoSuchCORSConfiguration`: the table's message differs from the wire's.
5. The user guide says a disallowed method or header in a CORS request is refused; S3
   refuses only preflights, and serves an actual request without CORS headers.
6. [ug-troubleshooting] says `*` in `AllowedMethods` matches every method; the data type
   allows five values, and S3 refuses others.
7. `*` matching nothing: s3-tests expects it (not `fails_on_aws`); LocalStack's own
   implementation, which is not what its recordings test, requires a character.
8. The OPTIONS sample carries an `Etag`; no recorded preflight response does.

## 9. Gaps

- UNVERIFIED: case sensitivity of origin matching; whether `*` spans `.` or `/`; a rule
  listing `*` beside specific origins; the errors for more than 100 rules, more than 64 KB,
  an ID over 255 characters, a negative `MaxAgeSeconds`, duplicate IDs, two `*` in an
  `AllowedHeader`, or a lowercase method; the raw XML of a GetBucketCors answer; OPTIONS with
  `Origin` and no `Access-Control-Request-Method` on a configured bucket (RGW answers 400);
  whether CORS headers are sent on an actual request's error today (a 2013 capture and
  s3-tests say yes); how soon a new configuration takes effect.
- Observed, and documented nowhere: `ResourceType` is `BUCKET` for a bucket URL and for a
  bucket with no configuration or none at all, and `OBJECT` when rules are evaluated for a
  key; `Method` is the requested method, or `OPTIONS` when none was requested; a preflight
  does not look up the key.

## Sources

- [API_PutBucketCors] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html
- [API_GetBucketCors] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html
- [API_DeleteBucketCors] https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html
- [API_CORSConfiguration] https://docs.aws.amazon.com/AmazonS3/latest/API/API_CORSConfiguration.html
- [API_CORSRule] https://docs.aws.amazon.com/AmazonS3/latest/API/API_CORSRule.html
- [OPTIONS] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTOPTIONSobject.html
- [ErrorResponses] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html
- [ug-cors] https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html
- [ug-elements] https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html
- [ug-testing] https://docs.aws.amazon.com/AmazonS3/latest/userguide/testing-cors.html
- [ug-troubleshooting] https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors-troubleshooting.html
- [repost] https://repost.aws/knowledge-center/s3-configure-cors (AWS-authored, updated 2025-10-27)
- [wb-put] https://web.archive.org/web/20190825093525/https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTcors.html
- [wb-options] https://web.archive.org/web/20191117072104/https://docs.aws.amazon.com/AmazonS3/latest/API/RESTOPTIONSobject.html
- [Fetch] https://fetch.spec.whatwg.org/commit-snapshots/357bd98924d94b81fbe8608192a2ee1f123b82f4/
- [RFC9110] https://www.rfc-editor.org/rfc/rfc9110
- [botocore] https://github.com/boto/botocore/tree/358f8eec8c76201bb1a7a35644abcbc9036de7ed (`service-2.json`, `httpchecksum.py`, `serialize.py`, `handlers.py`)
- [s3-tests] https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py (L6881–L7145, L3495–L3512)
- [localstack] https://github.com/localstack/localstack/tree/8b9a79f05846835cf4dff63ab7eefdde9df83783/tests/aws/services/s3 (`test_s3_cors.py`, `test_s3_cors.snapshot.json`, `test_s3_cors.validation.json`)
- [vgw-1842], [vgw-1863], [vgw-1870], [vgw-1893], [vgw-2083] https://github.com/versity/versitygw/issues/ (by number)
- [aws-cli-229] https://github.com/aws/aws-cli/issues/229
- [amazonka-610] https://github.com/brendanhay/amazonka/issues/610
- [aws-sdk-js-3667] https://github.com/aws/aws-sdk-js/issues/3667
- [amplify-3267] https://github.com/aws-amplify/amplify-hosting/issues/3267
- [xinu-8] https://github.com/xinu-os/boot.xinu-os.org/issues/8
- [boto3-2972] https://github.com/boto/boto3/issues/2972
- [librelio-53] https://github.com/libreliodev/javascript/issues/53
