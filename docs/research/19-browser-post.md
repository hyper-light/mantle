# 19 — Browser uploads: ground truth for POST Object with a POST policy

Research note for mantle's POST Object in the S3 protocol layer (`crates/s3`): an upload from
an HTML form, `multipart/form-data`, authorized by a signed POST policy rather than a signed
request. It covers:

- where AWS documents the operation now, its form fields and its responses;
- SigV4 signing of a POST policy, with two exact test vectors;
- the POST policy language: expiration, conditions, and which fields they cover;
- `multipart/form-data` as RFC 7578 and RFC 2046 define it, and what clients send;
- botocore's and aws-sdk-js-v3's presigned POSTs, and ceph s3-tests' tests;
- S3's observed answers, from recordings made against it.

Everything here was fetched with curl on 2026-09-29.

**Pins**
- AWS pages: the `.md` renditions as served 2026-09-29.
- botocore develop: `358f8eec8c76201bb1a7a35644abcbc9036de7ed` (release 1.43.104, committed 2026-09-28T18:17:09Z). This is still the head, and the same pin as notes 13/16/17.
- ceph s3-tests master: `5522d1c351f75bc00ae0f64f742f3f095f5939d9`. Still the head; same pin as note 05.
- LocalStack main: `8b9a79f05846835cf4dff63ab7eefdde9df83783`, the archived final state and still the head. Its POST recordings were all validated against S3 on 2026-02-21.
- aws-sdk-js-v3 main: `c935fdd2bc9b36c689ef8e17b75c57bf9a780990`.
- WHATWG HTML main: `92f248013013096b5a780afffd8377fb9a6eba87` ("Last Updated 29 September 2026").
- react-native-aws3 master: `a02d5390cd65933fdeabc00be78ba8dac9c48730`.
- `requests` 2.34.2 and `urllib3` 2.8.0 from PyPI, used offline to reproduce the bytes clients send.
- RFC errata: the RFC Editor's `errata.json`, same day.

**Labels** (as in notes 16/17)
- **Secondary**: behaviour observed in S3 by others and recorded in their tests or issues.
- **Third-party**: another implementation's claim about S3.
- **DERIVED**: an inference of mine.
- **UNVERIFIED**: no fetched source states it.

---

## 1. Where AWS documents it now

- **API reference.** There is no PostObject page. `API/RESTObjectPOST.html`, `API/API_PostObject.html` and every `API/sigv4-*POST*` URL answer 302 to the API index.
- **botocore's model** has 116 S3 operations and no `PostObject`.
- **Developer guide.** The pages live under `https://docs.aws.amazon.com/AmazonS3/latest/developerguide/`:
  - SigV4: `RESTObjectPOST` ("POST Object"), `sigv4-UsingHTTPPOST`, `sigv4-authentication-HTTPPOST`, `sigv4-HTTPPOSTForms`, `sigv4-HTTPPOSTConstructPolicy` ("POST Policy"), `sigv4-post-example`, `browser-based-uploads-aws-amplify`.
  - SigV2: `UsingHTTPPOST`, `HTTPPOSTForms` (whose "Policy construction" section holds the old HTTPPOSTConstructPolicy text), `HTTPPOSTExamples`, `HTTPPOSTFlash`.
- **User guide.** The pages are gone:
  - `userguide/UsingHTTPPOST.html`, `userguide/HTTPPOSTForms.html`, `userguide/HTTPPOSTConstructPolicy.html` and `userguide/HTTPPOSTExamples.html` answer 302 to the guide root.
  - The `dev/...` URLs 301 into those dead user-guide URLs.
  - `developerguide/HTTPPOSTConstructPolicy.md` answers 404.
- **Wayback.**
  - RESTObjectPOST in 2015 (`web.archive.org/web/20150608131326`) is materially identical to today's page. It has no 201 sample body either.
  - The 2016 "Additional Considerations" page (`sigv4-post-additional-considerations`, `20160325090336`) became HTTPPOSTFlash.
  - The POST Policy page's "Matching Content-Types in a Comma-Separated List" row is absent on 2019-07-16 and present by 2020-05-18 (CDX snapshots).

## 2. The operation ([RESTObjectPOST], [sigv4-HTTPPOSTForms], [HTTPPOSTForms-v2])

### 2.1 Request

- "The `POST` operation adds an object to a specified bucket by using HTML forms. `POST` is an alternate form of `PUT` that enables browser-based uploads as a way of putting objects in buckets. Parameters that are passed to `PUT` through HTTP headers are instead passed as form fields to `POST` in the multipart/form-data encoded message body."
- "Amazon S3 never stores partial objects. If you receive a successful response, you can be confident that the entire object was stored."
- Content-MD5 appears only in prose. It is not in the field table: "use the `Content-MD5` form field. When you use this form field, Amazon S3 checks the object against the provided MD5 value. If they do not match, Amazon S3 returns an error."
- "**Important** When constructing your request, make sure that the `file` field is the last field in the form."
- The Syntax block is `POST / HTTP/1.1`, `Host: {{destinationBucket}}.s3.amazonaws.com`, `Content-Type: multipart/form-data; boundary=9431149156168`. Its parts are, in order: `key`, `tagging`, `success_action_redirect`, `Content-Type`, `x-amz-meta-uuid`, `x-amz-meta-tag`, `AWSAccessKeyId`, `Policy`, `Signature`, `file` (with `filename="{{MyFilename.jpg}}"` and `Content-Type: image/jpeg`), then `submit` after the file. "This implementation of the operation does not use request parameters."
- Form declaration:
  - "`action` – The URL that processes the request, which must be set to the URL of the bucket." "The key name is specified in a form field."
  - "`method` – The method must be POST."
  - "`enctype` – The enclosure type (`enctype`) must be set to multipart/form-data for both file uploads and text area uploads."
  - The v2 page adds: "If any of these values is improperly set, the request fails."
- Encoding: "The form and policy must be UTF-8 encoded."
- Size limit: "The form data and boundaries (excluding the contents of the file) cannot exceed 20KB." The v2 page writes "20 KB".
- No query authentication. SigV2 overview: "Query string authentication is not supported for POST." Forms page: "The HTML form declaration does not accept query string authentication parameters."
- Anonymous requests: "If you don't provide elements required for authenticated requests, such as the `policy` element, the request is assumed to be anonymous and will succeed only if you have configured the bucket for public read and write." Also: "Conditional items are required for authenticated requests and are optional for anonymous requests."
- The RESTObjectPOST "Sample Request" is a leftover. It is a header-signed `POST /Neo` with `Content-Type: text/plain`, not a form. Its "versioning suspended" sample shows `x-amz-version-id: default` and then says "the version ID is `null`".

### 2.2 Form fields (verbatim where quoted)

- **`key`** (Yes): "The name of the uploaded key. To use the file name provided by the user, use the `${filename}` variable. For example, if a user named Mary uploads the file `example.jpg` and you specify `/user/mary/${filename}`, the key name is `/user/mary/example.jpg`."
- **`acl`**:
  - RESTObjectPOST: "If the specified ACL is not valid, an error is generated."
  - sigv4 forms: "If an invalid ACL is specified, Amazon S3 denies the request."
  - Default `private`. Values: `private | public-read | public-read-write | aws-exec-read | authenticated-read | bucket-owner-read | bucket-owner-full-control`. The 2015 page lacked `aws-exec-read`.
- **`Cache-Control, Content-Type, Content-Disposition, Content-Encoding, Expires`**: "The REST-specific headers. For more information, see PutObject."
- **`file`** (Yes):
  - "The file or text content. The file or text content must be the last field in the form. You cannot upload more than one file at a time."
  - v2 page: "Any fields below it are ignored."
- **`policy`** (Conditional): "Requests without a security policy are considered anonymous and work only on publicly writable buckets." Constraint: "A security policy is required if the bucket is not publicly writable."
- **`success_action_redirect, redirect`**:
  - "If `success_action_redirect` is not specified, Amazon S3 returns the empty document type specified in the `success_action_status` field. If Amazon S3 cannot interpret the URL, it acts as if the field is not present. If the upload fails, Amazon S3 displays an error and does not redirect the user to a URL."
  - "The `redirect` field name is deprecated, and support for the `redirect` field name will be removed in the future."
  - v2 page: "Amazon S3 appends the bucket, key, and etag values as query string parameters to the URL."
- **`success_action_status`**: "This field accepts the values `200`, `201`, or `204` (the default). If the value is set to `200` or `204`, Amazon S3 returns an empty document with a 200 or 204 status code. If the value is set to `201`, Amazon S3 returns an XML document with a 201 status code. If the value is not set or if it is set to a value that is not valid, Amazon S3 returns an empty document with a 204 status code." The forms page recommends 201 for Adobe Flash.
- **`tagging`**: "To add tags, use the following encoding scheme." `<Tagging><TagSet><Tag><Key>{{TagName}}</Key><Value>{{TagValue}}</Value></Tag>...</TagSet></Tagging>`.
  - User guide [object-tagging]: "If the tags you specify exceed the header size limit, you can use this POST method in which you include the tags in the body."
- **`x-amz-storage-class`**: default `STANDARD`; values `REDUCED_REDUNDANCY | EXPRESS_ONEZONE | DEEP_ARCHIVE | GLACIER | GLACIER_IR | INTELLIGENT_TIERING | ONEZONE_IA | STANDARD | STANDARD_IA`.
- **`x-amz-meta-*`**: "Headers starting with this prefix are user-defined metadata. Each one is stored and returned as a set of key-value pairs. Amazon S3 doesn't validate or interpret user-defined metadata."
  - [UsingMetadata]: "you should conform to using US-ASCII characters when using REST and UTF-8 when using SOAP or browser-based uploads through `POST`." Non-US-ASCII values "are character decoded as per RFC 2047 before storing and encoded as per RFC 2047 to make them mail-safe before returning". "Amazon S3 stores user-defined metadata keys in lowercase."
- **`x-amz-security-token`**:
  - RESTObjectPOST still calls it "The Amazon DevPay security token".
  - sigv4 forms: "If the request is using session credentials, it requires one `x-amz-security-token` form."
- **`x-amz-signature`** (Conditional): "(AWS Signature Version 4) The HMAC-SHA256 hash of the security policy."
- **`x-amz-algorithm`, `x-amz-credential`, `x-amz-date`**: "Required for authenticated requests" (§3.1).
- **`x-amz-website-redirect-location`**: "The value must be prefixed by `/`, `http://`, or `https://`. The length of the value is limited to 2 KB."
- **`x-amz-checksum-algorithm`**: "If a value is specified, you must include the matching checksum header. Otherwise, your request will generate a 400 error. Possible values include `CRC32`, `CRC32C`, `SHA1`, and `SHA256`."
- **`x-amz-checksum-crc32` / `-crc32c` / `-sha1` / `-sha256`**: "the base64-encoded ... of the object", "required if the value of `x-amz-checksum-algorithm` is" the matching name. CRC64NVME is not listed.
- **SSE-S3/KMS/DSSE**: `x-amz-server-side-encryption` (`AES256`, `aws:kms`, `aws:kms:dsse`), `-aws-kms-key-id`, `-context` ("a base64-encoded UTF-8 string that contains JSON-formatted key-value pairs"), `-bucket-key-enabled`.
- **SSE-C**: `-customer-algorithm` (`AES256`), `-customer-key`, `-customer-key-MD5`. "If you have ... (SSE-C) blocked for your general purpose bucket, you will get an HTTP 403 Access Denied error when you specify the SSE-C request headers".
- **SigV2 only**: `AWSAccessKeyId` ("Required if a policy document is included") and `signature` ("The HMAC signature constructed by using the secret access key that corresponds to the provided AWSAccessKeyId").
- **Other**: `x-amz-*`: "See POST Object ... for other `x-amz-*` headers."
- **`bucket`** is not a documented form field. It is a policy element. aws-sdk-js-v3 (§6.2) and s3-tests (`wrong_bucket`) send it as one.

### 2.3 `${filename}` ([sigv4-HTTPPOSTForms])

- "The variable `${filename}` is automatically replaced with the name of the file provided by the user and is recognized by all form fields. If the browser or client provides a full or partial path to the file, only the text following the last slash (/) or backslash (\\) is used (for example, `C:\Program Files\directory1\file.txt` is interpreted as `file.txt`). If no file or file name is provided, the variable is replaced with an empty string."
- The Policy page: "All variables within the form are expanded prior to validating the POST policy. Therefore, all condition matching should be against the expanded form fields." The example is `[ "starts-with", "$key", "user/user1/" ]`, not `.../${filename}`.
- Observed behaviour differs when the file part has no `filename` parameter (§8.1, §10).

### 2.4 Field-name case, file last, fields after it

- [sigv4-post-example]: "The post parameters are case insensitive. For example, you can specify `x-amz-signature` or `X-Amz-Signature`."
  - The example form uses `X-Amz-Credential`, `X-Amz-Algorithm`, `X-Amz-Date`, `Policy` and `X-Amz-Signature` against lowercase policy conditions.
- File last:
  - RESTObjectPOST: "The file or text content must be the last field in the form."
  - v2 forms: "Any fields below it are ignored."
  - Both example forms: `<!-- The elements after this will be ignored -->` before `<input type="submit" name="submit" ...>`.
- Duplicates (v2): "If you have multiple fields with the same name, the values must be separated by commas. For example, if you have two fields named "x-amz-meta-tag" and the first one has a value of "Ninja" and second has a value of "Stallman", you would set the policy document to `Ninja,Stallman`."
- Observed (secondary): fields placed after `file` are reported missing, and names are lowercased in messages (§8.2).

### 2.5 Responses

- Response headers listed:
  - `x-amz-checksum-crc32|crc32c|sha1|sha256`, `x-amz-expiration`;
  - `success_action_redirect, redirect` ("Ancestor: PostResponse", a doc artifact);
  - `x-amz-server-side-encryption`, `-aws-kms-key-id`, `-bucket-key-enabled`, `-customer-algorithm`, `-customer-key-MD5`;
  - `x-amz-version-id`.
- Response elements, all "Ancestor: PostResponse":
  - `Bucket` ("The name of the bucket that the object was stored in.");
  - `ETag` ("The entity tag (ETag) is an MD5 hash of the object ...");
  - `Key` ("The object key name.");
  - `Location` ("The URI of the object.").
- "This implementation of the operation does not return special errors."
- No AWS page, now or in 2015, shows a 201 body. Observed bodies are in §8.
- 303 samples (v2 examples, verbatim):
  - `HTTP/1.1 303 Redirect`, `Content-Type: application/xml`, `Location: https://awsexamplebucket1.s3.us-west-1.amazonaws.com/successful_upload.html?bucket=awsexamplebucket1&key=user/eric/MyPicture.jpg&etag=&quot;39d459dfbc0faabbb5e179358dfb94c3&quot;`. The HTML source holds `&amp;quot;`, so the page literally displays `&quot;`.
  - The text-area sample: `...new_post.html?bucket=awsexamplebucket1&key=user/eric/NewEntry.html&etag=40c3271af26b7f1672e41b8a274d28d4`, with the etag unquoted.
- "Pre-upload redirection" (v2): "If your bucket was created using <CreateBucketConfiguration>, your end users might require a redirect. If this occurs, some browsers might handle the redirect incorrectly."

### 2.6 Documented error codes ([ErrorResponses])

| Code | Description | Status |
|---|---|---|
| `IncorrectNumberOfFilesInPostRequest` | "POST requires exactly one file upload per request." | 400 |
| `InvalidPolicyDocument` | "The content of the form does not meet the conditions specified in the policy document." | 400 |
| `MalformedPOSTRequest` | "The body of your POST request is not well-formed multipart/form-data." | 400 |
| `MaxPostPreDataLengthExceededError` | "Your POST request fields preceding the upload file were too large." | 400 |
| `RequestIsNotMultiPartContent` | "A bucket POST request must be of the enclosure-type multipart/form-data." | 412 |
| `UserKeyMustBeSpecified` | "The bucket POST request must contain the specified field name. If it is specified, check the order of the fields." | 400 |
| `InvalidRequest` | among its reasons: "Conflicting values provided in HTTP headers and POST form fields." | 400 |
| `EntityTooSmall` | "Your proposed upload is smaller than the minimum allowed object size." | 400 |
| `EntityTooLarge` | "Your proposed upload exceeds the maximum allowed object size." | 400 |
| `SignatureDoesNotMatch` | 403 | |
| `InvalidAccessKeyId` | 403 | |
| `AccessDenied` | "Access Denied" | 403 |
| `ExpiredToken` | 400 | |
| `InvalidStorageClass` | 400 | |
| `BadDigest` | 400 | |
| `InvalidDigest` | 400 | |

## 3. SigV4 signing and the test vector

### 3.1 [sigv4-authentication-HTTPPOST], [sigv4-HTTPPOSTForms]

- The form must carry:
  - `policy`: "The Base64-encoded security policy ... For signature calculation this policy is the string you sign."
  - `x-amz-algorithm`: "For AWS Signature Version 4, the value is `AWS4-HMAC-SHA256`."
  - `x-amz-credential`: `<your-access-key-id>/<date>/<aws-region>/<aws-service>/aws4_request`, e.g. `AKIAIOSFODNN7EXAMPLE/20130728/us-east-1/s3/aws4_request`.
  - `x-amz-date`: "the date value in ISO8601 format. For example, `20130728T000000Z`. It is the same date you used in creating the signing key. This must also be the same value you provide in the policy (`x-amz-date`) that you signed."
  - `x-amz-signature`.
- "The POST policy must include the following elements: `x-amz-algorithm`, `x-amz-credential`, `x-amz-date`."
- The credential scope carries the region: "The bucket must be in the region that you specified in the credential scope (`x-amz-credential` form parameter), because the signature you provided is valid only within this scope" ([sigv4-post-example]).
- `s3:authType` is `POST` for these requests. On `s3:signatureAge` for presigned POST: "In Signature Version 4, the signing key is valid for up to seven days. Therefore, the signatures are also valid for up to seven days." ([sigv4-conditions]; see note 17 §5.)

### 3.2 Calculating the signature

"1. Create a policy using UTF-8 encoding. 2. Convert the UTF-8-encoded policy to Base64. The result is the string to sign. 3. Create the signature as an HMAC-SHA256 hash of the string to sign. You will provide the signing key as key to the hash function. 4. Encode the signature by using hex encoding."

The HMAC input is the base64 text, not the decoded JSON.

### 3.3 AWS's example, byte for byte (verified locally)

Credentials: `AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`.

The policy is 648 bytes with CRLF line ends. The page: "it should have carriage returns and new lines for your computed hash to match this value (ie. ASCII text, with CRLF line terminators)". Decoded exactly:

```
{ "expiration": "2015-12-30T12:00:00.000Z",\r\n  "conditions": [\r\n    {"bucket": "sigv4examplebucket"},\r\n    ["starts-with", "$key", "user/user1/"],\r\n    {"acl": "public-read"},\r\n    {"success_action_redirect": "http://sigv4examplebucket.s3.amazonaws.com/successful_upload.html"},\r\n    ["starts-with", "$Content-Type", "image/"],\r\n    {"x-amz-meta-uuid": "14365123651274"},\r\n    {"x-amz-server-side-encryption": "AES256"},\r\n    ["starts-with", "$x-amz-meta-tag", ""],\r\n\r\n    {"x-amz-credential": "AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request"},\r\n    {"x-amz-algorithm": "AWS4-HMAC-SHA256"},\r\n    {"x-amz-date": "20151229T000000Z" }\r\n  ]\r\n}
```

Base64 (864 characters, the StringToSign, as the page gives it):

```
eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLA0KICAiY29uZGl0aW9ucyI6IFsNCiAgICB7ImJ1Y2tldCI6ICJzaWd2NGV4YW1wbGVidWNrZXQifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS8iXSwNCiAgICB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LA0KICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL3NpZ3Y0ZXhhbXBsZWJ1Y2tldC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwNCiAgICBbInN0YXJ0cy13aXRoIiwgIiRDb250ZW50LVR5cGUiLCAiaW1hZ2UvIl0sDQogICAgeyJ4LWFtei1tZXRhLXV1aWQiOiAiMTQzNjUxMjM2NTEyNzQifSwNCiAgICB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sDQogICAgWyJzdGFydHMtd2l0aCIsICIkeC1hbXotbWV0YS10YWciLCAiIl0sDQoNCiAgICB7IngtYW16LWNyZWRlbnRpYWwiOiAiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAxNTEyMjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LA0KICAgIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwNCiAgICB7IngtYW16LWRhdGUiOiAiMjAxNTEyMjlUMDAwMDAwWiIgfQ0KICBdDQp9
```

- Signature, as stated: `8afdbf4008c03f22c2cd3cdb72e4afbb1f6a588f3255ac628749a66d7f09699e`. **Reproduced exactly.**
- Values I computed (not stated by AWS):
  - SigV4 signing key for 20151229/us-east-1/s3: `cbcef1ebeaefc82cce6530b9f0a9ae598846065f5c5bae0674bd5ebc4ba52d28`.
  - SHA-256 of the policy bytes: `bdbc873103bd66b82f3a98478e5938ca1c7d8204563309232bdd4b1bbdcf74fb`.
- The decoded policy parses as strict RFC 8259 JSON.
- The example's form fields: `key=user/user1/${filename}`, `acl=public-read`, `success_action_redirect=...`, `Content-Type=image/jpeg`, `x-amz-meta-uuid=14365123651274`, `x-amz-server-side-encryption=AES256`, `X-Amz-Credential`, `X-Amz-Algorithm`, `X-Amz-Date`, `x-amz-meta-tag=""`, `Policy`, `X-Amz-Signature`, `file`, then `submit`.

### 3.4 SigV2 (still documented, and still accepted by S3 in 2026)

- v2 policy: "The policy is a UTF-8 and Base64-encoded JSON document"; "Expiration is required in a policy."
- Signing: "Encode the policy by using UTF-8. Encode those UTF-8 bytes by using Base64. Sign the policy with your secret access key by using HMAC SHA-1. Encode the SHA-1 signature by using Base64."
- "AWS Regions created before January 30, 2014 will continue to support ... Signature Version 2. Any new regions after January 30, 2014 will support only Signature Version 4".
- The v2 samples are not test vectors:
  - The base64 policies still encode bucket `johnsmith` and `http://johnsmith.s3.amazonaws.com/...`, while the displayed JSON says `awsexamplebucket1`.
  - `0RavWzkygo6QX9caELEqKi9kDbU=` is not HMAC-SHA1 under the example secret (that gives `gp42sMsv4L5xLO0OjT+56V64o+A=`); the page says "Using your credentials create a signature, for example".
  - `qA7FWXKq6VvU68lI9KdveT1cWgF=` is non-canonical base64 (canonical would be `...WgE=`).
  - The text-area request has a first delimiter `-178521717625888` (one dash), a key `ser/eric/NewEntry.html`, and a Policy part with no blank line after its header.
- Secondary: LocalStack `test_post_object_policy_casing[s3]` uploaded twice with botocore SigV2 POST and got 204 from S3 on 2026-02-21.

## 4. The POST policy language ([sigv4-HTTPPOSTConstructPolicy], v2 "Policy construction")

### 4.1 The document and `expiration`

- "a UTF-8 and base64-encoded document written in JavaScript Object Notation (JSON) that specifies conditions that the request must meet."
- "The POST policy always contains the `expiration` and `conditions` elements."
- "The `expiration` element specifies the expiration date and time of the POST policy in ISO8601 GMT date format. For example, `2013-08-01T12:00:00.000Z` specifies that the POST policy is not valid after midnight GMT on August 1, 2013." That says midnight for a noon timestamp; v2 makes the same error.
- Documented examples use `.000Z` milliseconds. botocore, aws-sdk-js-v3 and s3-tests send `%Y-%m-%dT%H:%M:%SZ` without fractional seconds, and S3 accepted those in every LocalStack recording.
- The example document on the page has a trailing comma: `["starts-with", "$key", "user/eric/"],` before `]`. That is not JSON.

### 4.2 Conditions: forms and operators

- "The `conditions` in a POST policy is an array of objects, each of which is used to validate the request."
- "Although you must specify at least one condition for each form field that you specify in the form, you can create more complex matching criteria by specifying multiple conditions for a form field." (2019 text: "one condition".)
- Exact match: `{"acl": "public-read" }` or "an alternate way ... `[ "eq", "$acl", "public-read" ]`".
- Starts with: "The value must start with the specified value." `["starts-with", "$key", "user/user1/"]`.
- Content-Type lists: "Content-Types values for a `starts-with` condition that include commas are interpreted as lists. Each value in the list must meet the condition for the whole condition to pass. ... `["starts-with", "$Content-Type", "image/"]` The following value would pass the condition: `"image/jpg,image/png,image/gif"` The following value would not pass the condition: `["image/jpg,text/plain"]` Data elements other than `Content-Type` are treated as strings, regardless of the presence of commas."
- Any content: "use `starts-with` with an empty value (""). This example allows any value for `success_action_redirect`: `["starts-with", "$success_action_redirect", ""]`".
- Ranges: "For form fields that accept a range, separate the upper and lower limit with a comma. This example allows a file size from 1 to 10 MiB: `["content-length-range", 1048576, 10485760]`". The v2 page and the 2019 SigV4 page write `1048579`.

### 4.3 Which fields each operator applies to (SigV4 table)

| Field | Matching |
|---|---|
| `acl` | exact and `starts-with` |
| `bucket` | "Specifies the acceptable bucket name. This condition supports exact matching condition match type." |
| `content-length-range` | "The minimum and maximum allowable size for the uploaded content", `content-length-range` only |
| `Cache-Control`, `Content-Type`, `Content-Disposition`, `Content-Encoding`, `Expires` | exact and `starts-with` |
| `key` | exact and `starts-with` |
| `success_action_redirect`, `redirect` | exact and `starts-with` |
| `success_action_status` | exact |
| `x-amz-algorithm`, `x-amz-credential`, `x-amz-date` | exact |
| `x-amz-meta-*` | exact and `starts-with` |
| `x-amz-*` | "This condition supports exact matching." |
| `x-amz-security-token` | no matching type named; DevPay text: "the values must be separated by commas ... `{ "x-amz-security-token": "eW91dHViZQ==,b0hnNVNKWVJIQTA=" }`" |

### 4.4 The coverage rule and its exemptions

- SigV4: "Each form field that you specify in a form (except `x-amz-signature`, `file`, `policy`, and field names that have an `x-ignore-` prefix) must appear in the list of conditions."
- SigV2: "(except AWSAccessKeyId, signature, file, policy, and field names that have an x-ignore- prefix)".
- "If your toolkit adds more form fields (for example, Flash adds `filename`), you must add them to the POST policy document. If you can control this functionality, prefix `x-ignore-` to the field so Amazon S3 ignores the feature".
- HTTPPOSTFlash writes the Flash condition as `['starts-with', '$Filename', '']`, in single quotes, which is not JSON.
- Observed: an uncovered field gives 403 "Invalid according to Policy: Extra input fields: <name>" (§8.2).

### 4.5 Case sensitivity

AWS states only that "post parameters are case insensitive" (§2.4). Nothing AWS publishes addresses:
- the case of condition keys or operators;
- the case of the `$` names;
- the case of the top-level `expiration`/`conditions` keys;
- the case of values.

The observed evidence is in §7 and §8.

### 4.6 Escaping

The page's table lists `\\` Backslash, `\$` Dollar symbol, `\b` Backspace, `\f` Form feed, `\n` New line, `\r` Carriage return, `\t` Horizontal tab, `\v` Vertical tab, `\uxxxx` "All Unicode characters". `\$` and `\v` are not RFC 8259 escapes.

## 5. multipart/form-data

### 5.1 RFC 2046 §5.1–5.1.1 (normative text a parser needs)

- "each body part is preceded by a boundary delimiter line ... The boundary delimiter MUST NOT appear inside any of the encapsulated parts, on a line by itself or as the prefix of any line."
- "NO header fields are actually required in body parts. A body part that starts with a blank line, therefore, is allowed".
- "The only header fields that have defined meaning for body parts are those the names of which begin with "Content-"."
- "The boundary delimiter MUST occur at the beginning of a line, i.e., following a CRLF, and the initial CRLF is considered to be attached to the boundary delimiter line rather than part of the preceding part. The boundary may be followed by zero or more characters of linear whitespace. It is then terminated by either another CRLF and the header fields for the next part, or by two CRLFs, in which case there are no header fields for the next part."
- "Boundary delimiters must not appear within the encapsulated material, and must be no longer than 70 characters, not counting the two leading hyphens."
- Close delimiter: "identical to the previous delimiter lines, with the addition of two more hyphens after the boundary parameter value."
- "Boundary string comparisons must compare the boundary value with the beginning of each candidate line. An exact match of the entire candidate line is not required; it is sufficient that the boundary appear in its entirety following the CRLF."
- Preamble and epilogue: "implementations must ignore anything that appears before the first boundary delimiter line or after the last one."
- Boundary parameter: "consists of 1 to 70 characters from a set of characters known to be very robust through mail gateways, and NOT ending with white space. (If a boundary delimiter line appears to end with white space, the white space must be presumed to have been added by a gateway, and must be deleted.)"
- Grammar, verbatim:

```
boundary := 0*69<bchars> bcharsnospace
bchars := bcharsnospace / " "
bcharsnospace := DIGIT / ALPHA / "'" / "(" / ")" / "+" / "_" / "," / "-" / "." / "/" / ":" / "=" / "?"
dash-boundary := "--" boundary
multipart-body := [preamble CRLF] dash-boundary transport-padding CRLF body-part *encapsulation close-delimiter transport-padding [CRLF epilogue]
transport-padding := *LWSP-char  ; Composers MUST NOT generate non-zero length transport padding, but receivers MUST be able to handle padding added by message transports.
encapsulation := delimiter transport-padding CRLF body-part
delimiter := CRLF dash-boundary
close-delimiter := delimiter "--"
discard-text := *(*text CRLF) *text
body-part := MIME-part-headers [CRLF *OCTET]  ; ... the delimiter must not appear anywhere in the body part.
```

- "IMPORTANT: The free insertion of linear-white-space and RFC 822 comments between the elements shown in this BNF is NOT allowed".
- "in no event are headers (either message headers or body part headers) allowed to contain anything other than US-ASCII characters."
- Errata:
  - EID 508 (Verified) corrects Appendix A's `discard-text` to the form above.
  - EID 6776 (Held for Document Update, §5.1.1) proposes restoring RFC 1341's text: "if the "preamble" area is not used, the entity headers must be followed by TWO CRLFs ... A tolerant mail reading program, however, may interpret a body of type multipart that begins with an encapsulation line NOT initiated by a CRLF as also being an encapsulation boundary". The grammar as published lets the body begin with `dash-boundary`, which is what every client below sends.

### 5.2 RFC 7578

- §4: "follows the model of multipart MIME data streams as specified in Section 5.1 of [RFC2046]".
- §4.1: "the boundary delimiter MUST NOT appear inside any of the encapsulated parts, and it is often necessary to enclose the "boundary" parameter values in quotes in the Content-Type header field."
- §4.2:
  - "Each part MUST contain a Content-Disposition header field [RFC2183] where the disposition type is "form-data". The Content-Disposition header field MUST also contain an additional parameter of "name"".
  - "For form data that represents the content of a file, a name for the file SHOULD be supplied as well, by using a "filename" parameter ... The file name isn't mandatory".
  - Receivers: "do not use the file name blindly, check and possibly change to match local file system conventions if applicable, and do not use directory path information that may be present."
  - "file names normally visible to users MAY be encoded using the percent-encoding method in Section 2".
  - "NOTE: The encoding method described in [RFC5987], which would add a "filename*" parameter to the Content-Disposition header field, MUST NOT be used."
  - "Some commonly deployed systems use multipart/form-data with file names directly encoded including octets outside the US-ASCII range. The encoding used for the file names is typically UTF-8".
- §4.3: "multiple files MUST be sent by supplying each file in a separate part but all with the same "name" parameter."
- §4.4: "Each part MAY have an (optional) "Content-Type" header field, which defaults to "text/plain"."
- §4.7: "Senders SHOULD NOT generate any parts with a Content-Transfer-Encoding header field."
- §4.8: "does not support any MIME header fields in parts other than Content-Type, Content-Disposition, and (in limited circumstances) Content-Transfer-Encoding. Other header fields MUST NOT be included and MUST be ignored."
- §5.2: "Intermediaries MUST NOT reorder the results. Form parts with identical field names MUST NOT be coalesced."
- §8: "Required parameters: boundary".
- Errata: EID 4676 (Verified) fixes the §4.6 example's delimiters. EIDs 5616, 7385 and 9032 are Reported.

### 5.3 Header parameter grammar

- RFC 2183 §2: `disposition := "Content-Disposition" ":" disposition-type *(";" disposition-parm)`; `disposition-type := "inline" / "attachment" / extension-token ; values are not case-sensitive`; `filename-parm := "filename" "=" value`.
  - §2.3: "The receiving MUA SHOULD NOT respect any directory path information that may seem to be present in the filename parameter."
- RFC 2045 §5.1: `parameter := attribute "=" value`, `attribute := token ; Matching of attributes is ALWAYS case-insensitive.`, `value := token / quoted-string`, with tspecials `( ) < > @ , ; : \ " / [ ] ? =`.
  - "multipart boundaries are case-sensitive".
  - "the quotation marks in a quoted-string are not a part of the value".
- RFC 9110 §5.6.4: `quoted-pair = "\" ( HTAB / SP / VCHAR / obs-text )`; "Recipients that process the value of a quoted-string MUST handle a quoted-pair as if it were replaced by the octet following the backslash."
  - §5.6.6: `parameters = *( OWS ";" OWS [ parameter ] )`; "Parameter names are case-insensitive."; "Parameters do not allow whitespace ... around the "=" character."
  - §8.3.1: "The type and subtype tokens are case-insensitive."
- RFC 8187 §3.2.1 (for a non-browser `filename*=`): `ext-value = charset "'" [ language ] "'" value-chars`, `value-chars = *( pct-encoded / attr-char )`; "Producers MUST use the "UTF-8" ... character encoding."

### 5.4 What browsers send (WHATWG HTML §4.10.22.8, at `92f24801`)

- Bare CR or LF in names and in non-file values become CRLF.
- "For field names and filenames for file fields, the result of the encoding in the previous bullet point must be escaped by replacing any 0x0A (LF) bytes with the byte sequence `%0A`, 0x0D (CR) with `%0D` and 0x22 (") with `%22`. The user agent must not perform any other escapes."
- "The parts ... that correspond to non-file fields must not have a `Content-Type` header specified."
- "The order of parts must be the same as the order of fields in entry list. Multiple entries with the same name must be treated as distinct fields."
- An empty file input: "If there are no selected files, then create an entry with name and a new File object with an empty name, application/octet-stream as type, and an empty body". The browser therefore sends `filename=""`.

### 5.5 What the test clients send (reproduced offline)

- **s3-tests** passes every field through `requests.post(url, files=OrderedDict(...))`. `requests` sets `fn = guess_filename(v) or k`, so every part carries `filename` equal to its own name:
  - e.g. `Content-Disposition: form-data; name="key"; filename="key"\r\n\r\n${filename}\r\n`;
  - no part Content-Type;
  - the file part is `name="file"; filename="file"` unless a tuple names it (`foo.txt`).
- **LocalStack** passes fields as `data=` (no filename) and `files={"file": ...}` (`filename="file"`). With `(None, value)` it sends no filename at all.
- Boundaries are 32 hex digits, unquoted, and the body starts with `--boundary`.

## 6. Clients that generate a POST

### 6.1 botocore `generate_presigned_post` at `358f8eec`

Code: [signers.py L850–990](https://github.com/boto/botocore/blob/358f8eec8c76201bb1a7a35644abcbc9036de7ed/botocore/signers.py#L850), `S3PostPresigner` L662, `S3SigV4PostAuth` [auth.py L818](https://github.com/boto/botocore/blob/358f8eec8c76201bb1a7a35644abcbc9036de7ed/botocore/auth.py#L818).

**Fields.** `Fields` (copied) plus `key`, then from `S3SigV4PostAuth.add_auth`:
- `x-amz-algorithm='AWS4-HMAC-SHA256'`;
- `x-amz-credential=<AK>/<yyyymmdd>/<region>/s3/aws4_request`;
- `x-amz-date=<'%Y%m%dT%H%M%SZ'>`;
- `x-amz-security-token` (with session credentials);
- `policy`;
- `x-amz-signature`.

All names are lowercase.

**Conditions.** User `Conditions` first, then:
- `{'bucket': Bucket}`;
- `["starts-with", '$key', <prefix>]` if the key ends with `${filename}`, else `{'key': Key}`;
- `{'x-amz-algorithm': ...}`, `{'x-amz-credential': ...}`, `{'x-amz-date': ...}`, and `{'x-amz-security-token': token}` when there is one.

The docstrings: "Note that if a particular element is included in the fields dictionary it will not be automatically added to the conditions list. You must specify a condition for the element as well." And: "Note that bucket related conditions should not be included".

**Policy.** `{'expiration': (now + ExpiresIn).strftime(ISO8601), 'conditions': [...]}`:
- `ISO8601 = '%Y-%m-%dT%H:%M:%SZ'` (auth.py L62); default `ExpiresIn=3600`.
- Serialized with `json.dumps` (separators `, ` and `: `), UTF-8, base64.
- `fields['x-amz-signature'] = self.signature(fields['policy'], request)`: the hex HMAC-SHA256 of the base64 text under the SigV4 key for `timestamp[0:8]`, the region and `s3`.
- The operation signed is `'PutObject'` with signing type `presign-post` → `s3v4-presign-post`. The CRT build does not override it.
- SigV2 (`signature_version='s3'`) uses `HmacV1PostAuth`: `AWSAccessKeyId`, `policy`, `signature` (base64 HMAC-SHA1). S3 Express uses `X-Amz-S3session-Token`.
- With `UNSIGNED` there is no policy: the fields are just `Fields` plus `key`.

**URL.** The CreateBucket operation's URL:
- default config in partition aws → `https://{bucket}.s3.amazonaws.com/`, even for eu-west-1 (`_should_use_global_endpoint`, L993);
- path style → `https://s3.us-west-2.amazonaws.com/{bucket}`;
- a custom `endpoint_url` such as `http://127.0.0.1:9000` → `http://127.0.0.1:9000/{bucket}` for every addressing style.

**Offline vector.** Clock fixed at 2015-12-29T00:00:00Z, the example credentials, us-east-1, `generate_presigned_post("sigv4examplebucket", "user/user1/photo.jpg")`. My recomputation matches.
- Fields in order: `key`, `x-amz-algorithm`, `x-amz-credential`, `x-amz-date`, `policy`, `x-amz-signature`.
- Policy (279 bytes): `{"expiration": "2015-12-29T01:00:00Z", "conditions": [{"bucket": "sigv4examplebucket"}, {"key": "user/user1/photo.jpg"}, {"x-amz-algorithm": "AWS4-HMAC-SHA256"}, {"x-amz-credential": "AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request"}, {"x-amz-date": "20151229T000000Z"}]}`
- Base64: `eyJleHBpcmF0aW9uIjogIjIwMTUtMTItMjlUMDE6MDA6MDBaIiwgImNvbmRpdGlvbnMiOiBbeyJidWNrZXQiOiAic2lndjRleGFtcGxlYnVja2V0In0sIHsia2V5IjogInVzZXIvdXNlcjEvcGhvdG8uanBnIn0sIHsieC1hbXotYWxnb3JpdGhtIjogIkFXUzQtSE1BQy1TSEEyNTYifSwgeyJ4LWFtei1jcmVkZW50aWFsIjogIkFLSUFJT1NGT0ROTjdFWEFNUExFLzIwMTUxMjI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSwgeyJ4LWFtei1kYXRlIjogIjIwMTUxMjI5VDAwMDAwMFoifV19`
- Signature: `3364624b39010d9b06f5d9b8c9a18a6e478d261c048159c92f2fd6fa885f14e6`.

### 6.2 aws-sdk-js-v3 `createPresignedPost` at `c935fdd2`

Source: [createPresignedPost.ts](https://github.com/aws/aws-sdk-js-v3/blob/c935fdd2bc9b36c689ef8e17b75c57bf9a780990/packages/s3-presigned-post/src/createPresignedPost.ts).

- **Fields** it adds: `bucket: Bucket`, `X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date` and, with a session token, `X-Amz-Security-Token`. It returns them with `key`, `Policy` and `X-Amz-Signature`.
- **Conditions**: every field it sends becomes a condition `{k: v}`, deduplicated as JSON strings, so the condition keys are capitalized. The key becomes `["starts-with","$key",prefix]` or `{key}`.
- **Expiration**: ISO 8601 without milliseconds.
- **Policy JSON** is compact.

A browser client of this SDK therefore sends a `bucket` field, capitalized names, and capitalized condition keys.

## 7. ceph s3-tests at `5522d1c3` (still head)

**Common shape.**
- URL: `_get_post_url` = `{endpoint}/{bucket}`, path style. Requests go through `requests.post(url, files=payload)`: no Authorization header, every part with `filename=<name>` (§5.5).
- **Every authenticated POST test signs with SigV2**: `AWSAccessKeyId` plus `signature = base64(HMAC-SHA1(secret, policy_b64))`. No test uses SigV4 fields.
- Base policy **C0** (expiration now+6000 s as `%Y-%m-%dT%H:%M:%SZ`, encoded by `json.JSONEncoder().encode`):

```
[{"bucket": B}, ["starts-with", "$key", "foo"], {"acl": "private"}, ["starts-with", "$Content-Type", "text/plain"], ["content-length-range", 0, 1024]]
```

- Base fields **F0**, in order: `key=foo.txt, AWSAccessKeyId, acl=private, signature, policy, Content-Type=text/plain, file='bar'`.
- Bucket: `get_new_bucket()` (private) unless the entry says public-read-write.

**Markers.** None of the 36 `test_post_object_*` tests carries `fails_on_aws` or `fails_on_rgw`. None asserts an error code, only statuses, apart from one XML check, one empty-body check and one redirect URL.

Links: `https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py#L<n>`.

1. `anonymous_request` L1948: public-read-write bucket, no policy. Fields key=foo.txt, acl=public-read, Content-Type, file. **204**; body `bar`.
2. `authenticated_request` L1962: C0, F0. **204**.
3. `authenticated_no_content_type` L2000: public-read-write bucket. C0 and F0, each without Content-Type. **204**.
4. `authenticated_request_bad_access_key` L2037: public-read-write bucket, `AWSAccessKeyId='foo'`. **403**. A bad key is not treated as anonymous.
5. `set_success_code` L2072: anonymous, public-read-write, `success_action_status=201`. **201**, and `ET.fromstring(r.content).find('Key').text == 'foo.txt'`. That needs no default namespace on `PostResponse`.
6. `set_invalid_success_code` L2087: anonymous, `success_action_status=404`. **204**, body `''`.
7. `upload_larger_than_chunk` L2102: content-length-range 0..5 MiB, file 3 MiB (`'foo'*1024*1024`). **204**.
8. `set_key_from_filename` L2141: C0, `key=${filename}`, file `('foo.txt','bar')`. **204**; object `foo.txt`.
9. `ignored_header` L2177: F0 plus `x-ignore-foo=bar`. **204**.
10. `case_insensitive_condition_fields` L2211: conditions `{"bUcKeT"}`, `["StArTs-WiTh", "$KeY", "foo"]`, `{"AcL"}`, `["StArTs-WiTh", "$CoNtEnT-TyPe", ...]`; fields `kEy`, `aCl`, `pOLICy`. **204**.
11. `escaped_field_values` L2246: `["starts-with", "$key", "\\$foo"]` (a JSON string whose value is `\$foo`), `key=\$foo.txt`. **204**; object `\$foo.txt`. No `\$` unescaping expected.
12. `success_redirect_action` L2282: public-read-write bucket; C0 plus `["eq", "$success_action_redirect", R]` with R the bucket URL; field `success_action_redirect=R`. After redirects **200**, and `r.url == R?bucket=B&key=foo.txt&etag=%22<etag>%22`. requests' `requote_uri` turns a raw `"` into `%22`, so the test cannot tell raw quotes from `%22`.
13. `invalid_signature` L2324: key prefix `\$foo`, `key=\$foo.txt`, signature reversed. **403**.
14. `invalid_access_key` L2357: as above, access key reversed. **403**.
15. `invalid_date_format` L2390: `"expiration": str(expires)` (e.g. `2026-09-29 12:00:00.123456+00:00`). **400**.
16. `no_key_specified` L2423: no key condition, no key field. **400**.
17. `missing_signature` L2455: no `signature` field; the key also mismatches `\$foo`. **400**.
18. `missing_policy_condition` L2488: no bucket condition, and key `foo.txt` vs `\$foo`. **403**. Two failures, so inconclusive about the bucket condition.
19. `user_specified_header` L2520: plus `["starts-with","$x-amz-meta-foo","bar"]`, field `x-amz-meta-foo=barclamp`. **204**; metadata foo=barclamp.
20. `request_missing_policy_specified_field` L2556: the same condition, field absent. **403**.
21. `condition_is_case_sensitive` L2590: `"CONDITIONS"`. **400**.
22. `expires_is_case_sensitive` L2623: `"EXPIRATION"`. **400**.
23. `expired_policy` L2656: expiration now−6000 s. **403**.
24. `wrong_bucket` L2689: policy for B; fields `key=${filename}`, `bucket=B`, file `('foo.txt','bar')`; posted to another bucket's URL. **403**.
25. `invalid_request_field_value` L2725: `["eq","$x-amz-meta-foo",""]` vs `barclamp`. **403**.
26. `missing_expires_condition` L2758: no `expiration`. **400**.
27. `missing_conditions_list` L2791: `{"expiration": ...}` only. **400**.
28. `upload_size_limit_exceeded` L2816: `["content-length-range", 0, 0]`, 3 bytes. **400**.
29. `missing_content_length_argument` L2849: `["content-length-range", 0]`. **400**.
30. `invalid_content_length_argument` L2882: `["content-length-range", -1, 0]`. **400**.
31. `upload_size_below_minimum` L2915: `[512, 1000]`, 3 bytes. **400**.
32. `upload_size_rgw_chunk_size_bug` L2948: `[4 MiB, 12 MiB]`, 4 MiB+200 bytes. **204**.
33. `empty_conditions` L2995: `"conditions": [{}]`. **400**.
34. `tags_anonymous_request` L12203 [tagging, fails_on_dbstore]: public-read-write bucket, `tagging=<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>`. **204**; tags equal.
35. `tags_authenticated_request` L12234 [tagging]: plus `["starts-with","$tagging",""]`. **204**.
36. `upload_checksum` L15299 [checksum]: key prefix `foo_cksum_test`, 0..5 MiB, 2 MiB of `x`.
    - Correct `x-amz-checksum-sha256=aTL9MeXa9HObn6eP93eygxsJlcwdCwCTysgGAZAgE7w=` (verified) gives **204**.
    - `sailorjerry` gives **400**.
    - No condition covers the checksum field.

**Also POSTs (SigV2).**
- `test_encryption_sse_c_post_object_authenticated_request` L11091 [encryption, fails_on_dbstore]: `starts-with ""` on the three SSE-C fields (named `...-key-md5`); **204**.
- `test_sse_kms_post_object_authenticated_request` L11410 [same markers]: **204**.
- `test_sse_s3_default_post_object_authenticated_request` L14807 [encryption, bucket_encryption, sse_s3, fails_on_dbstore]: `["starts-with","$x-amz-server-side-encryption",""]` with **no such field** gives **204**; GET shows `AES256`.
- `test_sse_kms_default_post_object_authenticated_request` L14852: the same for KMS.

## 8. Observed S3 behaviour (secondary)

### 8.1 LocalStack recordings

Source: `tests/aws/services/s3/test_s3.py`, class `TestS3PresignedPost` (L10844). All 20 tests are `@markers.aws.validated`, last validated 2026-02-21. Links: `https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/tests/aws/services/s3/test_s3.py#L<n>`.

The snapshots sort keys, so XML element order is not preserved. Every recorded ETag and CRC64NVME value I checked is the MD5 or CRC-64/NVME of the file content alone (verified).

**Expired** (L10886, ExpiresIn=2, sleep 3): 403 `AccessDenied`, "Invalid according to Policy: Policy expired."

**Truncated policy** (L10918, `policy[:-2]`, SigV2 and SigV4): 403 `SignatureDoesNotMatch`.
- Message: "The request signature we calculated does not match the signature you provided. Check your key and signing method."
- Elements `AWSAccessKeyId`, `SignatureProvided` (asserted equal to the signature field sent), `StringToSign` (asserted equal to the `policy` field as sent), `StringToSignBytes`.

**Missing signature** (L10965): 400 `InvalidArgument`, "Bucket POST must contain a field named 'X-Amz-Signature'.  If it is specified, please check the order of the fields."
- Two spaces after the first period.
- `ArgumentName` `X-Amz-Signature`, `ArgumentValue` empty.
- SigV2: `'Signature'`.

**Missing fields** (L11007):
- Dropping `x-amz-algorithm` and `x-amz-credential` gives 400 `InvalidArgument` naming `'X-Amz-Algorithm'`.
- SigV2 dropping `AWSAccessKeyId` gives the same naming `'AWSAccessKeyId'`.
- Keeping only `key` and `policy` gives 403 `AccessDenied`, "Access Denied". The request is anonymous and the bucket is private.

**201** (L11061): key `key-${filename}`, file `("my-file", "something body")`, `success_action_status=201`.
- `PostResponse` with `Bucket`, `Key` = `key-my-file`, `ETag` = `"43281e21fce675ac3bcb3524b38ca4ed"`, and `Location`.
- Header `ETag` equals the element.
- Header `Location` equals the element and equals `https://{bucket}.s3.amazonaws.com/key-my-file`; the test strips the region for non-us-east-1.

**303** (L11106):
- Status 303, empty body, `Location` query with `key` and `bucket`. The etag assertion is commented out.
- A relative `success_action_redirect` (`/wrong/redirect/relative`) gave **204**.

**Tags** (L11176):
- A single tag and a list are stored.
- `<InvalidXmlTagging></InvalidXmlTagging>` gave **204 with an empty TagSet**.
- `not-xml` gave 400 `MalformedXML`, "The XML you provided was not well-formed or did not validate against our published schema".

**Metadata** (L11214): field `x-amz-meta-TEST-2` is stored as `test-2`; `Content-Type` and `Expires` are applied.

**Unicode** (L11255):
- 201 body `Key` = `test_unicode—_file.pdf` raw; `Location` = `<bucket-url>/test_unicode%E2%80%94_file.pdf`.
- Metadata `ÄMÄZÕÑ S3` came back as `=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=`.
- Bytes `\x00..\x04` came back as `=?UTF-8?B?AAECAwQ=?=`.
- Values already RFC-2047-looking came back unchanged.
- The Cache-Control and Content-Disposition values came back with the em dash as a space, as botocore decoded the header.

**Storage class** (L11332): `STANDARD_IA` is stored. `FakeClass` gives 400 `InvalidStorageClass`, "The storage class you specified is not valid", `StorageClassRequested` `FakeClass`.

**Wrong Content-Type** (L11389, `Content-Type: text/html`): **412 `PreconditionFailed`**, Message "At least one of the pre-conditions you specified did not hold", Condition "Bucket POST must be of the enclosure-type multipart/form-data".

**Default checksum** (L11418): the 204 carries `x-amz-checksum-crc64nvme` and `x-amz-checksum-type: FULL_OBJECT`.

**File part without filename** (L11452):
- Accepted; the default Content-Type is `binary/octet-stream`.
- With key `file-as-field-${filename}` the object was stored with key **`file-as-field-${filename}`, literally**.

**`eq`** (L11521):
- Wrong value: 403 `AccessDenied`, "Invalid according to Policy: Policy Condition failed: [\"eq\", \"$success_action_redirect\", \"http://localhost.test/random\"]".
- Missing `$`: 403, "Policy Condition failed: [\"eq\", \"success_action_redirect\", ...]".
- `{"bucket": B, "success_action_redirect": R}`: 400 `InvalidPolicyDocument`, "Invalid Policy: Invalid Simple-Condition: Simple-Conditions must have exactly one property specified."
- Value `HTTP://...` vs `http://...`: 403. Values are case-sensitive.
- `$content-type`, `$x-amz-meta-test-2` (field `...-TEST-2`) and `$success_Action_REDIRECT` are all accepted. Condition names are case-insensitive.

**`starts-with`** (L11699):
- Wrong prefix: 403; wrong-case prefix: 403.
- Correct prefix: 303 with `Location` starting with the redirect.
- `starts-with ""` accepted any redirect: 303.

**Size** (L11790, `["content-length-range", 5, 10]`):
- 12 bytes: 400 `EntityTooLarge`, "Your proposed upload exceeds the maximum allowed size", `ProposedSize` 12, `MaxSizeAllowed` 10.
- 1 byte: 400 `EntityTooSmall`, "Your proposed upload is smaller than the minimum allowed size", `ProposedSize` 1, `MinSizeAllowed` 5.
- 5 and 10 bytes: 204. The bounds are inclusive.
- `["content-length-range", "5", "10"]` (strings): 204.
- `["test", "10"]`: 403 `AccessDenied`, "Invalid according to Policy: Policy Condition failed: [\"content-length-range\", \"test\", \"10\"]".

**Session credentials** (L11898): the conditions were exactly bucket, key, x-amz-security-token, x-amz-credential, x-amz-date and x-amz-algorithm. **204 with no `Content-Type` header**.

**Casing** (L12010): `Policy` for `policy`, `X-Amz-Credential`, and SigV2 `awsaccesskeyid` all gave 204.

Third-party: LocalStack's own implementation (`presigned_url.py`, `provider.py` at the same pin) does not verify POST signatures or enforce the coverage rule. It keeps `/` unescaped in `Location`. Its answer for a missing key, `InvalidArgument` naming `'key'`, matches the captures below.

### 8.2 Captures in issue trackers

**Missing `key`** (fields after `file` are ignored):
- [nukulb-1] 2014: `<Error><Code>InvalidArgument</Code><Message>Bucket POST must contain a field named 'key'.  If it is specified, please check the order of the fields.</Message><ArgumentValue></ArgumentValue><ArgumentName>key</ArgumentName><RequestId>…</RequestId><HostId>…</HostId></Error>`.
- [k6-1382] 2020: the same message with `ArgumentName` before `ArgumentValue`, and an empty `HostId`. Also: "anything below the file prop is reported as missing".
- [requests-36] 2023: "S3 ignores all other fields after files".
- The code is `InvalidArgument`, not the table's `UserKeyMustBeSpecified`.

**Extra input fields:**
- [moxie-165] 2016: 403 `<Code>AccessDenied</Code><Message>Invalid according to Policy: Extra input fields: filename</Message>`. The form's field was `Filename`, so the name is reported lowercased. An `Upload` field after `file` was not reported. (A later capture names a field as sent, `StorageClass`: §13.2.)
- [s3-beam-42] 2017: "Invalid according to Policy: Extra input fields: content-disposition".
- [vue-dropzone-471] 2019: "Extra input fields: dzuuid".

**Absent field with `starts-with ""`:**
- [s3du-29] 2012: `AccessDenied` "Invalid according to Policy: Policy Condition failed: ["starts-with", "$Content-Type", ""]".
- Cause: "The Content-Type hidden field isn't being included in your form". Adding `hidden_field_tag "Content-type", ""` fixed it.
- So a condition on a field that is absent fails even with `""`. This contradicts s3-tests' SSE-default tests.

**Simple conditions:**
- [modbox-10] 2022: a policy with `{"Content-Length":6199034}` got 400 `InvalidPolicyDocument` "Invalid Policy: Invalid Simple-Condition: value must be a string."
- [boto3-4514] 2025: botocore's `{"key": "…$$…"}` failed with `AccessDenied` "Invalid according to Policy: Policy Condition failed: ["eq", "$key", "/2025/0267$$Copy.csv"]". S3 prints simple conditions as `eq` arrays, and keys containing `$$` fail. A boto3 maintainer reproduced it and filed it with the S3 team (ticket P230041718).

**Invalid JSON:** [tpyo-53] 2013 screenshot: `<Code>InvalidPolicyDocument</Code><Message>Invalid Policy: Invalid JSON.</Message>`.

**File count:** [blueimp-1832] 2014 comment: sending multiple files gives "POST requires exactly one file upload per request".

**201 bodies** (no namespace in any capture; `Location` percent-encodes the key including `/`; `Key` is raw; `ETag` quoted):
- [s3rver-504] 2019: `<?xml version="1.0" encoding="UTF-8"?>\n<PostResponse><Location>https://foobar.s3.amazonaws.com/pr%C3%A9sident.jpg</Location><Bucket>foobar</Bucket><Key>président.jpg</Key><ETag>"14dd4667441ada2b3914e5c1b72573b3"</ETag></PostResponse>`.
- [ceph-20330] 2018, path style: `<PostResponse><Location>http://s3.amazonaws.com/happybucket2/prefix%2F111222</Location><Bucket>happybucket2</Bucket><Key>prefix/111222</Key><ETag>"22cf483cd7c9ca14793b86144ad31fba"</ETag></PostResponse>`.
- Also [s3du-93] 2013 (`http://s3.amazonaws.com/application/uploads%2Flectures%2F…`), [blueimp-1339] 2012 (`…/uploads%2F…%2Fch%C3%A9.jpg`), [dropzone-33] 2016 ("S3 will url encode the key ... fakeslug/fatboy_sand.jpg -> fakeslug%2Ffatboy_sand.jpg"), and [rn-aws3-70] 2018 (parsed by regex, `location: 'https://xyz.s3.amazonaws.com/uploads%2Fundefined'`).
- [s3du-155] 2014: a submission with no file selected got key `uploads/…/` (the `${filename}` became empty) and ETag `"d41d8cd98f00b204e9800998ecf8427e"`, the MD5 of nothing. `{`/`}` were encoded `%7B`/`%7D` in `Location`.

### 8.3 Check order (DERIVED from the above)

1. The multipart Content-Type is checked (412).
2. Required fields are checked (400 `InvalidArgument`; the SigV4 fields are all or none; algorithm is named before credential).
3. With none of them present, the request is anonymous (403 on a private bucket).
4. The signature is checked over the policy text as received, before decoding (a truncated policy gives `SignatureDoesNotMatch`, not `InvalidPolicyDocument`).
5. The policy is parsed (`InvalidPolicyDocument`).
6. Expiration and conditions follow (403 or 400).

s3-tests agrees that a missing signature (400) comes before condition failures. No source orders expiration against the conditions.

## 9. Error summary: documented vs observed

| Case | Docs | Observed |
|---|---|---|
| Not multipart | 412 `RequestIsNotMultiPartContent` | 412 `PreconditionFailed` + `Condition` |
| Missing key / field after file | 400 `UserKeyMustBeSpecified` | 400 `InvalidArgument` + `ArgumentName` |
| Missing signature field | none | 400 `InvalidArgument` naming the canonical field |
| Bad signature | 403 `SignatureDoesNotMatch` | same, + `StringToSign`=policy, `StringToSignBytes` |
| Expired | none | 403 `AccessDenied` "Invalid according to Policy: Policy expired." |
| Condition fails | 400 `InvalidPolicyDocument` "The content of the form does not meet the conditions..." | 403 `AccessDenied` "Invalid according to Policy: Policy Condition failed: [...]" |
| Uncovered field | none | 403 `AccessDenied` "... Extra input fields: <lowercased name>" |
| Bad policy JSON / shape | none | 400 `InvalidPolicyDocument` "Invalid Policy: ..." |
| Size | `EntityTooLarge`/`EntityTooSmall` "... allowed object size." | 400 with `ProposedSize`, `Max/MinSizeAllowed`, message "... allowed size" |

## 10. Discrepancies

1. **Status and code for a failed condition.** The error table maps condition failures to 400 `InvalidPolicyDocument`. S3 answered 403 `AccessDenied`. `InvalidPolicyDocument` was observed only for malformed policies.
2. **Missing key.** `UserKeyMustBeSpecified` in the table; `InvalidArgument` in the 2014, 2020 and 2026 recordings.
3. **Non-multipart body.** `RequestIsNotMultiPartContent` in the table; `PreconditionFailed` observed.
4. **`${filename}` with no filename parameter.** The docs say it is "replaced with an empty string". S3 kept it literally (2026). With `filename=""` it became empty (2014).
5. **Condition on an absent field with `starts-with ""`.** Fails on S3 (2012); passes in s3-tests' `sse_*_default_post_object` tests (RGW).
6. **Fields no condition covers.** Required by the docs and enforced by S3 (Extra input fields). s3-tests' `upload_checksum` sends `x-amz-checksum-sha256` uncovered and expects 204.
7. **`starts-with` on fields the table marks exact-only.** s3-tests uses it on `x-amz-server-side-encryption*` and expects success. The table says exact only. S3 is unrecorded.
8. **Operator case.** s3-tests expects `StArTs-WiTh` to work. No S3 source covers it.
9. **Redirect etag.** Quoted as `&quot;` in the docs' first sample, unquoted in the second, `%22`-quoted in s3-tests. The key is shown with raw `/` in the docs. No S3 capture exists.
10. **Expiration.** "midnight" vs `12:00:00.000Z` in both policy pages. The range example `1048579` vs `1048576`.
11. **Non-JSON in the docs.** Trailing commas (SigV4 and v2 examples), `\$` and `\v` escapes, and the single-quoted Flash condition. AWS's signed SigV4 example itself is strict JSON.
12. **`\$`.** The docs call `\$` an escape. s3-tests expects `\$foo` to match the key `\$foo.txt` literally. S3 fails keys with `$$` [boto3-4514].
13. **RESTObjectPOST's own samples.** The "Sample Request" is not a form POST, and `x-amz-version-id: default` contradicts "the version ID is `null`".
14. **Error messages.** The table's `EntityTooLarge`/`Small` text ("object size.") differs from the wire message ("allowed size").

## 11. Not stated by any fetched source (UNVERIFIED)

§13 answers several of these from further captures: the 20 KB limit, the answer to no file, a
wrong region, an empty access key, a malformed `x-amz-date`, an unreadable expiration, the
redirect's encoding and the headers of a 204.

**Size and time limits**
- Whether 20 KB means 20,480 bytes, what exactly it counts, and the message for `MaxPostPreDataLengthExceededError`.
- The largest object a POST may carry; 5 GB is stated only for PUT.
- Whether `x-amz-date` is checked against the clock, whether the credential date must equal it, whether the 7-day limit applies to POST, and whether a request at exactly the expiration instant is expired.

**Missing or unexpected fields**
- S3's answer to a missing `x-amz-date` alone, a wrong algorithm value, or a credential in the wrong region.
- Zero `file` fields.
- Whether a part with a `filename` but a name other than `file` counts as a file. s3-tests sends one on every part.
- Whether the file part's own Content-Type sets the object's type.
- Whether `Content-MD5` or `x-amz-checksum-*` must be covered by a condition.
- A `bucket` field that differs from the URL's bucket.
- The `acl` field under BucketOwnerEnforced (see note 13 §6.8).
- `x-amz-checksum-crc64nvme` as a field.

**Policy parsing and matching**
- Whether S3 accepts trailing commas, `\$`, `\v`, duplicate keys, or a missing `bucket` condition.
- A policy with no `expiration` or no `conditions`, and a bad date format: only s3-tests' statuses (400) are known, not S3's messages.
- Content-length-range with min > max.
- The comparison rules for `${filename}` names that browsers escaped as `%22`.

**Response shape**
- Whether S3 keeps existing query parameters in `success_action_redirect`, and how it encodes the key and etag in the redirect.
- The headers on 204, 200 and 303 beyond those recorded: ETag and Location on 204 are unasserted.

**Multipart tolerance**
- Leading CRLF or a preamble, transport padding, quoted boundaries, `filename*`, `Content-Transfer-Encoding`, bare LF line ends.

## 12. DERIVED notes for mantle

- **Routing.** POST Object is `POST /` (virtual-hosted) or `POST /{bucket}` (path) with no subresource. Clients target the bucket URL (botocore; aws-sdk-js-v3). Anything that is not multipart/form-data gets S3's 412 with the `Condition` element.
- **Parsing.**
  - Identify the file by the name `file`, never by the presence of `filename`.
  - Match field and condition names case-insensitively and values case-sensitively.
  - Treat everything after the file part as ignored.
  - Bound the pre-file bytes by the documented 20 KB.
- **Signing.** `crates/s3/src/sigv4.rs` already has `signing_key` and `hex`. The POST signature is `hex(HMAC(signing_key(date, region, "s3"), policy_b64_as_received))`. Verify it before decoding the policy. §3.3 and §6.1 are two independent vectors.
- **SigV2.** Every authenticated s3-tests POST test uses SigV2. mantle refuses SigV2 (sigv4.rs L230, "use AWS4-HMAC-SHA256"), so only the anonymous and ACL-based ones can pass as written. Those also need public-read-write bucket ACLs, which mantle does not have.
- **JSON.** `crates/s3/src/json.rs` is strict. AWS's signed example parses. Its illustrative examples do not, and S3's leniency is unrecorded.

## 13. Further captures (compiled 2026-09-29)

A second search, of GitHub issues, Stack Overflow, AWS re:Post and blogs, for S3's answers in
the cases §11 lists. LocalStack's POST tests and snapshot on main are unchanged since
`8b9a79f`, and its snapshot redacts `StringToSignBytes`. versitygw is pinned at
`b91e178a12fae7a6acf31597a70aef8b8b3e72f4`; its messages are third-party, modelled on S3's.

### 13.1 The policy document

- **An expiration S3 cannot read (secondary).** 400 `InvalidPolicyDocument`, "Invalid Policy:
  Invalid 'expiration' value: '2011-09-13T07:52:58+02:00'" ([cwd-2], 2011); the fix sent UTC,
  and the next error was about fields, so the UTC form passed. "Invalid Policy: Invalid
  'expiration' value: '2015-10-17 03:15:59 UTC'" ([cwd-189], 2015). An offset is refused, and
  so is a space for the `T`.
- **A member the policy language lacks (secondary).** An IAM bucket policy sent as a POST
  policy, with `Version`, `Id` and `Statement`, drew `InvalidPolicyDocument`, "Invalid Policy:
  Unexpected: 'statement'" ([so-31169349], 2015): the name lowercased, in single quotes, and
  reported before the missing expiration and conditions. Why `statement` rather than
  `version`, the first member, is not shown. versitygw writes "Unexpected: %q" (double quotes).
- **Missing expiration, missing or non-list conditions: not found.** versitygw: "Invalid
  Policy: Policy missing expiration.", "Invalid Policy: Policy missing conditions.", "Invalid
  Policy: Invalid 'conditions' value: must be a List."; RGW's internal texts are the same
  phrases. s3-tests expects 400 for each (§7 items 21, 22, 26, 27).
- **A float bound (secondary).** `["content-length-range",0,512.0]`, decoded from the capture's
  policy: 400 `InvalidPolicyDocument`, "Invalid Policy: Invalid JSON." ([repost-float], 2023;
  [so-77643928]).
- **A null simple-condition value (secondary).** `{"success_action_redirect": null}`: "Invalid
  Policy: Invalid JSON." ([cwd-230], 2019), where a number drew "value must be a string" (§8.2).
- **"Invalid Policy: Invalid Condition: missing operation identifier." (secondary)**
  ([so-43431505], 2017), its policy not shown; versitygw gives it for an empty condition `[]`.
- **"Invalid Policy: Token - must be enclosed in quotes." (secondary)** ([so-73624713], 2022),
  its policy not shown.
- **Not found**, with versitygw's texts: an unknown operation, "Invalid Policy: Invalid
  Condition: unknown operation '%s'."; the wrong number of items, "Invalid Policy: Invalid %s:
  wrong number of arguments.", `%s` the operation; a condition of another JSON type, "Invalid
  Policy: Invalid condition test: must be a List or Object."; base64 that does not decode,
  "Invalid Policy: invalid Base64 encoding."; operands that are not strings, "Invalid JSON."
  in its tests.

### 13.2 The form

- **The fields before the file (secondary).** Code `MaxPostPreDataLengthExceeded`, without the
  table's `Error`; message "Your POST request fields preceeding the upload file was too
  large." (sic); the element `<MaxPostPreDataLengthBytes>20480</MaxPostPreDataLengthBytes>`
  ([s3du-45], 2013; [ember-45], 2015; [rnbu-55], 2018; [so-65267176], 2020). The limit is 20,480
  bytes. Each was a form whose file part had another name, so its content counted as a field.
  A file part sent as `Content-Disposition: file; name="file"` drew the same ([so-18541652],
  2013): only a `form-data` part named `file` is the file.
- **No file (secondary).** Never `IncorrectNumberOfFilesInPostRequest`: 400 `InvalidArgument`,
  "POST requires exactly one file upload per request.", `ArgumentName` `file`, `ArgumentValue`
  `0` ([vue-dropzone-303], 2018; [rnip-97], 2016; [boto3-934], 2016, a part named after its path;
  [so-64554749], 2020). Two parts named `file`: the same message in prose only.
- **A Content-Type without a boundary (secondary).** 400 `MalformedPOSTRequest`, "The body of
  your POST request is not well-formed multipart/form-data." ([repost-boundary], 2017).
- **A field no condition covers is named as sent (secondary).** "Invalid according to Policy:
  Extra input fields: StorageClass" ([so-54014351], 2019), against §8.2's lowercase `filename`,
  whose form may have carried a lowercase field Flash added.
- **A failed condition keeps the policy's spelling and escapes quotes (secondary).** `["eq",
  "$Content-Disposition", "filename=\"test.png\""]` ([so-79055241], 2024); `["eq",
  "$X-Amz-Date", ...]` from an SDK and `["eq", "$x-amz-date", ...]` from a user ([sdkjs-1514],
  2017).

### 13.3 Signing

- **Another region (secondary).** 400 `InvalidArgument`, "the region 'us-east-1' is wrong;
  expecting 'us-west-1'", `ArgumentName` `X-Amz-Credential`, `ArgumentValue` the credential,
  and `<Region>us-west-1</Region>` ([slingshot-150], 2015; [sdkjs-2529], 2019): the message a
  presigned URL gets, without its "Error parsing the X-Amz-Credential parameter; " prefix.
- **An empty access key (secondary).** 400 `InvalidArgument`, "a non-empty Access Key (AKID)
  must be provided in the credential.", naming the credential ([outline-7060], 2024).
- **A malformed `x-amz-date` (secondary).** `InvalidArgument`, "X-Amz-Date must be formated via
  ISO8601 Long format" (sic), `ArgumentName` `X-Amz-Date`, `ArgumentValue` as sent
  ([s3upload-1], 2017).
- **Not found**, with versitygw's texts: a malformed credential, "the Credential is mal-formed;
  expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\"."; its date, "incorrect date
  format %q. This date in the credential must be in the format \"yyyyMMdd\"."; its service,
  "incorrect service %q. This endpoint belongs to \"s3\"."; its terminal, "incorrect terminal
  %q. This endpoint uses \"aws4_request\"."; another algorithm. Nor whether S3 checks the
  credential's day against `x-amz-date`; versitygw does not.
- **An unknown access key (secondary).** `InvalidAccessKeyId`, "The AWS Access Key Id you
  provided does not exist in our records.", `<AWSAccessKeyId>` the credential's key
  ([cognito-1], 2019; [sdkjs-3878], 2021; [sdknet-1989], 2022); 403 from prose and s3-tests.
- **A signature that does not match (secondary).** Elements in order: `AWSAccessKeyId`,
  `StringToSign`, `SignatureProvided`, `StringToSignBytes`, with no canonical request.
  `StringToSignBytes` is two lowercase hex digits a byte, single spaces between, and decodes to
  the policy as sent ([fausto-65], 2019, SigV4; SigV2 captures alike).

### 13.4 Answers

- **Redirects (secondary).** S3 keeps a query the URL has, raw space and all, appends with `&`,
  and adds `bucket`, `key` and `etag` in that order, form-encoded: the key's `/` as `%2F`, a
  space as `+`, the ETag quoted as `%22...%22`:
  `...upload_complete?md5=...&remote_url=https://s3.amazonaws.com/omnivore-scratch/8329/1391639479.7579765/Untitled copy.sketch&bucket=omnivore-scratch&key=8329%2F1391639479.7579765%2FUntitled+copy.sketch&etag=%2247371919f8bdb6e35df40d8744db818a%22`
  ([layervault-8], 2014); `...?bucket=%bucket%&key=images%2F1851413185242563.jpg&etag=%22...%22`
  ([so-18390253], 2013). Other reserved and non-ASCII characters: not shown.
- **A 204 (secondary).** `HTTP/1.1 204 No Content` with `ETag` quoted and `Location`, the
  object's URL with the key's `/` as `%2F`, no `Content-Type` and no `Content-Length`
  ([boto3-2851], 2021, an SDK maintainer's wire trace; [lepozepo-72], 2015).
- **A request header that conflicts with a field: not found.**

### 13.5 Sources

- [cwd-2] https://github.com/dwilkie/carrierwave_direct/issues/2
- [cwd-189] https://github.com/dwilkie/carrierwave_direct/pull/189
- [cwd-230] https://github.com/dwilkie/carrierwave_direct/issues/230
- [so-31169349] https://stackoverflow.com/q/31169349
- [repost-float] https://repost.aws/questions/QUo27u4OYpTA-XvAWr72xeRg ; [so-77643928] https://stackoverflow.com/q/77643928
- [so-43431505] https://stackoverflow.com/q/43431505
- [so-73624713] https://stackoverflow.com/q/73624713
- [s3du-45] https://github.com/waynehoover/s3_direct_upload/issues/45
- [ember-45] https://github.com/benefitcloud/ember-uploader/issues/45
- [rnbu-55] https://github.com/Vydia/react-native-background-upload/issues/55
- [so-65267176] https://stackoverflow.com/q/65267176 ; [so-18541652] https://stackoverflow.com/q/18541652
- [vue-dropzone-303] https://github.com/rowanwins/vue-dropzone/issues/303
- [rnip-97] https://github.com/react-native-image-picker/react-native-image-picker/issues/97
- [boto3-934] https://github.com/boto/boto3/issues/934 ; [so-64554749] https://stackoverflow.com/q/64554749
- [repost-boundary] https://repost.aws/questions/QUeXkR-SeeSaCFrB_koqLpKA
- [so-54014351] https://stackoverflow.com/q/54014351 ; [so-79055241] https://stackoverflow.com/q/79055241
- [sdkjs-1514] https://github.com/aws/aws-sdk-js/issues/1514
- [slingshot-150] https://github.com/CulturalMe/meteor-slingshot/issues/150 ; [sdkjs-2529] https://github.com/aws/aws-sdk-js/issues/2529
- [outline-7060] https://github.com/outline/outline/issues/7060
- [s3upload-1] https://github.com/michaeldyrynda/s3upload/issues/1
- [cognito-1] https://github.com/furaiev/amazon-cognito-identity-dart-2/issues/1 ; [sdkjs-3878] https://github.com/aws/aws-sdk-js/issues/3878 ; [sdknet-1989] https://github.com/aws/aws-sdk-net/issues/1989
- [fausto-65] https://github.com/Fausto95/aws-s3/issues/65
- [layervault-8] https://github.com/layervault/layervault_ruby_client/issues/8 ; [so-18390253] https://stackoverflow.com/q/18390253
- [boto3-2851] https://github.com/boto/boto3/issues/2851 ; [lepozepo-72] https://github.com/Lepozepo/S3/issues/72
- [versitygw] https://github.com/versity/versitygw/blob/b91e178a12fae7a6acf31597a70aef8b8b3e72f4/s3err/post-object.go

## Sources

- [RESTObjectPOST] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTObjectPOST.html (and web.archive.org/web/20150608131326)
- [sigv4-UsingHTTPPOST] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-UsingHTTPPOST.html
- [sigv4-authentication-HTTPPOST] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-authentication-HTTPPOST.html
- [sigv4-HTTPPOSTForms] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-HTTPPOSTForms.html
- [sigv4-HTTPPOSTConstructPolicy] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-HTTPPOSTConstructPolicy.html (web.archive.org/web/20190708023712, CDX for dating)
- [sigv4-post-example] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-post-example.html
- [UsingHTTPPOST-v2], [HTTPPOSTForms-v2], [HTTPPOSTExamples-v2], [HTTPPOSTFlash] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/{UsingHTTPPOST,HTTPPOSTForms,HTTPPOSTExamples,HTTPPOSTFlash}.html; web.archive.org/web/20160325090336 (sigv4-post-additional-considerations)
- [ErrorResponses] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html
- [sigv4-conditions] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/bucket-policy-s3-sigv4-conditions.html; [sig-v4] …/developerguide/sig-v4-authenticating-requests.html
- [UsingMetadata], [object-tagging], [upload-objects] https://docs.aws.amazon.com/AmazonS3/latest/userguide/{UsingMetadata,object-tagging,upload-objects}.html
- RFC 7578, RFC 2046 §5.1, RFC 2045 §5.1, RFC 2183 §2, RFC 9110 §5.6.4/§5.6.6/§8.3.1, RFC 8187 §3.2.1 (https://www.rfc-editor.org/rfc/rfcNNNN.txt); errata EID 508, 6776 (RFC 2046) and 4676 (RFC 7578) from https://www.rfc-editor.org/errata.json
- [WHATWG-HTML] https://html.spec.whatwg.org/multipage/form-control-infrastructure.html#multipart-form-data at 92f248013013096b5a780afffd8377fb9a6eba87
- [botocore] https://github.com/boto/botocore/tree/358f8eec8c76201bb1a7a35644abcbc9036de7ed (signers.py, auth.py, tests/unit/test_signers.py; run offline with a fixed clock)
- [aws-sdk-js-v3] https://github.com/aws/aws-sdk-js-v3/tree/c935fdd2bc9b36c689ef8e17b75c57bf9a780990/packages/s3-presigned-post/src
- [s3-tests] https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py
- [localstack] https://github.com/localstack/localstack/tree/8b9a79f05846835cf4dff63ab7eefdde9df83783 (tests/aws/services/s3/test_s3.py, .snapshot.json, .validation.json; localstack-core/localstack/services/s3/{presigned_url,provider,utils}.py)
- [requests] PyPI requests 2.34.2 (`models.py` `_encode_files`, `utils.requote_uri`)
- Issues:
  - [nukulb-1] github.com/nukulb/s3-angular-file-upload/issues/1
  - [k6-1382] github.com/grafana/k6/issues/1382
  - [requests-36] github.com/asmcos/requests/pull/36
  - [uppy-4449] github.com/transloadit/uppy/issues/4449
  - [moxie-165] github.com/moxiecode/moxie/issues/165
  - [s3-beam-42] github.com/martinklepsch/s3-beam/issues/42
  - [vue-dropzone-471] github.com/rowanwins/vue-dropzone/issues/471
  - [s3du-29] github.com/waynehoover/s3_direct_upload/issues/29
  - [s3du-93] …/issues/93
  - [s3du-155] …/issues/155
  - [modbox-10] github.com/toniopelo/modbox/issues/10
  - [boto3-4514] github.com/boto/boto3/issues/4514
  - [tpyo-53] github.com/tpyo/amazon-s3-php-class/issues/53
  - [blueimp-1832] github.com/blueimp/jQuery-File-Upload/issues/1832
  - [blueimp-1339] …/issues/1339
  - [s3rver-504] github.com/jamhall/s3rver/issues/504
  - [ceph-20330] github.com/ceph/ceph/pull/20330
  - [dropzone-33] github.com/enyo/dropzone/issues/33
  - [minio-1477] github.com/minio/minio/issues/1477 (truncated quote)
  - [rn-aws3-70] github.com/benjreinhart/react-native-aws3/issues/70, parser at a02d5390cd65933fdeabc00be78ba8dac9c48730
