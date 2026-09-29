# 20 — Server-side encryption: ground truth for SSE-S3, SSE-KMS, SSE-C and bucket defaults

Research note for mantle's server-side encryption: the headers S3's object operations take and
return, the bucket encryption configuration, S3's recorded answers, and the cryptography an
implementation of encryption at rest rests on. It covers:

- SSE-S3, S3's default for every new object since 5 January 2023;
- SSE-KMS and DSSE-KMS, their headers and KMS's errors;
- SSE-C, its headers, its errors, and the April 2026 change that blocks it on new buckets;
- PutBucketEncryption, GetBucketEncryption and DeleteBucketEncryption;
- ETags, checksums and listings under each mode;
- botocore's model and its SSE-C handling, ceph s3-tests' tests, LocalStack's recordings;
- AES-GCM (NIST SP 800-38D), key management (SP 800-57), key wrap (SP 800-38F, RFC 3394,
  RFC 5649), AES-GCM-SIV (RFC 8452), AWS's descriptions of its envelope encryption, and what
  aws-lc-rs exposes.

All sources were fetched on 2026-09-29 (UTC). Labels: **primary** for AWS documentation, AWS
announcements, NIST publications, RFCs and vendor API references; **secondary** for S3's
answers captured by those who sent the requests; **third-party** for another implementation's
or client's claim; **DERIVED** for an inference of ours; **UNVERIFIED** where nothing was found.

## 0. Pins

### AWS documentation

These are the `.md` renditions, fetched 2026-09-29. URL bases:
- `UG` = https://docs.aws.amazon.com/AmazonS3/latest/userguide/
- `API` = https://docs.aws.amazon.com/AmazonS3/latest/API/
- `DG` = https://docs.aws.amazon.com/AmazonS3/latest/developerguide/

`DG` is the new "Amazon S3 Developer Guide". It now holds ErrorResponses, RESTObjectPOST, the SigV4 POST pages and common headers. The old `/API/ErrorResponses.html` and `/API/RESTObjectPOST.html` redirect to Welcome.html, and `/API/ErrorResponses.md` returns 404.

SHA-256 of each fetched file, first 16 hex characters:

| File | SHA-256 prefix |
|---|---|
| UG ServerSideEncryptionCustomerKeys | 53cebd1f979a77dd |
| UG specifying-s3-c-encryption | 2b22b89d3e928ce3 |
| UG blocking-unblocking-s3-c-encryption-gpb | 969cda5d586f132f |
| UG default-s3-c-encryption-setting-faq | 47af63e5fc973257 |
| UG default-encryption-faq | c0c9cbe84611de2d |
| UG bucket-encryption | 425af315db1364c9 |
| UG default-bucket-encryption | 3df7b2d7e1d888f6 |
| UG UsingServerSideEncryption | 8cbc5e910a9f9c7e |
| UG serv-side-encryption | 03413fefc5d8abe6 |
| UG specifying-s3-encryption | af5f41d362bd2900 |
| UG UsingKMSEncryption | 3297096d5334e989 |
| UG specifying-kms-encryption | 5301f457accbdeed |
| UG bucket-key | ee97d17b00ce9f08 |
| UG UsingDSSEncryption | 738bacc053dcdeb7 |
| UG update-sse-encryption | 93d1825f54534711 |
| UG troubleshoot-403-errors | 7d5692329e7b70df |
| UG checking-object-integrity-upload | 51aaf5ef273fe778 |
| API PutObject | 061bb77f1d293914 |
| API CopyObject | 48d7d9e4d768c703 |
| API CreateMultipartUpload | e83a678422da96a9 |
| API UploadPart | c7b5bfc9caf6bc63 |
| API CompleteMultipartUpload | ccb0b8b56fafb653 |
| API GetObject | ec810213f755e0a8 |
| API HeadObject | fe5216a6d102071c |
| API PutBucketEncryption | b4e8d3c7dacd25e3 |
| API GetBucketEncryption | 15fc34c09e77c3e3 |
| API DeleteBucketEncryption | 1ed6fbfe546ae1bf |
| API ServerSideEncryptionByDefault | 7835ea5113e0fe24 |
| API ServerSideEncryptionRule | 99352a5664e1cf77 |
| API BlockedEncryptionTypes | dcc5126f4a96cfd3 |
| API Object | 75831da5af639ae4 |
| API UpdateObjectEncryption | fe5bc16871c7aac8 |
| DG ErrorResponses | 25cd6367b79b9ff0 |
| DG RESTObjectPOST | 6b94be121d0e9727 |

### Announcements (HTML, fetched 2026-09-29)
- AWS Storage Blog, 19 Nov 2025: https://aws.amazon.com/blogs/storage/advanced-notice-amazon-s3-to-disable-the-use-of-sse-c-encryption-by-default-for-all-new-buckets-and-select-existing-buckets-in-april-2026/
- What's New, 19 Nov 2025: https://aws.amazon.com/about-aws/whats-new/2025/11/amazon-s3-bucket-level-standardize-encryption-types/
- What's New, 29 Jan 2026: https://aws.amazon.com/about-aws/whats-new/2026/01/change-the-server-side-encryption-type-of-s3-objects/
- What's New, 6 Apr 2026: https://aws.amazon.com/about-aws/whats-new/2026/04/s3-default-bucket-security-setting/

### Repositories
- **botocore** `develop` at `358f8eec8c76201bb1a7a35644abcbc9036de7ed` (2026-09-28T18:17:09Z, version 1.43.104).
- **s3transfer** at `379338c7fb44d7aa7a1eb18c7b2c8c990f148f87` (version 0.19.2).
- **boto3** at `f2605d9b8bcc9682c3f9540879b87429d4f33b21`.
- **ceph/s3-tests** `master` at `5522d1c351f75bc00ae0f64f742f3f095f5939d9` (2026-05-27). This is the last commit on that repo: the suite moved into ceph/ceph.
- **ceph/ceph** `main` at `d732a9bd76697df3927f61ef37c40c9ec6d81913` (2026-09-29). The suite is at `src/test/rgw/s3-tests/`, imported by `868d20182a08685265e0b5831bad703fdcf3189b` on 2026-06-22. ceph CI runs this in-tree copy.
- **LocalStack** at `8b9a79f05846835cf4dff63ab7eefdde9df83783` (2026-03-23). The repo is archived. Its SSE snapshots were re-recorded against AWS on 2026-02-21, which is before the April 2026 SSE-C change.
- **aws-lc-rs** 1.18.1, git `22e629d5c46276497a24ee3e575be4315940e7cb` (tag v1.18.1).
- **AWS-LC** `main` at `fa1b1fb9281d5a6d1435771b37fc0b183dd9c634`.

### Standards (SHA-256)
- NIST SP 800-38D: `d99f3921ccebca049e7522426553aba071dae14ec3d5b6041e8c111a6cb57bba`
- NIST SP 800-57 Pt 1 Rev. 5: `cc32391022c1382ac7c91490f6bcc8838e0f889925270da23ef4e800e2ecb7ad`

---

## 1. SSE-S3 (`x-amz-server-side-encryption: AES256`)

### 1.1 Values of the header (primary, API PutObject.md and every object API page)
> "Valid Values: `AES256 | aws:fsx | aws:backup | aws:kms | aws:kms:dsse`"

- **`aws:kms`** is SSE-KMS; **`aws:kms:dsse`** is DSSE-KMS. Both are covered in item 2.
- **`aws:fsx`** (primary):
  > "S3 access points for Amazon FSx - When accessing data stored in Amazon FSx file systems using S3 access points, the only valid server side encryption option is `aws:fsx`. All Amazon FSx file systems have encryption configured by default and are encrypted at rest."
- **`aws:backup`**: the S3 docs never describe it; it appears only in the enum.
  - botocore added it in 1.43.66 (2026-08-06), with the CHANGELOG line "AWS Backup now lets you create read-only access points for Amazon S3 recovery points…" [third-party].
  - AWS Backup docs: "AWS Backup lets you access S3 backup data directly through S3 access points, without initiating a restore." [primary, https://docs.aws.amazon.com/aws-backup/latest/devguide/s3-backups.md]
  - DERIVED: `aws:backup` is the value reported for objects read through AWS Backup access points, analogous to `aws:fsx`. The actual semantics are UNVERIFIED.
- **Directory buckets** (primary): "there are only two supported options for server-side encryption: … (`AES256`) and … (`aws:kms`)."

### 1.2 Which operations take and return the headers

The matrix below is primary: it comes from the Request Syntax and Response Syntax of each API page. The botocore model matches it exactly (third-party, item 6a).

Abbreviations:
- **SSE**: `x-amz-server-side-encryption`
- **KMS**: `-aws-kms-key-id`
- **CTX**: `-context`
- **BK**: `-bucket-key-enabled`
- **C-trio**: `-customer-algorithm`, `-customer-key`, `-customer-key-MD5`
- **copy-C-trio**: `x-amz-copy-source-server-side-encryption-customer-{algorithm,key,key-MD5}`

| Operation | Request | Response |
|---|---|---|
| PutObject | SSE, KMS, CTX, BK, C-trio | SSE, KMS, CTX, BK, C-algorithm, C-key-MD5 |
| POST Object (form fields, DG RESTObjectPOST.md) | SSE (`aws:kms`, `AES256`, `aws:kms:dsse`), KMS, CTX, BK, C-trio | SSE, KMS, BK, C-algorithm, C-key-MD5 |
| CopyObject | as PutObject, plus copy-C-trio | as PutObject |
| CreateMultipartUpload | as PutObject | as PutObject |
| UploadPart | C-trio only | SSE, KMS, BK, C-algorithm, C-key-MD5 (no CTX) |
| UploadPartCopy | C-trio and copy-C-trio | as UploadPart |
| CompleteMultipartUpload | C-trio (conditional, see 3.1) | SSE, KMS, BK only (no SSE-C echo, no CTX) |
| GetObject / HeadObject | C-trio | SSE, KMS, BK, C-algorithm, C-key-MD5 (**no CTX**) |
| GetObjectAttributes, ListParts, SelectObjectContent | C-trio | none |
| CreateSession (directory buckets) | SSE, KMS, CTX, BK | same |
| WriteGetObjectResponse | `x-amz-fwd-header-…` forms | none |

**SSE-S3 in the user guide (primary, UG specifying-s3-encryption.md):**
> "Set the value of the header to the encryption algorithm `AES256`, which Amazon S3 supports. Amazon S3 confirms that your object is stored with SSE-S3 by returning the response header `x-amz-server-side-encryption`."

The same page lists the operations that accept the header: PUT Object, PUT Object - Copy, POST Object and Initiate Multipart Upload. The response header comes back on those four plus Upload Part, Upload Part - Copy, Complete Multipart Upload, Get Object and Head Object. It also says:
> "Do not send encryption request headers for `GET` requests and `HEAD` requests if your object uses SSE-S3, or you'll get an HTTP status code 400 (Bad Request) error."

**GetObject (primary, API GetObject.md):**
> "Encryption request headers, like `x-amz-server-side-encryption`, should not be sent for the `GetObject` requests, if your object uses … (SSE-S3), … (SSE-KMS), or … (DSSE-KMS). If you include the header in your `GetObject` requests for the object that uses these types of keys, you'll get an HTTP `400 Bad Request` error."

HeadObject.md says the same and adds: "It's because the encryption method can't be changed when you retrieve the object."

**Multipart (primary, API UploadPart.md):**
> "Unless you are using a customer-provided encryption key (SSE-C), you don't need to specify the encryption parameters in each UploadPart request. Instead, you only need to specify the server-side encryption parameters in the initial Initiate Multipart request."

### 1.3 Default encryption of every new object since 5 January 2023

**Primary, UG default-encryption-faq.md:**
> "Starting January 5, 2023, all new object uploads to Amazon S3 are automatically encrypted at no additional cost and with no impact on performance. SSE-S3, which uses 256-bit Advanced Encryption Standard (AES-256), is automatically applied to all new buckets and to any existing S3 bucket that doesn't already have default encryption configured."

> "Amazon S3 now configures default encryption on all existing unencrypted buckets to apply server-side encryption with S3 managed keys (SSE-S3) as the base level of encryption for new objects uploaded to these buckets. Objects that are already in an existing unencrypted bucket won't be automatically encrypted."

> "Can I disable encryption for the new objects being written to my bucket? No. SSE-S3 is the new base level of encryption … You can no longer disable encryption for new object uploads."

On what responses carry:
> "check the response header `x-amz-server-side-encryption` when you use object action APIs, such as PutObject and GetObject"

and CloudTrail records `"SSEApplied":"Default_SSE_S3"`.

**CopyObject** (primary):
> "Amazon S3 automatically encrypts all new objects that are copied to an S3 bucket. When copying an object, if you don't specify encryption information in your copy request, the encryption setting of the target object is set to the default encryption configuration of the destination bucket."

> "If the encryption setting in your request is different from the default encryption configuration of the destination bucket, the encryption setting in your request takes precedence."

**Secondary evidence:**
- LocalStack, AWS-validated, 2026-02-21: a plain PutObject to a never-configured bucket returns `"ServerSideEncryption":"AES256"` with ETag `"42832cdec7083e70a9cd6f2d5852e004"`, which is MD5("test-sse").
- A copy of an SSE-C source with no target encryption returns `"ServerSideEncryption":"AES256"`, so the copy became SSE-S3.
- ceph's `test_sse_s3_default_*` (third-party) expects `AES256` in PUT and GET responses when a default is configured.

### 1.4 Invalid values and their errors

**Documented (primary):**
- API CopyObject: "Unrecognized or unsupported values won't write a destination object and will receive a `400 Bad Request` response."
- DG ErrorResponses.md: "`InvalidEncryptionAlgorithmError` — The encryption request that you specified is not valid. The valid value is AES256. — 400 Bad Request". API Error.md has the same text minus "that".

**Recorded:**
- Empty value: `x-amz-server-side-encryption:` with no value returned HTTP 400 [secondary, aws/aws-sdk-cpp#1771, 2021-09-11, `server: AmazonS3`]:
  ```
  <Error><Code>InvalidArgument</Code><Message>The encryption method specified is not supported</Message><ArgumentName>x-amz-server-side-encryption</ArgumentName><ArgumentValue/>…</Error>
  ```
- The captured wording of `InvalidEncryptionAlgorithmError` differs from the docs: "The Encryption request you specified is not valid. Supported value: AES256." [secondary: LocalStack 2026-02-21, aws-sdk-go#2724 (2019), aws-sdk-js#1789 (2017)]. It is returned for a bad **SSE-C algorithm** value (item 3), not for a bad SSE value.
- An old header-on-wrong-operation message exists: "400 (InvalidArgument): x-amz-server-side-encryption header is not supported for this operation." [secondary, s3tools/s3cmd#276, 2014; the operation was never identified].
- ceph asserts these, and none is marked `fails_on_aws` [third-party]:
  - `'aes:kms'` → 400 `InvalidArgument`
  - an empty value → 400 `InvalidArgument` (in-tree copy only)
  - `AES256` plus `-aws-kms-key-id` → 400 `InvalidArgument`
- **UNVERIFIED on AWS:**
  - The exact answer for other garbage values (for example `aes256` or `AES512`).
  - The answer for `aws:fsx` or `aws:backup` sent to a normal bucket.
  - The answer for `AES256` with BK or KMS headers.
  - DERIVED guess: "The encryption method specified is not supported" (`InvalidArgument`) for unrecognized values, from the empty-value capture.

### 1.5 What AWS says SSE-S3 does internally

Two different wordings are published (both primary):
- UG serv-side-encryption.md: "Each object is encrypted with a unique key. As an additional safeguard, SSE-S3 encrypts the key itself with a root key that it regularly rotates. SSE-S3 uses one of the strongest block ciphers available, 256-bit Advanced Encryption Standard (AES-256), to encrypt your data."
- UG UsingServerSideEncryption.md: "Amazon S3 encrypts each object with a unique key. As an additional safeguard, it encrypts the key itself with a key that it rotates regularly. Amazon S3 server-side encryption uses 256-bit Advanced Encryption Standard Galois/Counter Mode (AES-GCM) to encrypt all uploaded objects."

Also primary: "Server-side encryption encrypts only the object data, not the object metadata." and "You can't apply different types of server-side encryption to the same object simultaneously."

UNVERIFIED: the root-key rotation interval, the wrap algorithm, the IV scheme and any chunking.

### 1.6 New: `UpdateObjectEncryption` (What's New, 29 Jan 2026; primary)
> "You can now change the server-side encryption type of encrypted objects in Amazon S3 without any data movement. You can use the UpdateObjectEncryption API to atomically change the encryption key of your objects regardless of the object size or storage class."

**API UpdateObjectEncryption.md:**
- Request: `PUT /{Key+}?encryption&versionId=` with body `<ObjectEncryption><SSE-KMS><BucketKeyEnabled/><KMSKeyArn/></SSE-KMS></ObjectEncryption>`. `KMSKeyArn` must be a full ARN matching the pattern `arn:aws[a-zA-Z0-9-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[a-zA-Z0-9-]+`.
- "The `UpdateObjectEncryption` operation uses envelope encryption to re-encrypt the data key used to encrypt and decrypt your object with your newly specified server-side encryption type … preserves all object metadata properties, including the storage class, creation date, last modified date, ETag, and checksum properties."
- "Source objects that are unencrypted, or encrypted with either … (DSSE-KMS) or … (SSE-C) aren't supported by this operation. Additionally, you cannot specify SSE-S3 encryption as the requested new encryption type"
- Errors are `InvalidRequest` 400, `AccessDenied` 403 and `NoSuchKey` 404, with listed messages. One is "Requests that modify an object encryption configuration require AWS Signature Version 4. Modify the request to use AWS Signature Version 4, and then try again." Another is an `AccessDenied` for objects under Object Lock.
- UG update-sse-encryption.md: requests "won't initiate replica events in the destination bucket"; the operation is "typically completed in milliseconds regardless of the size of the object".

DERIVED: this is AWS's clearest public statement that SSE-S3 is envelope encryption with a per-object data key. It also means an SSE-KMS object can carry an MD5 ETag if it was converted from SSE-S3.

---

## 2. SSE-KMS (`aws:kms`) and DSSE-KMS (`aws:kms:dsse`)

### 2.1 Headers (primary, API PutObject.md)

**`x-amz-server-side-encryption-aws-kms-key-id`:**
> "Specifies the AWS KMS key ID (Key ID, Key ARN, or Key Alias) to use for object encryption. If the KMS key doesn't exist in the same account that's issuing the command, you must use the full Key ARN not the Key ID. **General purpose buckets** - If you specify `x-amz-server-side-encryption` with `aws:kms` or `aws:kms:dsse`, this header specifies the ID … If you specify `x-amz-server-side-encryption:aws:kms` or `x-amz-server-side-encryption:aws:kms:dsse`, but do not provide `x-amz-server-side-encryption-aws-kms-key-id`, Amazon S3 uses the AWS managed key (`aws/s3`) to protect the data."

For directory buckets the key must match the bucket's default customer managed key: "Incorrect key specification results in an HTTP `400 Bad Request` error."

**`x-amz-server-side-encryption-context`:**
> "Specifies the AWS KMS Encryption Context as an additional encryption context to use for object encryption. The value of this header is a Base64 encoded string of a UTF-8 encoded JSON, which contains the encryption context as key-value pairs. This value is stored as object metadata and automatically gets passed on to AWS KMS for future `GetObject` operations on this object. **General purpose buckets** - This value must be explicitly added during `CopyObject` operations if you want an additional encryption context for your object."

CopyObject adds: "The additional encryption context of the source object won't be copied to the destination object."

**`x-amz-server-side-encryption-bucket-key-enabled`:**
> "Setting this header to `true` causes Amazon S3 to use an S3 Bucket Key for object encryption with SSE-KMS. Also, specifying this header with a PUT action doesn't affect bucket-level settings for S3 Bucket Key."

UG specifying-kms-encryption.md: "If you specify the `x-amz-server-side-encryption:aws:kms` header but don't provide the `x-amz-server-side-encryption-aws-bucket-key-enabled` header, your object uses the S3 Bucket Key settings for the destination bucket to encrypt your object."
- Doc inconsistency: this page names the header `-aws-bucket-key-enabled`. The API reference and botocore use `x-amz-server-side-encryption-bucket-key-enabled`.

**Response headers (primary):**
- Key ID: "If present, indicates the ID of the KMS key that was used for object encryption."
- GET and HEAD never return CTX; see the matrix in 1.2.

**Secondary (LocalStack 2026-02-21):**
- A bucket default configured with a bare key UUID is echoed back as that UUID by GetBucketEncryption.
- Objects report `SSEKMSKeyId` as a full ARN: `arn:<partition>:kms:<region>:111111111111:key/<uuid>`.
- With `BucketKeyEnabled` false, PutObject omits the field. The test comment says: "if the BucketKeyEnabled is False, S3 does not return the field from PutObject".
- `ServerSideEncryption=aws:kms` with no key ID gets the AWS-managed key, whose DescribeKey shows `"Description":"Default key that protects my S3 objects when no other key is defined"`.
- A request-level `aws:kms` inherits the bucket's BucketKeyEnabled.

### 2.2 Key ID forms (primary)
- **API ServerSideEncryptionByDefault.md:** "Key ID: `1234abcd-12ab-34cd-56ef-1234567890ab` / Key ARN: `arn:aws:kms:us-east-2:111122223333:key/1234abcd-…` / Key Alias: `alias/alias-name`". Also: "If you use a KMS key alias instead, then AWS KMS resolves the key within the requester's account." and "Amazon S3 only supports symmetric encryption KMS keys."
- **KMS API GenerateDataKey.md** (https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.md): "To specify a KMS key, use its key ID, key ARN, alias name, or alias ARN. When using an alias name, prefix it with `"alias/"`. To specify a KMS key in a different AWS account, you must use the key ARN or alias ARN." Length constraints: 1–2048.
- DERIVED from the LocalStack captures: S3 expands a bare key ID into an ARN in the bucket's own region and account.

### 2.3 Encryption context (primary, UG UsingKMSEncryption.md)
- "By default, Amazon S3 uses the object or bucket Amazon Resource Name (ARN) as the encryption context pair … If you use SSE-KMS without enabling an S3 Bucket Key, the object ARN is used as the encryption context … If you use SSE-KMS and enable an S3 Bucket Key, the bucket ARN is used as the encryption context."
- "When it processes your `PUT` request, Amazon S3 appends the default encryption context of `aws:s3:arn` to the one that you provide."
- "AWS KMS uses the encryption context as additional authenticated data (AAD)".
- CloudTrail shows `"encryptionContext": {"aws:s3:arn": "arn:aws:s3:::…"}`.

KMS encrypt_context.md says: "When you use an encryption context to encrypt data, you must specify the same (an exact case-sensitive match) encryption context to decrypt the data." (GenerateDataKey.md).
- Its prose also contains a garbled sentence: "Encryption context keys and their values can be arbitrary strings with `aws`."
- UNVERIFIED: whether S3 rejects user-supplied keys starting with `aws:`.

**Invalid context, recorded** [secondary, HADOOP-19197 comment, 2024-06-07, https://www.mail-archive.com/common-issues@hadoop.apache.org/msg299928.html]:
> "The header 'x-amz-server-side-encryption-context' shall be Base64-encoded UTF-8 string holding JSON which represents a string-string map (Service: S3, Status Code: 400, …)"

The error code is UNVERIFIED. DERIVED: likely `InvalidArgument`. ceph has no context test, and botocore sends non-base64 values unchanged.

### 2.4 SigV4 and TLS requirement
- **Primary, API CopyObject.md:** "All GET and PUT requests for an object protected by AWS KMS will fail if they're not made via SSL or using SigV4."
- **Primary, CreateMultipartUpload.md and UG specifying-kms-encryption.md:** "All `GET` and `PUT` requests for an object protected by AWS KMS fail if you don't make them by using Secure Sockets Layer (SSL), Transport Layer Security (TLS), or Signature Version 4."
- **Doc inconsistency, UG UsingKMSEncryption.md:** "All `GET` and `PUT` requests for AWS KMS encrypted objects must be made using Secure Sockets Layer (SSL) or Transport Layer Security (TLS). Requests must also be signed using valid credentials, such as AWS Signature Version 4 (or AWS Signature Version 2)."
- **Recorded** [secondary: boto/botocore#377 (2014), aws/aws-sdk-net#2543 (2023, presigned GET), boto/boto3#2952 (2021), tailscale/tailscale#15195 (2025)]:
  ```
  <Error><Code>InvalidArgument</Code><Message>Requests specifying Server Side Encryption with AWS KMS managed keys require AWS Signature Version 4.</Message><ArgumentName>Authorization</ArgumentName><ArgumentValue>null</ArgumentValue>…
  ```
- PutBucketEncryption also "requires AWS Signature Version 4" (primary).
- UNVERIFIED: the answer to a SigV4 SSE-KMS request sent over plain HTTP.

### 2.5 Errors when KMS is unavailable or the key is invalid

**Documented error codes (primary, DG ErrorResponses.md), all "HTTP status code: 400 Bad Request":**
- `KMS.DisabledException`: "The request was rejected because the specified KMS key is not enabled."
- `KMS.InvalidKeyUsageException`: "The request was rejected for one of the following reasons: + The KeyUsage value of the KMS key is incompatible with the API operation. …"
- `KMS.KMSInvalidStateException`: "The request was rejected because the state of the specified resource is not valid for this request. …"
- `KMS.NotFoundException`: "The request was rejected because the specified entity or resource could not be found."

The "InvalidEncryptionMethod", "InvalidKMSEncryptionKeyId" and "MissingEncryptionMethod" rows on the same page belong to Storage Lens, not object SSE.

**Recorded messages, all 400:**

| Condition | Code | Message (verbatim) |
|---|---|---|
| Key ID `fake-key-id` (PutObject, CreateMultipartUpload, CopyObject) | `KMS.NotFoundException` | "Invalid keyId 'fake-key-id'" |
| UUID or ARN of a key that does not exist | `KMS.NotFoundException` | "Key 'arn:&lt;partition>:kms:&lt;region>:111111111111:key/134f2428-…' does not exist" |
| ARN of an existing key in another region | `KMS.NotFoundException` | "Invalid arn us-west-2" (in 2015 it was just "Invalid arn") |
| Key disabled (GET or PUT) | `KMS.DisabledException` | "arn:…:key/&lt;uuid> is disabled." |
| Key pending deletion (GET) | `KMS.KMSInvalidStateException` | "arn:…:key/&lt;uuid> is pending deletion." |
| Bucket default `KMSMasterKeyID "aws/s3"`, then PutObject | `KMS.NotFoundException` | "Invalid keyId aws/s3" |
| Key ID header without `aws:kms` | `InvalidArgument` | "Server Side Encryption with AWS KMS managed key requires HTTP header x-amz-server-side-encryption : aws:kms" |
| Missing kms:GenerateDataKey or kms:Decrypt | `AccessDenied` (403) | "Access Denied" |

Sources [all secondary]: LocalStack 2026-02-21 (`test_s3_sse_validate_kms_key`, `test_s3_sse_validate_kms_key_state`); aws/aws-cli#1517 (2015); aws/aws-cli#4507 (2019, reproduced 2024); aws/aws-cli#1431 (2015); aws/aws-cli#6713 (2022).

**Key validation at configuration time (primary, API PutBucketEncryption.md):**
> "If you use PutBucketEncryption to set your default bucket encryption to SSE-KMS, you should verify that your KMS key ID is correct. Amazon S3 doesn't validate the KMS key ID provided in PutBucketEncryption requests."

For directory buckets, "Amazon S3 validates the KMS key ID".

**KMS throttling (primary, KMS requests-per-second.md):**
> "Each time you upload or download an S3 object that's encrypted with SSE-KMS, Amazon S3 makes a `GenerateDataKey` (for uploads) or `Decrypt` (for downloads) request to AWS KMS on your behalf. These requests count toward your quota, so AWS KMS throttles the requests if you exceed a combined total of 5,500 (or 10,000 or 50,000 depending upon your AWS Region) uploads or downloads per second"

UNVERIFIED: the status and code S3 returns to its own client when KMS throttles or is unavailable. KMS itself returns `ThrottlingException`, and 500-class `KeyUnavailableException`, `DependencyTimeoutException` and `KMSInternalException`.

### 2.6 Bucket Keys (primary, UG bucket-key.md; the cryptography detail is in item 8)
> "When you configure an S3 Bucket Key, objects that are already in the bucket do not use the S3 Bucket Key."

> "Regardless of your S3 Bucket Key setting, you can include the `x-amz-server-side-encryption-bucket-key-enabled` header with a `true` or `false` value in your request, to override the bucket setting."

> "S3 Bucket Keys aren't supported for dual-layer server-side encryption with AWS Key Management Service (AWS KMS) keys (DSSE-KMS)."

The response header BK is returned by HeadObject, GetObject, UploadPartCopy, UploadPart and CompleteMultipartUpload.

### 2.7 DSSE-KMS (primary, UG UsingDSSEncryption.md and specifying-dsse-encryption.md)
> "*First layer:* Your data is encrypted using a unique data encryption key (DEK) generated by AWS KMS / *Second layer:* The already-encrypted data is encrypted again using a separate AES-256 encryption key managed by Amazon S3"

> "when DSSE-KMS is requested for the object, the S3 checksum that's part of the object's metadata is stored in encrypted form."

- It takes SSE plus KMS and CTX, with no BK. The context is always the object ARN.
- It launched on 13 June 2023 (UG document history).
- There is no AWS capture of any DSSE request or error, and ceph and LocalStack have no DSSE test [UNVERIFIED].

---

## 3. SSE-C

### 3.1 Headers and where each is required (primary)

**UG specifying-s3-c-encryption.md:**
- "`x-amz-server-side-encryption-customer-algorithm` Use this header to specify the encryption algorithm. The header value must be AES256."
- "`x-amz-server-side-encryption-customer-key` Use this header to provide the 256-bit, base64-encoded encryption key for Amazon S3 to use to encrypt or decrypt your data."
- "`x-amz-server-side-encryption-customer-key-MD5` Use this header to provide the base64-encoded 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error."
- The copy-source trio: "…This encryption key must be the one that you provided Amazon S3 when you created the source object. Otherwise, Amazon S3 cannot decrypt the object."

**Write operations** (same page): CopyObject, CreateMultipartUpload, CompleteMultipartUpload, POST Object, PutObject, UploadPart and UploadPartCopy.

**Multipart:**
- UG: "You specify these headers in the initiate request … and each subsequent part upload request … the encryption information must be the same as what you provided in the initiate multipart upload request."
- API UploadPart: "you must provide identical encryption information in each part upload"
- API UploadPart key doc: "This must be the same encryption key specified in the initiate multipart upload request."

**GET, HEAD, GetObjectAttributes (primary):**
> "If you encrypt an object by using server-side encryption with customer-provided encryption keys (SSE-C) when you store the object in Amazon S3, then when you GET the object, you must use the following headers: `x-amz-server-side-encryption-customer-algorithm` `x-amz-server-side-encryption-customer-key` `x-amz-server-side-encryption-customer-key-MD5`"

HeadObject and GetObjectAttributes say the same for metadata retrieval.

**CompleteMultipartUpload, ListParts, SelectObjectContent (primary):**
- CompleteMultipartUpload algorithm header: "This parameter is required only when the object was created using a checksum algorithm or if your bucket policy requires the use of SSE-C."
- CompleteMultipartUpload key and key-MD5: "This parameter is needed only when the object was created using a checksum algorithm."
- ListParts and SelectObjectContent use the same "needed only when … checksum algorithm" wording.
- UG, on bucket policies requiring SSE-C: "you must include the `x-amz-server-side-encryption-customer-algorithm` header in all multipart upload requests (CreateMultipartUpload, UploadPart, and CompleteMultipartUpload)."
- Secondary (LocalStack): an SSE-C multipart upload without a Create-time checksum algorithm completed with 200 without SSE-C headers on Complete, and ListParts worked without the key.

**CopyObject (primary):**
> "If the source object for the copy is stored in Amazon S3 using SSE-C, you must provide the necessary encryption information in your request so that Amazon S3 can decrypt the object for copying."

Secondary:
- A copy from an SSE-S3 source to an SSE-C target needed no source headers [minio/minio#6581, the real-AWS half, 2018].
- A copy from an SSE-C source with no target SSE produced an SSE-S3 object whose ETag is the MD5 [LocalStack].

**Presigned URLs (primary):**
> "When creating a presigned URL, you must specify the algorithm by using the `x-amz-server-side-encryption-customer-algorithm` header in the signature calculation. … you must provide all the encryption headers in your client application's request."

> "you can use presigned URLs for SSE-C objects only programmatically."

**Other scope limits (primary):**
- "SSE-C is not supported in the Amazon S3 Console."
- "If your bucket is versioning-enabled, each object version that you upload can have its own encryption key." LocalStack confirms per-version keys; see 3.5.
- "Server-side encryption with customer-provided keys (SSE-C) is not supported for default encryption." (UG default-bucket-encryption.md)
- "Objects encrypted with SSE-C do not support annotations." (UG serv-side-encryption.md)
- SSE-C objects cannot be torrented (botocore model doc).
- AWS Backup "does not offer support for backups of SSE-C-encrypted objects".
- UpdateObjectEncryption does not support SSE-C.
- Replication (primary, UG replication-config-for-kms-objects.md): "S3 Replication supports objects that are encrypted with SSE-C. … There aren't additional SSE-C permissions". DERIVED: S3 replicates SSE-C ciphertext without holding the key.

### 3.2 Key size, base64 and MD5
- The key is a 256-bit value, base64-encoded; the MD5 is base64(MD5 of the raw 32 bytes) (primary, above).
- DERIVED: checked locally against ceph's fixed keys and LocalStack's `SSECustomerKeyMD5`.
- botocore auto-computes the MD5 [third-party, handlers.py#L329-L369]. If `SSECustomerKey` is set and `SSECustomerKeyMD5` is not, it base64-encodes the key bytes and computes base64(MD5(raw)). If the caller supplies the MD5, both are sent unchanged. Full detail is in 6a.
- Wrong-length key: "The secret key was invalid for the specified algorithm." (3.5).

### 3.3 HTTPS requirement
Primary, UG ServerSideEncryptionCustomerKeys.md:
> "You must use HTTPS when specifying SSE-C headers on your requests. Amazon S3 rejects any requests made over HTTP when using SSE-C. For security considerations, we recommend that you consider any key that you erroneously send over HTTP to be compromised. Discard the key and rotate as appropriate."

SelectObjectContent: "For objects that are encrypted with customer-provided encryption keys (SSE-C), you must use HTTPS".

- UNVERIFIED: AWS's exact error for SSE-C over HTTP. No real-AWS capture was found.
- The only captured text is from IONOS's S3 [third-party, vmware-tanzu/velero#7837]: "InvalidArgument: Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection. status code: 400".
- ceph CI sets `rgw crypt require ssl: false` so it can test over HTTP.

### 3.4 What S3 stores and returns (primary)
> "Amazon S3 does not store the encryption key that you provide. Instead, it stores a randomly salted Hash-based Message Authentication Code (HMAC) value of the encryption key to validate future requests. The salted HMAC value cannot be used to derive the value of the encryption key or to decrypt the contents of the encrypted object. That means if you lose the encryption key, you lose the object." (UG specifying-s3-c-encryption.md)

> "When you upload an object specifying SSE-C, Amazon S3 uses the encryption key that you provide to apply AES-256 encryption to your data. Amazon S3 then removes the encryption key from memory. When you retrieve an object, you must provide the same encryption key as part of your request. Amazon S3 first verifies that the encryption key that you provided matches, and then it decrypts the object before returning the object data to you."

The response header docs:
- "If server-side encryption with a customer-provided encryption key was requested, the response will include this header to confirm the encryption algorithm that's used."
- "…to provide the round-trip message integrity verification of the customer-provided encryption key."

UNVERIFIED: the HMAC hash function, salt size and storage, and the cipher mode used for SSE-C.

Secondary (LocalStack): an SSE-C PutObject returns `{"ChecksumCRC32":"qIrZrA==","ChecksumType":"FULL_OBJECT","ETag":"\"b84c1aee7b2787381547d4277d117b01\"","SSECustomerAlgorithm":"AES256","SSECustomerKeyMD5":"JMwgiexXqwuPqIPjYFmIZQ=="}`. There is **no** `ServerSideEncryption` field.

### 3.5 Every documented and recorded SSE-C error
Unless noted, the sources are LocalStack's AWS-validated captures of 2026-02-21, taken before the default block [secondary].

| Condition | Status | Code (extra fields) | Message (verbatim) | Other sources |
|---|---|---|---|---|
| SSE-C plus `x-amz-server-side-encryption` | 400 | `InvalidArgument` (ArgumentName `x-amz-server-side-encryption`, ArgumentValue `AES256`) | "Server Side Encryption with Customer provided key is incompatible with the encryption method specified" | kafka-connect-storage-cloud#389 (2021, XML); checked before the algorithm |
| Algorithm not AES256 (PUT, GET, GetObjectAttributes, Copy source or target) | 400 | `InvalidEncryptionAlgorithmError` (ArgumentName `x-amz-server-side-encryption`, ArgumentValue the value) | "The Encryption request you specified is not valid. Supported value: AES256." | aws-sdk-go#2724, aws-sdk-js#1789. The copy-source case also names `x-amz-server-side-encryption` |
| Key or MD5 without an algorithm | 400 | `InvalidArgument` (ArgumentName `x-amz-server-side-encryption`) | "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm." | aws-sdk-php#1050 (2016, ArgumentValue `null`) |
| Algorithm without a key | 400 | `InvalidArgument` | "Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key." | |
| Key of the wrong length (10 bytes) with a matching MD5 | 400 | `InvalidArgument` | "The secret key was invalid for the specified algorithm." | aws-sdk-js-v3#5651 (2024) |
| MD5 does not match | 400 | `InvalidArgument` (ArgumentName `x-amz-server-side-encryption`, ArgumentValue `null`) | "The calculated MD5 hash of the key did not match the hash that was provided." | aws-sdk-go#1726 (2018), aws-cli#6906 (2022). Both sent 24-byte keys, so MD5 is checked **before** length (DERIVED) |
| MD5 header absent | – | – | UNVERIFIED on AWS. An AWS re:Post poster (2025-11-11) reports GET succeeds without it. MinIO (third-party) answers "…must provide the client calculated MD5 of the secret key." | |
| GET, HEAD, GetObjectAttributes or copy of an SSE-C object without the key | 400 | `InvalidRequest` | "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object." | boto3#1482 (2018): GetObject error, HeadObject via CloudTrail |
| Wrong key on GET (including another version's key) | **403** | `AccessDenied` | "Requests specifying Server Side Encryption with Customer provided keys must provide the correct secret key." | LocalStack also: a wrong-length key on a read gives this 403, not "secret key was invalid" (DERIVED) |
| SSE-C headers on a non-SSE-C object | 400 | `InvalidRequest` | "The encryption parameters are not applicable to this object." | boto3#982 (2017), aws-sdk-go#2724 (2019) |
| HEAD failures | 400 or 403 | no body | botocore synthesizes `{"Code":"400","Message":"Bad Request"}` | aws-cli#1820, aws-cli#6012 |
| UploadPart with SSE-C on a non-SSE-C upload, or without SSE-C on an SSE-C upload | 400 | `InvalidRequest` | "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters." | aws-sdk-ruby#1084 (2016) |
| UploadPart with a different key | 400 | `InvalidRequest` | "The provided encryption parameters did not match the ones used originally." | |
| SSE-C over HTTP | UNVERIFIED | UNVERIFIED | See 3.3 | |
| SSE-C write to a bucket that blocks SSE-C | 403 | `AccessDenied` | "User: arn:aws:iam::123456789012:user/MaryMajor is not authorized to perform: s3:PutObject on resource: "arn:aws:s3:::amzn-s3-demo-bucket1/object-name" because this bucket has blocked upload requests that specify Server Side Encryption with Customer provided keys (SSE-C). Please specify a different server-side encryption type" | **primary**: UG troubleshoot-403-errors.md. **secondary**: DataDog/stratus-red-team#946 (2026-08-25, CopyObject, same wording with `s3:CopyObject`) |

- DeleteObject of an SSE-C object needs no key: it returned 204 [secondary, LocalStack].
- ceph asserts 400, not 403, for a wrong key (item 6b). That disagrees with the capture.

### 3.6 The 2025–2026 change: SSE-C blocked by default

**Timeline (all primary):**
- **19 Nov 2025**, What's New "Amazon S3 adds new bucket-level setting to standardize encryption types used in your buckets": "Using the PutBucketEncryption API, you can disable server-side encryption with customer-provided keys (SSE-C) on specific buckets or in your AWS CloudFormation templates. This enhancement to the PutBucketEncryption API is now available in all AWS Regions."
  - botocore 1.41.0, 2025-11-19: "Adds support for blocking SSE-C writes to general purpose buckets." [third-party]
- **19 Nov 2025**, Storage Blog (Will Cavin), "Advanced notice: Amazon S3 to disable the use of SSE-C encryption by default for all new buckets and select existing buckets in April 2026":
  > "Starting on April 6, 2026, we will be changing how server-side encryption with customer-provided keys (SSE-C) is enabled for Amazon S3 buckets. With this change, SSE-C will be disabled by default on all new S3 general purpose buckets. Furthermore, SSE-C will also be disabled for all existing buckets in Amazon Web Services (AWS) Accounts that do not have any SSE-C encrypted data. This change will start on April 6, 2026 and will be rolled out to all AWS Regions within weeks."

  > "If you have SSE-C encrypted objects in any of your buckets in an account, then all your existing buckets will continue to support SSE-C encryption."

  > "All new buckets will now have SSE-C as BlockedEncryptionType by default."

  > "A subsequent attempt to upload an object with SSE-C encryption specified will be rejected with an HTTP 403 Access Denied error."

  - Its illustrative GetBucketEncryption response is dated `Mon, 6 Apr 2026` and shows `<SSEAlgorithm>aws:kms</SSEAlgorithm><KMSKeyID>arn:aws:kms:us-east-1:123456789012</KMSKeyID>` together with `<BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes>`. This is odd for a new bucket and uses `KMSKeyID`, not `KMSMasterKeyID`.
- **6 Apr 2026**, What's New "Amazon S3 starts rolling out new security best practice to new and existing buckets by default":
  > "As announced on November 19, 2025, Amazon S3 is now deploying a new default bucket security setting which will automatically disable server-side encryption with customer-provided keys (SSE-C) for all new general purpose buckets. For existing buckets in AWS accounts with no SSE-C encrypted objects, S3 will also disable SSE-C for all new write requests. For AWS accounts with SSE-C usage, S3 will not change the bucket encryption configuration on any of the existing buckets in those accounts. … in 37 AWS Regions including the AWS China and AWS GovCloud (US) Regions over the next few weeks."
- **UG default-s3-c-encryption-setting-faq.md:**
  > "This deployment completed in 37 AWS Regions, including the AWS China and AWS GovCloud (US) Regions, in April 2026."

  > "All newly created buckets in all AWS Regions except Middle East (Bahrain) and Middle East (UAE) will have SSE-C disabled by default."

**Semantics (primary, UG blocking-unblocking-s3-c-encryption-gpb.md):**
> "When SSE-C is blocked for a bucket, any `PutObject`, `CopyObject`, `PostObject`, Multipart Upload, or replication request that specifies SSE-C encryption will be rejected with an HTTP 403 `AccessDenied` error. Existing SSE-C encrypted objects in the bucket are unaffected, you can still read them with `GetObject` or `HeadObject` by providing the required SSE-C headers."

> "If a destination bucket for replication has SSE-C blocked and the source objects being replicated are encrypted with SSE-C, the replication will fail with an HTTP 403 `AccessDenied` error."

- Permissions: `s3:PutEncryptionConfiguration` to block or unblock, `s3:GetEncryptionConfiguration` to view.
- UploadPart and UploadPartCopy docs: "If you have server-side encryption with customer-provided keys (SSE-C) blocked for your general purpose bucket, you will get an HTTP 403 Access Denied error when you specify the SSE-C request headers while writing new data to your bucket."
- The rule body to allow SSE-C again is `{"Rules":[{"BlockedEncryptionTypes":{"EncryptionType":["NONE"]}}]}`. The docs also show a rule that combines `ApplyServerSideEncryptionByDefault` with `BlockedEncryptionTypes`.
- Doc slip: the FAQ calls it "the new `BlockedEncryptionTypes` header"; it is a body element.

**Evidence about state, all post-rollout:**
- **Secondary**, hashicorp/terraform-provider-aws#47320, opened 2026-04-07:
  - An existing configuration read back as `blocked_encryption_types = ["NONE"]`, which the reporter never set.
  - Maintainer acceptance-test output on 2026-04-08 (us-west-2 and us-east-1): a bucket the test created and then configured with AES256 and no `BlockedEncryptionTypes` read back as `blocked_encryption_types = ["SSE-C"]`. Fixed in PR #47359, released in provider v6.40.0.
  - DERIVED: GetBucketEncryption now always carries `BlockedEncryptionTypes`, as `NONE` or `SSE-C`. A PutBucketEncryption that omits the element does not clear the block. Capture dates: https://github.com/hashicorp/terraform-provider-aws/issues/47320.
- **Secondary**, INTENTIUS/choudoufu#1525 (2026-09-22, us-east-2): `{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"arn:aws:kms:us-east-2:&lt;acct redacted>:key/…"},"BucketKeyEnabled":true,"BlockedEncryptionTypes":{"EncryptionType":["SSE-C"]}}]}`.
- **Report, no raw capture**, Fog Security blog, 2026-04-27: "even if our bucket had objects stored with SSE-C encryption, we could still set SSE-C encryption to disabled."
- **UNVERIFIED:**
  - What DeleteBucketEncryption does to `BlockedEncryptionTypes`. It "resets the default encryption for the bucket as … (SSE-S3)"; whether the block survives is not documented or captured.
  - The GetBucketEncryption document of a never-configured post-April bucket, as opposed to a configured one.
  - Blocked-bucket answers for PostObject, multipart and replication (other than the documented 403).
- **ceph's view** [third-party]: the in-tree tests (item 6b) and RGW option `rgw_s3_block_sse_c_by_default` (default false) implement the same 403 `AccessDenied`. ceph's docs add: "An SSE-C multipart upload started before the block can no longer upload parts or complete; abort it instead." That is not stated by AWS [UNVERIFIED on AWS].

---

## 4. Bucket default encryption: PutBucketEncryption, GetBucketEncryption, DeleteBucketEncryption

### 4.1 Document shape (primary, API PutBucketEncryption.md)
```
PUT /?encryption HTTP/1.1
Content-MD5: …   x-amz-sdk-checksum-algorithm: …   x-amz-expected-bucket-owner: …
<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Rule>
      <ApplyServerSideEncryptionByDefault>
         <KMSMasterKeyID>string</KMSMasterKeyID>
         <SSEAlgorithm>string</SSEAlgorithm>
      </ApplyServerSideEncryptionByDefault>
      <BlockedEncryptionTypes>
         <EncryptionType>string</EncryptionType>
         ...
      </BlockedEncryptionTypes>
      <BucketKeyEnabled>boolean</BucketKeyEnabled>
   </Rule>
   ...
</ServerSideEncryptionConfiguration>
```

- `Rule`: "Type: Array of ServerSideEncryptionRule data types / Required: Yes".
- `ApplyServerSideEncryptionByDefault` (Required: No): "If a PUT Object request doesn't specify any server-side encryption, this default encryption will be applied."
- `SSEAlgorithm` (Required: Yes): "Valid Values: `AES256 | aws:fsx | aws:backup | aws:kms | aws:kms:dsse`".
- `KMSMasterKeyID`: "**General purpose buckets** - This parameter is allowed if and only if `SSEAlgorithm` is set to `aws:kms` or `aws:kms:dsse`."
- `BucketKeyEnabled`: "**General purpose buckets** - By default, S3 Bucket Key is not enabled."
- `BlockedEncryptionTypes` / `EncryptionType`: "Type: Array of strings / Valid Values: `NONE | SSE-C`". Also: "Currently, this parameter only supports blocking or unblocking server side encryption with customer-provided keys (SSE-C)."
- Response: "HTTP/1.1 200 … with an empty HTTP body."
- Also primary: "this action requires AWS Signature Version 4" and "Amazon S3 doesn't validate the KMS key ID provided in PutBucketEncryption requests" (general purpose buckets).
- botocore marks the operation `httpChecksum.requestChecksumRequired: true`; it sends `x-amz-checksum-crc32` [third-party].
- Doc slips: the PutBucketEncryption and GetBucketEncryption examples use `<KMSKeyID>`, not `<KMSMasterKeyID>`. The example titled "Setting SSE-KMS" uses `aws:kms:dsse`.

### 4.2 GetBucketEncryption and DeleteBucketEncryption (primary)
- **Get:** "Returns the default encryption configuration for an Amazon S3 bucket. By default, all buckets have a default encryption configuration that uses server-side encryption with Amazon S3 managed keys (SSE-S3). This operation also returns the BucketKeyEnabled and BlockedEncryptionTypes statuses." Response 200 with the same XML.
- **Delete:** "This implementation of the DELETE action resets the default encryption for the bucket as server-side encryption with Amazon S3 managed keys (SSE-S3)." Response "HTTP/1.1 204".
- **Errors:** DG ErrorResponses.md still lists "`ServerSideEncryptionConfigurationNotFoundError` — The server-side encryption configuration was not found. — 400 Bad Request". The pre-2023 captures show status 404; see 4.4.

### 4.3 Validation errors (secondary, LocalStack `test_s3_default_bucket_encryption_exc`, 2026-02-21)

| Request | Answer |
|---|---|
| `Rules: []` | 400 `{"Code":"MalformedXML","Message":"The XML you provided was not well-formed or did not validate against our published schema"}` |
| Two rules | 400, the same `MalformedXML` |
| AES256 with `KMSMasterKeyID` | 400 `{"ArgumentName":"ApplyServerSideEncryptionByDefault","Code":"InvalidArgument","Message":"a KMSMasterKeyID is not applicable if the default sse algorithm is not aws:kms or aws:kms:dsse"}` |
| Get, Put or Delete on a missing bucket | 404 `NoSuchBucket` "The specified bucket does not exist". The Put with `Rules: []` also got 404, so the existence check comes before XML validation (DERIVED) |
| PutBucketEncryption | 200 |
| DeleteBucketEncryption, including a repeat | 204 both times |
| SSE-S3 default with `BucketKeyEnabled: true` | accepted; objects report no BK |

No capture exists of an invalid `SSEAlgorithm` or of an invalid `EncryptionType` [UNVERIFIED].

### 4.4 What GetBucketEncryption returns for a bucket never configured
- **Before 2023** [secondary]: "An error occurred (ServerSideEncryptionConfigurationNotFoundError) when calling the GetBucketEncryption operation: The server side encryption configuration was not found" (boto/boto3#1899, 2019). terraform-provider-aws#24232 (2022) shows "status code: 404".
- **2023 to early 2026** [secondary]:
  - ceph/s3-tests#613 (2025-01-23, us-east-1): `{"ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}, "BucketKeyEnabled": false}]}}`. The reporter adds: "This is also true after you delete the bucket encryption - the default will be returned and not an Error".
  - LocalStack, 2026-02-21, gives the identical document, with no `BlockedEncryptionTypes`.
- **After April 2026:** see 3.6. The configurations read back carry `BlockedEncryptionTypes` (`NONE` or `SSE-C`) [secondary]. DERIVED: a never-configured new bucket returns AES256 plus BucketKeyEnabled false plus `BlockedEncryptionTypes SSE-C` (Bahrain and UAE excepted). No raw capture exists [UNVERIFIED].
- No post-2023 capture of `ServerSideEncryptionConfigurationNotFoundError` on a general purpose bucket was found. ceph still asserts it (item 6b, a conflict).

---

## 5. Object metadata, ETags, checksums, and what list and attribute calls report

### 5.1 ETag (primary)
**API Object.md:**
> "The ETag may or may not be an MD5 digest of the object data. Whether or not it is depends on how the object was created and how it is encrypted as described below: + Objects created by the PUT Object, POST Object, or Copy operation, or through the AWS Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that are an MD5 digest of their object data. + Objects created by the PUT Object, POST Object, or Copy operation, or through the AWS Management Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5 digest of their object data. + If an object is created by either the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest, regardless of the method of encryption."

UG checking-object-integrity-upload.md says the same. DSSE-KMS is not mentioned (UNVERIFIED; DERIVED likely non-MD5).

Other primary statements:
- PutObject SSE-C example: "In the response, Amazon S3 returns the encryption algorithm and MD5 of the encryption key that you specified when uploading the object. The ETag that is returned is not the MD5 of the object."
- POST Object: "If you use SSE-C, the `ETag` value that Amazon S3 returns in the response is not the MD5 of the object."
- Replication: "If objects in the source bucket are not encrypted, the replica objects … are encrypted by using the default encryption settings of the destination bucket. As a result, the entity tags (ETags) of the source objects differ from the ETags of the replica objects."
- UpdateObjectEncryption preserves the ETag (1.6).
- Content-MD5:
  - UG checking-object-integrity.md: "The legacy `Content-MD5` header remains available for single part uploads using SSE-S3 encryption."
  - UG checking-object-integrity-upload.md: "The `content-MD5` header is only available using the S3 ETag for objects uploaded in a single part upload (`PUT` operation) that uses the SSE-S3 encryption."
  - It is ambiguous whether S3 still validates Content-MD5 on SSE-KMS or SSE-C PUTs [UNVERIFIED].

**DERIVED from the LocalStack captures, computed locally:**
- SSE-S3 ETags equal the content MD5.
- The same body under the same SSE-C key produced three different ETags (`b84c1aee…`, `b68bda52…`, `b876fdc9…`). Identical SSE-C parts got different ETags. So SSE-C and SSE-KMS ETags are not deterministic in the content.
- Multipart ETags still follow MD5(concatenated binary part ETags) + "-N" for both KMS (`93a0cb8e…-1`) and SSE-C (`2d296eb6…-3`).
- AWS SDK for Java v1 [third-party] claims the ETag is "the MD5 of the ciphertext". That is UNVERIFIED.

### 5.2 Checksums
- **Primary, UG UsingKMSEncryption.md:** "when SSE-KMS is requested for the object, the S3 checksum (as part of the object's metadata) is stored in encrypted form." DSSE says the same.
- **Primary, API HeadObject `x-amz-checksum-mode`:** "If you enable checksum mode and the object is uploaded with a checksum and encrypted with an AWS Key Management Service (AWS KMS) key, you must have permission to use the `kms:Decrypt` action to retrieve the checksum."
- DERIVED: for SSE-C, the rule that the key is "needed only when the object was created using a checksum algorithm" on CompleteMultipartUpload, ListParts and Select implies SSE-C checksums are also stored encrypted.
- DERIVED from captures: every recorded CRC32 and CRC64NVME is over the **plaintext**, for all three modes.
  - SSE-C with no client checksum still stores a default `ChecksumCRC64NVME` [secondary, LocalStack `test_put_object_default_checksum_with_sse_c`].
- **Third-party:** ceph's in-tree `test_multipart_sse_c_checksum_complete` expects a checksummed SSE-C Complete sent without the key to return 400 `InvalidArgument`.

### 5.3 What list and attribute calls report
- **ListObjects / ListObjectsV2** (primary, API Object.md): fields ChecksumAlgorithm, ChecksumType, ETag, Key, LastModified, Owner, RestoreStatus, Size and StorageClass. There is no SSE field.
  - UG serv-side-encryption.md: "when you list objects in your bucket, the list API operations return a list of all objects, regardless of whether they are encrypted."
  - ceph asserts `Size` is the plaintext size for SSE-C multipart [third-party].
- **HeadObject / GetObject**: SSE, KMS, BK and the SSE-C algorithm and key-MD5 headers. Never the context header (1.2).
- **GetObjectAttributes** (primary): the response has ETag, Checksum, ObjectParts, StorageClass and ObjectSize, and no SSE fields. SSE-C objects require the C-trio.
  - Secondary (LocalStack): with the key it returns `{"ETag":"b84c1aee…","ObjectSize":9,…}`; the ETag is unquoted.
- **S3 Inventory** (primary, UG storage-inventory.md): "Encryption status … Set to `SSE-S3`, `SSE-KMS`, `DSSE-KMS`, `SSE-C`, or `NOT-SSE`" and "S3 Bucket Key status – Set to `ENABLED` or `DISABLED`".
- **Metadata** (primary): "Server-side encryption encrypts only the object data, not the object metadata."

---

## 6. botocore model and customizations; ceph s3-tests

### 6a. botocore (third-party)
Model: https://github.com/boto/botocore/blob/358f8eec8c76201bb1a7a35644abcbc9036de7ed/botocore/data/s3/2006-03-01/service-2.json. The metadata is `"signatureVersion":"s3"` with `"auth":["aws.auth#sigv4"]`.

**Shapes (line numbers in that file):**
- `ServerSideEncryption` enum `["AES256","aws:fsx","aws:backup","aws:kms","aws:kms:dsse"]` (#L13151).
- `SSECustomerKey` (#L12950), `SSEKMSKeyId` (#L12987), `SSEKMSEncryptionContext` (#L12983) and `CopySourceSSECustomerKey` (#L3336) are `"sensitive":true`. No shape has length or pattern constraints.
- `BucketKeyEnabled` is a boxed boolean.
- Bucket configuration:
  - `ServerSideEncryptionConfiguration` (#L13176): flattened `Rule` list.
  - `ServerSideEncryptionRule` (#L13188): `ApplyServerSideEncryptionByDefault`, `BucketKeyEnabled`, `BlockedEncryptionTypes`.
  - `ServerSideEncryptionByDefault` (#L13161): required `SSEAlgorithm`, plus `KMSMasterKeyID`.
  - `BlockedEncryptionTypes` (#L2039): flattened `EncryptionType` list, enum `["NONE","SSE-C"]` (#L4763).
- UpdateObjectEncryption:
  - `ObjectEncryption` (#L9639) is a union with the single member `SSEKMS`, locationName `SSE-KMS`.
  - `SSEKMSEncryption` (#L12967): `KMSKeyArn` (required, min 20, max 2048, the pattern in 1.6) and `BucketKeyEnabled`.
- Error shape `EncryptionTypeMismatch` (#L4778, 400): "The existing object was created with a different encryption type. Subsequent write requests must include the appropriate encryption parameters in the request or while creating the session." It is listed only on PutObject. DERIVED: it concerns appends to directory-bucket objects.
- Absent from the model: any `KMS.*` code, "secret key was invalid", "calculated MD5 hash", `ServerSideEncryptionConfigurationNotFoundError`, and the salted HMAC.

**Operation/member matrix:** identical to 1.2, plus GetObjectAnnotation and PutObjectAnnotation, which return SSE. POST is not modeled. Every doc string quoted in items 1–5 also appears in the model, lightly reworded ("Amazon Web Services KMS").

**When each feature entered the model** (CHANGELOG plus a model diff):

| Change | botocore release | Date |
|---|---|---|
| `aws:kms:dsse` | 1.29.153 | 2023-06-13 |
| SSE-KMS for directory buckets | 1.35.22 | 2024-09-18 |
| `aws:fsx` | 1.38.44 | 2025-06-25 |
| `BlockedEncryptionTypes` | 1.41.0 | 2025-11-19 |
| UpdateObjectEncryption | 1.42.37 | 2026-01-28 |
| `aws:backup` | 1.43.66 | 2026-08-06 |

**SSE-C auto-computation** (https://github.com/boto/botocore/blob/358f8eec8c76201bb1a7a35644abcbc9036de7ed/botocore/handlers.py#L329-L369):
```
def sse_md5(params, **kwargs):
    """
    S3 server-side encryption requires the encryption key to be sent to the
    server base64 encoded, as well as a base64-encoded MD5 hash of the
    encryption key. This handler does both if the MD5 has not been set by
    the caller.
    """
    _sse_md5(params, 'SSECustomer')
...
def _sse_md5(params, sse_member_prefix='SSECustomer'):
    if not _needs_s3_sse_customization(params, sse_member_prefix):
        return
    sse_key_member = sse_member_prefix + 'Key'
    sse_md5_member = sse_member_prefix + 'KeyMD5'
    key_as_bytes = params[sse_key_member]
    if isinstance(key_as_bytes, str):
        key_as_bytes = key_as_bytes.encode('utf-8')
    md5_val = get_md5(key_as_bytes, usedforsecurity=False).digest()
    key_md5_str = base64.b64encode(md5_val).decode('utf-8')
    key_b64_encoded = base64.b64encode(key_as_bytes).decode('utf-8')
    params[sse_key_member] = key_b64_encoded
    params[sse_md5_member] = key_md5_str

def _needs_s3_sse_customization(params, sse_member_prefix):
    return (
        params.get(sse_member_prefix + 'Key') is not None
        and sse_member_prefix + 'KeyMD5' not in params
    )
```

- **Registration** (handlers.py#L1612-L1622): `before-parameter-build.s3.` for HeadObject, GetObject, PutObject, CopyObject, CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload and SelectObjectContent calls `sse_md5`. CopyObject and UploadPartCopy also call `copy_source_sse_md5`.
- **Gap (DERIVED, reproduced locally):** GetObjectAttributes, ListParts and WriteGetObjectResponse are not registered. They send a raw string key unencoded and send no MD5.
- **Docs customization:** handlers.py#L1685-L1696 marks `SSECustomerKeyMD5` and `CopySourceSSECustomerKeyMD5` as "automatically populated" on every S3 operation.
- **FIPS:** `get_md5` raises `MD5UnavailableError` where MD5 is blocked.
- **No client-side validation:** botocore sends `ServerSideEncryption='bogus'`, a non-base64 context and 16-byte keys unchanged. The server must validate.
- **Local serialization check** (DERIVED; nothing was sent over the network): key `b'a'*32` produced `x-amz-server-side-encryption-customer-key: YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE=` and `…-customer-key-MD5: Xsqb0+sHwAbNQ65I395/0w==`.
- **Consequence for tests:** because of this encoding, passing an already-base64 key without an MD5 double-encodes it (44 bytes). Several "no MD5" captures in the wild are really this bug (LocalStack, and the AWS re:Post thread).
- **Signing:**
  - botocore uses `s3v4` for S3 by default (client.py#L886-L887).
  - Presigned URLs default to SigV2 in legacy regions (client.py#L424-L499; signers.py#L762-L771).
  - SigV2 query auth moves every `x-amz-*` header into the query string, including the SSE-C key and MD5 (auth.py#L1055-L1085).
  - A SigV4 presign signs `x-amz-server-side-encryption-customer-{algorithm,key,key-md5}` as headers.
- **Unit test** tests/unit/test_handlers.py#L1250-L1314 (`TestSSEMD5`) uses a mocked MD5: key `'bar'` gives `SSECustomerKey == 'YmFy'` and `SSECustomerKeyMD5 == 'Zm9v'`.
- **s3transfer** 379338c7 [third-party]:
  - `ALLOWED_DOWNLOAD_ARGS` carries the SSE-C trio.
  - Upload and copy arguments carry `ServerSideEncryption`, `SSECustomer*`, `SSEKMSKeyId` and `SSEKMSEncryptionContext`. `BucketKeyEnabled` is **not** allowed and raises "Invalid extra_args key".
  - UploadPart and CompleteMultipartUpload receive only the SSE-C trio.
  - Copies map `CopySourceSSECustomer*` onto the source `head_object`.

### 6b. ceph s3-tests (third-party)
Two copies are cited:
- **S** = https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py (standalone).
- **C** = https://github.com/ceph/ceph/blob/d732a9bd76697df3927f61ef37c40c9ec6d81913/src/test/rgw/s3-tests/s3tests/functional/test_s3.py (in-tree, what CI runs).

All SSE tests are in `test_s3.py`. Collected counts:
- S: 147 tests carry `encryption`, `sse_s3` or `bucket_encryption`. 36 are `fails_on_aws`.
- C: 154 tests, with `sse_c_block_by_default` added.

A test **not** marked `fails_on_aws` is one ceph claims passes on AWS; it remains a third-party claim.

**Configuration:**
- The markers in pytest.ini are bare names: `bucket_encryption`, `encryption`, `fails_on_aws`, `fails_on_dbstore`, `fails_on_rgw`, `sse_s3`, and in C also `sse_c_block_by_default` and `checksum`.
- s3tests.conf.SAMPLE L49-50: `## replace with key id obtained when secret is created, or delete if KMS not tested` / `#kms_keyid = 01234567-89ab-cdef-0123-456789abcdef`. `is_secure = False` at L10-11.
- `__init__.py` L225-233 defaults `kms_keyid` to `'testkey-1'` and `kms_keyid2` to `'testkey-2'`. DERIVED: every "skip if kms_keyid is None" guard is dead code, so KMS tests assume a KMS backend holding those keys.
- ceph CI configures `rgw crypt s3 kms backend: testing` with `testkey-1=YmluCmJvb3N0CmJvb3N0LWJ1aWxkCmNlcGguY29uZgo= testkey-2=aWIKTWFrZWZpbGUKbWFuCm91dApzcmMKVGVzdGluZwo=` and `rgw crypt require ssl: false`.
- Clients use SigV4 (`Config(signature_version='s3v4')`). POST tests sign their policy with V2 HMAC-SHA1.
- Errors are read from `response['Error']['Code']` and `['ResponseMetadata']['HTTPStatusCode']`.

**Literal values:**
- Key A `'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs='`, MD5 `'DWygnHRtgiJ77HCm+1rvHw=='`.
- Key B `'6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4='`, MD5 `'arxBvwY2V4SiOne6yppVPQ=='`.
- Bad MD5 `'AAAAAAAAAAAAAAAAAAAAAA=='`.
- Both keys decode to 32 bytes and both MD5s check out (DERIVED).

**Helpers (S line / C line):**
- `_test_encryption_sse_customer_write` (L9485 / L9753)
- `_multipart_upload_enc` (L10846 / L11551)
- `_check_content_using_range_enc` (L10880 / L11585)
- `_test_sse_kms_customer_write` (L11206 / L11911)
- `_put_bucket_encryption_s3` and `_put_bucket_encryption_kms` (L14494, L14510 / L15213, L15229)
- `_test_sse_s3_default_upload` (L14631 / L15492)
- `_test_sse_kms_default_upload` (L14678 / L15539)
- `_test_sse_s3_encrypted_upload` (L14899 / L15760)
- `_copy_enc_source_modes` and `_copy_enc_dest_modes` (L20415, L20448 / L21558, L21591). The `unencrypted` mode is marked `fails_on_aws` with no stated reason.
- `_put_bucket_blocked_encryption_types` (C only, L15350).

**Tests.** Format: name — S line / C line — markers — what it sends → what it asserts.

SSE-C (no KMS backend needed):
- **test_encrypted_transfer_1b / _1kb / _1MB / _13b** — L10688–10706 / L11393–11411 — encryption, fails_on_dbstore — round trip with key A → body matches.
- **test_encryption_sse_c_method_head** — L10711 / L11416 — encryption — HEAD without the key → **400**; with the key → 200.
- **test_encryption_sse_c_present** — L10736 / L11441 — GET without the key → **400**.
- **test_encryption_sse_c_other_key** — L10756 / L11461 — GET with key B → **400**. This conflicts with the AWS capture, which is 403 `AccessDenied`.
- **test_encryption_sse_c_invalid_md5** — L10783 / L11488 — → **400**.
- **test_encryption_sse_c_no_md5** — L10801 / L11506 — → any ClientError.
- **test_encryption_sse_c_no_key** — L10816 / L11521 — → any ClientError.
- **test_encryption_key_no_sse_c** — L10830 / L11535 — key and MD5 without the algorithm → **400**.
- **test_encryption_sse_c_multipart_upload** — L10897 / L11602 — encryption, fails_on_dbstore — 30 MiB in 5 MiB parts with the key on Create, each Part and Complete; List `Size == objlen`; metadata, content-type, body and ranged reads match.
- **test_encryption_sse_c_unaligned_multipart_upload** — L10943 / L11648 — the same with parts of 5 MiB + 1 byte.
- **test_encryption_sse_c_multipart_invalid_chunks_1** — L10990 / L11723 — encryption, **fails_on_rgw** — UploadPart with key B → **400**.
- **test_encryption_sse_c_multipart_invalid_chunks_2** — L11018 / L11723 — encryption, **fails_on_rgw** — UploadPart with a bad MD5 → **400**.
- **test_encryption_sse_c_multipart_bad_download** — L11044 / L11749 — wrong-key GET → **400**.
- **test_encryption_sse_c_post_object_authenticated_request** — L11091 / L11796 — POST with the SSE-C form fields → **204**.
- **test_encryption_sse_c_enforced_with_bucket_policy** — L11146 / L11851 — Null-condition Deny → plain PUT **403**.
- **test_encryption_sse_c_deny_algo_with_bucket_policy** — L11176 / L11881 — `AES192` → **403**.
- **test_multipart_sse_c_get_part** — L6669 / L6863 — PartNumber=1 before Complete → **404 `NoSuchKey`**; PartNumber=5 → **400 `InvalidPart`**; part ETag equals the Complete ETag.
- **test_non_multipart_sse_c_get_part** — L6786 / L7014 — PartNumber=2 → **400 `InvalidPart`**.
- **test_get_sse_c_encrypted_object_attributes** — L19285 / L20357 — GetObjectAttributes without the key → **400**; with the key, ETag equals the PUT ETag.
- **test_multipart_sse_c_checksum_complete** — C only, L6919 — encryption, checksum, fails_on_dbstore — Complete without the key → **400 `InvalidArgument`**; with the key → `ChecksumSHA256 'Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1'`.

SSE-KMS (tests marked "KMS" need `testkey-1` / `testkey-2`):
- **test_sse_kms_transfer_1b / _1kb / _1MB / _13b** — L11456–11483 / L12179–12206 — encryption, fails_on_dbstore — KMS.
- **test_sse_kms_method_head** — L11231 / L11936 — HEAD shows `aws:kms` and the key ID; sending the SSE headers on HEAD → **400** — KMS.
- **test_sse_kms_present** — L11258 / L11963 — KMS.
- **test_sse_kms_no_key** — L11278 / L11983 — `aws:kms` with no key ID → any error. This conflicts with AWS, which uses the `aws/s3` fallback (documented and captured).
- **test_sse_kms_not_declared** — L11294 / L11999 — key ID without `aws:kms` → **400**.
- **test_sse_kms_multipart_upload** — L11312 / L12035 — KMS.
- **test_sse_kms_multipart_invalid_chunks_1 / _2** — L11357 and L11384 / L12080 and L12107 — no assertion at all — KMS.
- **test_sse_kms_post_object_authenticated_request** — L11410 / L12133 — 204 — KMS.
- **test_sse_kms_read_declare** — L11491 / L12214 — GET with SSE-KMS headers on a plain object → **400**.
- **test_sse_empty_algorithm** — C only, L12016 — empty SSE value → **400 `InvalidArgument`**.

Conflicting headers (all `encryption`, all expect **400 `InvalidArgument`**):
- **test_put_obj_enc_conflict_c_s3** — L12900 / L13622 — AES256 plus SSE-C.
- **test_put_obj_enc_conflict_c_kms** — L12923 / L13645 — `aws:kms` plus SSE-C.
- **test_put_obj_enc_conflict_s3_kms** — L12950 / L13672 — AES256 plus a KMS key ID.
- **test_put_obj_enc_conflict_bad_enc_kms** — L12974 / L13696 — `'aes:kms'`.

Bucket policies (all → **403** when denied):
- **test_bucket_policy_put_obj_s3_noenc** — L13000 / L13722 — also checks the AES256 echo.
- **test_bucket_policy_put_obj_s3_incorrect_algo_sse_s3** — L13029 / L13751.
- **test_bucket_policy_put_obj_s3_kms** — L13057 / L13779.
- **test_bucket_policy_put_obj_kms_noenc** — L13103 / L13825 — KMS.
- **test_bucket_policy_put_obj_kms_s3** — L13149 / L13871.

Bucket encryption:
- **test_put_bucket_encryption_s3 / _kms** — L14532, L14538 / L15251, L15257 — → 200.
- **test_get_bucket_encryption_s3 / _kms** — L14545, L14565 / L15264, L15284 — Get on a new bucket → `ServerSideEncryptionConfigurationNotFoundError`. **This conflicts with AWS since 2023** (see 4.4 and ceph/s3-tests issue #613). After a Put, the algorithm and key ID are echoed.
- **test_delete_bucket_encryption_s3 / _kms** — L14589, L14611 / L15308, L15330 — Delete → 204; a later Get → NotFound (the same conflict).

Default encryption applied (markers encryption, bucket_encryption, sse_s3, fails_on_dbstore):
- **test_sse_s3_default_upload_1b / 1kb / 1mb / 8mb** — L14654–14675 / L15515–15536.
- **test_sse_kms_default_upload_*** — L14706–14727 / L15567–15588 — KMS.
- **test_sse_s3_default_method_head** — L14736 / L15597 — HEAD with `AES256` → **400**.
- **test_sse_s3_default_multipart_upload** — L14761 / L15622.
- **test_sse_s3_default_post_object_authenticated_request** — L14807 / L15668 — 204.
- **test_sse_kms_default_post_object_authenticated_request** — L14852 / L15713 — 204 — KMS.
- **test_sse_s3_encrypted_upload_*** — L14918–14936 / L15779–15797.

Copy:
- **test_copy_enc** — L20748 / L21883 — 64 cases: 4 source modes × 4 destination modes × 4 sizes. The destination's SSE headers are checked on the Copy response and on GET. An SSE-C destination echoes `-customer-key-md5 'arxBvwY2V4SiOne6yppVPQ=='`.
- **test_copy_part_enc** — L20703 / L21838 — for an SSE-C destination, UploadPartCopy or UploadPart without, or with the wrong, destination key → **400**, and Complete with the wrong key → **400**. Complete with key B must echo the SSE-C headers. That echo is not in AWS's response syntax (UNVERIFIED).
- **test_lifecycle_transition_encrypted** — L20820 / L21955 — `fails_on_aws`; skipped by default.

Logging:
- **test_put_bucket_logging_errors** — L16526 / L17497 — an SSE-S3-default target → `InvalidArgument`.
- **test_put_bucket_logging_blocked_encryption_target** — C only, L17607 — blocked types SSE-C or NONE → 200.

SSE-C blocking (C only, from ceph PR #71247, merged 2026-09-28):
- **test_bucket_blocks_sse_c_by_default** — L15374 — marker `sse_c_block_by_default` — a new bucket's Get → `EncryptionType == ['SSE-C']`; SSE-C PUT → **403 `AccessDenied`**; after `['NONE']` the PUT succeeds.
- **test_bucket_block_sse_c** — L15392 — after blocking, SSE-C PutObject and CreateMultipartUpload → 403 `AccessDenied`; plain PUT succeeds; the old object is still readable.
- **test_bucket_block_sse_c_copy** — L15425 — CopyObject → 403 `AccessDenied`.
- **test_bucket_unblock_sse_c** — L15444.
- **test_bucket_block_sse_c_multipart** — L15461 — for an upload started before the block, UploadPart → 403 and Complete → 403; Abort succeeds.

**Error codes the suite ever asserts:** `InvalidArgument`, `AccessDenied`, `InvalidPart`, `NoSuchKey`, `ServerSideEncryptionConfigurationNotFoundError`. Everything else is checked by status only.

**Not covered by ceph at all:** the encryption context, Bucket Keys, `aws:kms:dsse` and `aws:fsx`, SSE-C over HTTP, wrong-length SSE-C keys, PutBucketEncryption validation, and KMS key errors on PutObject.

**Related ceph notes:**
- ceph tracker 79606 claims "The AWS S3 API does not define SSE-C headers on CompleteMultipartUpload". The AWS API page contradicts this (3.1).
- RGW's own error for an SSE-C Complete without the key was "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm."

---

## 7. LocalStack snapshots and issue-tracker captures

- LocalStack is pinned at `8b9a79f05846835cf4dff63ab7eefdde9df83783` (archived 2026-03-23). All SSE tests are `@markers.aws.validated`, with `last_validated_date` 2026-02-21, re-recorded in PR #13824. These predate the April 2026 SSE-C block.
- Snapshot transformers replace the account (`111111111111`), region, key IDs, bucket names, dates, upload and version IDs, and copy-ETags. Every other value is verbatim AWS output. `skip_snapshot_verify($..ETag)` only affects LocalStack's own runs.
- Base URL: https://github.com/localstack/localstack/blob/8b9a79f05846835cf4dff63ab7eefdde9df83783/

**Test locations:**
- `tests/aws/services/s3/test_s3_api.py`, class `TestS3BucketEncryption` (L1200):
  - `test_s3_default_bucket_encryption` (L1201)
  - `_exc` (L1215)
  - `test_s3_bucket_encryption_sse_s3` (L1276)
  - `_sse_kms` (L1307)
  - `_sse_kms_aws_managed_key` (L1367)
  - Snapshots in `test_s3_api.snapshot.json`.
- `tests/aws/services/s3/test_s3.py`:
  - `test_copy_object_kms` (L299)
  - `test_s3_copy_object_in_place_with_encryption` (L1872)
  - `test_copy_in_place_with_bucket_encryption` (L1937)
  - `test_s3_sse_validate_kms_key` (L4856)
  - `test_s3_sse_validate_kms_key_state` (L5007)
  - `test_s3_multipart_upload_sse` (L5649)
  - `test_s3_sse_bucket_key_default` (L5692)
  - `test_s3_sse_default_kms_key` (L5752)
  - `test_presigned_url_v4_signed_headers_in_qs` (L7740)
  - Class `TestS3SSECEncryption` (L12057): `test_put_object_lifecycle_with_sse_c` (L12070), `test_put_object_validation_sse_c` (L12116), `test_object_retrieval_sse_c` (L12203), `test_copy_object_with_sse_c` (L12308), `test_multipart_upload_sse_c` (L12405), `test_multipart_upload_sse_c_validation` (L12485), `test_sse_c_with_versioning` (L12561), `test_put_object_default_checksum_with_sse_c` (L12638).

**The recorded answers** are merged into items 2.1, 2.5, 3.5, 4.3, 4.4 and 5 above [all secondary]. Additional captures:
- **Illegal copy in place** [secondary]: `test_s3_copy_object_in_place` → 400 `{"Code":"InvalidRequest","Message":"This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."}`.
  - DERIVED from `test_s3_copy_object_in_place_with_encryption` and `test_copy_in_place_with_bucket_encryption`:
    - An in-place copy naming any SSE header is allowed, even when the value is unchanged.
    - An in-place copy with no parameters is allowed when the bucket has an *explicitly configured* default encryption.
    - LocalStack's code comment says: "S3 will allow copy in place if the bucket has encryption configured".
- **Copy to SSE-KMS** [secondary]: CopyObject to SSE-KMS with BucketKeyEnabled returns `{"BucketKeyEnabled":true,"CopyObjectResult":{"ChecksumCRC32":"DUoRhQ==","ChecksumType":"FULL_OBJECT",…},"SSEKMSKeyId":…,"ServerSideEncryption":"aws:kms"}`.
- **KMS multipart** [secondary]:
  - UploadPart returns `{"BucketKeyEnabled":true,"ChecksumCRC32":"KHcEKQ==","ETag":"\"1a9f0234d9c670a8c2f8b1ceda267641\"","SSEKMSKeyId":…,"ServerSideEncryption":"aws:kms"}`.
  - Complete returns `ETag "\"93a0cb8ec20934211e19adabef9a6407-1\""` plus BucketKeyEnabled, SSEKMSKeyId and ServerSideEncryption.
- **SSE-C multipart** [secondary]: Create and UploadPart echo the algorithm and key-MD5. Complete, sent without the key, returned 200 with no SSE-C fields and ETag `"2d296eb6540ecd21430c385336c5dc9c-3"`.
- **Presigned SSE header** [secondary, AWS-validated, assertions only]: AWS accepts `x-amz-server-side-encryption` as a SigV4 query parameter. botocore does not hoist it into presigned URLs; AWS SDK JS v2 does.
- **Presigned SSE-C, JS v3** [secondary, aws-sdk-js-v3#6978, 2025-03-19]: `<Error><Code>AccessDenied</Code><Message>There were headers present in the request which were not signed</Message><HeadersNotSigned>x-amz-server-side-encryption</HeadersNotSigned>…`.

**LocalStack's own code** [third-party, `localstack-core/localstack/services/s3/`]:
- `validation.py` L441-499 `validate_sse_c` copies AWS's six SSE-C messages. It checks key length **before** MD5, which is the opposite of the order derived from the AWS captures.
- A comment there says the ArgumentName is "weirdly … wrong, it should be `x-amz-server-side-encryption-customer-key-MD5`".
- `utils.py` L643-715 holds the KMS messages ("Invalid arn {key_region}", "{Arn} is pending deletion.", "{Arn} is disabled.").
- `provider.py`: `copy_object` answers a wrong source SSE-C key with `AccessDenied("Access Denied")` (L1568); no AWS capture exists for this. After `delete_bucket_encryption`, `get_bucket_encryption` returns empty; AWS resets to SSE-S3.

**Other third-party behaviour:**
- MinIO requires the MD5 ("…must provide the client calculated MD5 of the secret key."). It also rejected the SSE-S3→SSE-C copy that AWS accepts.
- Ceph RGW and Linode answer a wrong key with 400 `InvalidArgument` "…must provide an appropriate secret key.", where AWS answers 403 "…correct secret key."
- IONOS requires HTTPS with 400 `InvalidArgument` (3.3).
- AWS SDK JS v3 requires the BK response value in lowercase (`false`) (LocalStack#6840).

---

## 8. Cryptography background (primary sources)

### 8.1 NIST SP 800-38D, GCM (Dworkin, November 2007)
https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf

- **§5.2.1.1:** "len(P) ≤ 2^39-256; • len(A) ≤ 2^64-1; • 1 ≤ len(IV) ≤ 2^64-1." Also: "For IVs, it is recommended that implementations restrict support to the length of 96 bits".
  - DERIVED: that is at most 2^36−32 bytes (about 64 GiB) per invocation. AWS-LC enforces this.
- **§5.2.1.2:** tags of 128, 120, 112, 104 or 96 bits; 64 and 32 only under Appendix C. "A single, fixed value for t … shall be associated with each key."
- **§8:** "The probability that the authenticated encryption function ever will be invoked with the same IV and the same key on two (or more) distinct sets of input data shall be no greater than 2^-32."
- **§8.2.1, deterministic construction:** a fixed field plus an invocation field. "For any given key, no two distinct devices shall share the same fixed field". It suggests "the leading (i.e., leftmost) 32 bits of the IV hold the fixed field; and that the trailing (i.e., rightmost) 64 bits hold the invocation field."
- **§8.2.2, RBG-based construction:** "the length of the random field shall be at least 96 bits".
- **§8.3:** "unless an implementation only uses 96-bit IVs that are generated by the deterministic construction: The total number of invocations of the authenticated encryption function shall not exceed 2^32, including all IV lengths and all instances of the authenticated encryption function with the given key."
- **§9.1:** "A loss of power to the module shall not cause the repetition of IVs."
- **App. A:** on IV reuse, "it is likely that an adversary will be able to determine the hash subkey … The adversary then could easily construct a ciphertext forgery."
- **App. B:** the lifetime data limit per key is "A reasonable limit for most applications would be 2^64 [blocks]".
- **Revision status:**
  - CSRC planning note (03/06/2024): "NIST has decided to revise this publication." The 5 March 2024 announcement says the revision will "remove support for authentication tags whose lengths are less than 96 bits, clarify that the construction of initialization vectors (IVs) for GCM in the Transport Layer Security (TLS) 1.3 protocol is approved, clarify the guidance in connection with the IV constructions".
  - No draft Rev. 1 exists [UNVERIFIED/not found].
- **NIST IR 8459 (September 2024), §7:** "the maximum plaintext length is 2^39 − 256 bits, which is about 64 GiB … going beyond the limit leads to a complete breakdown in security".

### 8.2 NIST SP 800-57 Part 1 Rev. 5 (May 2020)
https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-57pt1r5.pdf

- **Definitions:**
  - "Cryptoperiod: The time span during which a specific key is authorized for use…"
  - The originator-usage and recipient-usage periods.
  - "Key-wrapping key: A symmetric key that is used to provide both confidentiality and integrity protection for other keys."
- **§5.3:** a cryptoperiod "Limits the amount of information that is available for cryptanalysis … Limits the amount of exposure if a single key is compromised". It also says "Sometimes, cryptoperiods are defined by an arbitrary time period or maximum amount of data protected by the key." The §5.3.1 factors include "the maximum number of invocations to avoid nonce reuse".
- **§5.3.3.1:** "Cryptoperiods are generally made longer for stored data because the overhead of generating new keys and re-encrypting all data that was encrypted using the old keys may be burdensome."
- **§5.3.6 item 6, data-encryption key:** "An encryption key used to encrypt smaller volumes of data might have an originator-usage period of up to two years. A recipient-usage period of no more than three years beyond the end of the originator-usage period is recommended." Also: "Where data is maintained in encrypted form, a symmetric data-encryption key needs to be maintained until that data is re-encrypted under a new key or destroyed."
- **§5.3.6 item 7, key-wrapping key:** the same structure, "on the order of a day or a week" for very high wrap volume, and "a wrapping operation shall not be performed using a key-wrapping key whose originator-usage period has expired."
- **Table 1:** symmetric data-encryption and key-wrapping keys have OUP ≤ 2 years and RUP ≤ OUP + 3 years. A master (key-derivation) key is about 1 year.
- **§5.6.2:** a 256-bit key "wrapped using AES-128 … is reduced to 128 bits".
- **§8.2.4:** use SP 800-108 KDFs; "keys derived from a key-derivation key are only as secure as the key-derivation key itself."
- **Rev. 6 initial public draft** (5 December 2025; comments closed 5 February 2026; DRAFT):
  - Data-at-rest keys: "The amount of information that is encrypted using a single data-encryption key should be limited … (e.g., limited to the encryption of no more than a single file or disk sector".
  - The cryptoperiod "should be measured in the amount of data to be encrypted using a single key".
  - Wrapping keys: "The number of keys that are wrapped using a single key-wrapping key should be limited".

### 8.3 Key wrap: SP 800-38F (December 2012), RFC 3394, RFC 5649
- **SP 800-38F §3.1:** "KW, KWP, and TKW are each approved for the protection of general data, as well as cryptographic keys."
- **§5.3.1 limits:**
  - KW: 2 to 2^54−1 semiblocks.
  - KWP: 1 to 2^32−1 octets.
- **§5.4:** "There is no requirement to limit the number of invocations for KW-AE or KWP-AE".
- **ICVs:** KW `0xA6A6A6A6A6A6A6A6`; KWP `0xA65959A6` plus a 32-bit length.
- **App. A.1:** KW and KWP are deterministic.
- **App. A.3:** forgery probability of "1 in 2^64".
- **TKW** "should not be used for new applications".
- **RFC 3394 §5:** on an integrity failure the implementation "MUST return an error, and it MUST NOT return any key data."
- **RFC 5649 §7:** "The KEK must be at least as good as the keying material it is protecting." Also "System designers should not use these algorithms to encrypt anything other than cryptographic keying material." That conflicts with SP 800-38F §3.1.
- **NIST IR 8459 §9:** "the same plaintext key should not be encrypted twice under the same key-wrapping key."

### 8.4 RFC 8452, AES-GCM-SIV (April 2019)
https://www.rfc-editor.org/rfc/rfc8452.txt

- Abstract: the algorithms are "nonce misuse resistant -- that is, they do not fail catastrophically if a nonce is repeated."
- §1: "encrypting two messages with the same nonce only discloses whether the messages were equal or not."
- §4: "The first step of encryption is to generate per-nonce, message-authentication and message-encryption keys."
- §6: "K_LEN is 32, P_MAX is 2^36, A_MAX is 2^36, N_MIN and N_MAX are 12".
- §9: "it is RECOMMENDED that AES-GCM-SIV nonces be randomly generated". The limits at adversary advantage ≤ 2^-32 are "2^32 messages, where each plaintext is at most 8 GiB", "2^48 … 32 MiB" and "2^64 … 128 KiB".
- App. B: encryption needs two passes, so it cannot stream.
- It is not a NIST mode, and it is absent from the AWS-LC FIPS security policy. DERIVED: it is not usable where FIPS-approved algorithms are required.

### 8.5 What AWS documents (primary)
- **SSE-S3:** the two wordings are in 1.5.
- **SSE-C:** the salted HMAC and AES-256 statements are in 3.4.
- **SSE-KMS workflow** (UG UsingKMSEncryption.md):
  1. "Amazon S3 requests a plaintext data key and a copy of the key encrypted under the specified KMS key."
  2. "AWS KMS generates a data key, encrypts it under the KMS key, and sends both … to Amazon S3."
  3. "Amazon S3 encrypts the data using the data key and removes the plaintext key from memory as soon as possible after use."
  4. "Amazon S3 stores the encrypted data key as metadata with the encrypted data."
- **S3 Bucket Keys** (UG bucket-key.md):
  - "When you use SSE-KMS to protect your data without an S3 Bucket Key, Amazon S3 uses an individual AWS KMS data key for every object."
  - "AWS generates a short-lived bucket-level key from AWS KMS, then temporarily keeps it in S3. This bucket-level key will create data keys for new objects during its lifecycle."
  - "Unique bucket-level keys are fetched at least once per requester"
  - "By design, subsequent requests that take advantage of this bucket-level key do not result in AWS KMS API requests or validate access against the AWS KMS key policy."
  - "Amazon S3 will only share an S3 Bucket Key for objects encrypted by the same AWS KMS key."
  - UNVERIFIED: the bucket-level key's lifetime and how it derives per-object keys.
- **KMS envelope encryption** (https://docs.aws.amazon.com/kms/latest/developerguide/kms-cryptography.md):
  - "Envelope encryption is the practice of encrypting plaintext data with a data key, and then encrypting the data key under another key."
  - "All symmetric key encrypt commands used within HSMs use … (AES), in Galois Counter Mode (GCM) using 256-bit keys."
  - "AWS KMS uses an key derivation function (KDF) to derive per-call keys for every encryption under an AWS KMS key. All KDF operations use the KDF in counter mode using HMAC … with SHA256."
- **KMS Cryptographic Details** (key-hierarchy.md, encrypt-operation.md):
  - Encrypt "Generates a random nonce N. Generates a 256-bit AES-GCM derived encryption key K from HBK and N."
  - The derived key is "Used once per encrypt and regenerated on decrypt".
  - The domain key is "Rotated daily".
  - UNVERIFIED: the size of N and the GCM IV length. No KMS page ties per-call derivation to the 2^32 limit.
- **KMS rotate-keys.md:** "It's best to use data keys once, or just a few times".
- **AWS Encryption SDK IV reference**, the only AWS text linking derivation to the bound: "Using a deterministic IV with a pseudo-random key derivation function to derive encryption keys from a data key allows the AWS Encryption SDK to encrypt 2^32 messages without exceeding cryptographic bounds."

### 8.6 aws-lc-rs 1.18.1 (docs.rs; git `22e629d5…`)
- **Algorithms:**
  - `AES_256_GCM`: "AES-256 in GCM mode with 128-bit tags and 96 bit nonces."
  - `AES_256_GCM_SIV`: "AES-256 in GCM mode with nonce reuse resistance, 128-bit tags and 96 bit nonces."
  - `NONCE_LEN` is 12 bytes; `MAX_TAG_LEN` is 16.
- **`RandomizedNonceKey`:** "AEAD Cipher key using a randomized nonce … supported: AES_128_GCM, AES_256_GCM, AES_128_GCM_SIV, AES_256_GCM_SIV. Prefer this type in place of LessSafeKey, OpeningKey, SealingKey." It does not count invocations, so the caller must keep each key under 2^32 (DERIVED).
- **`Nonce`:** "The user must ensure, for a particular key, that each nonce is unique."
- **`NonceSequence`:** must never repeat, and once `advance()` fails it must always fail. `Counter64` is the 32-bit fixed field plus 64-bit counter layout of SP 800-38D.
- **`TlsRecordSealingKey`** enforces monotonically increasing nonces.
- **`key_wrap`:** `AesKek = KeyEncryptionKey<AesBlockCipher>` implements "the NIST SP 800-38F key wrapping algorithm". `KeyWrap::wrap`, `KeyWrapPadded::wrap_with_padding` and the unwrap methods consume the KEK by value.
  - KW needs input that is a multiple of 8 bytes and at least 16 bytes, with output ≥ input + 8.
  - KWP output must be ≥ input + 15.
  - Only AES-128 and AES-256 KEKs are available.
- **`kdf::kbkdf_ctr_hmac`:** "KDF in Counter Mode using HMAC PRF specified in NIST SP 800-108r1-upd1 section 4.1". This is the construction KMS documents.
- **FIPS** (AWS-LC 3 security policy, certificate #5314): "The 96-bit AES-GCM IV, containing 96 bits of entropy, is generated randomly internal to the module using module's approved DRBG, without outputting the IV to the calling application."
  - AES-GCM encryption is approved only with IVs the module generates itself. An AWS-LC source comment says: "Only internal IV for AES-GCM is approved."
  - AES-KW and AES-KWP are approved.
  - GCM-SIV is absent.
  - `fips_mode()` panics if not FIPS; use `try_fips_mode()`.
  - The random-nonce path calls `abort()` if the RNG fails. DERIVED: an unwind boundary cannot catch this, which matters for mantle's no-panic rule.
  - UNVERIFIED: whether the aws-lc-fips-sys 4.x module is certified yet.

---

## 9. Doc inconsistencies and gaps to note in the research note

**Inconsistencies inside AWS's own docs:**
- UG UsingKMSEncryption.md allows "(or AWS Signature Version 2)" for SSE-KMS. Other pages and every capture require SigV4.
- UG specifying-kms-encryption.md names the header `x-amz-server-side-encryption-aws-bucket-key-enabled`. The API name is `x-amz-server-side-encryption-bucket-key-enabled`.
- API CreateMultipartUpload.md says a bucket default can be SSE-C. UG default-bucket-encryption.md says "SSE-C is not supported for default encryption".
- The UG specifying-{s3,kms,dsse} pages say a copy's destination "is not encrypted unless you explicitly request server-side encryption". This is pre-2023 text; CopyObject now applies the bucket default.
- DG RESTObjectPOST.md says "Starting May 2022, all Amazon S3 buckets have encryption configured by default". Every other page says 5 January 2023.
- The same POST page marks the KMS key-ID field "Yes, if … aws:kms", yet notes that `aws/s3` is used if it is absent.
- The Put/GetBucketEncryption examples use `<KMSKeyID>`, not `<KMSMasterKeyID>`. The blog's illustrative response does the same.
- The documented `InvalidEncryptionAlgorithmError` text differs from what S3 returns ("…Supported value: AES256.").
- The two SSE-S3 wordings ("root key that it regularly rotates" versus "a key that it rotates regularly … AES-GCM").

**Where ceph disagrees with AWS captures or docs (DERIVED):**
- GetBucketEncryption returns NotFound on a new or deleted bucket.
- A wrong SSE-C key returns 400 (AWS: 403).
- `aws:kms` without a key ID errors (AWS falls back to `aws/s3`).
- SSE-C writes to new buckets succeed (AWS blocks them since April 2026).

**UNVERIFIED (nothing found):**
- AWS's exact error for SSE-C over HTTP.
- Whether the SSE-C MD5 header is optional on AWS.
- Answers for invalid SSE values other than the empty string.
- Anything about DSSE requests or errors.
- The error code for a bad encryption context.
- What S3 returns when KMS throttles.
- DeleteBucketEncryption's effect on `BlockedEncryptionTypes`.
- A never-configured post-April-2026 GetBucketEncryption document.
- Whether CompleteMultipartUpload ever echoes SSE-C headers.
- The meaning of `aws:backup`.
- SSE-S3's internal IV and wrap scheme.
- The algorithm behind SSE-C's salted HMAC.
- KMS's nonce size.
