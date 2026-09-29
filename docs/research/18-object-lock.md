# 18 — Object Lock: ground truth for write-once retention and legal holds

Research note for mantle's Object Lock: its documents and headers in the S3 protocol layer
(`crates/s3`), and its enforcement where versions live, the metadata layer's Name ranges. It
covers:

- S3's Object Lock operations, documents and object headers;
- the model: retention modes, legal holds, default retention, and which changes each allows;
- deletes, lifecycle and versioning under Object Lock;
- botocore's model and serializer, and ceph s3-tests' tests;
- S3's observed answers, from recordings made against it.

Compiled 2026-09-29 with curl from public pages; the AWS pages are the `.md` renditions as
served that day. Labels as in notes 16 and 17: **secondary** for S3 behaviour recorded by
others, **third-party** for another implementation's claim, **DERIVED** for an inference of
ours, **UNVERIFIED** for what no fetched source states.

Pins: botocore develop at `358f8eec8c76201bb1a7a35644abcbc9036de7ed` (release 1.43.104); ceph
s3-tests at `5522d1c351f75bc00ae0f64f742f3f095f5939d9`; LocalStack at
`8b9a79f05846835cf4dff63ab7eefdde9df83783`, whose 20 Object Lock tests were recorded against S3
on 2026-02-21; the Service Authorization Reference's S3 JSON, `v1.4`.

---

## 1. Operations and documents

### 1.1 The bucket's configuration ([API_PutObjectLockConfiguration], [API_GetObjectLockConfiguration])

- `PUT /?object-lock`: "Places an Object Lock configuration on the specified bucket. The rule
  specified in the Object Lock configuration will be applied by default to every new object
  placed in the specified bucket." "The `DefaultRetention` settings require both a mode and a
  period. The `DefaultRetention` period can be either `Days` or `Years` but you must select
  one. You cannot specify `Days` and `Years` at the same time. You can enable Object Lock for
  new or existing buckets." It answers 200; the page lists no errors.

```
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <ObjectLockEnabled>string</ObjectLockEnabled>
   <Rule>
      <DefaultRetention>
         <Days>integer</Days>
         <DefaultEventHold>
            <Days>integer</Days>
            <Years>integer</Years>
         </DefaultEventHold>
         <Mode>string</Mode>
         <Years>integer</Years>
      </DefaultRetention>
   </Rule>
</ObjectLockConfiguration>
```

- `ObjectLockEnabled`: "Valid Values: `Enabled` Required: No". `Mode`: `GOVERNANCE |
  COMPLIANCE`. `Days` and `Years`: "Must be used with `Mode`"; no page gives their range.
- `x-amz-bucket-object-lock-token`: "A token to allow Object Lock to be enabled for an existing
  bucket", from when enabling it on an existing bucket went through AWS Support (before
  2023-11-20, [whatsnew-2023]).
- `GET /?object-lock` answers the document, its Response Syntax root without a namespace.

### 1.2 An object version's retention and legal hold ([API_PutObjectRetention], [API_GetObjectRetention], [API_PutObjectLegalHold], [API_GetObjectLegalHold])

- `PUT /{Key}?retention&versionId=`: `<Retention><Mode>GOVERNANCE | COMPLIANCE</Mode>
  <RetainUntilDate>timestamp</RetainUntilDate><EventHold>ON | OFF</EventHold>
  <EventHoldDuration>...</EventHoldDuration></Retention>`, with `x-amz-bypass-governance-retention`:
  "Indicates whether this action should bypass Governance-mode restrictions." "Bypassing a
  Governance Retention configuration requires the `s3:BypassGovernanceRetention` permission."
- `PUT /{Key}?legal-hold&versionId=`: `<LegalHold><Status>ON | OFF</Status></LegalHold>`; no
  bypass header.
- The GETs answer the same documents. None of the six pages lists errors.

### 1.3 Object headers ([API_PutObject], [API_CopyObject], [API_CreateMultipartUpload], [API_GetObject], [API_HeadObject])

- PutObject, CopyObject and CreateMultipartUpload take `x-amz-object-lock-mode` (`GOVERNANCE |
  COMPLIANCE`), `x-amz-object-lock-retain-until-date` ("Must be formatted as a timestamp
  parameter") and `x-amz-object-lock-legal-hold` (`ON | OFF`), and since 2026-09 the event-hold
  headers (§2.6).
- "The `Content-MD5` or `x-amz-sdk-checksum-algorithm` header is required for any request to
  upload an object with a retention period configured using Amazon S3 Object Lock." UploadPart's
  `Content-MD5`: "This parameter is required if object lock parameters are specified."
- GetObject and HeadObject return the same headers; `x-amz-object-lock-mode` and
  `-retain-until-date` "only if the requester has the `s3:GetObjectRetention` permission", and
  `-legal-hold` "only ... if the requester has the `s3:GetObjectLegalHold` permission. This header
  is not returned if the specified version of this object has never had a legal hold applied."
- "Copied objects will not retain the Object Lock settings from the original objects"
  ([ug-copy]).
- CreateBucket's `x-amz-bucket-object-lock-enabled`: "Specifies whether you want S3 Object Lock
  to be enabled for the new bucket", needing `s3:PutBucketObjectLockConfiguration` and
  `s3:PutBucketVersioning`. s3-tests expects such a bucket's versioning `Enabled`.

### 1.4 Error table ([ErrorResponses])

- `NoSuchObjectLockConfiguration`, "The specified object does not have an ObjectLock
  configuration.", 404. `ObjectLockConfigurationNotFoundError`, "The Object Lock configuration
  does not exist for this bucket.", 404. `InvalidBucketState`, "The request is not valid for the
  current state of the bucket.", 409. `AccessDenied`, 403; `InvalidArgument`, `InvalidRequest`,
  `MalformedXML`, 400.
- No code `InvalidRetentionPeriod` is among the table's 125.

## 2. The model ([ug-object-lock], [ug-considerations], [ug-configure])

### 2.1 Versions

- "Object Lock works only in buckets that have S3 Versioning enabled. ... Placing a retention
  period or a legal hold on an object protects only the version that's specified in the
  request. Retention periods and legal holds don't prevent new versions of the object from
  being created, or delete markers to be added on top of the object."
- "After you enable Object Lock on a bucket, you can't disable Object Lock or suspend
  versioning for that bucket."

### 2.2 Modes

- Compliance: "a protected object version can't be overwritten or deleted by any user,
  including the root user in your AWS account. When an object is locked in compliance mode,
  its retention mode can't be changed, and its retention period can't be shortened."
- Governance: "users can't overwrite or delete an object version or alter its lock settings
  unless they have special permissions."
- "If you have the `s3:BypassGovernanceRetention` permission, you can perform operations on
  object versions that are locked in governance mode as if they were unprotected. These
  operations include deleting an object version, shortening the retention period, or removing
  the Object Lock retention period by placing a new `PutObjectRetention` request with empty
  parameters." "you must have the `s3:BypassGovernanceRetention` permission and must explicitly
  include `x-amz-bypass-governance-retention:true` as a request header".
- Extending: "submit a new Object Lock request for the object version with a *Retain Until
  Date* that is later than the one currently configured ... Any user with permissions to place
  an object retention period can extend a retention period"; in 2022, "locked in either mode"
  ([wb-overview-2022]).

### 2.3 Legal holds

- "Legal holds can be freely placed and removed by any user who has the `s3:PutObjectLegalHold`
  permission." "Legal holds are independent from retention periods." "If the retention period
  expires, the object doesn't lose its WORM protection. Rather, the legal hold continues to
  protect the object until an authorized user explicitly removes the legal hold."
- "Bypassing governance mode doesn't affect an object version's legal hold status."

### 2.4 Default retention

- "When you place an object in the bucket, Amazon S3 calculates a *Retain Until Date* for the
  object version by adding the specified duration to the object version's creation timestamp.
  The object version is then protected exactly as though you explicitly placed an individual
  lock with that retention period on the object version."
- "the object version's individual Object Lock settings override any bucket property retention
  settings." Default settings apply only to new objects ([wb-overview-2022]).
- "The maximum retention period is 100 years", said of bucket policies limiting retention.
- Removing the default: put `{"ObjectLockEnabled": "Enabled"}`.

### 2.5 Deletes and lifecycle

- "**Permanent `DELETE` request** – If you issued a permanent `DELETE` request (a request that
  specifies a version ID), Amazon S3 returns an Access Denied (`403 Forbidden`) error". "**Simple
  `DELETE` request** – ... Amazon S3 returns a `200 OK` response and inserts a delete marker".
  DeleteObject's status is 204 (botocore, LocalStack).
- "Delete markers are not WORM-protected, regardless of any retention period or legal hold in
  place on the underlying object."
- "a locked version of an object cannot be deleted by a S3 Lifecycle expiration policy";
  "Amazon S3 doesn't take any action on noncurrent versions of objects that have the S3 Object
  Lock configuration applied" ([ug-lifecycle-expire]).

### 2.6 Variable retention (2026-09)

Event holds arrived in botocore 1.43.90, uploaded 2026-09-08: `EventHold` and
`EventHoldDuration` in `Retention`, `DefaultEventHold` in `DefaultRetention`, the headers
`x-amz-object-lock-event-hold` and `-event-hold-duration-days`/`-years`. "While the event hold
is on, Amazon S3 computes the retain-until-date as the current time plus the duration. ...
When you release the hold, Amazon S3 sets the retain-until-date to the release time plus the
duration." A duration is "1 to 36,500 days, or 1 to 100 years"; "Amazon S3 counts 1 year as 365
days". Its faults are answered 400 with no code named. No recording of S3 exists yet.

## 3. botocore ([botocore])

- `PutObjectLockConfiguration`, `PutObjectRetention` and `PutObjectLegalHold` are
  `requestChecksumRequired`, sent with CRC32 and no Content-MD5. `DeleteObjects` also.
- Timestamps go out as `%Y-%m-%dT%H:%M:%SZ`, with `.%fZ` when there are microseconds; a header
  timestamp is truncated to whole seconds first.
- No enumeration or range is checked client-side, so `governance`, `Disabled`, `Days: 0` and
  `Years: -1` reach the wire.

## 4. ceph s3-tests ([s3-tests])

The 39 `test_object_lock_*` tests, none `fails_on_aws`. Among them:

- A lock configuration on a bucket without Object Lock, or with versioning not `Enabled`: 409
  `InvalidBucketState`; suspending versioning on a lock bucket: 409 `InvalidBucketState`.
- `Days` and `Years` together, a mode `abc` or `governance`, `ObjectLockEnabled` `Disabled`:
  400 `MalformedXML`. `Days` 0 and `Years` -1: 400 `InvalidRetentionPeriod`, which S3 does not
  answer (§5; s3-tests issue #344, open since 2020).
- Retention and legal-hold operations on a bucket without Object Lock: 400 `InvalidRequest`.
- Extending a GOVERNANCE date: 200. Shortening it: 403 `AccessDenied`; with bypass, 200.
  Changing GOVERNANCE to COMPLIANCE without bypass, and COMPLIANCE to GOVERNANCE: 403.
- Deleting a locked version, or one under a legal hold: 403 `AccessDenied`; with bypass under
  GOVERNANCE, 204. DeleteObjects: 200, the locked key an `AccessDenied` error.
- Teardown sends `x-amz-bypass-governance-retention: true` with DeleteObjects on every bucket,
  which S3 refuses on a bucket without Object Lock (§5); s3-tests PR #714, which stops it, is
  open.

## 5. S3's observed answers (secondary)

**LocalStack** (recorded 2026-02-21):

- Retention and legal-hold operations on a bucket without Object Lock: 400 `InvalidRequest`,
  "Bucket is missing Object Lock Configuration"; a missing body first: 400 `MalformedXML`.
- GetObjectLockConfiguration without one: 404 `ObjectLockConfigurationNotFoundError`, "Object
  Lock configuration does not exist for this bucket", with `BucketName`.
- PutObjectLockConfiguration with versioning not `Enabled`: 409 `InvalidBucketState`,
  "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration". Suspending
  versioning on a lock bucket: 409 `InvalidBucketState`, "An Object Lock configuration is present
  on this bucket, so the versioning state cannot be changed."
- Configurations without `ObjectLockEnabled`, empty, with an empty `Rule` or `DefaultRetention`,
  a mode without days, a mode `BAD-VALUE`, or days and years together: 400 `MalformedXML`.
- A version with no retention or legal hold: GetObjectRetention and GetObjectLegalHold 404
  `NoSuchObjectLockConfiguration`, "The specified object does not have a ObjectLock
  configuration".
- PutObjectRetention with only a mode: 400 `MalformedXML`. A past date: 400 `InvalidArgument`,
  "The retain until date must be in the future!", `ArgumentName` `RetainUntilDate`.
- Shortening GOVERNANCE without bypass: 403 `AccessDenied`, "Access Denied because object
  protected by object lock."; extending: 200. COMPLIANCE: deleting the version, with or without
  bypass, shortening it, and changing it to GOVERNANCE with a later date: 403. An empty
  `Retention` with bypass on GOVERNANCE: 200, and the version is unprotected.
- Deleting a locked version: 403 with that message; DeleteObjects: 200, the key an
  `AccessDenied` error with that message. A simple delete: 204 and a delete marker.
- A delete marker's retention: 405 `MethodNotAllowed`, `ResourceType` `DeleteMarker`.
- PutObject and CreateMultipartUpload with only a mode, or only a date: 400 `InvalidArgument`,
  "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied",
  `ArgumentName` the one missing. A bad mode with a date: "Unknown wormMode directive.",
  `ArgumentName` `x-amz-object-lock-mode`.
- `x-amz-bypass-governance-retention`, `true` or `false`, on a bucket without Object Lock, with
  DeleteObject or DeleteObjects: 400 `InvalidArgument`, "x-amz-bypass-governance-retention is only
  applicable to Object Lock enabled buckets.", `ArgumentName` `x-amz-bypass-governance-retention`.
- Default retention of GOVERNANCE 1 day: HeadObject shows the mode and a date within 2 minutes of
  `LastModified` plus a day. A copy of a locked source carries no lock.

**Issue trackers:**

- `Days` -1 and 0: 400 `InvalidArgument`, "Default retention period must be a positive integer
  value.", `ArgumentName` `Days`; `Days` 999999999: "Default retention period too large."
  ([storj-528], 2024-11; [vgw-1738], 2026-01).
- PutObject with Object Lock parameters, a bucket default included, or a legal hold alone,
  without Content-MD5 or an `x-amz-checksum-*` header or trailer: 400 `InvalidRequest`,
  "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock
  parameters" ([vgw-1740], [vgw-1776], 2026; [nextflow-5347], 2024). A SigV4 payload hash does not
  count.
- An anonymous PutObject with Object Lock parameters: 400 `InvalidArgument`, "Put Object requests
  with Object Lock parameters require AWS Signature Version 4", `ArgumentName` `Authorization`
  ([vgw-1542], 2025).
- `x-amz-object-lock-retain-until-date: abc`: 400 `InvalidArgument`, "The retain until date must
  be provided in ISO 8601 format"; a past date: "The retain until date must be in the future!";
  `x-amz-object-lock-legal-hold: wrong`: "Legal Hold must be either of 'ON' or 'OFF'"; each with
  `ArgumentName` and `ArgumentValue`, and each answered before the bucket's lock is looked at
  ([vgw-1733], [vgw-1734], [vgw-1736], [vgw-1775], 2026).
- Extending in the same mode without bypass succeeds in GOVERNANCE and COMPLIANCE ([vgw-1559]).

**Third-party:** versitygw reports S3 answering object writes whose bucket has no Object Lock
"Bucket is missing ObjectLockConfiguration", and requiring the integrity header on UploadPart
for an upload with lock parameters ([vgw-2450]).

## 6. Discrepancies

1. s3-tests' `InvalidRetentionPeriod` against S3's `InvalidArgument` for a default period.
2. s3-tests' teardown sends the bypass header to buckets without Object Lock, which S3 refuses.
3. The integrity rule: the docs name `Content-MD5` or `x-amz-sdk-checksum-algorithm`; S3 needs
   `Content-MD5` or a checksum, and the algorithm header alone is refused.
4. A simple DELETE: "200 OK" in the user guide, 204 from DeleteObject.
5. Messages: "does not have an ObjectLock configuration." in the table, "a ObjectLock" on the
   wire; "The Object Lock configuration does not exist for this bucket." against "Object Lock
   configuration does not exist for this bucket".
6. GOVERNANCE to COMPLIANCE without bypass: s3-tests expects 403; no recording of S3.

## Sources

- [API_PutObjectLockConfiguration] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
- [API_GetObjectLockConfiguration] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html
- [API_PutObjectRetention] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html
- [API_GetObjectRetention] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html
- [API_PutObjectLegalHold] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html
- [API_GetObjectLegalHold] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html
- [API_PutObject], [API_CopyObject], [API_CreateMultipartUpload], [API_GetObject], [API_HeadObject]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_{PutObject,CopyObject,CreateMultipartUpload,GetObject,HeadObject}.html
- [ErrorResponses] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html
- [ug-object-lock] https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
- [ug-considerations] https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html
- [ug-configure] https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html
- [ug-copy] https://docs.aws.amazon.com/AmazonS3/latest/userguide/copy-object.html
- [ug-lifecycle-expire] https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html
- [whatsnew-2023] https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-s3-enabling-object-lock-buckets/
- [wb-overview-2022] https://web.archive.org/web/20221230184325/https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html
- [botocore] https://github.com/boto/botocore/tree/358f8eec8c76201bb1a7a35644abcbc9036de7ed (`service-2.json`, `serialize.py`, `httpchecksum.py`, `CHANGELOG.rst`)
- [s3-tests] https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py (L13279–L14024) and `__init__.py` (L84–L159); issue #344, PR #714
- [localstack] https://github.com/localstack/localstack/tree/8b9a79f05846835cf4dff63ab7eefdde9df83783/tests/aws/services/s3 (`test_s3_api.py` `TestS3ObjectLock`, `test_s3.py` `TestS3ObjectLockRetention`, `TestS3ObjectLockLegalHold`, and their `.snapshot.json`)
- [storj-528] https://github.com/storj/edge/issues/528
- [vgw-1542], [vgw-1559], [vgw-1733], [vgw-1734], [vgw-1736], [vgw-1738], [vgw-1740], [vgw-1775], [vgw-1776] https://github.com/versity/versitygw/issues/ (by number); [vgw-2450] https://github.com/versity/versitygw/pull/2450
- [nextflow-5347] https://github.com/nextflow-io/nextflow/issues/5347
