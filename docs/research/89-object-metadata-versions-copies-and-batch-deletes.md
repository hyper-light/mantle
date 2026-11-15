# 89. An object's metadata, its versions, copies and batch deletes

Status: research note, 2026-11-14. For the S3 listener and the native protocol (node.md §4, §5;
`crates/s3/src/metadata.rs`, `copy.rs`; `crates/node/src/node/host.rs`): what S3 keeps of an
object's metadata and returns, how a request names a version, and what CopyObject, UploadPartCopy
and DeleteObjects do and refuse. It extends research/05 (§7–§10), which it cites as "05 §x"; the
decisions it supports are in s3-protocol.md §14 and metadata.md §2. Sources were read on
2026-11-14 from AWS's API reference and user guide, and from the conformance suites and the
SDK code named below; quotations are verbatim.

## Sources

[1] Amazon S3 User Guide, "Working with object metadata",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html.
[2] Amazon S3 API Reference, PutObject, https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html.
[3] Amazon S3 API Reference, GetObject, https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html.
[4] Amazon S3 API Reference, CopyObject, https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html.
[5] Amazon S3 API Reference, UploadPartCopy,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html.
[6] Amazon S3 API Reference, DeleteObjects,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html.
[7] Amazon S3 developer guide, "Error responses", List of error codes,
https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html.
[8] OpenStack Swift's S3 conformance tests, `test/s3api/test_input_errors.py` at commit
259afd2a756bd300ffac0591ab81d2faa160fd18
(https://opendev.org/openstack/swift/src/commit/259afd2a756bd300ffac0591ab81d2faa160fd18/test/s3api/test_input_errors.py),
which assert S3's answers to requests with and without each integrity header.
[9] moto, issue 3884 (https://github.com/getmoto/moto/issues/3884), quoting the errors boto3
raised against AWS for a malformed and for an absent version ID.
[10] boto/s3transfer, `s3transfer/utils.py` and `s3transfer/copies.py` on `develop`
(https://github.com/boto/s3transfer), the managed transfers of boto3 and the AWS CLI.
[11] ceph/s3-tests at 5522d1c351f75bc00ae0f64f742f3f095f5939d9, as research/05 §4.3, §9 and §15
cite it.
[12] moto, `tests/test_s3/test_s3_copyobject.py` on `master`
(https://github.com/getmoto/moto/blob/master/tests/test_s3/test_s3_copyobject.py): its tests marked
`@s3_aws_verified`, which moto runs against AWS as well as against itself.
[13] aws/aws-cli, `awscli/customizations/s3/subscribers.py` on `v2`
(https://github.com/aws/aws-cli/blob/v2/awscli/customizations/s3/subscribers.py), the copy
properties of `aws s3 cp` between two S3 locations.
[14] Amazon S3 API Reference, GetObjectTagging,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html.

## 1. Stored metadata

### 1.1 The content headers

PutObject's request names six headers S3 keeps with the object [2]: `Cache-Control` ("Can be
used to specify caching behavior along the request/reply chain"), `Content-Disposition`
("Specifies presentational information for the object"), `Content-Encoding` ("Specifies what
content encodings have been applied to the object and thus what decoding mechanisms must be
applied to obtain the media-type referenced by the Content-Type header field"),
`Content-Language` ("The language the content is in"), `Content-Type` ("A standard MIME type
describing the format of the contents") and `Expires` ("The date and time at which the object is
no longer cacheable"). GetObject's response carries the same six [3]. The user guide's table of
system-defined metadata marks `Cache-Control`, `Content-Disposition`, `Content-Encoding` and
`Content-Type` as ones the user can modify [1]. A version kept without a `Content-Type` is
served with S3's `binary/octet-stream`, which mantle's GET answered every object with before
this note; S3 documents no other default.

`aws-chunked`, the framing of a streamed body, is not a coding of the object: "Amazon S3 stores
the resulting object without the `aws-chunked` value in the `content-encoding` header. If
`aws-chunked` is the only value ... S3 considers the `content-encoding` header empty and does not
return this header", which s3-tests pins for `gzip, aws-chunked`, `aws-chunked, gzip`,
`aws-chunked` and `aws-chunked, aws-chunked` (05 §1.8).

### 1.2 User-defined metadata

"When you upload objects using the REST API, the optional user-defined metadata names must begin
with `x-amz-meta-` to distinguish them from other HTTP headers. When you retrieve the object using
the REST API, this prefix is returned." "Amazon S3 stores user-defined metadata keys in
lowercase." "When metadata is retrieved through the REST API, Amazon S3 combines headers that have
the same name (ignoring case) into a comma-delimited list." [1] A PUT without metadata replaces
what an earlier version had, and an empty value round-trips (05 §10.2).

### 1.3 Values past US-ASCII

"Values of such headers are character decoded as per RFC 2047 before storing and encoded as per
RFC 2047 to make them mail-safe before returning. If the string contains only US-ASCII
characters, it is presented as is." The guide's example [1]:

```
PUT /Key HTTP/1.1
x-amz-meta-nonascii: ÄMÄZÕÑ S3

HEAD /Key HTTP/1.1
x-amz-meta-nonascii: =?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?=
```

The base64 decodes to `C3 83 C2 84 4D C3 83 C2 84 5A C3 83 C2 95 C3 83 C2 91 20 53 33`: the
UTF-8 of the value's own octets (`C3 84 4D C3 84 5A C3 95 C3 91 20 53 33`, the UTF-8 of
`ÄMÄZÕÑ S3`) each read as an ISO-8859-1 character. So S3 takes a field's octets as ISO-8859-1,
as RFC 9110 §5.5 lets a recipient treat `obs-text`, and returns RFC 2047's B encoding of that
text's UTF-8. mantle keeps a value as its field carried it and writes it back the same way
(`metadata::shown`), which reproduces the example byte for byte. "If some metadata contains
unprintable characters, it is not returned. Instead, the `x-amz-missing-meta` header is returned
with a value of the number of unprintable metadata entries" [1]; GetObject's reference says this
"can happen if you create metadata using an API like SOAP that supports more flexible metadata
than the REST API" [3]. mantle keeps only values an HTTP field can carry (RFC 9110 §5.5), from
either door, so it never has one to leave out.

### 1.4 Limits

"The `PUT` request header is limited to 8 KB in size. Within the `PUT` request header, the
user-defined metadata is limited to 2 KB in size. The size of user-defined metadata is measured by
taking the sum of the number of bytes in the UTF-8 encoding of each key and value." And: "Within
the `PUT` request header, the system-defined metadata is limited to 2 KB in size. The size of
system-defined metadata is measured by taking the sum of the number of bytes in the US-ASCII
encoding of each key and value." [1] Past either, `MetadataTooLarge`, 400, "Your metadata headers
exceed the maximum allowed metadata size" [7].

What a "key" is the guide does not say in so many words (05 §10.2 left it UNVERIFIED). Its own
words read it as the name past the prefix: metadata is "a set of key-value pairs", the keys are
what S3 "stores ... in lowercase", and "this prefix is returned" on reads, added back to the key
it stored. mantle counts a user-defined entry's key past `x-amz-meta-`, and a content header by
its field name, each with its value; a kilobyte is 1024 octets, as S3's 8 KB of headers is
throughout mantle (`route::MAX_HEADERS`, `http::MAX_HEAD`).

### 1.5 A read's overrides

"The response headers that you can override for the `GetObject` response are `Cache-Control`,
`Content-Disposition`, `Content-Encoding`, `Content-Language`, `Content-Type`, and `Expires`",
by the query parameters `response-cache-control`, `response-content-disposition`,
`response-content-encoding`, `response-content-language`, `response-content-type` and
`response-expires`; "These modified response header values are included only in a successful
response, that is, when the HTTP status code `200 OK` is returned", and "you must sign the
request" [3]. Every request mantle serves is signed.

## 2. Versions

### 2.1 GetObject and HeadObject with `versionId`

"By default, the `GetObject` operation returns the current version of an object. To return a
different version, use the `versionId` subresource." [3] For a delete marker: "If the current
version of the object is a delete marker, Amazon S3 behaves as if the object was deleted and
includes `x-amz-delete-marker: true` in the response. If the specified version in the request is a
delete marker, the response returns a `405 Method Not Allowed` error and the `Last-Modified:
timestamp` response header." [3] The error list names `MethodNotAllowed`, 405, "The specified
method is not allowed against this resource. For example, this error occurs when the request
targets a delete marker" [7], and `NoSuchVersion`, 404, "The version ID specified in the request
does not match an existing version" [7].

A version ID S3 cannot have made is not an absent one: boto3 against AWS raised "An error
occurred (InvalidArgument) when calling the GetObject operation: Invalid version id specified" for
a made-up ID, and "An error occurred (NoSuchVersion) when calling the GetObject operation: The
specified version does not exist." for a well-formed one no version has [9]. mantle's IDs are
thirteen characters of Crockford's base 32 or `null` (metadata.md §1): any other text is
InvalidArgument, and one of those that names no version NoSuchVersion.

### 2.2 DeleteObject with `versionId`

"DELETE with `versionId` permanently deletes that version", answered with the version's ID and,
for a delete marker, `x-amz-delete-marker: true`; the null version is named `null` (05 §7.3). The
Name range already removes a named version under its Object Lock rules (metadata.md §2); only the
listener's door to it was missing.

### 2.3 ListObjectVersions

The listing of versions and delete markers, newest first within a key, its markers, and the
`passed` marker of a page that paused, are research/05 §6.4 and s3-protocol.md §3; the Name
layer's scan (`name::next_version`) and the document (`response::list_object_versions`) were
built, and no door reached them. `version-id-marker` applies only with a key marker (05 §6.4).

## 3. CopyObject

### 3.1 The source

`x-amz-copy-source` is "the name of the source bucket and the key of the source object, separated
by a slash (/) ... The value must be URL-encoded"; "To copy a different version, use the
`versionId` query parameter. Specifically, append `?versionId=<version-id>` to the value"; "If
the current version is a delete marker, Amazon S3 behaves as if the object was deleted" [4]. AWS
writes it with a leading slash and without; the version is split off before the rest is
decoded, once (05 §9.2). "The source object can be up to 5 GB", larger copies go by
UploadPartCopy, and the error list gives `InvalidRequest`, 400, for a "CopyObject request made on
objects larger than 5GB in size" [4, 7]. "If the source object is an object that was uploaded by
using a multipart upload, the object copy will be a single part object" [4]. A missing source
bucket or key is 404 (05 §9.2). A copy source through an access point's ARN names an access point,
which mantle has none of: `501 NotImplemented`.

### 3.2 The metadata directive

`x-amz-metadata-directive`: "Specifies whether the metadata is copied from the source object or
replaced with metadata that's provided in the request. When copying an object, you can preserve all
metadata (the default) or specify new metadata. If this header isn't specified, `COPY` is the
default behavior." "Valid Values: `COPY | REPLACE`" [4]. `x-amz-website-redirect-location` "is
unique to each object and is not copied when using the `x-amz-metadata-directive` header" [4];
mantle keeps no redirect. A directive outside the two is refused `400 InvalidArgument`, AWS's code for "The
specified argument was not valid" [7], with mantle's message, "Unknown metadata directive.", AWS
documenting none.

### 3.3 The source's conditions

`x-amz-copy-source-if-match`, `-if-none-match`, `-if-modified-since` and `-if-unmodified-since`
[4]: "If both the `x-amz-copy-source-if-match` and `x-amz-copy-source-if-unmodified-since`
headers are present in the request and evaluate as follows, Amazon S3 returns `200 OK` and copies
the data: `x-amz-copy-source-if-match` condition evaluates to true;
`x-amz-copy-source-if-unmodified-since` condition evaluates to false". "If both the
`x-amz-copy-source-if-none-match` and `x-amz-copy-source-if-modified-since` headers are present
... [none-match] false ... [modified-since] true ... Amazon S3 returns the `412 Precondition
Failed` response code." RFC 9110 §13.2.2's order, which mantle's reads follow, gives both, a
condition a read answers 304 failing the copy 412 (05 §9.4). The destination's `If-Match` and
`If-None-Match` are conditional writes (05 §2.2).

### 3.4 The copy's checksum and ETag

"When you copy an object, if the source object has a checksum, that checksum value will be copied
to the new object by default. If the `CopyObject` request does not include this
`x-amz-checksum-algorithm` header, the checksum algorithm will be copied from the source object to
the destination object (if it's present on the source object)." [4] The user guide adds that a
copy's checksum "is a direct checksum of the full object", changing for a multipart source "even
though the data doesn't", and that a source with none takes CRC-64/NVME (05 §3.5). mantle writes
the copy's checksum as a full-object value of the source's algorithm, the request's where it names
one, CRC-64/NVME where the source keeps none, computed over the bytes copied; the ETag is their
MD5, a single PUT's (05 §5.1). Where the copy is of the whole source, the bytes are also held to
the source's own digests, its ETag where that is an MD5 and its full-object checksum of the same
algorithm, so a copy whose bytes differ from what was stored commits nothing.

### 3.5 A copy onto itself

The user guide's way to change metadata is a copy onto the same object (05 §9.6). One that
changes nothing is `400 InvalidRequest` (s3-tests' `test_object_copy_to_itself` [11]), "This copy
request is illegal because it is trying to copy an object to itself without changing the object's
metadata, storage class, website redirect location or encryption attributes." in moto's
AWS-verified `test_copy_object_in_place_with_versioning` [12], which also finds that the same copy
naming a version, `null` included, is taken, and its `test_copy_object_in_place_with_metadata` that
metadata sent without `REPLACE` changes nothing [12]. mantle keeps neither a storage class a copy
can change nor a redirect, and encrypts every object alike, so a copy onto its own key that names
no version, under `COPY`, is the one refused.

### 3.6 The response

"Returns the ETag of the new object. The ETag reflects only changes to the contents of an object,
not its metadata." [4] `CopyObjectResult` holds the ETag, `LastModified`, `ChecksumType` and the
checksum, and the response's `x-amz-version-id` the new version's ID, "always null" where the
destination is unversioned, and `x-amz-copy-source-version-id` the source's [4]. The new version's
`LastModified` is its commit's time in the Name range, its order inverted (metadata.md §1); a null
version's ID names no order, so the time is read from the mark its write left, which names the
version by the copy's own file whatever wrote the key since.

"A `200 OK` response can contain either a success or an error ... If the error occurs during the
copy operation, the error response is embedded in the `200 OK` response"; "When the request is an
HTTP 1.1 request, the response is chunk encoded" [4]. mantle answers once the copy is committed or
refused, with its status: an answer S3 allows ("If the error occurs before the copy action starts,
you receive a standard Amazon S3 error"), which carries the version's ID in its head. The cost is a
client's read timeout, botocore's 60 s (`bounds::STOCK_PATIENCE`), on a copy that runs longer; §6.

### 3.7 What a managed copy asks

s3transfer, the managed copy of boto3 and the AWS CLI, copies an object below
`multipart_threshold` with one CopyObject, and a larger one in parts, "to ensure a stored object is
not modified during a multipart copy" holding each part to the source's ETag by
`CopySourceIfMatch`; its CreateMultipartUpload carries the metadata HeadObject answered, `ContentType`
among it, unless the copy names `MetadataDirective: REPLACE` [10]. Its chunk size is the
configured one, doubled "while num_parts > self.max_parts" (10,000), within 5 MiB and 5 GiB [10].
mantle's own command line copies within the cell by one CopyObject to one copy's 5 GiB and past
it by part copies of S3's largest part, 5 GiB, the fewest a copy past one can take; it holds every
part to the ETag it headed, as s3transfer does.

### 3.8 Tags on a copy

GetObjectTagging "Returns the tag-set of an object", the current version's by default, another's by
`versionId`, answered `200` with `x-amz-version-id` and a `Tagging` holding its `TagSet` [14]. The
AWS CLI's `aws s3 cp` between two S3 locations, under its default copy properties, asks it of the
source: "Tags only need to be set if the operation is a multipart copy", so for a copy at or past
`multipart_threshold` it calls `get_object_tagging` on the source, does nothing more where the set
is empty, puts a set whose header form fits `_MAX_TAGGING_HEADER_SIZE = 2 * 1024` in the
CreateMultipartUpload's `Tagging`, and a longer one by `put_object_tagging` once the copy is done
[13]. A single CopyObject takes the source's tags server-side: "If you choose `COPY` for the
`x-amz-tagging-directive`, you don't need to set the `x-amz-tagging` header, because the tag-set
will be copied from the source object directly", "the default value of `x-amz-tagging-directive`
is `COPY`" [4].

S3 itself states how a store that keeps only the empty tag set answers. Of directory buckets:
"only the empty tag-set is supported. Any requests that attempt to write non-empty tags into
directory buckets will receive a `501 Not Implemented` status code", among them "When you attempt
to `REPLACE` the tag-set of a source object and set a non-empty value to `x-amz-tagging`", while a
`REPLACE` with the empty value, or none, "the default value of `x-amz-tagging` is the empty
value", is allowed [4]. mantle keeps no object tags yet, so it answers as S3 answers a directory
bucket: every version's tag set is the empty one, which GetObjectTagging answers, current or named,
a missing key or version answered as a head's is; and a write naming a non-empty set, PutObject's
or CreateMultipartUpload's `x-amz-tagging` or a CopyObject's under `x-amz-tagging-directive:
REPLACE`, is `501 NotImplemented`, the empty set taken. Before this, the listener took such a
write and kept none of its tags, and GetObjectTagging was `501`, which ended the CLI's multipart
copies.

## 4. UploadPartCopy

### 4.1 The source and its range

The source is named as CopyObject's, its conditions the same [5]. `x-amz-copy-source-range`: "The
range value must use the form bytes=first-last, where the first and last are the zero-based byte
offsets to copy. For example, bytes=0-9 indicates that you want to copy the first 10 bytes of the
source. You can copy a range only if the source object is greater than 5 MB." [5] "If the current
version is a delete marker and you don't specify a versionId in the `x-amz-copy-source` request
header, Amazon S3 returns a `404 Not Found` error, because the object does not exist. If you
specify versionId in the `x-amz-copy-source` and the versionId is a delete marker, Amazon S3
returns an HTTP `400 Bad Request` error, because you are not allowed to specify a delete marker as
a version for the `x-amz-copy-source`." [5] Its special errors: `NoSuchUpload`, 404, and
`InvalidRequest`, 400, "The specified copy source is not supported as a byte-range copy source."
[5]. s3-tests holds a range past a 5-byte source to `InvalidRange`, 400 or 416, and the malformed
ones (`0-2`, `bytes=0`, `bytes=hello-world`, `bytes=0-bar`, `bytes=hello-`, `bytes=0-2,3-5`) to
`400 InvalidArgument` (05 §4.3). So a range is judged by its form first, then against the
source's size, then by the 5 MB rule, whose 5 MB is S3's least part, 5 MiB.

### 4.2 The response

`CopyPartResult` holds the ETag, `LastModified`, "Date and time at which the object was
uploaded", and the part's checksum, "present if the multipart upload request was created with"
that algorithm, with the header `x-amz-copy-source-version-id` [5]. A part's row in mantle kept no
time (ListParts answered 1970); a part now records the gateway's time as its body ended, which
both ListParts and this answer state.

## 5. DeleteObjects

### 5.1 The request

"The request can contain a list of up to 1,000 keys that you want to delete. In the XML, you
provide the object key names, and optionally, version IDs if you want to delete a specific version
of the object from a versioning-enabled bucket." [6] Each object may carry an `ETag`, which
conditions its delete as `If-Match` conditions a DeleteObject's (05 §2.3). "The Content-MD5 request
header is required for all Multi-Object Delete requests" for general purpose buckets [6], yet
SDKs since December 2024 send a CRC checksum instead (05 §3.6). Swift's conformance tests [8]
assert what S3 answers: with neither, `400 InvalidRequest`, "Missing required header for this
request: Content-MD5 OR x-amz-checksum-*"; with a good CRC header and no Content-MD5, `200`; a
mismatched CRC, `400 BadDigest`, "The CRC32 you specified did not match the calculated
checksum."; a mismatched Content-MD5, `400 BadDigest`, "The Content-MD5 you specified did not
match what we received." 1,001 keys are refused 400 (s3-tests, 05 §8.1), which the document's
reader answers `MalformedXML`.

### 5.2 The response

"For each key, Amazon S3 performs a delete operation and returns the result of that delete,
success or failure, in the response. If the object specified in the request isn't found, Amazon S3
confirms the deletion by returning the result as deleted." "In quiet mode the response includes
only keys where the delete operation encountered an error. For a successful deletion in a quiet
mode, the operation does not return any information about the delete in the response body." [6]
"When a multi-object Delete request results in Amazon S3 either adding a delete marker or removing
a delete marker, the response returns" `DeleteMarker` true and `DeleteMarkerVersionId`; a delete of
a version names the version [6]. A failure is an `Error` of its code, key, message and version, in
a `200` (05 §8.2).

## 6. Open

- A CopyObject or UploadPartCopy that outlasts a stock client's 60 s read timeout: S3's early `200
  OK` with whitespace kept flowing and the result or error at the end (§3.6) would keep the client
  waiting, at the cost of the destination's `x-amz-version-id`, which the head would go out before
  the Name range assigns. Stock clients copy objects past 8 MiB in parts of 8 MiB (§3.7), each well
  within it.
- Object tags: PutObjectTagging, DeleteObjectTagging, tags at PutObject, CreateMultipartUpload and
  a copy under `x-amz-tagging-directive`, and `x-amz-tagging-count` on reads need a tag set kept
  with each version, a change to the Name layer's version record and so an upgrade fence
  (node.md §1.7); until then a version's set is the empty one and a write naming tags is refused
  (§3.8).
</content>
</invoke>
