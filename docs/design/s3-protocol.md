# S3 protocol: how mantle reads requests and writes responses

Status: design, 2026-09-28. Sources: docs/research/05 (S3 API semantics, cited as "05 §x"),
docs/research/13 (XML, cited as "13 §x").

`mantle-s3` is the protocol layer the gateway (STATUS item 2) is built from: pure functions
over requests and bodies, with no I/O and no knowledge of where objects live. Each module
states the part of 05 it implements; this record holds the decisions that go beyond
restating S3.

## 1. What the layer does

| Module | Does | Verified against |
|---|---|---|
| `sigv4` | Signature Version 4 in the `Authorization` header and in presigned URLs | every worked example in 05 §1.5 |
| `chunked` | `aws-chunked` bodies: signed chunks, signed or unsigned trailers | AWS's example bodies, byte for byte |
| `checksum` | the ten checksum algorithms, full-object CRCs combined from parts, composite values, ETags | AWS's multipart tutorial, ceph s3-tests |
| `route` | virtual-hosted and path-style addressing to an operation, bucket and key | 05 §14 |
| `conditional` | `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` in RFC 9110's order | 05 §2, s3-tests' matrix |
| `range` | `Range` and `x-amz-copy-source-range` | 05 §12, s3-tests |
| `list` | ListObjects and ListObjectsV2 paging | 05 §6, s3-tests |
| `xml`, `body` | the XML reader and writer, and request documents read against their schemas | 13; roxmltree as an oracle |
| `response` | the documents responses carry: listings, multipart and copy results, batch deletes, errors, bucket settings | 23 of AWS's sample responses, s3-tests |

## 2. XML bodies

**Decision: mantle reads request XML with its own reader, and writes responses with its own
writer.**

The documents S3's requests carry are small and fixed (13 §6): a root in S3's namespace or
none, elements that hold either elements or text, and no attributes but namespace
declarations. They arrive from clients mantle does not trust.

- **No entities.** XML's entity mechanism turns a small document into an arbitrarily large
  one, and external entities reach outside the document (RFC 7303 §10; 13 §2). The reader
  refuses a document type declaration, so the five predefined entities are the only ones
  (XML 1.0 §4.1; 13 §1).
- **Work linear in the body, memory in the values read.** roxmltree, a strict Rust XML
  reader, builds the whole tree before a caller sees it and finds duplicate
  attributes by scanning a tag's earlier attributes, with nothing bounding their number
  (13 §8). A body inside mantle's limits could put hundreds of thousands of attributes on one
  tag. mantle's reader holds at most 16 attributes on a tag, eight times the most AWS's
  samples carry (13 §6.8), keeps no tree, and descends only into elements the schema names,
  so the schema bounds the nesting.
- **Checked against an oracle.** roxmltree is kept as a test dependency. Generated S3-shaped
  documents, each written one of the many ways XML allows (declarations, BOM, comments,
  processing instructions, CDATA, character and entity references, line ends, namespace
  prefixes, empty-element tags), read identically in both. Every mutation of them roxmltree
  refuses, mantle refuses: in a 20,000-mutation run roxmltree refused 13,994, and mantle
  refused all of them (`crates/s3/src/xml.rs` tests).

Three deliberate differences from XML 1.0, each for a stated reason:

1. **A character reference may name any character but NUL,** as XML 1.1 allows (13 §4). S3
   lists a key holding a control character as `&#x4;` (13 §7); a client that sends that key
   back in DeleteObjects must be understood.
2. **UTF-8 only.** XML 1.0 requires processors to accept UTF-16 (13 §1). Every S3 sample
   and SDK body is UTF-8 (13 §6), so a body in, or declaring, another encoding is
   `MalformedXML` rather than a second decoding path.
3. **No attributes but namespace declarations,** which is all S3's request documents in
   scope carry. The ACL grantee's `xsi:type` (13 §6.8) is admitted when ACL documents are.

**Size limits are computed, not chosen.** Each document's limit is the longest body S3's own
limits allow, doubled for white space (`crates/s3/src/body.rs`):

- every field at its longest: keys 1,024 bytes (05 §10.1), version IDs 1,024 (13 §6.4),
  the longest ETag mantle writes, all ten checksums on a part (13 §6.2), a region name as a
  63-octet DNS label (RFC 1035 §2.3.4);
- arbitrary text written at six bytes a byte (`&quot;`, `&apos;`, `&#x0D;`, the longest
  escapes of one byte), while digits, hex and base64, which never need escaping, count once;
- the most items S3 allows: 10,000 parts (05 §4.1), 1,000 objects (05 §8.1);
- then as much white space again, since white space carries nothing and has no length of
  its own to bound.

That gives 14,040,274 bytes for CompleteMultipartUpload, 14,774,246 for DeleteObjects, 490
for CreateBucketConfiguration and 388 for VersioningConfiguration. The gateway reads no more
than the limit and answers `MaxMessageLengthExceeded` beyond it (05 §11.2). A compact
10,000-part body with one CRC-32 per part is about 1.3 MB, so real bodies sit far inside.

**Each part is checked as it is read.** CompleteMultipartUpload's parts must run from 1 to
10,000 (`InvalidPart`) in ascending order (`InvalidPartOrder`) (13 §6.2). Checking each part
against the one before bounds the list at 10,000 however long the body; the cost is that a
list that is out of order before it is malformed reports the order.

**The writer escapes what a reader would change.** `&` and `<` as XML requires, `>` so that
`]]>` cannot form (XML 1.0 §2.4), a carriage return as `&#xd;` because a reader turns a
literal one into a line feed (§2.11), and each character XML 1.0 cannot carry as a character
reference, as S3 does (13 §7). A property test reads back exactly whatever text the writer
was given.

## 3. Listing

A page is computed over an ordered index (`crates/s3/src/list.rs`). For keys the index
answers one question, the first key at or after a byte string that lists; for versions and
multipart uploads, the first entry at or after a key and an ID. The Name layer's scans
answer them (`crates/meta/src/name.rs`: `next_current`, `next_version`, `next_upload`).

- **One seek per common prefix.** Keys that roll up into a common prefix "count as a single
  return" (05 §6.2), so the page passes the rest of a common prefix with one seek to the
  first key beyond it, however many keys it holds.
- **Resuming inside a common prefix.** A common prefix "is filtered out from results if it is
  not lexicographically greater than the `StartAfter` value" (05 §6.2), and likewise than the
  key marker of ListObjects, ListObjectVersions and ListMultipartUploads (05 §6.3–§6.4,
  §4.7). The common prefix a marker itself rolls up into is a prefix of it, so never greater,
  and every key after it under that prefix rolls up into it: the page starts beyond that
  common prefix. Every other common prefix the page meets is greater than the marker.
- **Markers of versions and uploads.** A key marker alone starts after all of its key's
  entries, "only the keys lexicographically greater than the specified key-marker"; with a
  version-ID or upload-ID marker, after that entry of the key (05 §4.7). Within a key,
  versions come newest first and uploads in the order they were created, which is their IDs'
  order (metadata.md §1). A version is its key's latest when it is the first the scan meets
  on entering the key, so no read is spent on it; a page resumed inside a key calls its first
  version not latest even if a newer one was deleted since the previous page.

**Decision: a page passes at most 1,000 keys that list nothing, then ends early, and the
next page starts just after the last key passed.**

An index passes keys that hold nothing a listing shows: for ListObjects, a key whose current
version is a delete marker, which "ListObjects/V2 do not return" (05 §6.4), or one holding
only uploads; for ListObjectVersions, a key holding only uploads; for ListMultipartUploads,
one holding only versions. A run of them can be as long as the bucket, and every loop needs
a bound (CLAUDE.md §2). The bound is S3's own page size, `MAX_KEYS`, "up to 1,000"
(05 §6.2): a page never reads more than twice the keys the largest page lists, and a listing
costs the keys it passes plus the items it shows. Reaching the bound ends the page early,
which S3 allows, "The response might contain fewer keys" (05 §6.2), possibly with nothing
listed.

The next page must start after the keys already passed, or a run longer than the bound would
be read again by every page and never passed. It must also still be able to list the common
prefix the last key passed is in, which the page did not list, and which the marker rule above
would drop. How each listing says so:

- **ListObjectsV2.** The continuation token is mantle's own, "obfuscated and is not a real
  key" (05 §6.2): the base64url of a tag and a key, `a` to start after an item listed, with
  the marker rule, and `p` to start just past a key passed, without it. A token a client
  alters only moves where the next page starts, as `start-after` could.
- **ListObjectVersions and ListMultipartUploads.** Clients echo `NextKeyMarker` with
  `NextVersionIdMarker` or `NextUploadIdMarker` (botocore's paginators; 05 §6.5). A page that
  paused names the last key passed and the ID marker `passed`, which no version or upload ID
  can be (IDs are 13 characters of Crockford's base 32, or `null`); that pair starts the next
  page just after the key without the marker rule.
- **ListObjects.** Clients echo `NextMarker`, or else the last key listed (05 §6.5). A page
  that paused names the last key passed as `NextMarker`, including without a delimiter, where
  S3 returns it "only if you have the delimiter request parameter specified" (05 §6.3):
  without it, a client would resume at the last key listed, before the keys passed, and a
  page that listed nothing would end the listing. A marker cannot say "without the marker
  rule", so a page that pauses inside a common prefix it has not listed lists that prefix
  and resumes beyond it. The keys under it lie ahead, and some may list; if none does, the
  listing shows a common prefix under which nothing lists. The alternatives are worse: to
  read on without a bound, to refuse the request, or to drop the prefix and every key under
  it. ListObjectsV2 has no such case.

ListMultipartUploads writes `NextKeyMarker` and `NextUploadIdMarker` on every page, as all
three of AWS's samples do: where the next page starts, or on a page that is not truncated,
the last upload listed (13 §9.3).

**Cost of uploads kept under their keys.** An upload sorts with its key (metadata.md §1), so
a listing of uploads passes every key between two keys with uploads, at two seeks each, and
pages through a bucket of many keys and few uploads. ListMultipartUploads is a client's
cleanup and resume path, not a read path, and completing an upload stays within one range. An
index of uploads kept in each range beside its keys would make the listing cost the uploads
alone; it waits for a measurement that the scan costs too much.

## 4. Response documents

**Decision: each response document is written from typed content, in the element order of its
operation's Response Syntax, and checked against AWS's own sample responses**
(`crates/s3/src/response.rs`).

- **Order.** SDKs find a response's elements by name: botocore strips the namespace from each
  tag and looks members up by name (13 §9.1). AWS's samples order elements differently from
  one another, and the Response Syntax is the one order the reference states, so mantle
  writes that order.
- **Namespace.** Every result is in S3's namespace, as ten of the thirteen results' samples
  are; the error document is in none, as no error sample is (13 §9.1).
- **Times are whole seconds.** Every listing, multipart and copy sample writes `.000`
  milliseconds (13 §9.2), and HEAD's `Last-Modified` is an HTTP-date, which has one-second
  resolution. The gateway takes an object's time in whole seconds once, and the header, the
  document and the conditional checks all use that value, so a client comparing a listing
  with a HEAD sees one instant. A time outside the years 0000–9999 is `InternalError`; mantle
  stores times as unsigned 64-bit nanoseconds, which end in 2554, so none arises.
- **Owners are IDs.** An `Owner` or `Initiator` holds `ID` alone: S3 stopped returning
  `DisplayName` in November 2025 (13 §9.4). The three s3-tests that still read it
  (`test_bucket_list_return_data`, its versioning twin, `test_list_multipart_upload_owner`)
  are deselected for that reason when the suite runs against mantle.
- **ETags** are passed without their quotes, the form the metadata layer keeps, and written
  quoted. **Storage class** is `STANDARD` for every object: mantle stores objects one way.

Where AWS's reference and samples leave a choice, mantle takes the one its evidence shows:

- **Next markers.** ListObjectVersions writes `NextKeyMarker` and `NextVersionIdMarker` only
  on a truncated page, as its samples do. ListMultipartUploads writes `NextKeyMarker` and
  `NextUploadIdMarker` on every page, as all three of its samples do, truncated or not.
  ListParts writes `NextPartNumberMarker` only when truncated, the Response Elements' words;
  no sample shows an untruncated list (13 §9.3).
- **Echoed parameters.** `Prefix` is written even when none was sent, in every listing
  including ListMultipartUploads, whose samples disagree; `Delimiter` only when one was sent
  and is not empty; `KeyMarker`, `VersionIdMarker` and `UploadIdMarker` always, empty when
  none was sent, as the samples write them (13 §9.3; 05 §6.2–§6.4).
- **GetBucketLocation** writes the region as the root's text, as the sample does, rather
  than the nested element the Response Syntax shows; botocore reads the root's text, and an
  empty root is S3's `null` for the default region (13 §9.5).
- **DeleteObjects** writes `DeleteMarker` only as `true`, beside the marker's version ID, as
  every sample does, and quiet mode leaves out what was deleted (13 §9.5; 05 §8).

**Verified** against 23 of AWS's sample responses covering all fourteen documents: for the
same content, mantle's document and the sample, both read by roxmltree, hold the same
elements with the same text, and the same namespace. Tests pin the s3-tests expectations of
the listings (05 §6.2–§6.3) and the elements `encoding-type=url` reaches. A property test
lists arbitrary keys and reads each back from the document: exactly, when XML 1.0 can carry
it, and through percent-decoding with `encoding-type=url` when it cannot, while roxmltree
refuses the document written without it (13 §7). Breaking any rule above, one at a time,
fails a test.
