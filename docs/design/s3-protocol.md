# S3 protocol: how mantle reads requests and writes responses

Status: design, 2026-09-28. Sources: docs/research/05 (S3 API semantics, cited as "05 §x"),
docs/research/13 (XML, cited as "13 §x"), docs/research/16 (CORS, cited as "16 §x"),
docs/research/17 (bucket policies and JSON, cited as "17 §x"), docs/research/18 (Object Lock,
cited as "18 §x"), docs/research/19 (browser uploads, cited as "19 §x"), docs/research/20
(server-side encryption, cited as "20 §x").

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
| `route` | virtual-hosted and path-style addressing to an operation, bucket and key | 05 §14; §6 |
| `conditional` | `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` in RFC 9110's order | 05 §2, s3-tests' matrix |
| `range` | `Range` and `x-amz-copy-source-range` | 05 §12, s3-tests |
| `list` | ListObjects and ListObjectsV2 paging | 05 §6, s3-tests |
| `xml`, `body` | the XML reader and writer, and request documents read against their schemas | 13; roxmltree as an oracle |
| `json` | the JSON reader and compact writer, for bucket policies | 17 §1; JSONTestSuite's 318 cases |
| `policy` | bucket policies checked, and requests judged against them | 17; S3's recorded answers, s3-tests' policies, IAM's documented semantics |
| `account` | account IDs, and the expected bucket owner a request names | 17 §2.1; S3's recorded answers |
| `lock` | Object Lock's documents and headers, and the rules for writes that carry locks | 18; S3's recorded answers, s3-tests |
| `form` | `multipart/form-data` bodies, decoded as they stream: the fields before the file, then the file | RFC 7578 and RFC 2046; property tests over every cut of a body |
| `post` | browser uploads: a form's policy, its signature and conditions, and the answer to it | AWS's signed example and botocore's presigned POST, byte for byte; S3's recorded answers, s3-tests' policies |
| `sse` | server-side encryption's headers and a bucket's encryption configuration | S3's recorded answers, ceph s3-tests' keys, botocore's SSE-C encoding |
| `seal` | data at rest: a key per file, AES key wrap, AES-256-GCM segments | RFC 3394's wrap vector, GCM's test cases 13 and 14, property tests |

| `tagging` | tag sets: S3's limits and characters, and the `x-amz-tagging` header | 13 §6.7, s3-tests, S3's observed answers |
| `acl` | canned and header ACLs, Object Ownership, and whether a request's ACL goes ahead with ACLs disabled | 13 §6.8, s3-tests |
| `lifecycle` | lifecycle rules checked, when each action falls due, and the expiration and abort headers | 13 §6.9, the user guide's worked examples, s3-tests |
| `cors` | CORS rules checked, preflights answered or refused, and the headers of an actual cross-origin request | 16, S3's recorded answers, s3-tests |
| `response` | the documents responses carry: listings, multipart and copy results, batch deletes, errors, bucket settings, tags, ACLs, lifecycle and CORS rules | 28 of AWS's sample responses, s3-tests |

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
3. **No attributes but namespace declarations,** which is all S3's request documents carry,
   and `xsi:type` on an ACL's `Grantee`, the one other attribute AWS's samples and service
   model put on a request (13 §6.8). An ACL document admits it there and nowhere else.

**Size limits are computed, not chosen.** Each document's limit is the longest document S3's
schema admits with every element in it at most once, alternatives S3 refuses together
included, so that each refusal gets its own answer (`crates/s3/src/body.rs`):

- every field at its longest: keys 1,024 bytes (05 §10.1), version IDs 1,024 (13 §6.4),
  the longest ETag mantle writes, all ten checksums on a part (13 §6.2), a region name, and a
  directory bucket's zone, as a 63-octet DNS label (RFC 1035 §2.3.4), tag keys and values of
  128 and 256 UTF-16 units at three bytes a unit (13 §6.7; RFC 3629 §3), an email address of
  254 octets (RFC 5321 §4.5.3.1.3), an ACL's IDs and ignored display names as 13 §6.8 bounds
  them, and every integer at `xs:int`'s or `xs:long`'s longest;
- arbitrary text written at six bytes a byte (`&quot;`, `&apos;`, `&#x0D;`, the longest
  escapes of one byte), while digits, hex and base64, which never need escaping, count once;
- the most items S3 allows: 10,000 parts (05 §4.1), 1,000 objects (05 §8.1), 10 tags on an
  object and 50 on a bucket (13 §6.7), 100 grants (13 §6.8), 1,000 lifecycle rules.

White space between elements is not counted: S3's documents give each element either
elements or text, never both (13 §6), so a run of white space after an end tag, an
empty-element tag or the XML declaration, or before a start tag, carries nothing, and the
gateway drops it as the body arrives (`xml::Compact`), keeping a run inside an element's text,
a key of one space, as data. Comments, CDATA sections and processing instructions are kept
whole, their end sought only after their whole opening as XML 1.0 [15], [16] and [18] read
them: `<!--->` opens a comment whose content starts `->`, so markup inside it that looks like
an end tag never makes the white space after the comment look like it lies between elements.
The limit bounds what is kept, and a body is refused with
`MaxMessageLengthExceeded` (05 §11.2) once what is kept passes it; white space costs the
gateway the time to read it and no memory. A reader given a body as sent counts it the same
way, so a document reads alike as sent or as kept. Before, each limit was doubled as an
allowance for white space, a factor nothing established, and the doubling hid four limits
that left out elements their readers answer: Object Lock's `DefaultEventHold` and `Years`,
Retention's event holds, a lifecycle expiration's `Days` and `ExpiredObjectDeleteMarker`, and
CreateBucket's directory-bucket elements.

That gives 7,020,137 bytes for CompleteMultipartUpload, 7,387,123 for DeleteObjects, 347,928
for CreateBucketConfiguration with its tags, 194 for VersioningConfiguration, 69,612 and
347,572 for an object's and a bucket's Tagging, 331,027 for AccessControlPolicy, 193 for
OwnershipControls, 393 for ObjectLockConfiguration, 319 for Retention and 83,033,135 for a
LifecycleConfiguration of 1,000 rules at their longest. A compact 10,000-part body with one
CRC-32 per part is about 1.3 MB, so real bodies sit far inside. A CORS configuration is held
to S3's own limit, 64 KB for the document as sent (16 §1.1), and read as sent. A bucket
policy is kept as sent, since GetBucketPolicy gives back the bytes set (17 §2.2), and S3 counts
its compact form against 20 KB; what S3 keeps of a policy's white space is not recorded, so
its body limit, twice the compact one, awaits a recording of S3.

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

**Verified** against 27 of AWS's sample responses covering all seventeen documents: for the
same content, mantle's document and the sample, both read by roxmltree, hold the same
elements with the same text, and the same namespace. Tests pin the s3-tests expectations of
the listings (05 §6.2–§6.3) and the elements `encoding-type=url` reaches. A property test
lists arbitrary keys and reads each back from the document: exactly, when XML 1.0 can carry
it, and through percent-decoding with `encoding-type=url` when it cannot, while roxmltree
refuses the document written without it (13 §7). Breaking any rule above, one at a time,
fails a test.

## 5. Tags and ACLs

**Decision: a tag set is held to S3's documented rules, and where the documents name no
error, to what S3 was observed to answer** (`crates/s3/src/tagging.rs`; 13 §6.7).

- **Limits.** 10 tags on an object and 50 on a bucket. Keys hold 1 to 128 and values 0 to 256
  UTF-16 code units, the reading of "internally represented in UTF-16 ... characters consume
  either 1 or 2 character positions". Keys are unique.
- **Characters.** Keys and values hold "Unicode letters or numbers, white space" and
  `_ . : / = + @ -`: AWS's pattern `[\p{L}\p{Z}\p{N}_.:/=+\-@]`, decided by Unicode 16.0's
  general categories. S3 refused a key outside the set on PutObject, so it checks objects'
  tags as well as buckets'. The categories come from the `unicode-general-category` crate,
  looked up behind an unwind boundary; the Rust standard library's `is_alphabetic` is the
  Alphabetic property, which also admits combining marks the pattern refuses.
- **No `aws:` keys.** The prefix is AWS's own. S3 keeps the system tags it applied when a
  request replaces a bucket's tags; mantle applies none, so a request naming one would be
  adding it.
- **Errors.** A bad key or value, a duplicate key or an `aws:` key is `InvalidTag`, as S3's
  error table describes the code. Too many tags is `BadRequest`, "Object tags cannot be
  greater than 10", which S3 answered PutObject's header and PutObjectTagging alike. s3-tests
  expects `InvalidTag` there, and that one assertion is expected to fail against mantle as it
  would against S3. A malformed header, or one naming a key twice, is `InvalidArgument`, as
  S3 answered.
- **Order.** A tag set is kept and given back in key order, as s3-tests expects of a set sent
  in the header in another order. Keys compare by code point; no source gives S3's order
  beyond ASCII.
- **The header** is a form-encoded query (WHATWG URL §5.1): `+` is a space, `%XX` a byte, each
  side UTF-8, a key without `=` holds the empty value, and a pair with a second `=` is
  refused. SDK presigners move `x-amz-tagging` into a presigned URL's query string, and S3
  reads it there, so the gateway takes it from either place.
- **CreateBucket's `Tags`** are read with the configuration and held to a bucket's rules.

**Decision: every bucket has ACLs disabled.** Object Ownership's bucket owner enforced
setting has been S3's default for every new bucket since April 2023 (13 §6.8). Under it ACLs
"no longer affect permissions", so no authorization decision in mantle reads an ACL: the
bucket owner holds every right, and granting others waits for bucket policies (STATUS)
(`crates/s3/src/acl.rs`).

- **Reading an ACL** answers the bucket owner's full control, for the bucket and every object
  in it, as S3 does: "Requests to read ACLs always return a response that shows full control
  for the bucket owner".
- **Writing an ACL.** PutObject, CopyObject, CreateMultipartUpload, PutBucketAcl and
  PutObjectAcl go ahead when they name no ACL, the `bucket-owner-full-control` canned ACL, or
  grants, in headers or an `AccessControlPolicy`, of full control to the bucket owner alone.
  Anything else is `AccessControlListNotSupported`. `private` is refused although it grants
  the same once the bucket owner owns the object: s3-tests expects that of all five
  operations, and S3's text names only the canned `bucket-owner-full-control` and the XML
  form.
- **CreateBucket** refuses an ACL that reaches another account with
  `InvalidBucketAclWithObjectOwnership`: every canned ACL but `private` and the two object ACLs
  S3 ignores on a new bucket, and grants to anyone but the requester.
- **Malformed before refused.** A request's ACL is read whole, and a malformed one answered
  as malformed, before the ownership rule is applied; S3 documents no order, and this reports
  the first fault a client can fix. A canned name S3 does not define is `InvalidArgument`; a
  grant header that is not a list of `id=`, `uri=` or `emailAddress=` pairs, quoted as AWS
  writes them or bare as s3-tests sends them, is `InvalidArgument`; a canned ACL beside grant
  headers is `InvalidRequest`, as S3 answered; headers beside a body is `InvalidRequest` too,
  by that analogy, since no source names the error; a document S3's schema refuses is
  `MalformedACLError`. More than 100 grants is `InvalidArgument` in headers and
  `MalformedACLError` in a document; no source names either.
- **Email grantees.** S3 answers 405 to an email grantee since October 2025. With ACLs
  disabled, a grant to an email address is not the owner's full control, so mantle answers
  `AccessControlListNotSupported` and resolves no addresses.
- **Ownership controls** answer the one setting. GetBucketOwnershipControls answers bucket
  owner enforced. PutBucketOwnershipControls and CreateBucket's `x-amz-object-ownership` take
  it, and answer the two settings that enable ACLs `501 NotImplemented`, as they do
  DeleteBucketOwnershipControls, which would leave the bucket with ACLs enabled.

## 6. Routing

**Decision: a subresource names its operation.** A request carrying a subresource mantle
serves is that subresource's operation for its method, or `405 MethodNotAllowed`; it is never
the bucket's or the object's own operation (`crates/s3/src/route.rs`). Routing on the method
first would take `DELETE /bucket?versioning` for DeleteBucket and `PUT /bucket/key?attributes`
for PutObject, so a client's mistake would delete a bucket or replace an object. A PUT to an
upload without `partNumber` is refused the same way rather than written as the object.
So is a subresource S3 defines only on buckets sent with a key, and one defined only on
objects sent to a bucket: `PUT /bucket/key?lifecycle` writes no object, and
`DELETE /bucket?uploadId=u` deletes no bucket. Subresources S3 defines and mantle does not
serve are `501 NotImplemented` whatever the method. A `POST` to a bucket with none is a browser
upload (§12), which a form sends to "the URL of the bucket" (19 §2.1).

## 7. Lifecycle configuration

**Decision: a bucket's lifecycle rules are checked as S3 checks them, and mantle takes S3's
expiration actions: of current versions, of noncurrent versions, of delete markers left
alone, and of incomplete multipart uploads** (`crates/s3/src/lifecycle.rs`; 13 §6.9).

- **Transitions are refused.** mantle stores every object in one class, so a rule with a
  `Transition` or `NoncurrentVersionTransition` is `501 NotImplemented`. The configuration is
  checked first, so one S3 would refuse is answered as S3 answers it, as s3-tests expects of a
  transition dated at no midnight. The checks S3 was recorded making of transitions are made:
  a class it defines, 30 days before `STANDARD_IA` or `ONEZONE_IA`, no class twice, no dates
  beside day counts. The order S3 requires between classes is not, since no transition is
  taken. Accepting a transition and never making it would give back a configuration that does
  not describe the bucket.
- **Two forms, as S3 has them.** A configuration whose first rule has a `Filter` is what S3
  calls Lifecycle V2; one whose first rule has its own `Prefix` is the form before it. S3
  refuses the other form beside the first, and in the older form refuses
  `NewerNoncurrentVersions` and two rules whose prefixes overlap taking the same kind of
  action, each with the message it was recorded answering. Overlap is found by sorting each
  kind's prefixes and comparing neighbours: a prefix that begins another begins the one after
  it.
- **A rule comes back as it was written.** A rule-level `Prefix` stays one and a `Filter`
  stays a `Filter`, as AWS's sample and s3-tests expect. A rule's elements are written in the
  order S3 was recorded writing them, `ID`, `Filter` or `Prefix`, `Status`, then its actions.
  The PUT's `x-amz-transition-default-minimum-object-size` is kept and answered on the GET,
  and when none was sent both answer `all_storage_classes_128K`, as S3 does.
- **IDs.** A rule given no ID, or an empty one, is named `rule-N` with the lowest N no rule was
  given, so the same document names its rules the same way each time it is put. S3 names such
  a rule with a random UUID in base64; no client can rely on its form, and s3-tests asks only
  that an ID be there. An ID holds at most 255 UTF-16 code units, counted as S3 counts a tag's
  characters.
- **Filters.** A `Filter` holds nothing, which applies to every object, or exactly one
  predicate; `And` holds two or more, an empty `Prefix` counting as one. Sizes exclude their
  bounds, and lie from 0, or 1 for `ObjectSizeLessThan`, to 1000 × 2^40 bytes, the range S3
  answered; the documented 50 TB is not what S3 enforces. A tag in a filter may leave out its
  `Value`, which then matches only a tag with no value, as the user guide says. No
  `ExpiredObjectDeleteMarker` or abort sits beside a tag or size predicate, as S3 refuses.
- **Limits.** 1,000 rules. A filter names at most 10 tags, and a prefix at most 1,024 bytes:
  an object holds at most 10 tags and a key at most 1,024 bytes, so a filter past either
  matches nothing, and S3 documents no limit of its own. `NewerNoncurrentVersions` is at least
  1 with no maximum: S3 accepted 500 against the documented 100. The body limit follows from
  these, as for the other documents (§2): about 165 MB, most of it 10,000 tags at their
  longest.
- **Due times.** An action falls due its days after the version was created, or after its
  successor was, rounded up to midnight UTC; a time already at midnight is its own ceiling,
  since no source says otherwise. Of the rules that apply, the one due first wins, and the
  first given on a tie, as S3 answered for rules of equal days: "the shorter expiration policy
  is honored". A `Date` rule is due on its date for every object it applies to.
- **What each action does.** An expiration applies only to a current version that is not a
  delete marker, and deletes it as DeleteObject without a version ID would in the bucket's
  versioning state. `NewerNoncurrentVersions` N keeps the N newest noncurrent versions, delete
  markers among them, whatever their age, as the API reference and s3-tests read it; the user
  guide's examples page, which would keep N+1, disagrees with both. A delete marker with no
  version beneath it is removed at once under `ExpiredObjectDeleteMarker`, and under an
  expiration of `Days` once it is that old. A delete marker has no tags and size 0, as the
  user guide's rule for noncurrent delete markers treats it. An upload is aborted by prefix.
- **Headers.** `x-amz-expiration` is `expiry-date="<HTTP-date>", rule-id="<ID>"`, and
  `x-amz-abort-date` an HTTP-date, as recorded. The ID goes as it is, as S3 was recorded
  sending IDs with spaces, although the documentation calls it URL-encoded. An ID may hold
  any character a document can carry, among them a line break or a quote, which a header
  cannot; such an ID is percent-encoded whole, as documented. Neither header is written for a
  date past 9999-12-31, which an HTTP-date cannot hold, and the gateway writes the expiration
  only for the current version asked for without a version ID, as S3 does.
- **Dates.** A `Date` is an XML Schema `dateTime` with its zone, which must name midnight UTC
  in a year from 0000 to 9999. A time without a zone names no instant and is refused.
- **Errors.** Each fault answers the code S3 was recorded answering, where one was recorded;
  otherwise the code of the recorded fault nearest it, an argument out of range
  `InvalidArgument`, and a combination S3 refuses `InvalidRequest`. For no rules and for more
  than 1,000, S3's error table and its recorded answers differ; mantle answers the table's
  `InvalidRequest`, which is current, where the recordings are from 2016 and 2017.
- **Storage** of a configuration is the metadata layer's, and open (metadata.md §6): at its
  largest it is about 13 MB unescaped.

## 8. CORS

**Decision: a bucket's CORS rules are checked as S3 was recorded checking them, and a request
carrying an `Origin` is answered as S3 answers it, where the Fetch standard, which browsers
enforce, agrees** (`crates/s3/src/cors.rs`; 16).

- **Checks.** 1 to 100 rules in at most 64 KB, as documented. A rule without an origin or a
  method, and no rules at all, are `MalformedXML`; a method other than the five a rule may
  allow is `InvalidRequest` naming it, and so is an origin with two `*`, with S3's recorded
  messages. An allowed header with two `*` is refused the same way, as S3 documents one `*` at
  most. An ID holds at most 255 characters and names one rule, as documented, and a negative
  `MaxAgeSeconds` is refused, since `Access-Control-Max-Age` holds delta-seconds. Exposed
  headers are not checked as names, as S3 accepted `GET` among them. An empty origin is
  accepted, as S3 accepted it.
- **Matching.** The first rule that matches decides. An origin matches a pattern byte for
  byte, as a browser compares the answer with its origin; a `*` stands for any run of
  characters, the empty one included, as s3-tests expects. No source says how S3 compares
  case, so mantle does not fold it. A method matches exactly. Each requested header must match
  an allowed header whatever its case, as S3 answered; a rule with no allowed headers allows
  none.
- **The answer.** A named origin is echoed with `Access-Control-Allow-Credentials: true`, and
  an origin a rule allows only by `*` is answered `*` without it, as S3 answered both. A
  wildcard rule so never grants a request with credentials, which a browser refuses `*`
  (16 §4). `Access-Control-Allow-Methods` lists the rule's methods, as every recorded answer
  does, and `Access-Control-Allow-Headers` the requested ones, lowercase, as S3 answered in
  2025.
- **Preflights.** `OPTIONS` on a bucket or an object is a preflight whatever its query, as a
  preflight may carry the parameters of the request it precedes, and it is answered without
  authentication: a browser sends none, and s3-tests preflights presigned URLs. Without an
  `Origin` it is `400 BadRequest`; to a bucket without rules `403 AccessForbidden`, "CORS is
  not enabled"; when no rule matches `403 AccessForbidden`, "This CORS request is not
  allowed", with S3's messages and the `Method` and `ResourceType` elements its error carries.
  An `Origin` without `Access-Control-Request-Method`, to a bucket with rules, is `400`, as
  s3-tests expects; no recording of S3 covers it.
- **Actual requests** are never refused for CORS, as S3 serves a PUT no rule allows. One that
  carries an `Origin` is matched by the method and headers it asks about, when it carries
  `Access-Control-Request-Method` or `Access-Control-Request-Headers`, and otherwise by its own
  method, as S3 answered; its real headers are not checked. The CORS headers go on its
  response whatever its status, as s3-tests expects of a 404 and a 403.
- **`Vary`** goes on every response to a request for a bucket with CORS rules, with or without
  an `Origin`, matched or not. S3 sends it only when a rule matches, so a cache in front of it
  may keep an answer without CORS headers and give it to a browser a rule allows; the Fetch
  standard names that failure and prescribes `Vary` against it (16 §4).

## 9. JSON bodies

**Decision: mantle reads JSON with its own reader, strict to RFC 8259's grammar, as it reads
XML with its own** (`crates/s3/src/json.rs`; 17 §1). A bucket policy is JSON, and its grammar
forbids a key given twice (17 §3.1). A general reader into a map keeps one of two same-named
members silently, as RFC 8259 notes "many implementations" do, so a policy read that way could
be enforced differently from the one its author reviewed.

- **What it refuses.** An object naming a member twice, compared after escapes are replaced,
  and a string holding an unpaired surrogate: I-JSON's rules for what a receiver may refuse
  to trust (RFC 7493 §2). Text that is not UTF-8, and a byte order mark, which begins no value.
  Nesting past 32 levels, where a policy needs six, so a text of 100,000 brackets is refused
  after 32 of them.
- **What it keeps.** Numbers as written, since the grammar bounds neither range nor precision
  and a policy compares a number only under a numeric condition. Member order, so a document
  written back compactly holds its members as sent.
- **Checked against JSONTestSuite**, vendored with its license: all 95 cases a parser must
  accept but the two of a repeated name, all 188 it must refuse, and each of the 35 left to
  the implementation answered as the rules above say. Property tests read back every value
  from its compact form, and answer any bytes without a panic.

## 10. Bucket policies

**Decision: a bucket policy is checked as S3 checks one and judged as IAM documents, for
mantle's principals: accounts and the anonymous requester** (`crates/s3/src/policy`; 17).
With ACLs disabled on every bucket (§5), a policy is how a bucket's owner lets anyone else in.

- **Expected owners.** `x-amz-expected-bucket-owner`, and a copy source's
  `x-amz-source-expected-bucket-owner`, refuse a request whose bucket another account owns,
  `403 AccessDenied`, and a value that is no 12-digit account ID first, `400
  InvalidBucketOwnerAWSAccountID` naming it, as S3 answered (`crates/s3/src/account.rs`).
- **Principals.** An account is named as AWS names one, by its 12-digit ID or its root's ARN,
  or by its canonical user ID, so a policy written for S3 names mantle's accounts unchanged.
  mantle has no IAM users, roles or federation, so a policy naming one names no principal
  mantle has, and is refused as S3 refuses one that does not exist, "Invalid principal in
  policy". `"*"` and `{"AWS": "*"}` are everyone, the anonymous requester included, as IAM and
  s3-tests have them. A service principal is accepted and matches no requester of mantle's.
- **The document** is read by the strict JSON reader (§9) from a body of at most 40,960 bytes,
  and held to 20,480 bytes written compactly, the normalized size S3 was recorded measuring.
  Each fault is `400 MalformedPolicy` with S3's recorded message, or with the one versitygw
  reports S3 answering where no recording has one. GetBucketPolicy gives back the bytes set, as
  s3-tests expects, where S3 re-serializes: a client comparing what it set with what it gets
  sees them equal.
- **Actions** are the 106 the Service Authorization Reference lists on a bucket or an object,
  generated into `policy/catalog.rs` by `scripts/s3-actions.py` from its vendored document and
  checked against it by a test. A pattern naming none is "Policy has invalid action", and one
  naming only actions on a kind of resource the statement's resources cannot be is "Action does
  not apply to any resource(s) in statement", as S3 answered `s3:PutObject` on a bucket's ARN.
- **Resources** are S3 ARNs whose bucket part can be the policy's own bucket: S3 refused `*`
  and another bucket. A `*` stands for any run, `/` and `:` included, as IAM documents, and a
  `?` for one character; resources compare with regard to case, actions without.
- **Condition keys.** S3's own keys are the catalog's, each accepted only where one of the
  statement's actions carries it; any `aws:` key is accepted, as AWS keeps adding global keys
  and a refusal would break a valid policy.
- **Each operation's action.** `policy::action` gives the action each routed operation asks
  for, as S3 authorizes it, in its `Version` form when the request names a version (17 §5); a
  test holds every one to the catalog, on the kind of resource the operation names. A copy is
  judged by parts, `s3:PutObject` on its destination and its source as a GetObject, and
  DeleteObjects key by key. A preflight asks for none.
- **Judgment.** A denying statement that applies refuses the request, the owner's included;
  the owner's account may do anything else, and may always read, set and delete the policy, as
  S3 keeps a root from locking itself out; anyone else needs an allowing statement. mantle's
  requesters are accounts, each its own root, so no identity policy stands between another
  account and what the bucket policy grants it, as s3-tests expects of another account's root.
- **Conditions** follow IAM: a missing key fails a positive operator and satisfies a negated
  one, `...IfExists` and `ForAllValues`; values OR, and NOR under a negated operator. Numbers
  compare as decimals, dates as instants from ISO 8601, a day or Unix seconds, addresses by
  CIDR with the whole address when no prefix is given. The requester's keys, `aws:PrincipalArn`
  and the rest, come from `principal_keys` as IAM gives them for a root and for an anonymous
  caller.
- **Variables**, under Version `2012-10-17` only, stand for a key's single value or a default,
  and match themselves: a value holding `*` is not a wildcard. A resource naming a variable
  the request has no value for matches nothing.
- **Public.** A policy is public as S3 judges it: an allowing statement to everyone, unless one
  of its conditions holds requests to fixed values of a key that confines their source or
  principal, or to address ranges no broader than `/8` for IPv4 and `/32` for IPv6. A condition
  a request without the key satisfies, `...IfExists` or `ForAllValues`, confines nothing.
  GetBucketPolicyStatus writes `IsPublic` in lowercase, which botocore reads, where AWS's sample
  writes `TRUE`, which it takes for false; without a policy it answers `NoSuchBucketPolicy`, as
  S3 did, where s3-tests expects `false`.
- **Block Public Access** starts with all four settings on for a new bucket, as S3's have been
  since April 2023. BlockPublicPolicy refuses a public policy, `403 AccessDenied`;
  RestrictPublicBuckets keeps what a public policy grants within the owner's account, anonymous
  requests and grants to named accounts included, as S3 documents. The two settings for ACLs
  change nothing on buckets without ACLs, and are kept so they read back as set. s3-tests sets
  public policies on new buckets without turning these off, and those tests fail against S3 as
  against mantle.

## 11. Object Lock

**Decision: Object Lock's documents and headers are checked as S3 was recorded checking them,
and the locks are kept and enforced on the versions they protect, in the metadata layer's Name
ranges** (`crates/s3/src/lock.rs`; 18). A retention or legal hold protects one version, so the
check that refuses to delete it must read the version in the same step that would remove it.

- **Documents.** A configuration is `ObjectLockEnabled` `Enabled`, with or without a default
  retention of a mode and one period; every other shape is `MalformedXML`, as S3 answered each
  LocalStack sent. A period of 0 or less is `InvalidArgument`, "Default retention period must be
  a positive integer value.", as S3 answers, where s3-tests expects `InvalidRetentionPeriod`, a
  code S3's error table does not have. A period past 36,500 days or 100 years is "too large", at
  the longest retention S3 documents. A retention is a mode and a date or, empty, a request to
  remove one; a legal hold is `ON` or `OFF`.
- **Headers.** An object write's mode and date come together or not at all; the date must be
  ISO 8601 and ahead; the legal hold `ON` or `OFF`; the mode one S3 defines, each with S3's
  message, `ArgumentName` and `ArgumentValue`, in the order the recordings show S3 checking them.
- **Writes with locks** carry `Content-MD5` or a checksum, a trailer counting and a SigV4
  payload hash not, and a signature, as S3 requires; a bucket's default retention makes every
  write to it one with locks. UploadPart of such an upload carries the same.
- **Bypass.** `x-amz-bypass-governance-retention` on a bucket without Object Lock is refused,
  `true` or `false`, as S3 refuses it. s3-tests' teardown sends it to every bucket, as it does
  to S3; running s3-tests against mantle takes its fix, PR #714, as against S3.
- **Dates** are kept to the millisecond and written as S3 writes a time,
  `2030-01-01T00:00:00.000Z`; a default retention runs from the version's creation, a year
  counted as 365 days, as S3 counts one for a retention duration.
- **Event holds**, added to S3 in September 2026 and documented with no recorded answers, are
  `501 NotImplemented` until there is behaviour to match.
- **Enforcement**, the metadata layer's (`crates/meta/src/name.rs`, `bucket.rs`; metadata.md
  §2): a version keeps its retention and legal hold; deleting it by ID while either holds is
  `403 AccessDenied`, "Access Denied because object protected by object lock.", and in
  DeleteObjects an error for that key; a retention holds while its date is ahead, as a date
  placed must be; it may be extended by anyone who may set one, shortened, removed or moved from
  GOVERNANCE to COMPLIANCE only under bypass, as s3-tests expects of the move with no S3
  recording (18 §6 item 6), and in COMPLIANCE never shortened or changed; versioning cannot be
  suspended on a bucket with Object Lock, nor Object Lock configured on one whose versioning is
  not enabled, `409 InvalidBucketState`; lifecycle leaves locked versions be.

## 12. Browser uploads

**Decision: a browser's upload is decoded as it streams and checked as S3 was recorded
checking one: the fields before the file are read whole, within S3's bound; the policy, its
signature and its conditions are checked before any of the file is kept; the file streams
through; and whatever follows it is ignored** (`crates/s3/src/form.rs`,
`crates/s3/src/post.rs`; 19). POST Object is the one upload whose authority travels in its
body, so nothing of the object may be kept until the fields that authorize it are read.

- **The body** is read as RFC 7578 and RFC 2046 define it: a boundary of RFC 2046's grammar,
  CRLF line ends, and on every part a `Content-Disposition` of `form-data` with a name; the
  preamble, transport padding and epilogue are ignored, and so is every other header field of
  a part, as RFC 7578 §4.8 requires. A part's own `Content-Type` is one of them: an object's
  type is its `Content-Type` field's, which the policy covers, and never a header no condition
  can reach. A request that is not `multipart/form-data` is `412 PreconditionFailed` with S3's
  `Condition` (19 §8.1).
- **The fields before the file** are bounded at 20,480 bytes, the figure S3's own answer to a
  form over it states: `MaxPostPreDataLengthExceeded`, with `MaxPostPreDataLengthBytes` (19
  §13.2). The fields with `${filename}` expanded are held to the same bound, since S3 checks
  the policy against the expanded form (19 §4.4) and a short form could otherwise expand to
  megabytes.
- **The file** is the `form-data` part named `file`, in any case, never a part merely
  carrying a `filename`: Python's `requests`, which s3-tests posts with, names one on every
  part (19 §5.5). A part of another disposition is a field, as S3 read one, and a form with no
  file is `InvalidArgument` naming `file`, as S3 answers, never the table's
  `IncorrectNumberOfFilesInPostRequest` (19 §13.2).
  It ends at the delimiter, whose line may hold only padding, then a CRLF or `--`. Any other
  byte means the boundary appeared inside the file, which RFC 2046 forbids a sender; taking it
  for the end would store a file cut short, and "Amazon S3 never stores partial objects"
  (19 §2.1), so the body is refused. memchr's SIMD search over the Two-Way algorithm finds the
  delimiter in time linear in the file, and the decoder holds at most a delimiter's length of
  it between pieces: 29–36 GB/s on one core, over thirty times the MD5 every upload pays
  (docs/measurements/2026-09-29-form-decoding.md).
- **Quoted strings.** A backslash quotes the `"` or `\` after it, the only characters RFC 9110
  §5.6.4 has a sender quote; before any other it stands for itself. A Windows path in a
  `filename` keeps its separators, as S3 documents reading
  `C:\Program Files\directory1\file.txt` as `file.txt` (19 §2.3).
- **`${filename}`** becomes the file's name after its last `/` or `\` in every field, and
  nothing for `filename=""`, a browser's empty file input. A file part with no `filename`
  leaves it as written, as S3 was recorded doing in 2026, where AWS's text says it becomes
  empty (19 §10 item 4). A field sent twice holds its values joined by commas, as AWS's text
  says to write their condition (19 §2.4).
- **Signed or anonymous.** A form with none of `x-amz-algorithm`, `x-amz-credential`,
  `x-amz-date` and `x-amz-signature` is anonymous, with or without a policy, as S3 treated one
  (19 §8.1), and uploads where a bucket policy lets anyone `s3:PutObject`. Signature Version
  2's fields are refused, as its header is. A signed form holds all four and `policy`; the
  first missing is named in S3's spelling and message, `X-Amz-Algorithm` before
  `X-Amz-Credential`, and `key` before any. The credential's scope must be this endpoint's
  region and `s3`, each fault `InvalidArgument` with S3's recorded message, a wrong region's
  with its `Region` (19 §13.3). AWS's text asks the credential's day to be the `x-amz-date`'s;
  S3's enforcement is unrecorded, and the signature under the day's key and the policy's
  `x-amz-date` condition already bind both, so the difference is not refused.
- **The signature** is the policy's as sent, under the day's signing key (19 §3.2), checked
  before the policy is decoded: a policy changed after signing is `SignatureDoesNotMatch` with
  the policy as `StringToSign`, as S3 answered one (19 §8.1). AWS's example and botocore's
  presigned POST verify byte for byte. `x-amz-date`'s age is not checked: a form is signed
  ahead of its use, and the policy's expiration is the bound AWS documents.
- **The policy** is read by the strict JSON reader (§9): `expiration` and `conditions`, named
  exactly so, as s3-tests expects, and any other member refused "Unexpected", as S3 refused
  one; the expiration an ISO 8601 time in UTC, as S3 refused an offset, after which the
  policy is "not valid" (19 §13.1). JSON's `null`, and a value of another type where a string
  or a whole size belongs, are "Invalid JSON.", as S3 answered a `null` and a bound of
  `512.0`. Conditions are simple (one member, a string), `eq`, `starts-with`
  and `content-length-range`; operators and field names in any case, values compared in
  theirs, and `starts-with` on `Content-Type` item by item of a comma list (19 §4.2). A
  `bucket` condition holds the bucket the URL names. A condition on a field the form lacks
  fails, even `starts-with ""`, as on S3 (19 §8.2), where RGW passes it. A range's bounds are
  inclusive; a number must be whole and not negative; a string holding digits is read as the
  number, and any other fails the condition, as S3 answered `"5"` and `"test"` (19 §8.1).
  Every field but `x-amz-signature`, `file`, `policy` and `x-ignore-*` must be named by a
  condition; the first that is not is named as sent, as S3 named `StorageClass` (19 §13.2).
- **S3's answers win over its error table** where they differ (19 §10): a failed condition is
  `403 AccessDenied`, "Invalid according to Policy: Policy Condition failed: [...]", the
  condition written as S3 writes it; a missing field is `400 InvalidArgument`; an expired
  policy `403 AccessDenied`; a malformed one `400 InvalidPolicyDocument`, "Invalid Policy:
  ...". A file outside the range is `EntityTooLarge` or `EntityTooSmall` with its proposed
  size and the bound. Where no answer of S3's is recorded, an unknown algorithm, a
  credential's other faults, and the policy's other shapes, the code is its recorded
  neighbour's and the message versitygw's wording of S3's (19 §13).
- **The upload's settings are its fields**: `acl` for `x-amz-acl`, the content fields, and every
  `x-amz-` field but those that sign the form, each as PutObject's header of the same name, and
  `tagging` as a `Tagging` document. The request's own headers set nothing, so the policy stays
  the upload's one authority; an `x-amz-` header that disagrees with the field of the same
  meaning is `400 InvalidRequest`, "Conflicting values provided in HTTP headers and POST form
  fields." (19 §2.6). The upload is judged as `s3:PutObject` on the key its form names.
- **The answer.** A `success_action_redirect`, or the deprecated `redirect`, that is an
  absolute `http` or `https` URL a `Location` header can carry is `303` to it with `bucket`,
  `key` and the quoted `etag` appended, form-encoded after any query it has, as S3 appends them;
  S3 ignored a relative one (19 §8.1, §13.4). Otherwise `success_action_status` `201` is a
  `PostResponse` in no namespace, its `Location` holding the key percent-encoded with its `/`,
  as S3's captured bodies do, `200` an empty 200, and anything else an empty 204; each carries
  the object's `ETag` and `Location` headers, as S3's 204 does (19 §2.2, §8.2, §13.4).
- **s3-tests** signs every authenticated POST with Signature Version 2, which mantle refuses,
  and its anonymous ones need public-read-write ACLs, which mantle's buckets do not have; the
  unit tests take the same policies signed with Version 4 (19 §5).

## 13. Server-side encryption

**Decision: every object is sealed at rest; S3's encryption headers choose whose key wraps its
data key, and are checked as S3 was recorded checking them** (`crates/s3/src/sse.rs`,
`crates/s3/src/seal.rs`; docs/design/encryption.md; 20).

- **SSE-S3 is every object's state.** A write without encryption headers, or with `AES256`, is
  SSE-S3, as S3's writes are since January 2023 (20 §1.3), and its responses say so on the
  operations S3 lists (20 §1.2). A KMS key named without `aws:kms` is S3's `InvalidArgument`;
  any other value of `x-amz-server-side-encryption`, the empty one included, is
  `InvalidArgument`, "The encryption method specified is not supported", the answer S3 gave the
  empty value (20 §1.4).
- **SSE-KMS and DSSE-KMS** are `501 NotImplemented`: they name keys in a key management service
  mantle does not have. So is UpdateObjectEncryption, which moves an object to SSE-KMS alone (20
  §1.6).
- **SSE-C** is checked in the order S3 was recorded refusing it (20 §3.5): with
  `x-amz-server-side-encryption`, incompatible; an algorithm other than `AES256`,
  `InvalidEncryptionAlgorithmError`; a key or MD5 without an algorithm, or an algorithm without a
  key; the key's MD5, before the key's length, as S3 answered a 24-byte key; a key not 256 bits.
  A request that leaves the MD5 out is taken and the echo computed, since whether S3 needs it is
  unrecorded. The key is wiped when the request drops it, and SSE-C needs TLS (20 §3.3). A read
  of an SSE-C object without a key, a key for an object that is not SSE-C, a wrong key (`403`,
  found by the key wrap's integrity check), and a part whose key its upload did not name, each
  take S3's answer.
- **The bucket's configuration** is S3's `ServerSideEncryptionConfiguration`: exactly one rule,
  S3 answering none or two `MalformedXML`; `AES256` the only default; a KMS key only with KMS;
  and `BlockedEncryptionTypes`, `SSE-C` or `NONE`. A new bucket blocks SSE-C, as S3's have since
  April 2026, answering a write `403 AccessDenied` with S3's message naming the requester, the
  action and the object (20 §3.6). A rule without `BlockedEncryptionTypes` leaves the block as it
  was, as S3 was recorded doing, and DeleteBucketEncryption resets the default and keeps it.
  GetBucketEncryption writes the blocked types on every bucket, as S3 does since April 2026.
