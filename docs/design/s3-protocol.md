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
| `xml`, `body` | reading request documents, writing response documents | 13; roxmltree as an oracle |

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
reference, as S3 does (13 §7). The error document has no namespace; results carry S3's
(13 §6.2). A property test reads back exactly whatever text the writer was given.

## 3. Listing

A page is computed over an ordered index that answers one question: the first key at or after
a byte string (`crates/s3/src/list.rs`).

- **One seek per common prefix.** Keys that roll up into a common prefix "count as a single
  return" (05 §6.2), so the page passes the rest of a common prefix with one seek to the
  first key beyond it. A page costs at most `max-keys + 1` seeks however many keys a common
  prefix holds.
- **Resuming inside a common prefix.** A common prefix "is filtered out from results if it is
  not lexicographically greater than the `StartAfter` value" (05 §6.2). The common prefix a
  `start-after` value itself rolls up into is a prefix of it, so never greater, and every key
  after it under that prefix rolls up into it: the page starts beyond that common prefix.
  Every other common prefix the page meets is greater than `start-after`.
- **Continuation tokens** are the base64url of the last item returned. S3 calls them
  "obfuscated and ... not a real key" (05 §6.2); a token a client alters only moves where
  the next page starts, as `start-after` could.

Open, for the metadata service: the index leaves out keys whose current version is a delete
marker (05 §6.4), so one seek can pass many of them. How far one seek may scan before a page
ends early belongs with the index; the page may end short, since "The response might contain
fewer keys" (05 §6.2).
