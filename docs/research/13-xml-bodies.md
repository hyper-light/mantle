# 13 — XML bodies: ground truth for reading S3's request documents and writing its responses

Research note for mantle's S3 protocol layer (`crates/s3/src/xml.rs`, `body.rs`). It covers:

- what XML 1.0 and Namespaces in XML require of a reader and a writer;
- the security record of XML's entity mechanism;
- the documents S3's requests carry, their limits, and how S3 writes keys XML cannot carry;
- the Rust parser evaluated for the job.

Compiled 2026-09-28 from the W3C recommendations, RFC 7303, the AWS S3 API reference and user
guide as served that day, and the roxmltree 0.21.1 source. This is research input; the
decision record is docs/design/s3-protocol.md §2.

---

## 1. XML 1.0 (Fifth Edition), W3C Recommendation 26 November 2008

Source: [XML10] https://www.w3.org/TR/xml/.

**Characters.** "[2] `Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] |
[#x10000-#x10FFFF]`" and "Legal characters are tab, carriage return, line feed, and the legal
characters of Unicode and ISO/IEC 10646." WFC Legal Character: "Characters referred to using
character references MUST match the production for Char."

**Escaping in content (§2.4).** "The ampersand character (&) and the left angle bracket (<)
MUST NOT appear in their literal form, except when used as markup delimiters, or within a
comment, a processing instruction, or a CDATA section. If they are needed elsewhere, they MUST
be escaped using either numeric character references or the strings `&amp;` and `&lt;`
respectively." "The right angle bracket (>) may be represented using the string `&gt;`, and
MUST ... be escaped using either `&gt;` or a character reference when it appears in the string
`]]>` in content."

**Line ends (§2.11).** "the XML processor MUST behave as if it normalized all line breaks in
external parsed entities (including the document entity) on input, before parsing, by
translating both the two-character sequence #xD #xA and any #xD that is not followed by #xA to
a single #xA character." A carriage return survives only as a character reference, which is
why S3 tells clients to send one as `&#13;` (§6.5).

**Attribute values (§3.3.3).** Line breaks are normalized first; then "For a character
reference, append the referenced character to the normalized value", "For a white space
character (#x20, #xD, #xA, #x9), append a space character (#x20) to the normalized value", and
any other character is appended as it is.

**Entities.** WFC Entity Declared: in a document without a DTD, an entity reference's name
"MUST match that in an entity declaration ... except that well-formed documents need not
declare any of the following entities: `amp`, `lt`, `gt`, `apos`, `quot`." Without a DTD,
those five are the only entities.

**Other well-formedness constraints.** Unique Att Spec: "An attribute name MUST NOT appear more
than once in the same start-tag or empty-element tag." Element Type Match: "The Name in an
element's end-tag MUST match the element type in the start-tag."

**Productions used** (verbatim):

```
[3]  S             ::= (#x20 | #x9 | #xD | #xA)+
[4]  NameStartChar ::= ":" | [A-Z] | "_" | [a-z] | [#xC0-#xD6] | [#xD8-#xF6] | [#xF8-#x2FF]
                       | [#x370-#x37D] | [#x37F-#x1FFF] | [#x200C-#x200D] | [#x2070-#x218F]
                       | [#x2C00-#x2FEF] | [#x3001-#xD7FF] | [#xF900-#xFDCF] | [#xFDF0-#xFFFD]
                       | [#x10000-#xEFFFF]
[4a] NameChar      ::= NameStartChar | "-" | "." | [0-9] | #xB7 | [#x0300-#x036F] | [#x203F-#x2040]
[5]  Name          ::= NameStartChar (NameChar)*
[10] AttValue      ::= '"' ([^<&"] | Reference)* '"' | "'" ([^<&'] | Reference)* "'"
[14] CharData      ::= [^<&]* - ([^<&]* ']]>' [^<&]*)
[15] Comment       ::= '<!--' ((Char - '-') | ('-' (Char - '-')))* '-->'
[17] PITarget      ::= Name - (('X' | 'x') ('M' | 'm') ('L' | 'l'))
[19] CDStart       ::= '<![CDATA['
[21] CDEnd         ::= ']]>'
[22] prolog        ::= XMLDecl? Misc* (doctypedecl Misc*)?
[23] XMLDecl       ::= '<?xml' VersionInfo EncodingDecl? SDDecl? S? '?>'
[25] Eq            ::= S? '=' S?
[26] VersionNum    ::= '1.' [0-9]+
[27] Misc          ::= Comment | PI | S
[32] SDDecl        ::= S 'standalone' Eq (("'" ('yes' | 'no') "'") | ('"' ('yes' | 'no') '"'))
[40] STag          ::= '<' Name (S Attribute)* S? '>'
[42] ETag          ::= '</' Name S? '>'
[44] EmptyElemTag  ::= '<' Name (S Attribute)* S? '/>'
[66] CharRef       ::= '&#' [0-9]+ ';' | '&#x' [0-9a-fA-F]+ ';'
[68] EntityRef     ::= '&' Name ';'
[80] EncodingDecl  ::= S 'encoding' Eq ('"' EncName '"' | "'" EncName "'")
```

**Encodings (§4.3.3).** "All XML processors MUST accept the UTF-8 and UTF-16 encodings of
Unicode." "XML processors SHOULD match character encoding names in a case-insensitive way."

## 2. XML's entity mechanism as an attack surface: RFC 7303 §10 (XML Media Types, July 2014)

Source: [RFC7303] https://www.rfc-editor.org/rfc/rfc7303.html §10.

- "it is also possible to construct XML documents that make use of what XML terms
  "[XML-]entity references" to construct repeated expansions of text."
- "Recursive expansions are prohibited by [XML] and XML processors are required to detect
  them. However, even non-recursive expansions may cause problems with the finite computing
  resources of computers, if they are performed many times. For example, consider the case
  where XML-entity A consists of 100 copies of XML-entity B, which in turn consists of 100
  copies of XML-entity C, and so on."
- "any information stored outside of the direct control of the user -- including CSS style
  sheets, XSL transformations, XML-entity declarations, and DTDs -- can be a source of
  insecurity, by either obvious or subtle means."

A reader that refuses the document type declaration has no entity beyond the five predefined
ones (§1), so neither expansion nor external entities exist.

## 3. Namespaces in XML 1.0 (Third Edition), W3C Recommendation 8 December 2009

Source: [NS10] https://www.w3.org/TR/xml-names/.

- Productions: `QName ::= PrefixedName | UnprefixedName`, `PrefixedName ::= Prefix ':'
  LocalPart`, `Prefix ::= NCName`, `LocalPart ::= NCName`, `NCName ::= Name - (Char* ':'
  Char*)`, `DefaultAttName ::= 'xmlns'`, `PrefixedAttName ::= 'xmlns:' NCName`.
- Reserved Prefixes and Namespace Names: "The prefix xml is by definition bound to the
  namespace name http://www.w3.org/XML/1998/namespace." "The prefix xmlns is used only to
  declare namespace bindings and is by definition bound to the namespace name
  http://www.w3.org/2000/xmlns/." (The constraint continues: `xml` may be declared only to
  its own name, `xmlns` never, no other prefix may be bound to either name, and neither may be
  the default namespace.)
- Prefix Declared: "The namespace prefix, unless it is xml or xmlns, MUST have been declared in
  a namespace declaration attribute."
- No Prefix Undeclaring: "In a namespace declaration for a prefix, the attribute value MUST NOT
  be empty."
- "The attribute value in a default namespace declaration MAY be empty. This has the same
  effect, within the scope of the declaration, of there being no default namespace."
- §7: "No entity names, processing instruction targets, or notation names contain any colons."

## 4. XML 1.1 (Second Edition), W3C Recommendation 16 August 2006

Source: [XML11] https://www.w3.org/TR/xml11/.

- `[2] Char ::= [#x1-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]`
- `[2a] RestrictedChar ::= [#x1-#x8] | [#xB-#xC] | [#xE-#x1F] | [#x7F-#x84] | [#x86-#x9F]`
- The restricted characters "still cannot be used directly in documents" and appear "only as
  character references"; "#x0 is still forbidden both directly and as a character reference."

## 5. XML Schema Part 2: Datatypes (Second Edition), W3C Recommendation 28 October 2004

Source: [XSD2] https://www.w3.org/TR/xmlschema-2/.

- `boolean` (§3.2.2): "can have the following legal literals {true, false, 1, 0}."
- `integer` (§3.3.13), and `int` (§3.3.17) by restriction: "a finite-length sequence of
  decimal digits (#x30-#x39) with an optional leading sign"; leading zeros are allowed; `int`
  runs from -2147483648 to 2147483647.
- whiteSpace (§4.3.6): "For all atomic datatypes other than string (and types derived by
  restriction from it) the value of whiteSpace is collapse", which removes leading and
  trailing white space. An enumeration restricting `string` keeps its white space.

## 6. S3's request documents

Sources: the AWS S3 API reference and user guide, as served 2026-09-28.

### 6.1 Namespace and form

- Request syntax declares `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` on the root, but
  AWS's own sample requests omit it: CompleteMultipartUpload's sample begins
  `<CompleteMultipartUpload>` and PutObjectTagging's `<Tagging>`
  ([API_CompleteMultipartUpload], [API_PutObjectTagging]). A reader accepts both.
- The samples are indented irregularly, and CreateBucket's ends `</CreateBucketConfiguration >`
  ([API_CreateBucket]), which [42] allows.
- `MalformedXML`, 400: "The XML that you provided was not well formed or did not validate
  against our published schema" (note 05 §11.2).

### 6.2 CompleteMultipartUpload ([API_CompleteMultipartUpload], [API_CompletedPart])

- Root `CompleteMultipartUpload`; repeated `Part`, each with `PartNumber`, `ETag`, and ten
  checksum elements: `ChecksumCRC32`, `ChecksumCRC32C`, `ChecksumCRC64NVME`, `ChecksumMD5`,
  `ChecksumSHA1`, `ChecksumSHA256`, `ChecksumSHA512`, `ChecksumXXHASH128`, `ChecksumXXHASH3`,
  `ChecksumXXHASH64`, each "The Base64 encoded" value of the part.
- PartNumber: "a positive integer between 1 and 10,000." "For each part in the list, you must
  provide the `PartNumber` value and the `ETag` value".
- "If you do not supply a valid `Part` with your request, the service sends back an HTTP 400
  response."
- `InvalidPartOrder`, 400: "The list of parts was not in ascending order. The parts list must
  be specified in order by part number." With additional checksums on general purpose
  buckets, "the `PartNumber` must start at 1 and the part numbers must be consecutive.
  Otherwise, Amazon S3 generates an HTTP `400 Bad Request` status code and an
  `InvalidPartOrder` error code."
- Response root `CompleteMultipartUploadResult`, with the namespace in the sample response.
  The error sample `<Error>` carries no namespace.

### 6.3 DeleteObjects ([API_Delete], [API_ObjectIdentifier])

- `Delete`: `Objects` (the repeated `Object`) "Required: Yes"; `Quiet`, Boolean, "When you
  add this element, you must set its value to `true`." Up to 1,000 keys (note 05 §8.1).
- `ObjectIdentifier`: `Key`, "Length Constraints: Minimum length of 1", "Required: Yes", with
  "Replacement must be made for object keys containing special characters (such as carriage
  returns) when using XML requests"; `VersionId`; `ETag`; `LastModifiedTime` and `Size`,
  each "only supported for directory buckets".

### 6.4 Version IDs

"Version IDs are Unicode, UTF-8 encoded, URL-ready, opaque strings that are no more than 1,024
bytes long." ([versioning-workflows], "Version IDs").

### 6.5 CreateBucket ([API_CreateBucket])

`CreateBucketConfiguration` holds `LocationConstraint`, `Location` (`Name`, `Type`) and
`Bucket` (`DataRedundancy`, `Type`), the last two "only supported by directory buckets", and
`Tags` (`Tag`: `Key`, `Value`).

### 6.6 PutBucketVersioning ([API_PutBucketVersioning])

`VersioningConfiguration` holds `Status` (`Enabled | Suspended`) and an MFA delete setting
(`Enabled | Disabled`). The request syntax and every sample name that element `MfaDelete`;
the member list calls it `MFADelete`. The element on the wire is `MfaDelete`.

### 6.7 Tags ([object-tagging], [API_Tag], [API_PutObjectTagging])

"You can associate up to 10 tags with an object." "A tag key can be up to 128 Unicode
characters in length, and tag values can be up to 256 Unicode characters in length. Amazon S3
object tags are internally represented in UTF-16." `Tag.Key` has "Minimum length of 1".

### 6.8 Attributes

The only attributes in AWS's request samples besides the root's `xmlns` are on an ACL grantee:
`<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">`
([API_PutObjectAcl], sample request), two on one tag.

## 7. How S3 writes a key that XML 1.0 cannot carry

- S3 writes such characters as character references. A ListObjectsV2 response quoted in
  aws-sdk-go issue 1914 held `<Key>|&#x4;ٽ5&#xe;չѕ</Key>`, which the SDK's XML parser refused:
  "XML syntax error on line 2: illegal character code U+0004"
  (https://github.com/aws/aws-sdk-go/issues/1914).
- AWS's remedy is `encoding-type=url` on listings (note 05 §6.2; ListObjectsV2 docs).
- Clients echo listed keys back in DeleteObjects, so a key S3 lists as `&#x4;` is one a client
  may send as `&#x4;`.

## 8. roxmltree 0.21.1

Source: the crate as published (MIT OR Apache-2.0; one dependency, memchr).

- Strict XML 1.0 with namespaces; its README says it "tries to mimic the behavior of Python's
  lxml".
- `ParsingOptions { allow_dtd: false, .. }` is the default, and a document type declaration
  is then `Error::DtdDetected`. `nodes_limit` bounds nodes; nothing bounds attributes per tag.
- Duplicate attributes are found by scanning the tag's earlier attributes for each new one
  (`src/parse.rs`: `ctx.doc.attributes[start_idx..].iter().any(..)`): quadratic in a tag's
  attribute count.
- It builds the whole tree before a caller sees any of it.
- Positions are stored as `u32` (`ShortRange::from` casts with only a `debug_assert!`), and
  the source holds panicking operations (`unwrap`, indexing) guarded by internal invariants.

## Sources

- [XML10] Extensible Markup Language (XML) 1.0 (Fifth Edition), W3C Recommendation, 26 November 2008.
- [XML11] Extensible Markup Language (XML) 1.1 (Second Edition), W3C Recommendation, 16 August 2006.
- [NS10] Namespaces in XML 1.0 (Third Edition), W3C Recommendation, 8 December 2009.
- [XSD2] XML Schema Part 2: Datatypes Second Edition, W3C Recommendation, 28 October 2004.
- [RFC7303] H. Thompson, C. Lilley, "XML Media Types", RFC 7303, July 2014.
- [API_CompleteMultipartUpload] https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
- [API_CompletedPart] https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompletedPart.html
- [API_Delete] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Delete.html
- [API_ObjectIdentifier] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectIdentifier.html
- [API_CreateBucket] https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
- [API_PutBucketVersioning] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
- [API_PutObjectTagging] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
- [API_Tag] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Tag.html
- [API_PutObjectAcl] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
- [object-tagging] https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
- [versioning-workflows] https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html
