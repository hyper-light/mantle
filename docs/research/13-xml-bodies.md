# 13 — XML bodies: ground truth for reading S3's request documents and writing its responses

Research note for mantle's S3 protocol layer (`crates/s3/src/xml.rs`, `body.rs`,
`tagging.rs`, `acl.rs`, `lifecycle.rs`). It covers:

- what XML 1.0 and Namespaces in XML require of a reader and a writer;
- the security record of XML's entity mechanism;
- the documents S3's requests carry, their limits, and how S3 writes keys XML cannot carry;
- tags, ACLs and Object Ownership, in documents and in headers;
- bucket lifecycle configuration: its document, when its actions fall due, and its headers;
- the Rust parser evaluated for the job;
- the documents S3's responses carry, as the API reference and its samples show them.

Compiled 2026-09-28 from the W3C recommendations, RFC 7303, the AWS S3 API reference and user
guide as served that day, and the roxmltree 0.21.1 source. §6.7, §6.8 and §9.6 were compiled
2026-09-29 from the API reference, user guide and other AWS pages as served that day, with
observed S3 behaviour from public issue trackers, each labelled as such; §6.9 the same day,
with botocore's serializer run offline. This is research input; the decision records are
docs/design/s3-protocol.md §2, §4, §5 and §7.

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

### 6.7 Tags

**The operations.** PutObjectTagging, GetObjectTagging and DeleteObjectTagging act on the
object's `?tagging` subresource, and on a version with `versionId`. PutBucketTagging,
GetBucketTagging and DeleteBucketTagging act on the bucket's ([API_PutObjectTagging],
[API_GetObjectTagging], [API_DeleteObjectTagging], [API_PutBucketTagging],
[API_GetBucketTagging], [API_DeleteBucketTagging]).

- PutObjectTagging "Sets the supplied tag-set to an object that already exists in a bucket",
  and "Note that Amazon S3 limits the maximum number of tags to 10 tags per object." It
  answers 200, with `x-amz-version-id`.
- PutBucketTagging: "When this operation sets the tags for a bucket, it will overwrite any
  current tags the bucket already has." Its Response Syntax is `HTTP/1.1 200` "with an empty
  HTTP body"; its sample shows `204 No Content`. botocore's model gives no response code for
  it, so 200 ([botocore]).
- DeleteObjectTagging "Removes the entire tag set from the specified object" and answers 204.
  DeleteBucketTagging answers 204.
- GetBucketTagging's special error: "`NoSuchTagSet`", "There is no tag set associated with
  the bucket", 404 in the error table ([ErrorResponses]).
- Both Put operations name `InvalidTag` ("The tag provided was not a valid tag. This error can
  occur if the tag did not pass input validation") and `MalformedXML` ("The XML provided does
  not match the schema"). The error table's `InvalidTag`, 400: "Your request contains tag
  input that is not valid. For example, your request might contain duplicate keys, keys or
  values that are too long, or system tags."
- botocore marks PutObjectTagging, PutBucketTagging, PutObjectAcl and PutBucketAcl
  `requestChecksumRequired` ([botocore]).

**The document.** The request and response are one document:
`<Tagging><TagSet><Tag><Key>string</Key><Value>string</Value></Tag></TagSet></Tagging>`.
`Tagging` and `TagSet` are "Required: Yes"; `Tag` holds `Key`, "Minimum length of 1",
"Required: Yes", and `Value`, "Required: Yes" ([API_Tag], [API_Tagging]). The request samples
of both Put operations carry no namespace; GetObjectTagging's sample response declares S3's,
GetBucketTagging's does not. "If you send this request with an empty tag set, Amazon S3
deletes the existing tag set on the object" ([object-tagging]). CreateBucket's configuration
carries the same `Tag` elements in `Tags`: "An array of tags that you can apply to the bucket
that you're creating", for general purpose and directory buckets alike ([API_CreateBucket]).

**The header.** PutObject, CreateMultipartUpload and CopyObject take `x-amz-tagging`: "The
tag-set for the object. The tag-set must be encoded as URL Query parameters. (For example,
"Key1=Value1")", with the sample `x-amz-tagging: tag1=value1&tag2=value2` ([API_PutObject]).
CopyObject pairs it with `x-amz-tagging-directive`, `COPY | REPLACE`, "The default value is
`COPY`" ([API_CopyObject]). GetObject and HeadObject answer `x-amz-tagging-count`, "The
number of tags, if any, on the object" ([API_GetObject]); the user guide's `x-amz-tag-count`
is the one page that names another header.

**The limits** ([object-tagging]; [CostAllocTagging]; [ug-tagging]):

- "You can associate up to 10 tags with an object. Tags that are associated with an object
  must have unique tag keys."
- "A tag key can be up to 128 Unicode characters in length, and tag values can be up to 256
  Unicode characters in length. Amazon S3 object tags are internally represented in UTF-16.
  Note that in UTF-16, characters consume either 1 or 2 character positions." The last
  sentence is the only statement of how S3 counts; mantle reads it as counting UTF-16 code
  units.
- For buckets: "A tag set can contain as many as 50 tags, or it can be empty. Keys must be
  unique within a tag set"; the key "can contain 1 to 128 Unicode characters", the value
  "from 0 to 256".
- "Keys can only contain Unicode letters or numbers, white space, and the following
  symbols: `_ . : / = + @ -`", and values the same ([ug-tagging]). AWS's API references write
  that set as a pattern: IAM's `Tag` gives its key "Pattern: `[\p{L}\p{Z}\p{N}_.:/=+\-@]+`",
  128 at most, and its value the same with `*`, 0 to 256 ([iam-tag]). S3's error table
  prints it for Storage Lens groups with `[\p{L}\p{Z}\p{N}` lost, `^(_.:/=+\-@]*)$`
  ([ErrorResponses]), and the EC2 guide gives the set in words ([ec2-tags]).
- "The reserved prefix is `aws:`", and "AWS-generated tag names and values are automatically
  assigned the `aws:` prefix, which you can't assign" ([billing-custom-tags]).

**Observed behaviour** (public issue trackers; secondary):

- S3 answered PutObject whose `x-amz-tagging` was `TagSet%3D%5B%7BKey%3Dstring%2CValue%3Dstring%7D%5D`
  with "`InvalidTag`: The TagKey you have provided is invalid", and one whose header was
  `TagSet=[{Key=string,Value=string}]` with "`InvalidArgument`: The header 'x-amz-tagging'
  shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name
  duplicates" ([aws-cli-2841]). The first decodes to one key holding `[`, `{`, `,`, `}` and
  `]`, none in the set above, so S3 checks characters on an object's tags; the second is one
  pair holding three `=`.
- More than 10 tags: "`BadRequest`: Object tags cannot be greater than 10", status 400, for
  PutObject's header ([tf-19895], 2021) and for PutObjectTagging ([tf-41747]).
- `aws:` keys: PutBucketTagging that left out a bucket's `aws:cloudformation` tags failed
  with "System Tags cannot be removed by requester" ([tf-7323]).
- A presigned PutObject carries `x-amz-tagging` in its query string, as SDK presigners hoist
  `x-amz-*` headers there, and S3 applies it: `x-amz-tagging=a%3Db%26c%3Dd` set the tags
  `a=b` and `c=d` ([floci-3608], checked against S3 2026-08-26).

**ceph s3-tests** at 5522d1c ([s3-tests]; secondary):

- `test_put_obj_with_tags`: `x-amz-tagging: foo=bar&bar` reads back as `bar` with the empty
  value, then `foo=bar`: a key without `=` holds the empty value, and the set comes back in
  key order, not header order. The test is not marked `fails_on_aws`.
- `test_set_bucket_tagging`: GetBucketTagging of an untagged bucket is 404 `NoSuchTagSet`, and
  so again after DeleteBucketTagging's 204. `test_put_delete_tags`: GetObjectTagging of an
  untagged object is an empty `TagSet`.
- `test_put_excess_tags`, `test_put_excess_key_tags`, `test_put_excess_val_tags`: 11 tags, a
  129-character key or a 257-character value is 400 `InvalidTag`; 10 tags of 128 and 256
  characters are accepted (`test_put_max_kvsize_tags`). The count disagrees with what S3
  answered above.
- `test_get_obj_head_tagging`: HEAD answers `x-amz-tagging-count`.

**Form encoding.** A URL query written by an HTML form is
`application/x-www-form-urlencoded`, whose parser replaces "any 0x2B (+) in name and value
with 0x20 (SP)" before percent-decoding ([WHATWG-URL] §5.1). Python's
`urllib.parse.urlencode` writes a space as `+` by default (`quote_via=quote_plus`)
([python-urlencode]).

### 6.8 ACLs and Object Ownership

**Attributes.** The only attributes in AWS's request samples besides the root's `xmlns` are
on an ACL grantee: `<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:type="CanonicalUser">` ([API_PutObjectAcl], sample request), two on one tag. botocore's
model makes `Type` the attribute `xsi:type` of `Grantee` (`"xmlAttribute":true`), and
requires it ([botocore]). The Response Syntax on the ACL pages shows `<xsi:type>` as an
element, and GetObjectAcl's samples a `<Type>` child; both are artefacts, since every request
sample, GetBucketAcl's sample and the model use the attribute. The user guide's default-ACL
sample types a grantee `"Canonical User"`, with a space ([acl-overview]).

**The document.** PutBucketAcl and PutObjectAcl take `<AccessControlPolicy>` holding
`AccessControlList`, a list of `Grant`, and `Owner` (`ID`, `DisplayName`), both "Required:
No". A `Grant` holds a `Grantee` and a `Permission`, "Valid Values: `FULL_CONTROL | WRITE |
WRITE_ACP | READ | READ_ACP`" ([API_Grant]). A `Grantee`'s type is "`CanonicalUser |
AmazonCustomerByEmail | Group`", and it holds `DisplayName`, `EmailAddress`, `ID` ("The
canonical user ID of the grantee") and `URI` ("URI of the grantee group") ([API_Grantee]).
The pages name each kind by one element: `ID` for a canonical user, `URI` for a group,
`EmailAddress` for an account by email; "DisplayName is optional and ignored in the request",
and an email grantee "is resolved to the CanonicalUser and, in a response to a GET Object acl
request, appears as the CanonicalUser" ([API_PutObjectAcl], [API_PutBucketAcl]).

- PutBucketAcl's sample declares S3's namespace on the root and undeclares it on the leaves:
  `<URI xmlns="">`, `<EmailAddress xmlns="">`, `<Permission xmlns="">`. PutObjectAcl's sample
  root carries none.
- "An ACL can have up to 100 grants" ([acl-overview]).
- A canonical user ID is "An alpha-numeric identifier, such as
  `79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be`" ([acct-identifiers]);
  every ID in the S3 samples is 64 hexadecimal digits. PutObjectAcl's samples give display
  names that are email addresses (`mtd@amazon.com`, `CustomersName@amazon.com`).
- `MalformedACLError`, 400: "The ACL that you provided was not well formed or did not validate
  against our published schema" ([ErrorResponses]).

**The headers.** `x-amz-acl` names a canned ACL. PutObjectAcl, PutObject, CopyObject and
CreateMultipartUpload list "`private | public-read | public-read-write | authenticated-read |
aws-exec-read | bucket-owner-read | bucket-owner-full-control`"; PutBucketAcl and CreateBucket
list the first four. The user guide's canned ACL table has eight rows ([acl-overview]):

| Canned ACL | Applies to | Grants |
|---|---|---|
| `private` | bucket and object | "Owner gets `FULL_CONTROL`. No one else has access rights (default)." |
| `public-read` | bucket and object | owner `FULL_CONTROL`; `AllUsers` `READ` |
| `public-read-write` | bucket and object | owner `FULL_CONTROL`; `AllUsers` `READ` and `WRITE` |
| `aws-exec-read` | bucket and object | owner `FULL_CONTROL`; "Amazon EC2 gets `READ` access to `GET` an Amazon Machine Image (AMI) bundle" |
| `authenticated-read` | bucket and object | owner `FULL_CONTROL`; `AuthenticatedUsers` `READ` |
| `bucket-owner-read` | object | object owner `FULL_CONTROL`, bucket owner `READ`; "If you specify this canned ACL when creating a bucket, Amazon S3 ignores it." |
| `bucket-owner-full-control` | object | "Both the object owner and the bucket owner get `FULL_CONTROL`"; ignored on a new bucket likewise |
| `log-delivery-write` | bucket | "The `LogDelivery` group gets `WRITE` and `READ_ACP` permissions" |

`x-amz-grant-read`, `-write`, `-read-acp`, `-write-acp` and `-full-control` each list
grantees: "You specify each grantee as a type=value pair, where the type is one of the
following: `id` ... `uri` ... `emailAddress`", as in `x-amz-grant-write:
uri="http://acs.amazonaws.com/groups/s3/LogDelivery", id="111122223333", id="555566667777"`
and `x-amz-grant-read: emailAddress="xyz@amazon.com", emailAddress="abc@amazon.com"`
([API_PutBucketAcl], [API_PutObjectAcl]). PutObject, CopyObject and CreateMultipartUpload
take all but `x-amz-grant-write`. s3-tests sends the values bare, `id=<uid>`
(`_get_acl_header`). The groups are `http://acs.amazonaws.com/groups/global/AllUsers`,
`.../global/AuthenticatedUsers` and `http://acs.amazonaws.com/groups/s3/LogDelivery`
([acl-overview]).

- "You can use either a canned ACL or specify access permissions explicitly. You cannot do
  both." S3 answered both with "`InvalidRequest`: Specifying both Canned ACLs and Header
  Grants is not allowed", 400 ([ack-1021], observed).
- PutBucketAcl: "You cannot specify access permission using both the body and the request
  headers." No page names the error.
- "End of support notice: As of October 1, 2025, Amazon S3 has discontinued support for Email
  Grantee Access Control Lists (ACLs). If you attempt to use an Email Grantee ACL in a request
  after October 1, 2025, the request will receive an `HTTP 405` (Method Not Allowed) error"
  (PutObjectAcl, PutBucketAcl, PutObject, CopyObject, CreateMultipartUpload). The notice
  names eight Regions, and no error code.
- s3-tests: a grant to a canonical ID that does not exist is 400 `InvalidArgument`
  (`test_bucket_acl_grant_nonexist_user`); `x-amz-acl: public-ready` is 400
  (`test_bucket_put_bad_canned_acl`).

**Object Ownership.**

- "By default, Object Ownership is set to the Bucket owner enforced setting and all ACLs are
  disabled." "Bucket owner enforced is the default setting for all newly created buckets"
  ([about-object-ownership]). The change "began deploying on April 5, 2023, and is now
  applied to all AWS Regions" ([whatsnew-2023-04-28]); "There is no change for existing
  buckets" ([whatsnew-2022-12-13]).
- `BucketOwnerEnforced`: "Access control lists (ACLs) are disabled and no longer affect
  permissions. The bucket owner automatically owns and has full control over every object in
  the bucket. The bucket only accepts PUT requests that don't specify an ACL or specify bucket
  owner full control ACLs (such as the predefined `bucket-owner-full-control` canned ACL or a
  custom ACL in XML format that grants the same permissions)" ([API_OwnershipControlsRule]).
- "Requests to set ACLs or update ACLs fail with a `400` error and return the
  `AccessControlListNotSupported` error code. Requests to read ACLs are still supported.
  Requests to read ACLs always return a response that shows full control for the bucket
  owner" ([object-ownership-error-responses]). `AccessControlListNotSupported`: "The bucket
  does not allow ACLs" ([ErrorResponses]).
- "If you have `PutBucketAcl` or `PutObjectAcl` requests with headers that grant ACL-based
  permissions, with the exception of the `bucket-owner-full-control` canned ACL, you must
  remove those headers before you can disable ACLs"
  ([object-ownership-migrating-acls-prerequisites]).
- "If your `CreateBucket` request sets Bucket owner enforced and specifies a bucket ACL that
  provides access to an external AWS account, your request fails with a `400` error and
  returns the `InvalidBucketAclWithObjectOwnership` error code"
  ([object-ownership-error-responses]); the code's text is "Bucket cannot have ACLs set with
  ObjectOwnership's BucketOwnerEnforced setting". CreateBucket: "To set an ACL on a bucket as
  part of a `CreateBucket` request, you must explicitly set S3 Object Ownership for the bucket
  to a different value than the default" ([API_CreateBucket]).
- s3-tests' `_test_object_ownership_bucket_owner_enforced`: PutObject, CreateMultipartUpload
  and CopyObject go ahead with no ACL or `bucket-owner-full-control`, and are 400
  `AccessControlListNotSupported` with `private`; so are PutBucketAcl and PutObjectAcl with
  `private`.

**The ownership controls operations** ([API_PutBucketOwnershipControls],
[API_GetBucketOwnershipControls], [API_DeleteBucketOwnershipControls], [API_OwnershipControlsRule]):

- PutBucketOwnershipControls, `PUT /?ownershipControls`, takes
  `<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>`,
  `Rule` "Type: Array of OwnershipControlsRule data types", and answers 200 with an empty body.
  `ObjectOwnership`: "Valid Values: `BucketOwnerPreferred | ObjectWriter | BucketOwnerEnforced`".
  botocore flattens the list under the name `Rule` ([botocore]); s3-tests reads one rule back
  (`get_bucket_ownership` asserts `1 == len(Rules)`).
- GetBucketOwnershipControls answers the same document; its sample is namespaced. "A bucket
  doesn't have `OwnershipControls` settings" when it predates bucket owner enforced and never
  had it applied, or after DeleteBucketOwnershipControls; "By default, Amazon S3 sets
  `OwnershipControls` for all newly created buckets." Without them it is 404
  `OwnershipControlsNotFoundError`, "The bucket ownership controls were not found"
  ([ErrorResponses]).
- DeleteBucketOwnershipControls "Removes `OwnershipControls`" and answers 204.
- CreateBucket takes the setting as `x-amz-object-ownership`, with the same three values
  ([API_CreateBucket]).

### 6.9 Lifecycle configuration

Sources: the API reference and user guide pages below, as served 2026-09-29; botocore's
model and serializer at 358f8ee (release 1.43.104), run offline; ceph s3-tests at 5522d1c;
and S3's answers as others recorded them, under "Observed answers".

**The operations** ([API_PutBucketLifecycleConfiguration],
[API_GetBucketLifecycleConfiguration], [API_DeleteBucketLifecycle]).

- PutBucketLifecycleConfiguration, `PUT /?lifecycle`: "Creates a new lifecycle configuration
  for the bucket or replaces an existing lifecycle configuration." It answers 200 with an
  empty body.
- GetBucketLifecycleConfiguration, `GET /?lifecycle`, answers the same document. Its special
  error is `NoSuchLifecycleConfiguration`, "The lifecycle configuration does not exist", 404.
- DeleteBucketLifecycle, `DELETE /?lifecycle`: "Amazon S3 removes all the lifecycle
  configuration rules in the lifecycle subresource associated with the bucket." It answers
  204. The page does not say what happens when there is no configuration.
- The deprecated PutBucketLifecycle and GetBucketLifecycle use the same method and URI
  ([API_PutBucketLifecycle], [API_GetBucketLifecycle]). Their `Rule` type requires a
  rule-level `Prefix` and has no `Filter`, so a server can tell them apart only by the body.
  "Previous configurations where a prefix is defined will continue to operate as before"
  ([API_LifecycleRule]).
- botocore marks both PUTs `requestChecksumRequired`. It sends
  `x-amz-sdk-checksum-algorithm: CRC32` and `x-amz-checksum-crc32` by default, never
  Content-MD5. It writes no XML declaration, and the root
  `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`. A rule's
  children follow the caller's dict order, not the model's. `Filter: {}` becomes `<Filter />`.
  Its client-side validation checks `required` and `min`, but not enumerations or maxima.

**The document** ([API_LifecycleRule], [API_LifecycleRuleFilter],
[API_LifecycleRuleAndOperator], [API_LifecycleExpiration],
[API_NoncurrentVersionExpiration], [API_AbortIncompleteMultipartUpload], [API_Transition],
[API_NoncurrentVersionTransition]; botocore's shapes).

- `LifecycleConfiguration` holds `Rule` elements, flattened. A rule holds:
  - `ID`: "The value cannot be longer than 255 characters."
  - `Status`, required: `Enabled | Disabled`.
  - `Filter`, or the deprecated rule-level `Prefix`: "`Filter` is required if the
    `LifecycleRule` does not contain a `Prefix` element."
  - the actions: `Expiration`, `Transition` (repeated), `NoncurrentVersionTransition`
    (repeated), `NoncurrentVersionExpiration`, `AbortIncompleteMultipartUpload`.
- `Filter`: "A `Filter` must have exactly one of `Prefix`, `Tag`, `ObjectSizeGreaterThan`,
  `ObjectSizeLessThan`, or `And` specified" (LifecycleRule). "If the `Filter` element is left
  empty, the Lifecycle Rule applies to all objects in the bucket" (LifecycleRuleFilter).
- `And`: "used in a Lifecycle Rule Filter to apply a logical AND to two or more predicates.
  The Lifecycle Rule will apply to any object matching all of the predicates configured
  inside the And operator." It holds `Prefix`, `ObjectSizeGreaterThan`, `ObjectSizeLessThan`
  and `Tag` elements, flattened.
- Types: `Days`, `NoncurrentDays`, `DaysAfterInitiation` and `NewerNoncurrentVersions` are
  integers; the sizes are longs; `Date` is an ISO 8601 timestamp;
  `ExpiredObjectDeleteMarker` is a boolean. botocore writes a date as `%Y-%m-%dT%H:%M:%SZ`,
  so `'2017-09-27'` goes out as `2017-09-27T00:00:00Z`, and `'20200101'`, read as epoch
  seconds, as `1970-08-22T19:08:21Z`.
- `Expiration`:
  - `Date`: "The date value must conform to the ISO 8601 format. The time is always midnight
    UTC."
  - `Days`: "The value must be a non-zero positive integer."
  - `ExpiredObjectDeleteMarker`: "If set to true, the delete marker will be expired; if set
    to false the policy takes no action. This cannot be specified with Days or Date in a
    Lifecycle Expiration Policy."
- `NoncurrentVersionExpiration`: `NoncurrentDays`, "a non-zero positive integer", and
  `NewerNoncurrentVersions`: "You can specify up to 100 noncurrent versions to retain."
- `Transition`'s `Days` "can be `0` or any positive integer". `NoncurrentVersionTransition`'s
  `NoncurrentDays` has no non-zero clause. `StorageClass` is one of `GLACIER`,
  `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `DEEP_ARCHIVE` or `GLACIER_IR`.
- `Tag`: `Key`, "Minimum length of 1", and `Value`, both "Required: Yes" ([API_Tag]).
- `x-amz-transition-default-minimum-object-size`, a request header of the PUT and a response
  header of the PUT and GET, is `varies_by_storage_class | all_storage_classes_128K`.
  "Configurations created before September 2024 retain the previous transition behavior
  unless you modify them" ([lifecycle-transition-general-considerations]). Under the newer
  default, "objects smaller than 128 KB will not be transitioned to any storage class"
  ([intro-lifecycle-filters]).

**Limits** ([intro-lifecycle-rules]; [intro-lifecycle-filters]).

- "An S3 Lifecycle configuration can have up to 1,000 rules per bucket. This limit is not
  adjustable. ... ID length is limited to 255 characters."
- `NewerNoncurrentVersions`: "(between 1 and 100)".
- Sizes: "Maximum filter size is 50 TB." The console requires "larger than 0 bytes and up to
  50 TB".
- No page gives a longest prefix, a most tags in a filter, or a largest document.

**Filters** ([intro-lifecycle-rules]; [intro-lifecycle-filters]).

- "Each tag must match _both_ the key and value exactly. If you specify only a `<Key>`
  element and no `<Value>` element, the rule will apply only to objects that match the tag
  key and that do _not_ have a value specified."
- "The rule applies to a subset of objects that has all the tags specified in the rule. If
  an object has additional tags specified, the rule will still apply."
- "When you specify multiple tags in a filter, each tag key must be unique."
- "A filter can have only one prefix, and zero or more tags." "You can specify an **empty
  filter**, in which case the rule applies to all objects in the bucket."
- "The `ObjectSizeGreaterThan` and `ObjectSizeLessThan` filters exclude the specified
  values. For example, if you set objects sized 128 KB to 1024 KB ... objects that are
  exactly 1024 KB and 128 KB won't transition". "If you're specifying an object size range,
  the `ObjectSizeGreaterThan` integer must be less than the `ObjectSizeLessThan` value."
- "S3 Lifecycle doesn't support excluding prefixes in your rules", nor "including multiple
  prefixes".

**When an action falls due** ([intro-lifecycle-rules]; [troubleshoot-lifecycle]).

- By age: "Amazon S3 calculates the time by adding the number of days specified in the rule
  to the object creation time and rounding up the resulting time to the next day at midnight
  UTC. For example, if an object was created on 1/15/2014 at 10:30 AM UTC and you specify 3
  days in a transition rule, then the transition date of the object would be calculated as
  1/19/2014 00:00 UTC." The creation date "is synonymous with the **Last modified** date".
- Noncurrent versions: the days count from "the time when the new successor version of the
  object is created", with the same rounding. The worked example deletes `photo.gif`,
  deleted on 1/2/2014 at 11:30 AM UTC, "On 1/8/2014 at 00:00 UTC ... five days after it
  became a noncurrent version."
- The troubleshooting page: an object created at 00:05 UTC on January 2 "becomes one day old
  at 00:05 UTC on January 3, which makes it eligible for expiration when S3 Lifecycle
  evaluates objects at 00:00 UTC on January 4."
- By date: "If you specify an S3 Lifecycle action with a date that is in the past, all
  qualified objects become immediately eligible for that lifecycle action. ... The date-based
  action is not a one-time action. Amazon S3 continues to apply the date-based action even
  after the date has passed, as long as the rule status is `Enabled`."
- No page says how a time already at midnight rounds.

**What each action does** ([lifecycle-expire-general-considerations];
[intro-lifecycle-rules]; [lifecycle-configuration-examples];
[mpu-abort-incomplete-mpu-lifecycle-config]).

- Expiration: "Object expiration applies only to an object's current version". In a bucket
  never versioned it "permanently remov[es] the object"; with versioning enabled, "If the
  current object version is not a delete marker, Amazon S3 adds a delete marker"; with
  versioning suspended it "creates a delete marker with null as the version ID ... If the
  version ID of the current version of the object is `null`, the `Expiration` action
  permanently deletes this version." "Amazon S3 doesn't take any action if there are one or
  more object versions and the delete marker is the current version."
- NoncurrentVersionExpiration: "`NewerNoncurrentVersions` ... specifies how many newer
  noncurrent versions must exist before Amazon S3 can expire a given version. Amazon S3 will
  permanently delete any additional noncurrent versions beyond the specified number to
  retain. For the deletion to occur, both the `<NoncurrentDays>` **and** the
  `<NewerNoncurrentVersions>` values must be exceeded. ... If you don't specify a `<Filter>`
  element, Amazon S3 generates an `InvalidRequest` error when you specify the number of
  noncurrent versions to retain." It has no effect in a bucket never versioned.
- ExpiredObjectDeleteMarker removes "a delete marker with zero noncurrent versions". "You
  can't specify both a `Days` and an `ExpiredObjectDeleteMarker` tag on the same rule. When
  you specify the `Days` tag, Amazon S3 automatically performs `ExpiredObjectDeleteMarker`
  cleanup when the delete markers are old enough to satisfy the age criteria. To clean up
  delete markers as soon as they become the only version, create a separate rule with only
  the `ExpiredObjectDeleteMarker` tag." "You can't specify this lifecycle action in a rule
  that has a filter that uses object tags."
- AbortIncompleteMultipartUpload applies to uploads "determined by the key name `prefix`
  specified in the Lifecycle rule", "to both existing multipart uploads and those that you
  create later", and "doesn't apply to objects". "You can't specify this lifecycle action in
  a rule that has a filter that uses object tags."
- "An object is eligible for only one S3 Lifecycle action per day." "If two expiration
  policies overlap, the shorter expiration policy is honored so that data is not stored for
  longer than expected"; "Permanent deletion takes precedence over transition. Transition
  takes precedence over creation of delete markers" ([lifecycle-conflicts]).
- "When you add an S3 Lifecycle configuration to a bucket, Amazon S3 replaces the bucket's
  current Lifecycle configuration", and "the configuration rules apply to both existing
  objects and objects that you add later" ([how-to-set-lifecycle-configuration-intro]).

**Headers** ([API_GetObject], [API_HeadObject], [API_PutObject], [API_CopyObject],
[API_CompleteMultipartUpload], [API_CreateMultipartUpload], [API_ListParts]).

- `x-amz-expiration` on GetObject, HeadObject, PutObject, CopyObject and
  CompleteMultipartUpload: "It includes the `expiry-date` and `rule-id` key-value pairs
  providing object expiration information. The value of the `rule-id` is URL-encoded."
  Samples: `x-amz-expiration: expiry-date="Fri, 23 Dec 2012 00:00:00 GMT",
  rule-id="picture-deletion-rule"` (GetObject, PutObject) and `expiry-date="Fri, 21 Dec 2012
  00:00:00 GMT", rule-id="Rule for testfile.txt"` (HeadObject). 23 December 2012 was a
  Sunday; 21 December 2012 was a Friday.
- `x-amz-abort-date` and `x-amz-abort-rule-id` on CreateMultipartUpload and ListParts: "If
  the bucket has a lifecycle rule configured with an action to abort incomplete multipart
  uploads and the prefix in the lifecycle rule matches the object name in the request, the
  response includes this header." botocore models the date as a header timestamp, whose
  default form is an HTTP-date.
- "To find when the current version of an object is scheduled to expire, use the HeadObject
  or GetObject API operation" ([lifecycle-expire-general-considerations]).

**Errors** ([ErrorResponses]). `InvalidRequest`, 400, lists "At least one action must be
specified in a lifecycle rule.", "At least one lifecycle rule must be specified." and "The
number of lifecycle rules must not exceed the allowed limit of 1000 rules."
`NoSuchLifecycleConfiguration` is 404. `MalformedXML` is "not well formed or did not validate
against our published schema". No other lifecycle fault has a code on any page.

**Samples** (quoted in the tests of `crates/s3/src/body.rs` and `response.rs`).

- PutBucketLifecycleConfiguration's Example 1 holds `<Filter><Prefix>documents/</Prefix>
  </Filter>` with a `Transition` to `GLACIER` at 30 days, and `logs/` expiring at 365 days.
- GetBucketLifecycleConfiguration's sample gives back a rule-level `<Prefix>projectdocs/
  </Prefix>` as sent, with two `Transition`s and an `Expiration` of 3650 days, in S3's
  namespace. Its Response Syntax root has no namespace.
- Examples 3, 4 and 5 write the root `<LifeCycleConfiguration>`. Example 5 closes `<And>`,
  `<Filter>` and `<Expiration>` with start tags, so it is not well-formed XML.

**ceph s3-tests at 5522d1c** ([s3-tests]; secondary). 48 tests are marked `lifecycle`, and
25 of them `fails_on_aws`: most wait on RGW's `rgw_lc_debug_interval`, which makes a "day"
last seconds. The ones that set configurations and check responses:

- `test_lifecycle_set`, `_get`, `_set_date`, `_set_noncurrent`, `_set_deletemarker`,
  `_set_filter`, `_set_empty_filter` and `_set_multipart` expect 200. `_get` expects the
  rules back exactly as sent: a rule-level `Prefix` stays one, and no `Filter` is added.
- `test_lifecycle_get_no_id` expects every rule read back to hold an `ID`.
- `test_lifecycle_delete`: GET with no configuration is 404 `NoSuchLifecycleConfiguration`,
  and DELETE is 204 with or without one.
- `test_lifecycle_id_too_long` (256 characters) and `test_lifecycle_same_id` expect 400
  `InvalidArgument`. `test_lifecycle_invalid_status` expects 400 `MalformedXML` for `enabled`,
  `disabled` and `invalid`. `test_lifecycle_expiration_days0` expects `InvalidArgument`: "days:
  0 is legal in a transition rule, but not legal in an expiration rule".
  `test_lifecycle_set_invalid_date` and `test_lifecycle_transition_set_invalid_date`, dates at
  no midnight, expect status 400 with no code asserted.
- `test_lifecycle_expiration_header_put` and `_head` expect `x-amz-expiration` matching
  `expiry-date="(.+)", rule-id="(.+)"`, whose date is one whole day, by `timedelta.days`,
  after the time taken before the PUT, for a rule of 1 day. `_tags_head` expects it for a
  tag rule the object matches once tagged, and not after the rule is replaced by one it does
  not match; `_and_tags_head` expects none when one of an `And`'s two tags differs.
- `test_lifecycle_expiration_newer_noncurrent` (fails_on_aws, for its timing) expects 6 of 10
  versions left by `NewerNoncurrentVersions` 5: "1 current and (9 - 5) noncurrent".
- The `lifecycle_transition` tests need at least two storage classes configured, and are
  skipped otherwise.

**Observed answers** (secondary). The principal source is LocalStack's lifecycle tests at
`8b9a79f05846835cf4dff63ab7eefdde9df83783`, its final commit before the repository was
archived: `tests/aws/services/s3/test_s3.py`, class `TestS3BucketLifecycle` (L8971–L9683),
each test `@markers.aws.validated`, with snapshots recorded against S3 on 21 February 2026
(`test_s3.validation.json` L1646–L1753) ([localstack]). The rest are S3's answers quoted
in issues and recordings, each dated. Some were seen only through CloudFormation, which
passes on S3's message but not its code or status; these are marked (CFN).

- A `Date` not at midnight: 400 `InvalidArgument`, "'Date' must be at midnight GMT"
  (LocalStack; [aws-sdk-478], 2023; CFN, 2026).
- `ExpiredObjectDeleteMarker` with `Days`: 400 `MalformedXML` (LocalStack; [aws-cli-8239],
  2023; [aws-cdk-25824], 2023). `Date` with `Days`, or an empty `Expiration`: no record.
- `AbortIncompleteMultipartUpload` with a tag filter: 400 `InvalidRequest`,
  "AbortIncompleteMultipartUpload cannot be specified with Tags." ([tfm-s3-109], 2021; and
  2025, 2026). With a size filter: "AbortIncompleteMultipartUpload cannot be specified with
  Object Size." ([cfn-lint-3554], 2024; code not captured).
- `ExpiredObjectDeleteMarker` with a size filter: 400 `InvalidRequest`,
  "ExpiredObjectDeleteMarker cannot be specified with Object Size.", with the element set to
  `false` ([cloudposse-137], 2022; [tfm-s3-376], 2026). With a tag filter: no record.
- Two predicates directly in a `Filter`: 400 `MalformedXML` (LocalStack, Prefix with
  ObjectSizeGreaterThan, and And beside Prefix; [s3-tests-638], 2025, Prefix with Tag). An
  `And` holding one `Prefix`: 400 `MalformedXML`, from a wire capture; an `And` of an empty
  `Prefix` and one other predicate was accepted ([tf-23882], 2022). An `And` of two tags is
  accepted (LocalStack).
- Sizes: `ObjectSizeLessThan` 0 was 400 `InvalidRequest`, "'ObjectSizeLessThan' should be
  between 1 and 1099511627776000." ([tf-41521], 2025); `ObjectSizeGreaterThan` equal to
  `ObjectSizeLessThan` was "'ObjectSizeLessThan' has to be a value greater than
  'ObjectSizeGreaterThan'." (CFN, 2026). Negative sizes, and sizes past 50 TB: no record.
- `NewerNoncurrentVersions` 500 was accepted (CFN, 2026), past the documented 100. In a
  configuration of rule-level prefixes: 400 `InvalidRequest`, "NewerNoncurrentVersions element
  can only be used in Lifecycle V2." ([tf-23228], 2022). Without `NoncurrentDays`:
  `MalformedXML`, as a NooBaa maintainer found testing S3 ([noobaa-8861]). An empty
  `NoncurrentVersionExpiration`: 400 `MalformedXML` (LocalStack).
- A rule with neither `Filter` nor `Prefix`: 400 `MalformedXML` (LocalStack; wire captures in
  [aws-sdk-go-v2-2874], 2023). Both in one rule: no record. Rules of both forms in one
  configuration: 400 `InvalidRequest`, "Filter element can only be used in Lifecycle V2."
  after a rule-level prefix ([ansible-53751], 2019), and "Base level prefix cannot be used in
  Lifecycle V2, prefixes are only supported in the Filter." after a filter ([tf-23299],
  2022).
- Rule-level prefixes that overlap: 400 `InvalidRequest`, "Found overlapping prefixes '' and
  'a' for same action type 'Expiration'" ([noobaa-8341], 2024). The user guide's conflicts
  page gives two rules with overlapping filters and the same action as a configuration S3
  resolves ([lifecycle-conflicts], Example 3).
- Duplicate tag keys in an `And`: 400 `InvalidRequest`, "Duplicate Tag Keys are not allowed."
  (LocalStack; CFN, 2026).
- Day counts: `Days` 0 in an expiration, "'Days' for Expiration action must be a positive
  integer" (CFN, 2026); `NoncurrentDays` 0, 400 `InvalidArgument`, "'NoncurrentDays' for
  NoncurrentVersionExpiration action must be a positive integer" ([tf-35328], 2024).
  `DaysAfterInitiation` 0: no record; NooBaa copies S3's form, "'DaysAfterInitiation' for
  AbortIncompleteMultipartUpload action must be a positive integer", `InvalidArgument`
  ([noobaa-8970]).
- Transitions: 400 `InvalidArgument`, "'Days' in Transition action must be greater than or
  equal to 30 for storageClass 'ONEZONE_IA'", for 0 days as for 29 ([zenn-thaim], 2024);
  "'StorageClass' must be different for 'Transition' actions in same 'Rule' with filter
  '(prefix=)'" and "Found mixed 'Date' and 'Days' based Expiration and Transition actions in
  lifecycle rule for filter '(prefix=)'" (CFN, 2026). `Days` 0 to `GLACIER_IR` was stored
  ([aws-ps-367], 2024).
- An ID over 255 characters: `InvalidArgument`, "ID length should not exceed allowed limit of
  255" ([noobaa-8628], 2025). Two rules with one ID: `InvalidArgument`, "Rule ID must be
  unique. Found same ID for more than one rule" ([tiflash-9889], 2025). `Status` `enabled`:
  `MalformedXML` ([noobaa-8664], 2025).
- A rule with no action: 400 `InvalidRequest`, "At least one action needs to be specified in
  a rule" ([mcaf-s3-46], 2025).
- No rules: `MalformedXML` ([s3life-6], 2017). 1,127 rules: `MalformedXML` ([hub-529],
  2016). Both are older than the error table's `InvalidRequest` for these faults.
- IDs S3 makes for rules sent without one are 48 characters of base64 holding a lowercase,
  hyphenated version 4 UUID, such as `OWI4YzMxM2UtYTAyOS00MTRjLTllMDAtYWJjMTI1NWI3ODMx`, in
  CloudFormation reads and in `x-amz-abort-rule-id` and `x-amz-expiration` headers
  ([aws-sdk-go-v2-3165], 2025).
- `x-amz-expiration`: `expiry-date="<RFC 1123 date> GMT", rule-id="<id>"`, with the ID as it
  is: spaces arrive as spaces in every capture, from 2016 to 2025, among them `rule-id="Delete
  after 14 days"`. No capture holds a character a header could not carry. With several rules
  for one key, all of 7 days, S3 named the first listed, which also had the longest prefix
  (LocalStack). A rule of `ExpiredObjectDeleteMarker` alone gives no header, and HEAD with a
  `versionId`, even the current version's, gives none (LocalStack).
- `x-amz-abort-date` is an HTTP-date at 00:00:00 GMT, such as `Tue, 19 Aug 2025 00:00:00 GMT`,
  and `x-amz-abort-rule-id` the ID as it is ([aws-sdk-go-v2-3165]; [aws-sdk-js-v3-8199], an AWS
  maintainer against S3).
- `x-amz-transition-default-minimum-object-size`: without it, both the PUT and the GET answer
  `all_storage_classes_128K`; a value S3 does not define is 400 `InvalidRequest`, "Invalid
  TransitionDefaultMinimumObjectSize found: value" (LocalStack).
- GetBucketLifecycleConfiguration writes a rule's elements as `ID`, `Filter`, `Status`, then
  its actions, and an empty filter as `<Filter/>`: `<Rule><ID>testglacierrule</ID><Filter/>
  <Status>Enabled</Status><Transition><Days>0</Days><StorageClass>GLACIER_IR</StorageClass>
  </Transition></Rule>` ([aws-ps-367], 2024). The PUT accepts its elements in any order.
- A `Date` written `Fri, 01 Jan 2016 00:00:00 GMT` was 400 `MalformedXML` ([aws-sdk-js-2352],
  2018). No record shows a `Date` S3 wrote.
- No record gives a limit on a body's size, on tags in an `And`, or on a prefix's length. The
  user guide's example "to expire noncurrent objects that have no data, including noncurrent
  delete marker objects" filters by `ObjectSizeLessThan` 1 ([lifecycle-configuration-examples]).

**Discrepancies.**

- The Filter is "exactly one" predicate in LifecycleRule and may be empty in
  LifecycleRuleFilter and the user guide. RGW's tests put several predicates directly in a
  `Filter`; all of them are `fails_on_aws`, and S3 refuses such a filter as malformed.
- NewerNoncurrentVersions. The API says S3 "will retain" N and "permanently delete any
  additional noncurrent versions beyond the specified number to retain", so a version with N
  newer noncurrent versions goes. The elements page agrees: N "newer noncurrent versions must
  exist before Amazon S3 can expire a given version". The examples page says twice "more
  than 5 [10] newer noncurrent versions must exist", which would keep N+1. Both pages end
  "both the `NoncurrentDays` and the `NewerNoncurrentVersions` values must be exceeded".
- `x-amz-expiration`'s `rule-id` "is URL-encoded", but the HeadObject sample and every
  recorded answer show spaces as spaces.
- The error table's `InvalidRequest` for no rules and for more than 1,000, against
  `MalformedXML` recorded in 2016 and 2017. Its "At least one action must be specified in a
  lifecycle rule." against the recorded "At least one action needs to be specified in a rule",
  with the same code.
- `NewerNoncurrentVersions` is documented up to 100, and 500 was accepted.
- The user guide's conflicts Example 3 transitions to `STANDARD_IA` at 10 days, which S3
  refuses: "'Days' in Transition action must be greater than or equal to 30".
- The API reference makes the Tag's `Value` required, and every SDK sends it; the user guide
  lets a filter's tag leave it out.

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

## 9. S3's response documents

Sources: the API reference pages for each operation as served 2026-09-28; botocore's
`parsers.py` and `handlers.py` at commit 358f8ee; ceph s3-tests at commit 5522d1c.

### 9.1 Element order and namespace

- Each operation's Response Syntax lists the root's elements in one order. ListObjectsV2's
  `ListBucketResult` holds `IsTruncated`, `Contents`, `Name`, `Prefix`, `Delimiter`,
  `MaxKeys`, `CommonPrefixes`, `EncodingType`, `KeyCount`, `ContinuationToken`,
  `NextContinuationToken`, `StartAfter` ([API_ListObjectsV2]). Within a nested type the
  members are listed alphabetically: `Contents` holds `ChecksumAlgorithm`, `ChecksumType`,
  `ETag`, `Key`, `LastModified`, `Owner`, `RestoreStatus`, `Size`, `StorageClass`.
- The samples follow neither that order nor one another's. ListObjectsV2's samples begin
  with `Name` and `Prefix` and write `IsTruncated` after `MaxKeys`; one ListObjects sample
  puts `Owner` after `StorageClass` and another before it ([API_ListObjects]);
  ListObjectVersions' first sample puts `Owner` after `StorageClass` and the others before
  it ([API_ListObjectVersions]).
- botocore finds a response's elements by name, with the namespace removed: `_node_tag`
  returns `self._namespace_re.sub('', node.tag)` for the pattern `{.*}`. It takes the request
  ID and host ID from the `x-amz-request-id` and `x-amz-id-2` headers ([botocore]).
- The samples of ListObjects, ListObjectsV2, ListObjectVersions, CreateMultipartUpload,
  CompleteMultipartUpload, ListParts, ListMultipartUploads, DeleteObjects, GetBucketLocation
  and GetBucketVersioning declare `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"` on the
  root. ListBuckets', CopyObject's and UploadPartCopy's declare none. No error sample declares
  one ([API_CompleteMultipartUpload], [API_DeleteObjects]).

### 9.2 Times

- Every time in the samples of ListObjects, ListObjectsV2, ListObjectVersions, ListParts,
  ListMultipartUploads, CopyObject and UploadPartCopy is whole seconds with `.000`
  milliseconds, such as `2009-10-12T17:50:30.000Z`. ListBuckets' samples write
  `2019-12-11T23:32:47+00:00` ([API_ListBuckets]), and one CopyObject sample
  `2009-10-28T22:32:00` with no zone ([API_CopyObject]).
- HEAD's `Last-Modified` is an HTTP-date, which has one-second resolution (RFC 9110
  §5.6.7). s3-tests compares a listing's `LastModified` with HEAD's after zeroing the
  listing's sub-second part (`_compare_dates`).

### 9.3 Listings

Rules for which elements a listing writes are in note 05 §6 and §4.6–§4.7. The reference
pages add:

- ListObjectsV2's `EncodingType`: "If you specify the `encoding-type` request parameter,
  Amazon S3 includes this element in the response, and returns encoded key name values in
  the following response elements: `Delimiter, Prefix, Key,` and `StartAfter`."
  ListObjectVersions names `KeyMarker, NextKeyMarker, Prefix, Key`, and `Delimiter`;
  ListMultipartUploads names `Delimiter`, `KeyMarker`, `Prefix`, `NextKeyMarker`, `Key`.
- ListObjectVersions' samples write `<KeyMarker/>` and `<VersionIdMarker/>` when no marker
  was sent, and `NextKeyMarker` and `NextVersionIdMarker` only in the truncated sample.
- ListMultipartUploads' Response Syntax: `Bucket`, `KeyMarker`, `UploadIdMarker`,
  `NextKeyMarker`, `Prefix`, `Delimiter`, `NextUploadIdMarker`, `MaxUploads`, `IsTruncated`,
  `Upload`, `CommonPrefixes`, `EncodingType`; `Upload` holds `ChecksumAlgorithm`,
  `ChecksumType`, `Initiated`, `Initiator`, `Key`, `Owner`, `StorageClass`, `UploadId`.
  "Delimiter: ... If you don't specify a delimiter in your request, this element is absent
  from the response." All three samples write `NextKeyMarker` and `NextUploadIdMarker`,
  including the two that are not truncated: one names the last upload listed, the other,
  which lists only common prefixes, writes both empty. Of the two samples whose request sent
  no prefix, one writes an empty `Prefix` and the other none ([API_ListMultipartUploads]).
- ListParts' Response Syntax: `Bucket`, `Key`, `UploadId`, `PartNumberMarker`,
  `NextPartNumberMarker`, `MaxParts`, `IsTruncated`, `Part`, `Initiator`, `Owner`,
  `StorageClass`, `ChecksumAlgorithm`, `ChecksumType`; `Part` holds the checksum values,
  `ETag`, `LastModified`, `PartNumber`, `Size`. "NextPartNumberMarker: When a list is
  truncated, this element specifies the last part in the list". "Initiator: ... If the
  initiator is an AWS account, this element provides the same information as the `Owner`
  element. If the initiator is an IAM User, this element provides the user ARN."
  ([API_ListParts]).
- ListBuckets' Response Syntax: `Buckets` (each `Bucket`: `BucketArn`, `BucketRegion`,
  `CreationDate`, `Name`), `Owner`, `ContinuationToken`, `Prefix`. `BucketArn` "is only
  supported for S3 directory buckets." `BucketRegion`: "If the request contains at least one
  valid parameter, it is included in the response." `ContinuationToken` "is included in the
  response when there are more buckets that can be listed". `Prefix`: "If `Prefix` was sent
  with the request, it is included in the response." ([API_ListBuckets], [API_Bucket]).

### 9.4 Owner

- API_Owner, as quoted in 2025 in s3path issue 206: "Beginning November 21, 2025, Amazon S3
  will stop returning DisplayName." Between July 15 and November 21, 2025, responses were to
  lack it at an increasing rate ([s3path-206]). As served 2026-09-28, the Owner and Grantee
  pages give `DisplayName` no description ([API_Owner], [API_Grantee]).
- s3-tests at 5522d1c still reads `Owner.DisplayName` from listings and from uploads:
  `test_bucket_list_return_data`, `test_bucket_list_return_data_versioning` and
  `test_list_multipart_upload_owner`.

### 9.5 Results, errors and bucket settings

- `InitiateMultipartUploadResult`: `Bucket`, `Key`, `UploadId` ([API_CreateMultipartUpload]).
- `CompleteMultipartUploadResult`: `Location` ("The URI that identifies the newly created
  object"), `Bucket`, `Key`, `ETag`, the ten checksum elements, `ChecksumType`
  ([API_CompleteMultipartUpload]).
- `CopyObjectResult`: `ETag`, `LastModified`, `ChecksumType`, the checksum elements
  ([API_CopyObject]). `CopyPartResult`: `ETag`, `LastModified`, the checksum elements, each
  present "if the multipart upload request was created with the" algorithm
  ([API_UploadPartCopy]).
- `DeleteResult`: `Deleted` (`DeleteMarker`, `DeleteMarkerVersionId`, `Key`, `VersionId`)
  and `Error` (`Code`, `Key`, `Message`, `VersionId`). The samples write `DeleteMarker` only
  as `true`, beside `DeleteMarkerVersionId`: a simple delete in a versioned bucket gives
  `Key`, `DeleteMarker`, `DeleteMarkerVersionId`; deleting a version gives `Key` and
  `VersionId`; deleting a delete marker by its version gives `Key`, `VersionId`,
  `DeleteMarker` and `DeleteMarkerVersionId`, with one ID in both ([API_DeleteObjects]).
- The error samples of CompleteMultipartUpload and DeleteObjects hold `Code`, `Message`,
  `RequestId`, `HostId`; note 05 §11.1's holds `Code`, `Message`, `Resource`, `RequestId`.
- GetBucketLocation's Response Syntax shows a `LocationConstraint` inside the root
  `LocationConstraint`; its sample is one element holding the region,
  `<LocationConstraint xmlns="...">us-west-2</LocationConstraint>`. "Buckets in Region
  `us-east-1` have a LocationConstraint of `null`." ([API_GetBucketLocation]). botocore reads
  the root's text (`parse_get_bucket_location`: `region = root.text`), and s3-tests expects
  `None` for a bucket created with an empty constraint (`test_bucket_get_location`).
- GetBucketVersioning: `Status`, then `MfaDelete`, which "is only returned if the bucket has
  been configured with MFA delete." "If you never enabled (or suspended) versioning on a
  bucket, the response is: `<VersioningConfiguration xmlns="..."/>`"
  ([API_GetBucketVersioning]).

### 9.6 Tags and ACLs

- GetObjectTagging and GetBucketTagging answer the `Tagging` document of §6.7. GetObjectTagging's
  sample root declares S3's namespace, GetBucketTagging's does not, and neither Response Syntax
  does ([API_GetObjectTagging], [API_GetBucketTagging]).
- GetBucketAcl and GetObjectAcl answer `AccessControlPolicy` holding `Owner`, then
  `AccessControlList`. GetBucketAcl's sample, verbatim:

  ```
  <AccessControlPolicy>
    <Owner>
      <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
    </Owner>
    <AccessControlList>
      <Grant>
        <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  			xsi:type="CanonicalUser">
          <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
        </Grantee>
        <Permission>FULL_CONTROL</Permission>
      </Grant>
    </AccessControlList>
  </AccessControlPolicy>
  ```

  With ACLs disabled, reading "return[s] the `bucket-owner-full-control` ACL with the owner
  being the account that created the bucket" ([object-ownership-error-responses]).

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
- [API_ListObjectsV2] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
- [API_ListObjects] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
- [API_ListObjectVersions] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
- [API_ListBuckets] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
- [API_Bucket] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Bucket.html
- [API_ListParts] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
- [API_ListMultipartUploads] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
- [API_CreateMultipartUpload] https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
- [API_CopyObject] https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
- [API_UploadPartCopy] https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
- [API_DeleteObjects] https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
- [API_GetBucketLocation] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
- [API_GetBucketVersioning] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
- [API_Owner] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Owner.html
- [API_Grantee] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Grantee.html
- [s3path-206] https://github.com/liormizr/s3path/issues/206
- [botocore] https://github.com/boto/botocore, `botocore/parsers.py`, `botocore/handlers.py` and `botocore/data/s3/2006-03-01/service-2.json` at 358f8eec8c76201bb1a7a35644abcbc9036de7ed
- [API_GetObjectTagging] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
- [API_DeleteObjectTagging] https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
- [API_PutBucketTagging] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
- [API_GetBucketTagging] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
- [API_DeleteBucketTagging] https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
- [API_Tagging] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Tagging.html
- [API_PutObject] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
- [API_GetObject] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
- [API_PutBucketAcl] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
- [API_Grant] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Grant.html
- [API_OwnershipControlsRule] https://docs.aws.amazon.com/AmazonS3/latest/API/API_OwnershipControlsRule.html
- [API_PutBucketOwnershipControls] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketOwnershipControls.html
- [API_GetBucketOwnershipControls] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketOwnershipControls.html
- [API_DeleteBucketOwnershipControls] https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketOwnershipControls.html
- [ErrorResponses] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html
- [CostAllocTagging] https://docs.aws.amazon.com/AmazonS3/latest/userguide/CostAllocTagging.html
- [ug-tagging] https://docs.aws.amazon.com/AmazonS3/latest/userguide/tagging.html
- [acl-overview] https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html
- [about-object-ownership] https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html
- [object-ownership-error-responses] https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-ownership-error-responses.html
- [object-ownership-migrating-acls-prerequisites] https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-ownership-migrating-acls-prerequisites.html
- [acct-identifiers] https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-identifiers.html
- [billing-custom-tags] https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/custom-tags.html
- [ec2-tags] https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html
- [iam-tag] https://docs.aws.amazon.com/IAM/latest/APIReference/API_Tag.html
- [whatsnew-2022-12-13] https://aws.amazon.com/about-aws/whats-new/2022/12/amazon-s3-automatically-enable-block-public-access-disable-access-control-lists-buckets-april-2023/
- [whatsnew-2023-04-28] https://aws.amazon.com/about-aws/whats-new/2023/04/amazon-s3-security-best-practices-buckets-default/
- [WHATWG-URL] URL Living Standard, WHATWG, §5.1 "application/x-www-form-urlencoded parsing", https://url.spec.whatwg.org/#urlencoded-parsing
- [python-urlencode] Python 3 documentation, `urllib.parse.urlencode`, https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlencode
- [API_PutBucketLifecycleConfiguration] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
- [API_GetBucketLifecycleConfiguration] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
- [API_DeleteBucketLifecycle] https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
- [API_PutBucketLifecycle] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycle.html
- [API_GetBucketLifecycle] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycle.html
- [API_LifecycleRule] https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html
- [API_LifecycleRuleFilter] https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRuleFilter.html
- [API_LifecycleRuleAndOperator] https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRuleAndOperator.html
- [API_LifecycleExpiration] https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleExpiration.html
- [API_NoncurrentVersionExpiration] https://docs.aws.amazon.com/AmazonS3/latest/API/API_NoncurrentVersionExpiration.html
- [API_NoncurrentVersionTransition] https://docs.aws.amazon.com/AmazonS3/latest/API/API_NoncurrentVersionTransition.html
- [API_AbortIncompleteMultipartUpload] https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortIncompleteMultipartUpload.html
- [API_Transition] https://docs.aws.amazon.com/AmazonS3/latest/API/API_Transition.html
- [API_HeadObject] https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
- [intro-lifecycle-rules] https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
- [intro-lifecycle-filters] https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-filters.html
- [lifecycle-expire-general-considerations] https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html
- [lifecycle-transition-general-considerations] https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html
- [lifecycle-conflicts] https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-conflicts.html
- [lifecycle-configuration-examples] https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html
- [how-to-set-lifecycle-configuration-intro] https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-set-lifecycle-configuration-intro.html
- [troubleshoot-lifecycle] https://docs.aws.amazon.com/AmazonS3/latest/userguide/troubleshoot-lifecycle.html
- [mpu-abort-incomplete-mpu-lifecycle-config] https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html
- [s3-tests] https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py and `test_headers.py`
- [localstack] https://github.com/localstack/localstack/tree/8b9a79f05846835cf4dff63ab7eefdde9df83783/tests/aws/services/s3 (`test_s3.py`, `test_s3.snapshot.json`, `test_s3.validation.json`)
- [aws-sdk-478] https://github.com/aws/aws-sdk/issues/478
- [aws-cli-8239] https://github.com/aws/aws-cli/issues/8239
- [aws-cdk-25824] https://github.com/aws/aws-cdk/issues/25824
- [tfm-s3-109] https://github.com/terraform-aws-modules/terraform-aws-s3-bucket/issues/109
- [tfm-s3-376] https://github.com/terraform-aws-modules/terraform-aws-s3-bucket/issues/376
- [cloudposse-137] https://github.com/cloudposse/terraform-aws-s3-bucket/issues/137
- [cfn-lint-3554] https://github.com/aws-cloudformation/cfn-lint/issues/3554
- [s3-tests-638] https://github.com/ceph/s3-tests/issues/638
- [tf-23882] https://github.com/hashicorp/terraform-provider-aws/issues/23882
- [tf-41521] https://github.com/hashicorp/terraform-provider-aws/issues/41521
- [tf-23228] https://github.com/hashicorp/terraform-provider-aws/issues/23228
- [tf-23299] https://github.com/hashicorp/terraform-provider-aws/issues/23299
- [tf-35328] https://github.com/hashicorp/terraform-provider-aws/issues/35328
- [ansible-53751] https://github.com/ansible/ansible/issues/53751
- [noobaa-8341] https://github.com/noobaa/noobaa-core/issues/8341
- [noobaa-8628] https://github.com/noobaa/noobaa-core/pull/8628
- [noobaa-8664] https://github.com/noobaa/noobaa-core/pull/8664
- [noobaa-8861] https://github.com/noobaa/noobaa-core/issues/8861
- [noobaa-8970] https://github.com/noobaa/noobaa-core/pull/8970
- [zenn-thaim] https://zenn.dev/thaim/articles/2024-04-s3-lifecycle-configuration-min-duration
- [aws-ps-367] https://github.com/aws/aws-tools-for-powershell/issues/367
- [tiflash-9889] https://github.com/pingcap/tiflash/issues/9889
- [mcaf-s3-46] https://github.com/schubergphilis/terraform-aws-mcaf-s3/issues/46
- [s3life-6] https://github.com/mapbox/s3life/issues/6
- [hub-529] https://github.com/flightstats/hub/issues/529
- [aws-sdk-go-v2-2874] https://github.com/aws/aws-sdk-go-v2/issues/2874
- [aws-sdk-go-v2-3165] https://github.com/aws/aws-sdk-go-v2/issues/3165
- [aws-sdk-js-v3-8199] https://github.com/aws/aws-sdk-js-v3/issues/8199
- [aws-sdk-js-2352] https://github.com/aws/aws-sdk-js/issues/2352
- [aws-cli-2841] https://github.com/aws/aws-cli/issues/2841
- [tf-19895] https://github.com/hashicorp/terraform-provider-aws/issues/19895
- [tf-41747] https://github.com/hashicorp/terraform-provider-aws/issues/41747
- [tf-7323] https://github.com/hashicorp/terraform-provider-aws/issues/7323
- [ack-1021] https://github.com/aws-controllers-k8s/community/issues/1021
- [floci-3608] https://github.com/floci-io/floci/issues/3608
