# 13 — XML bodies: ground truth for reading S3's request documents and writing its responses

Research note for mantle's S3 protocol layer (`crates/s3/src/xml.rs`, `body.rs`,
`tagging.rs`, `acl.rs`). It covers:

- what XML 1.0 and Namespaces in XML require of a reader and a writer;
- the security record of XML's entity mechanism;
- the documents S3's requests carry, their limits, and how S3 writes keys XML cannot carry;
- tags, ACLs and Object Ownership, in documents and in headers;
- the Rust parser evaluated for the job;
- the documents S3's responses carry, as the API reference and its samples show them.

Compiled 2026-09-28 from the W3C recommendations, RFC 7303, the AWS S3 API reference and user
guide as served that day, and the roxmltree 0.21.1 source. §6.7, §6.8 and §9.6 were compiled
2026-09-29 from the API reference, user guide and other AWS pages as served that day, with
observed S3 behaviour from public issue trackers, each labelled as such. This is research
input; the decision records are docs/design/s3-protocol.md §2, §4 and §5.

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
- [s3-tests] https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py and `test_headers.py`
- [aws-cli-2841] https://github.com/aws/aws-cli/issues/2841
- [tf-19895] https://github.com/hashicorp/terraform-provider-aws/issues/19895
- [tf-41747] https://github.com/hashicorp/terraform-provider-aws/issues/41747
- [tf-7323] https://github.com/hashicorp/terraform-provider-aws/issues/7323
- [ack-1021] https://github.com/aws-controllers-k8s/community/issues/1021
- [floci-3608] https://github.com/floci-io/floci/issues/3608
