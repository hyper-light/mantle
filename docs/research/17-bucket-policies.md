# 17 — Bucket policies: ground truth for granting access beyond a bucket's owner

Research note for mantle's bucket policies in the S3 protocol layer (`crates/s3`). mantle's
buckets have ACLs disabled (note 13 §6.8), so a bucket policy is how a bucket's owner grants
anyone else access. It covers:

- JSON as the policy document is written in, and the conformance suite a reader is tested
  against;
- S3's policy operations, their limits and errors;
- the IAM policy language: its grammar, elements, variables and condition operators;
- how AWS evaluates a policy, and how S3 authorizes a request, anonymous ones included;
- S3's actions, resources and condition keys;
- principals, and what makes a policy public;
- botocore's model and ceph s3-tests' tests;
- S3's observed answers, from recordings made against it.

Compiled 2026-09-29 with curl from public pages; the AWS pages are the `.md` renditions as
served that day. Labels:

- **Secondary** marks behaviour observed in S3 by others and recorded in their tests or
  issues.
- **Third-party** marks another implementation's claim about S3, recorded nowhere.
- **DERIVED** marks an inference of ours.
- **UNVERIFIED** marks what no fetched source states.

Pins: botocore develop at `358f8eec8c76201bb1a7a35644abcbc9036de7ed` (as note 13); ceph
s3-tests at `5522d1c351f75bc00ae0f64f742f3f095f5939d9` (as note 05); LocalStack main at
`8b9a79f05846835cf4dff63ab7eefdde9df83783`, its final state, whose policy snapshots were
recorded against S3 on 2026-02-21; versitygw main at `b91e178a12fae7a6acf31597a70aef8b8b3e72f4`;
the Service Authorization Reference's S3 JSON, `"Version": "v1.4"`; JSONTestSuite at
`1ef36fa01286573e846ac449e8683f8833c5b26a`.

---

## 1. JSON

### 1.1 RFC 8259 ([RFC8259])

- The text: `JSON-text = ws value ws`; `ws = *( %x20 / %x09 / %x0A / %x0D )`; "Insignificant
  whitespace is allowed before or after any of the six structural characters" (§2).
- Objects (§4): "The names within an object SHOULD be unique." "When the names within an
  object are not unique, the behavior of software that receives such an object is
  unpredictable. Many implementations report the last name/value pair only. Other
  implementations report an error or fail to parse the object".
- Numbers (§6): `number = [ minus ] int [ frac ] [ exp ]`, `int = zero / ( digit1-9 *DIGIT )`,
  `frac = decimal-point 1*DIGIT`, `exp = e [ minus / plus ] 1*DIGIT`. "Leading zeros are not
  allowed." "Numeric values that cannot be represented in the grammar below (such as Infinity
  and NaN) are not permitted." "This specification allows implementations to set limits on
  the range and precision of numbers accepted."
- Strings (§7): "All Unicode characters may be placed within the quotation marks, except for
  the characters that MUST be escaped: quotation mark, reverse solidus, and the control
  characters (U+0000 through U+001F)." Escapes are `\"`, `\\`, `\/`, `\b`, `\f`, `\n`, `\r`,
  `\t` and `\uXXXX`, the hexadecimal digits in either case; a character beyond the Basic
  Multilingual Plane is "represented as a 12-character sequence, encoding the UTF-16
  surrogate pair".
- Encoding (§8.1): "JSON text exchanged between systems that are not part of a closed
  ecosystem MUST be encoded using UTF-8". "Implementations MUST NOT add a byte order mark
  (U+FEFF) to the beginning of a networked-transmitted JSON text. In the interests of
  interoperability, implementations that parse JSON texts MAY ignore the presence of a byte
  order mark rather than treating it as an error."
- Unpaired surrogates (§8.2): the grammar allows "\uDEAD"; "The behavior of software that
  receives JSON texts containing such values is unpredictable".
- Comparison (§8.3): names compared "code unit by code unit" after escapes are converted are
  interoperable; "implementations that compare strings with escaped characters unconverted
  may incorrectly find that "a\\b" and "a\b" are not equal."
- Parsers (§9): "A JSON parser MUST accept all texts that conform to the JSON grammar. A JSON
  parser MAY accept non-JSON forms or extensions. An implementation may set limits on the
  size of texts that it accepts. An implementation may set limits on the maximum depth of
  nesting. An implementation may set limits on the range and precision of numbers. An
  implementation may set limits on the length and character contents of strings."

### 1.2 I-JSON, RFC 7493 ([RFC7493])

- §2.1: "I-JSON messages MUST be encoded using UTF-8". "Object member names, and string
  values in arrays and object members, MUST NOT include code points that identify Surrogates
  or Noncharacters as defined by [UNICODE]. This applies both to characters encoded directly
  in UTF-8 and to those which are escaped; thus, "\uDEAD" is invalid because it is an
  unpaired surrogate, while "𐊭" would be legal."
- §2.2: numbers beyond a binary64 double's magnitude or precision SHOULD NOT be sent; "An
  I-JSON sender cannot expect a receiver to treat an integer whose absolute value is greater
  than 9007199254740991 ... as an exact value."
- §2.3: "Objects in I-JSON messages MUST NOT have members with duplicate names. In this
  context, "duplicate" means that the names, after processing any escaped characters, are
  identical sequences of Unicode characters."
- §3: "Protocols that use I-JSON messages can be written so that receiving implementations
  are required to reject (or, as in the case of security protocols, not trust) messages that
  do not satisfy the constraints of I-JSON."

### 1.3 JSONTestSuite ([JSONTestSuite])

Nicolas Seriot's suite, MIT licensed, holds 318 parsing cases in `test_parsing/`: 95 named
`y_` that a parser must accept, 188 named `n_` that it must reject, and 35 named `i_` on which
RFC 8259 leaves the choice to the implementation (numbers past a double's range, unpaired
surrogates, invalid UTF-8, a byte order mark, deep nesting). Two `n_` cases are large: 100,000
opening brackets, and 250,001 bytes of `[{"":` repeated.

## 2. S3's policy operations

### 2.1 PutBucketPolicy ([API_PutBucketPolicy])

- `PUT /?policy`, the body "Policy in JSON format". Headers: `Content-MD5`, not marked
  required; `x-amz-sdk-checksum-algorithm`; `x-amz-confirm-remove-self-bucket-access`, "Set
  this parameter to true to confirm that you want to remove your permissions to change this
  bucket policy in the future", and nothing more; `x-amz-expected-bucket-owner`.
- "If you don't have `PutBucketPolicy` permissions, Amazon S3 returns a `403 Access Denied`
  error. If you have the correct permissions, but you're not using an identity that belongs
  to the bucket owner's account, Amazon S3 returns a `405 Method Not Allowed` error."
- "To ensure that bucket owners don't inadvertently lock themselves out of their own buckets,
  the root principal in a bucket owner's AWS account can perform the `GetBucketPolicy`,
  `PutBucketPolicy`, and `DeleteBucketPolicy` API actions, even if their bucket policy
  explicitly denies the root principal's access." The Get and Delete pages repeat it.
- Status: the Response Syntax says 200, the sample 204. S3 answered 204 (secondary:
  LocalStack), and s3-tests asserts 204.
- The sample request's policy: `{"Version":"2008-10-17","Id":"aaaa-bbbb-cccc-dddd",
  "Statement":[{"Effect":"Allow","Sid":"1","Principal":{"AWS":["111122223333",
  "444455556666"]},"Action":["s3:*"],"Resource":"arn:aws:s3:::bucket/*"}]}`.
- botocore marks the operation `requestChecksumRequired`.
- `x-amz-expected-bucket-owner`, on these operations as on most others: "If the account ID
  that you provide does not match the actual owner of the bucket, the request fails with the
  HTTP status code `403 Forbidden` (access denied)." S3 answered a mismatch 403 `AccessDenied`,
  "Access Denied", and the values `0000`, `0000000000020`, `abcd` and `invalid` 400
  `InvalidBucketOwnerAWSAccountID`, "The value of the expected bucket owner parameter must be an
  AWS Account ID... [0000]", the value in brackets (secondary: LocalStack
  `test_get_bucket_policy_invalid_account_id`, `test_put_bucket_policy_expected_bucket_owner`,
  `test_delete_bucket_policy_expected_bucket_owner`, recorded 2026-02-21). The error table's
  message is "The value of the expected bucket owner parameter must be an AWS account ID."

### 2.2 GetBucketPolicy, DeleteBucketPolicy, GetBucketPolicyStatus

- GetBucketPolicy, `GET /?policy`, answers the policy as JSON. Without one S3 answered 404
  `NoSuchBucketPolicy`, "The bucket policy does not exist", with a `BucketName` element
  (secondary: LocalStack); the error table's message is "The specified bucket does not have
  a bucket policy." ([ErrorResponses]).
- S3 gives back its own serialization, not the bytes sent: members sent as `Action, Effect,
  Resource, Principal` came back `"Version","Statement":[{"Effect","Principal","Action",
  "Resource"}]` (secondary: LocalStack, whose snapshot keeps the order inside a policy
  string). s3-tests asserts the bytes sent come back (`test_set_get_del_bucket_policy`,
  `test_bucket_policy_deny_self_denied_policy`).
- DeleteBucketPolicy, `DELETE /?policy`, answers 204, and 204 again without a policy
  (secondary: LocalStack).
- GetBucketPolicyStatus, `GET /?policyStatus`, answers `<PolicyStatus><IsPublic>boolean
  </IsPublic></PolicyStatus>`. Its sample writes `TRUE`; botocore reads a boolean as `text ==
  'true'`, so `TRUE` reads as false ([botocore] `parsers.py`). Without a policy S3 answered
  404 `NoSuchBucketPolicy` (secondary: [cloudquery-12163], 2023); s3-tests expects `IsPublic`
  false.

### 2.3 Size and errors

- "Bucket policies are limited to 20 KB in size" ([add-bucket-policy]). S3 answered
  `MalformedPolicy`, "Normalized policy document exceeds the maximum allowed size of 20480
  bytes" (secondary: [hca-372], 2021): the limit is 20,480 bytes of the document normalized.
- The error table: `MalformedPolicy`, "Your policy contains a principal that is not valid.",
  400; `NoSuchBucketPolicy`, 404.
- S3's answers to documents that are not policies (secondary: LocalStack):
  - `""` and `"invalid json"`: 400 `MalformedPolicy`, "Policies must be valid JSON and the
    first byte must be '{'".
  - `"{}"`: 400 `MalformedPolicy`, "Missing required field Statement".
- Other `MalformedPolicy` messages S3 answered (secondary):
  - "Invalid principal in policy" ([invalid-principal], AWS-authored).
  - "Policy has invalid resource": a bucket in `Resource` other than the policy's
    ([appsync-4], 2018; [custodian-3523], 2019), and `"Resource": ["*"]` ([noobaa-10058],
    2026).
  - "Action does not apply to any resource(s) in statement" ([mojmp-10762], 2025).
  - "Policy has an invalid condition key", for `s3:VersionStatus` ([openondemand-31], 2026).
  - "Policy has invalid action", for `s3:PutObjectLockConfiguration` ([edge-280], 2026).
  - "Invalid policy syntax.", for `"Action": null` ([carina-3576], 2026).
- versitygw's catalogue of what it says S3 answers (third-party): "This policy contains
  invalid Json" for a body that begins `{`; "Could not parse the policy: Statement is empty!";
  "Invalid effect: <value>"; "The policy must contain a valid version string"; "Invalid
  Condition type : <name>" for an unknown or mis-cased operator; "Conditions do not apply to
  combination of actions and resources in statement"; "Invalid IP address in Conditions".

## 3. The policy language

### 3.1 Grammar ([iam-grammar], BNF verbatim, the doubled colon the page's)

```
policy  = {
     <version_block?>,
     <id_block?>,
     <statement_block>
}
<version_block> = "Version" : ("2008-10-17" | "2012-10-17")
<id_block> = "Id" : <policy_id_string>
<statement_block> = "Statement" : [ <statement>, <statement>, ... ]
<statement> = {
    <sid_block?>,
    <principal_block?>,
    <effect_block>,
    <action_block>,
    <resource_block>,
    <condition_block?>
}
<sid_block> = "Sid" : <sid_string>
<effect_block> = "Effect" : ("Allow" | "Deny")
<principal_block> = ("Principal" | "NotPrincipal") : ("*" | <principal_map>)
<principal_map> = { <principal_map_entry>, <principal_map_entry>, ... }
<principal_map_entry> = ("AWS" | "Federated" | "Service" | "CanonicalUser") :
    [<principal_id_string>, <principal_id_string>, ...]
<action_block> = ("Action" | "NotAction") :
    ("*" | <action_string> | [<action_string>, <action_string>, ...])
<resource_block> = ("Resource" | "NotResource") :
    : ("*" | <resource_string> | [<resource_string>, <resource_string>, ...])
<condition_block> = "Condition" : { <condition_map> }
<condition_map> = {
  <condition_type_string> : { <condition_key_string> : <condition_value_list> },
  <condition_type_string> : { <condition_key_string> : <condition_value_list> }, ...
}
<condition_value_list> = [<condition_value>, <condition_value>, ...]
```

- "If the element takes an array (marked with [ and ]) but only one value is included, the
  brackets are optional." "Quotation marks are optional for numeric and Boolean values."
- "Individual elements must not contain multiple instances of the same key." "Blocks can
  appear in any order."
- "The `principal_block` element is required in resource-based policies (for example, in
  Amazon S3 bucket policies)". "The `principal_map` element in Amazon S3 bucket policies can
  include the `CanonicalUser` ID."
- "you cannot use both `Action` and `NotAction` in the same policy statement. Other pairs that
  are mutually exclusive include `Principal`/`NotPrincipal` and `Resource`/`NotResource`"
  ([iam-elements]).
- "Note that all policies must be in UTF-8" ([iam-datatypes]).
- Version: "The default policy version is "2008-10-17."" ([iam-troubleshoot]). Under
  `2008-10-17`, "variables such as `${aws:username}` aren't recognized as variables and are
  instead treated as literal strings" ([iam-version]).
- Sid: IAM requires it unique and alphanumeric; "some services allow additional characters
  such as spaces in the `Sid` value", and S3's own samples use spaces ([iam-grammar]).
- Effect: "The `Effect` value is case sensitive" ([iam-effect]).

### 3.2 Actions and resources

- "Statements must include either an `Action` or `NotAction` element." "The prefix and the
  action name are case insensitive." "You can use multi-character match wildcards (`*`) and
  single-character match wildcards (`?`)" ([iam-action]). `NotAction` "explicitly matches
  everything except the specified list of actions" ([iam-notaction]).
- "Statements must include either a `Resource` or a `NotResource` element." "You can use
  multiple * or ? characters in each segment. If the * wildcard is the last character of a
  resource ARN segment, it can expand to match beyond the colon boundaries." "The asterisk (*)
  character can expand to replace everything within a segment, including characters like a
  forward slash (/)". Its example: `arn:aws:s3:::amzn-s3-demo-bucket/*/test/*` matches
  `amzn-s3-demo-bucket/1///test///object.jpg`, `amzn-s3-demo-bucket//test/object.jpg` and
  `amzn-s3-demo-bucket/1/test/`, and not `amzn-s3-demo-bucket/1-test/object.jpg`,
  `amzn-s3-demo-bucket/test/object.jpg` or `amzn-s3-demo-bucket/1/2/test.jpg`
  ([iam-resource]).
- S3's page: "a wildcard character can't span segments" ([s3-iam]), against IAM's trailing
  `*` crossing colons. An S3 key may hold a colon.
- ArnEquals and ArnLike are "Case-sensitive matching of the ARN" ([iam-operators]). No page
  says how a `Resource` naming an S3 object compares case (UNVERIFIED); S3 keys are
  case-sensitive.

### 3.3 Conditions ([iam-condition], [iam-operators], [iam-multivalued])

- "Context key *names* are not case-sensitive." "A context key that is not present in the
  request is considered a mismatch."
- "If the key that you specify in a policy condition is not present in the request context,
  the values do not match and the condition is *false*. If the policy condition requires that
  the key is *not* matched, such as `StringNotLike` or `ArnNotLike`, and the right key is not
  present, the condition is *true*. This logic applies to all condition operators except
  ...IfExists and Null check."
- String: `StringEquals` "Exact matching, case sensitive"; `StringNotEquals`;
  `StringEqualsIgnoreCase`, `StringNotEqualsIgnoreCase`; `StringLike` "Case-sensitive
  matching. The values can include multi-character match wildcards (*) and single-character
  match wildcards (?) anywhere in the string"; `StringNotLike`.
- Numeric: `NumericEquals`, `NumericNotEquals`, `NumericLessThan`,
  `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals`: "comparing a key
  to an integer or decimal value".
- Date: `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`,
  `DateGreaterThan`, `DateGreaterThanEquals`: "one of the W3C implementations of the ISO 8601
  date formats or in epoch (UNIX) time".
- `Bool`; `BinaryEquals`, base64; `IpAddress` and `NotIpAddress`: "The value must be in the
  standard CIDR format ... If you specify an IP address without the associated routing prefix,
  IAM uses the default prefix value of `/32`."
- `ArnEquals`, `ArnLike`, `ArnNotEquals`, `ArnNotLike`: "Each of the six colon-delimited
  components of the ARN is checked separately and each can include multi-character match
  wildcards (*) or single-character match wildcards (?)."
- `...IfExists`: "If the key is not present, evaluate the condition element as true." Any
  operator but `Null`.
- `Null`: `true` for a key that does not exist, `false` for one that does.
- Several operators, and several keys under one operator, are ANDed; several values for one
  key are ORed, and for a negated operator NORed.
- `ForAllValues`: "true if every context key value in the request matches a context key value
  in the policy. It also returns `true` if there are no context keys in the request."
  `ForAnyValue`: "true if any one of the context key values in the request matches any one of
  the context key values in the policy. For no matching context key or if the key does not
  exist, the condition returns `false`."

### 3.4 Policy variables ([iam-variables])

- "Variables are marked using a `$` prefix followed by a pair of curly braces", naming "any
  single-valued condition key"; "Key names are case-insensitive". They may be used "in the
  `Resource` element and in string comparisons in the `Condition` element", in a resource
  "only in the resource portion of the ARN", and only under Version `2012-10-17`.
- "When you use a variable with no value in the condition element ... `StringEquals` or
  `StringLike` do not match ... Inverted condition operators like `StringNotEquals` or
  `StringNotLike` do match against a null value". A `Resource` holding a variable with no
  value "will not match any resource".
- `${*}`, `${?}` and `${$}` stand for the literal characters; a default is written
  `${aws:PrincipalTag/team, 'company-wide'}`.
- For an anonymous caller "(Amazon SQS, Amazon SNS, and Amazon S3 only)", `aws:userid` is
  `anonymous` and `aws:PrincipalType` `Anonymous`; `aws:username` is absent.

## 4. Evaluation

- "By default, all requests are implicitly denied with the exception of the AWS account root
  user, which has full access." "An explicit deny overrides an explicit allow." "An implicit
  denial occurs when there is no applicable `Deny` statement but also no applicable `Allow`
  statement" ([iam-evaluation], [iam-deny-allow], [iam-interplay]).
- Within one account, "If either the identity-based policy or the resource-based policy
  within the same account allows the request and the other doesn't, the request is still
  allowed." Across accounts, "The request is allowed only if both evaluations return a
  decision of `Allow`" ([iam-basics], [iam-cross-account]).
- S3: "If there is no bucket policy in place, then the bucket implicitly allows requests from
  any AWS Identity and Access Management (IAM) identity in the bucket-owner's account. The
  bucket also implicitly denies requests from any other IAM identities from any other
  accounts, and anonymous (unsigned) requests" ([troubleshoot-403]).
- "All unauthenticated requests are made by the anonymous user." "For resource-based
  policies, using a wildcard (*) with an `Allow` effect grants access to all users, including
  anonymous users (public access)" ([s3-policy-language], [iam-principal]).
- "`Allow` statements in a bucket policy apply only to objects that are owned by the same
  bucket-owning account. However, `Deny` statements in a bucket policy apply to all objects
  regardless of object ownership." With ACLs disabled the bucket owner owns every object
  ([troubleshoot-403], [bucket-policies]).
- "You can't use a bucket policy to prevent deletions or transitions by an S3 Lifecycle rule"
  ([bucket-policies]).
- A missing object is 404 to a requester with `s3:ListBucket` and 403 to one without
  ([API_GetObject]).
- Denials read "User {user-arn} is not authorized to perform {action} on "{resource-arn}"
  because {context}", "with an explicit deny in a {type} policy" or "because no {type} policy
  allows the {action} action", only within one account or organization ([troubleshoot-403]).

## 5. S3's actions, resources and condition keys ([sar-s3], [s3-policy-actions])

- Resources: `arn:${Partition}:s3:::${BucketName}` and
  `arn:${Partition}:s3:::${BucketName}/${ObjectName}`. "The object ARN must contain a forward
  slash after the bucket name."
- The actions of the operations mantle serves:

| Operation | Action | Resource |
|---|---|---|
| ListBuckets | `s3:ListAllMyBuckets` | none; not in a bucket policy |
| CreateBucket | `s3:CreateBucket` | bucket |
| DeleteBucket | `s3:DeleteBucket` | bucket |
| HeadBucket, ListObjects, ListObjectsV2 | `s3:ListBucket` | bucket |
| ListObjectVersions | `s3:ListBucketVersions` | bucket |
| ListMultipartUploads | `s3:ListBucketMultipartUploads` | bucket |
| GetBucketLocation | `s3:GetBucketLocation` | bucket |
| Get/PutBucketVersioning | `s3:GetBucketVersioning` / `s3:PutBucketVersioning` | bucket |
| Get/Put/DeleteBucketTagging | `s3:GetBucketTagging` / `s3:PutBucketTagging` | bucket |
| Get/PutBucketAcl | `s3:GetBucketAcl` / `s3:PutBucketAcl` | bucket |
| Get/Put/DeleteBucketOwnershipControls | `s3:GetBucketOwnershipControls` / `s3:PutBucketOwnershipControls` | bucket |
| Get/Put/DeleteBucketLifecycle(Configuration) | `s3:GetLifecycleConfiguration` / `s3:PutLifecycleConfiguration` | bucket |
| Get/Put/DeleteBucketCors | `s3:GetBucketCORS` / `s3:PutBucketCORS` | bucket |
| Get/Put/DeleteBucketPolicy | `s3:GetBucketPolicy` / `s3:PutBucketPolicy` / `s3:DeleteBucketPolicy` | bucket |
| GetBucketPolicyStatus | `s3:GetBucketPolicyStatus` | bucket |
| GetObject, HeadObject, GetObjectAttributes | `s3:GetObject`, or `s3:GetObjectVersion` with a version ID | object |
| PutObject, CreateMultipartUpload, UploadPart, CompleteMultipartUpload | `s3:PutObject` | object |
| CopyObject, UploadPartCopy | the source's `s3:GetObject(Version)`, the destination's `s3:PutObject` | object |
| DeleteObject | `s3:DeleteObject`, or `s3:DeleteObjectVersion` with a version ID | object |
| DeleteObjects | the same, for each key; a refused key is an `AccessDenied` entry in a 200 | object |
| Get/Put/DeleteObjectTagging | `s3:GetObjectTagging` / `s3:PutObjectTagging` / `s3:DeleteObjectTagging`, and `...VersionTagging` | object |
| Get/PutObjectAcl | `s3:GetObjectAcl` / `s3:PutObjectAcl`, and `...VersionAcl` | object |
| AbortMultipartUpload | `s3:AbortMultipartUpload` | object |
| ListParts | `s3:ListMultipartUploadParts` | object |

- Conditionally also: `s3:PutObjectTagging` for PutObject's `x-amz-tagging`, `s3:PutObjectAcl`
  for its ACL headers, `s3:GetObjectTagging` for GetObject's tag count. The API pages and the
  user guide disagree for DeleteObject ("you must always have the `s3:DeleteObject`
  permission"), GetObjectAttributes and GetObjectTagging.
- S3's condition keys, with their types: `s3:prefix`, `s3:delimiter` (String), `s3:max-keys`
  (Numeric), on `ListBucket` and `ListBucketVersions` only; `s3:x-amz-acl`,
  `s3:x-amz-copy-source`, `s3:x-amz-metadata-directive`, `s3:x-amz-grant-*`,
  `s3:x-amz-storage-class`, `s3:x-amz-server-side-encryption*` (String);
  `s3:ExistingObjectTag/<key>` (String), on reads of an object and its tags and ACL, not on
  PutObject or DeleteObject; `s3:RequestObjectTag/<key>` (String) and `s3:RequestObjectTagKeys`
  (ArrayOfString); `s3:versionid` (String); `s3:signatureversion`, `s3:authType` (String),
  `s3:signatureAge`, `s3:TlsVersion` (Numeric), `s3:x-amz-content-sha256` (String), on every
  action; `s3:if-match` on PutObject and DeleteObject and `s3:if-none-match` on PutObject;
  `s3:ObjectCreationOperation` (Bool); `s3:ResourceAccount` (String); `s3:locationconstraint`.
- `s3:signatureversion` is `AWS` for Signature Version 2 and `AWS4-HMAC-SHA256` for version 4;
  `s3:authType` is `REST-HEADER`, `REST-QUERY-STRING` or `POST`; `s3:signatureAge` is
  milliseconds ([sigv4-conditions]).
- Global keys: `aws:SourceIp` (IP address), `aws:SecureTransport` (Boolean, "always
  included"), `aws:CurrentTime` and `aws:EpochTime` ("always included"), `aws:PrincipalArn`
  ("Anonymous requests do not include this key"), `aws:PrincipalAccount`, `aws:PrincipalType`
  and `aws:userid` ("all requests, including anonymous requests"), `aws:username`,
  `aws:referer` and `aws:UserAgent` (which "should not be used to prevent unauthorized parties
  from making direct AWS requests") ([iam-condition-keys]).

## 6. Principals ([s3-iam], [iam-principal], [invalid-principal])

- An account: `{"AWS": "arn:aws:iam::123456789012:root"}` or `{"AWS": "123456789012"}`: "The
  account ARN and the shortened account ID behave the same way. Both delegate permissions to
  the account."
- IAM users, roles, role sessions and federated users by ARN; S3's `{"CanonicalUser": "..."}`;
  a service, `{"Service": "cloudfront.amazonaws.com"}`, never `"*"`.
- Everyone: `"Principal": "*"` or `{"AWS": "*"}`. "For anonymous users, these two methods are
  equivalent." "You cannot use a wildcard to match part of a principal name or ARN." A list of
  principals is an OR.
- "If your Amazon S3 bucket policy contains an invalid value of the Principal element, then
  you receive the "Invalid principal in policy" error." Its supported values: an IAM user or
  role ARN, "An AWS account ID or AWS service principals", "The wildcard (*) to represent all
  users"; a unique identifier is refused.
- "`NotPrincipal` must be used with `"Effect":"Deny"`"; s3-tests expects `Allow` with
  `NotPrincipal` refused, `InvalidArgument` or `MalformedPolicy`.

## 7. Block Public Access and "public" ([block-public-access])

- BlockPublicPolicy makes S3 "reject calls to `PutBucketPolicy` if the specified bucket
  policy allows public access", 403 `AccessDenied`. RestrictPublicBuckets limits a bucket with
  a public policy "to only AWS service principals and authorized users within the bucket
  owner's account" and "rejects all anonymous (or unsigned) calls". "Since April 2023, all
  Block Public Access settings are enabled by default for new buckets."
- "The meaning of public": "Amazon S3 begins by assuming that the policy is public. ... To be
  considered non-public, a bucket policy must grant access only to fixed values (values that
  don't contain a wildcard or an AWS Identity and Access Management Policy Variable) for one
  or more of the following": a principal; `aws:SourceIp` CIDRs no broader than `/8` for IPv4
  and `/32` for IPv6, RFC 1918 ranges excepted; `aws:SourceArn`, `aws:SourceVpc`,
  `aws:SourceVpce`, `aws:SourceOwner`, `aws:SourceAccount`; `aws:userid` outside `AROLEID:*`;
  `s3:DataAccessPointArn`, `s3:DataAccessPointAccount`.

## 8. botocore ([botocore])

- `PutBucketPolicy`: `PUT /{Bucket}?policy`, `requestChecksumRequired`, payload `Policy`, a
  string sent as its UTF-8 bytes. `GetBucketPolicy`: the body handed back unparsed.
  `DeleteBucketPolicy`: `responseCode` 204. `GetBucketPolicyStatus`: `GET /{Bucket}?policyStatus`,
  payload `PolicyStatus` with a boolean `IsPublic`.
- `ConfirmRemoveSelfBucketAccess` is the header `x-amz-confirm-remove-self-bucket-access`.

## 9. ceph s3-tests ([s3-tests])

`make_json_policy` writes `{"Version":"2012-10-17","Statement":[{"Action","Principal",
"Effect","Resource"[,"Condition"]}]}` with Python's `json.dumps`. No `bucket_policy` test is
`fails_on_aws`. Among them:

- `test_bucket_policy`: an Allow of `s3:ListBucket` to `{"AWS": "*"}` on the bucket and its
  objects lets another user list.
- `test_set_get_del_bucket_policy`: GetBucketPolicy gives back the bytes sent; after
  DeleteBucketPolicy, `NoSuchBucketPolicy`.
- `test_bucket_policy_multipart`: `s3:PutObject` on the bucket's ARN grants nothing on its
  objects, and the policy is expected to be accepted; S3 refuses such a statement, "Action
  does not apply to any resource(s) in statement" (§2.3).
- `test_bucket_policy_another_bucket`: `arn:aws:s3:::*` and `arn:aws:s3:::*/*` grant on any
  bucket.
- `test_bucket_policy_get_obj_existing_tag`, `..._get_obj_tagging_existing_tag`,
  `..._put_obj_tagging_existing_tag`: `s3:ExistingObjectTag/security` = `public` grants reads
  of objects so tagged; a tag change is judged by the tags before it.
- `test_bucket_policy_put_obj_copy_source` and `..._copy_source_meta`: `s3:x-amz-copy-source`
  matched with `StringLike` as `bucket/public/*`, without a leading slash;
  `s3:x-amz-metadata-directive` absent when the header is.
- `test_bucket_policy_put_obj_acl`: a Deny with `StringLike s3:x-amz-acl "public*"` refuses a
  `public-read` PUT.
- `test_encryption_sse_c_enforced_with_bucket_policy`: a Deny binds the bucket's owner.
- `test_bucket_policy_allow_notprincipal`: `Allow` with `NotPrincipal` is 400.
- `test_block_public_policy`: a public policy under BlockPublicPolicy is 403;
  `test_block_public_restrict_public_buckets`: RestrictPublicBuckets turns an anonymous GET
  the policy allowed from 200 to 403.
- `test_head_object_404_with_policy_prefix`: `s3:ListBucket` with `s3:prefix` `public/*` makes
  a HEAD of a missing `public/object` 404 and of `private/object` 403.
- `test_get_bucket_policy_status`: a bucket with no policy is not public;
  `test_get_publicpolicy_acl_bucket_policy_status`: an Allow to `{"AWS": "*"}` makes it public;
  `test_get_nonpublicpolicy_acl_bucket_policy_status`: the same restricted to
  `aws:SourceIp` `10.0.0.0/32` does not.
- `test_multipart_upload_on_a_bucket_with_policy`: `"Action": "*"` is accepted.

## 10. Discrepancies

1. PutBucketPolicy's status: 200 in its Response Syntax and botocore's model, 204 in its
   sample, S3's answer and s3-tests.
2. GetBucketPolicy: S3 gives back its own serialization; s3-tests expects the bytes sent.
3. GetBucketPolicyStatus without a policy: 404 from S3, `IsPublic` false in s3-tests.
4. A trailing `*` crossing `:`: IAM says it does, S3's page that no wildcard spans segments.
5. `"*"` against `{"AWS": "*"}`: S3's page contradicts itself; IAM and s3-tests make them one.
6. `s3:PutObject` on a bucket's ARN: S3 refuses the statement, s3-tests expects it accepted.
7. `x-amz-confirm-remove-self-bucket-access`: s3-tests reads it as giving up the root's
   protection; AWS says only that it confirms giving up access.
8. The action a DeleteObject, GetObjectAttributes or GetObjectTagging needs: the API pages
   and the user guide differ (§5).

## Sources

- [RFC8259] T. Bray, "The JavaScript Object Notation (JSON) Data Interchange Format", RFC 8259, December 2017.
- [RFC7493] T. Bray, "The I-JSON Message Format", RFC 7493, March 2015.
- [JSONTestSuite] https://github.com/nst/JSONTestSuite at 1ef36fa01286573e846ac449e8683f8833c5b26a (`test_parsing/`, MIT License)
- [API_PutBucketPolicy] https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
- [API_GetObject] https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
- [ErrorResponses] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html
- [add-bucket-policy] https://docs.aws.amazon.com/AmazonS3/latest/userguide/add-bucket-policy.html
- [bucket-policies] https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-policies.html
- [troubleshoot-403] https://docs.aws.amazon.com/AmazonS3/latest/userguide/troubleshoot-403-errors.html
- [s3-policy-language] https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-policy-language-overview.html
- [s3-iam] https://docs.aws.amazon.com/AmazonS3/latest/userguide/security_iam_service-with-iam.html
- [s3-policy-actions] https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html
- [sigv4-conditions] https://docs.aws.amazon.com/AmazonS3/latest/developerguide/bucket-policy-s3-sigv4-conditions.html
- [block-public-access] https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html
- [sar-s3] https://docs.aws.amazon.com/service-authorization/latest/reference/list_s3.html and https://servicereference.us-east-1.amazonaws.com/v1/s3/s3.json
- [iam-grammar] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html
- [iam-elements] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements.html
- [iam-datatypes] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_datatypes.html
- [iam-version] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
- [iam-effect] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_effect.html
- [iam-troubleshoot] https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_policies.html
- [iam-action] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_action.html
- [iam-notaction] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_notaction.html
- [iam-resource] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_resource.html
- [iam-principal] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html
- [iam-condition] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition.html
- [iam-operators] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html
- [iam-multivalued] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-single-vs-multi-valued-context-keys.html
- [iam-variables] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html
- [iam-condition-keys] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html
- [iam-evaluation] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html
- [iam-deny-allow] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic_policy-eval-denyallow.html
- [iam-interplay] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic_AccessPolicyLanguage_Interplay.html
- [iam-basics] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic_policy-eval-basics.html
- [iam-cross-account] https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic-cross-account.html
- [invalid-principal] https://repost.aws/knowledge-center/s3-invalid-principal-in-policy-error (AWS-authored)
- [botocore] https://github.com/boto/botocore/tree/358f8eec8c76201bb1a7a35644abcbc9036de7ed (`service-2.json`, `parsers.py`, `serialize.py`)
- [s3-tests] https://github.com/ceph/s3-tests/blob/5522d1c351f75bc00ae0f64f742f3f095f5939d9/s3tests/functional/test_s3.py and `policy.py`
- [localstack] https://github.com/localstack/localstack/tree/8b9a79f05846835cf4dff63ab7eefdde9df83783/tests/aws/services/s3 (`test_s3_api.py` `TestS3BucketPolicy`, `test_s3.py`, their `.snapshot.json` and `.validation.json`)
- [versitygw] https://github.com/versity/versitygw/tree/b91e178a12fae7a6acf31597a70aef8b8b3e72f4 (`auth/bucket_policy.go`, `tests/integration/PutBucketPolicy.go`)
- [cloudquery-12163] https://github.com/cloudquery/cloudquery/issues/12163
- [hca-372] https://github.com/ebi-ait/hca-ebi-dev-team/issues/372
- [appsync-4] https://github.com/aws-samples/aws-serverless-appsync-app/issues/4
- [custodian-3523] https://github.com/cloud-custodian/cloud-custodian/issues/3523
- [noobaa-10058] https://github.com/noobaa/noobaa-core/issues/10058
- [mojmp-10762] https://github.com/ministryofjustice/modernisation-platform/pull/10762
- [openondemand-31] https://github.com/scttfrdmn/aws-openondemand/issues/31
- [edge-280] https://github.com/bibAtWork/Edge_GitOps/pull/280
- [carina-3576] https://github.com/carina-rs/carina/issues/3576
