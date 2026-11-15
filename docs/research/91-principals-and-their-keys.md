# 91. Principals and their keys: IAM users, access keys and identity policies

Status: research note, 2026-11-14. For a cell's principals, their access keys and their identity
policies (docs/design/principals.md). Sources [1] to [20] were read from AWS's documentation on
2026-11-14, each quoted as it was read; research/17 holds the policy language and its evaluation
for bucket policies, research/05 S3's error codes, research/84 the forms an access key takes.

## The question

A cell has one account and, until now, one access key, the one `mantle init` writes into every
founder's configuration: the account's root's. Kiln's runners and other services each need keys of
their own, each with its own permissions, and a key that is revoked must stop working everywhere at
once, never one configuration file at a time. Four things follow. What a principal is, and the
bounds on how many there are and how many keys each holds. How a key is made, shown, rotated and
revoked. What a principal's permissions are written in and how a request is judged by them. And
what the stock clients that sign with these keys (the AWS CLI, boto3) expect of all of it.

## Sources

[1] IAM User Guide, "Manage access keys for IAM users",
https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html: "Access keys
consist of two parts: an access key ID (for example, `AKIAIOSFODNN7EXAMPLE`) and a secret access key
(for example, `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)." "The secret access key can be retrieved
only at the time you create it. If you lose your secret access key, you must delete the access key
and create a new one." "You can have a maximum of two access keys per user."

[2] IAM User Guide, "Update access keys",
https://docs.aws.amazon.com/IAM/latest/UserGuide/id-credentials-access-keys-update.html, "To update
access keys without interrupting your applications (AWS CLI)": "1. While the first access key is
still active, create a second access key, which is active by default. ... At this point, the user
has two active access keys. 2. Update all applications and tools to use the new access key. 3.
Determine whether the first access key is still in use ... 4. ... change the state of the first
access key to `Inactive` ... 5. Use only the new access key to confirm that your applications are
working. ... 6. After you wait some period of time to make sure that all applications and tools have
been updated, you can delete the first access key".

[3] AWS General Reference, "AWS Identity and Access Management endpoints and quotas",
https://docs.aws.amazon.com/general/latest/gr/iam-service.html: "Access keys per user | Each
supported Region: 2 | No | The maximum number of access keys that you can create for an IAM user."
"Users per account | Each supported Region: 5,000 | No | The maximum number of IAM users you can
create for your AWS account." "Managed policy length | Each supported Region: 6,144 | No".

[4] IAM User Guide, "IAM and AWS STS quotas",
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html: "Policy documents can
contain only the following Unicode characters: horizontal tab (U+0009), linefeed (U+000A), carriage
return (U+000D), and characters in the range U+0020 to U+00FF." "Names of users, groups, roles,
policies, instance profiles, server certificates, and paths must be alphanumeric, including the
following common characters: plus (+), equals (=), comma (,), period (.), at (@), underscore (_),
and hyphen (-)." "Names of users, groups, roles, and instance profiles must be unique within the
account. They aren't distinguished by case". "User name | 64 characters". Inline policies: "the
total aggregate policy size (the sum size of all inline policies) per entity can't exceed the
following limits: User policy size can't exceed 2,048 characters. ... IAM doesn't count white space
when calculating the size of a policy against these limits."

[5] IAM API Reference, PutUserPolicy,
https://docs.aws.amazon.com/IAM/latest/APIReference/API_PutUserPolicy.html: PolicyDocument
"Length Constraints: Minimum length of 1. Maximum length of 131072. Pattern:
`[\u0009\u000A\u000D -ÿ]+`"; errors "LimitExceeded ... HTTP Status Code: 409",
"MalformedPolicyDocument: The request was rejected because the policy document was malformed. The
error message describes the specific error. HTTP Status Code: 400", "NoSuchEntity ... HTTP Status
Code: 404".

[6] IAM API Reference, CreateAccessKey,
https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateAccessKey.html: "The default status
for new keys is `Active`." "To ensure the security of your AWS account, the secret access key is
accessible only during key and user creation." "LimitExceeded: The request was rejected because it
attempted to create resources beyond the current AWS account limits. ... HTTP Status Code: 409".

[7] IAM API Reference, DeleteUser,
https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteUser.html: "when you delete a user
programmatically, you must delete the items attached to the user manually, or the deletion fails.
... Before attempting to delete a user, remove the following items: ... Access keys
(DeleteAccessKey) ... Inline policies (DeleteUserPolicy)"; "DeleteConflict: The request was rejected
because it attempted to delete a resource that has attached subordinate entities. ... HTTP Status
Code: 409".

[8] IAM API Reference, ListUsers,
https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListUsers.html: MaxItems "If you do not
include this parameter, the number of items defaults to 100. ... Valid Range: Minimum value of 1.
Maximum value of 1000."

[9] IAM User Guide, "IAM identifiers",
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html: "arn:aws:iam::{account}:
user/{user-name-with-path}"; "The user name is case-insensitive for the user, such as during the
sign in process, but is case-sensitive when you use it in a policy or as part of an ARN."; "every
IAM user has a unique ID, even if you create a new IAM user that reuses a friendly name you deleted
before. In the example, the old IAM user `John` and the new IAM user `John` have different unique
IDs."; prefixes "AIDA | IAM user", "AKIA | Access key".

[10] IAM User Guide, "AWS JSON policy elements: Principal",
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html: "You
cannot use the `Principal` element in an identity-based policy. Identity-based policies are
permissions policies that you attach to IAM identities (users, groups, or roles). In those cases,
the principal is implicitly the identity where the policy is attached." "The account ARN and the
shortened account ID behave the same way. Both delegate permissions to the account." IAM user
principals: `"Principal": { "AWS": "arn:aws:iam::{AWS-account-ID}:user/{user-name}" }`.

[11] IAM User Guide, "IAM JSON policy elements: Resource",
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_resource.html:
"Statements must include either a `Resource` or a `NotResource` element." "Some AWS services do not
allow you to specify actions for individual resources. In these cases, any actions that you list in
the `Action` or `NotAction` element apply to all resources in that service. When this is the case,
you use the wildcard character (`*`) in the `Resource` element." "The asterisk (`*`) character can
expand to replace everything within a segment, including characters like a forward slash (`/`)".

[12] IAM User Guide, "Policy evaluation for requests within a single account",
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic_policy-eval-basics.html:
"When access to a resource is requested, AWS evaluates all the permissions granted by the policies
for at least one Allow within the same account. An explicit deny in any of the policies overrides
the allow." "If either the identity-based policy or the resource-based policy within the same
account allows the request and the other doesn't, the request is still allowed." Its example policy
for the user `carlossalazar` grants `s3:ListAllMyBuckets` with `"Resource": "arn:aws:s3:::*"`, and
denies `s3:*` on `"arn:aws:s3:::*log*"`, which "denies Carlos access to any S3 bucket with `log` in
its name".

[13] S3 User Guide, "How Amazon S3 authorizes a request",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-s3-evaluates-access-control.html: "User
context – In the user context, the parent account to which the user belongs is the context
authority. ... A user must have permission from the parent account to perform the operation. This
step applies only if the request is made by a user in an AWS account. If the request is made by
using the root user credentials of an AWS account, Amazon S3 skips this step." "Bucket context – In
the bucket context, Amazon S3 evaluates policies owned by the AWS account that owns the bucket."
"To perform a specific operation on a resource, an IAM principal needs permission from both the
parent AWS account to which it belongs and the AWS account that owns the resource."

[14] IAM User Guide, "IAM policy elements: Variables and tags",
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html, "Principal key
values": "AWS account root user | (not present) | AWS account ID | Account"; "IAM user |
{IAM-user-name} | unique ID | User" (for `aws:username`, `aws:userid`, `aws:PrincipalType`);
"Variables were introduced in version `2012-10-17`."; "If a variable that has no value in the
authorization context is used as part of the `Resource` or `NotResource` element of a policy, the
resource that includes a policy variable with no value will not match any resource."

[15] IAM User Guide, "Amazon S3: Allows IAM users access to their S3 home directory,
programmatically and in the console",
https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_home-directory-console.html:
statements `s3:ListAllMyBuckets` and others on `"Resource": "*"`; `s3:ListBucket` on
`arn:aws:s3:::amzn-s3-demo-bucket` under `"StringLike": {"s3:prefix": ["", "home/",
"home/${aws:username}/*"]}`; and `s3:*` on `arn:aws:s3:::amzn-s3-demo-bucket/home/${aws:username}`
and `.../home/${aws:username}/*`.

[16] IAM User Guide, "Troubleshoot IAM", "Changes that I make are not always immediately visible",
https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html: "IAM uses a distributed
computing model called eventual consistency. Any changes that you make in IAM ... take time to
become visible from all possible endpoints. ... IAM also uses caching to improve performance, but in
some cases this can add time."

[17] S3 API Reference, CopyObject, https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html,
read 2026-11-14, "Permissions": "If the source object is in a general purpose bucket, you must have
`s3:GetObject` permission to read the source object that is being copied. If the destination bucket
is a general purpose bucket, you must have `s3:PutObject` permission to write the object copy to the
destination bucket." "To copy a different version, use the `versionId` query parameter.
Specifically, append `?versionId=<version-id>` to the value".

[18] S3 API Reference, UploadPartCopy,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html, read 2026-11-14,
"Permissions": "You must have `READ` access to the source object and `WRITE` access to the
destination bucket." "If the source object is in a general purpose bucket, you must have the
`s3:GetObject` permission to read the source object that is being copied. If the destination bucket
is a general purpose bucket, you must have the `s3:PutObject` permission to write the object copy
to the destination bucket."

[19] S3 API Reference, DeleteObjects,
https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html, read 2026-11-14: "For each
key, Amazon S3 performs a delete operation and returns the result of that delete, success or
failure, in the response." "`s3:DeleteObject` - To delete an object from a bucket, you must always
specify the `s3:DeleteObject` permission. `s3:DeleteObjectVersion` - To delete a specific version
of an object from a versioning-enabled bucket, you must specify the `s3:DeleteObjectVersion`
permission." Its example "Multi-object delete resulting in mixed success/error response": "the
requester does not have permission to delete the `sample2.txt` object", answered `<Deleted>`
`<Key>sample1.txt</Key>` and `<Error><Key>sample2.txt</Key><Code>AccessDenied</Code><Message>Access
Denied</Message></Error>`.

[20] S3 User Guide, "Required permissions for Amazon S3 API operations",
https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html, read
2026-11-14. CopyObject, "For source object: (Required) Either `s3:GetObject` or
`s3:GetObjectVersion`": "`s3:GetObject` – Required if you want to copy an object from the source
bucket without specifying `versionId` in the request." "`s3:GetObjectVersion` – Required if you want
to copy a specific version of an object from the source bucket by specifying `versionId` in the
request."; "For destination object: (Required) `s3:PutObject`"; "(Conditionally required)
`s3:PutObjectTagging` | Required if you want to put the copied object with object tagging to the
destination bucket". UploadPartCopy: the same for its source; "For destination part: (Required)
`s3:PutObject`". DeleteObjects: "(Required) Either `s3:DeleteObject` or `s3:DeleteObjectVersion`":
"`s3:DeleteObject` – Required if you want to remove an object without specifying `versionId` in the
request." "`s3:DeleteObjectVersion` – Required if you want to remove a specific version of an
object by specifying `versionId` in the request."

## What the sources settle

### §1 A principal is an IAM user of the cell's account

A principal is named as IAM names a user [4]: 1 to 64 characters of letters, digits and `+=,.@_-`,
unique in the account without regard to case, its ARN `arn:aws:iam::ACCOUNT:user/NAME` [9]. It has a
unique ID drawn when it is made and never reused, so a principal deleted and made again under its
name is another principal [9]. What it makes is its account's: S3 judges a user's request in the
user context, the parent account's, and the account owns the bucket [13]. A request signed with the
root's key, `mantle init`'s, skips the user context [13].

### §2 IAM's bounds, each a stated one

5,000 users an account and 2 keys a user, neither adjustable [3]; a name of 64 characters [4]; a
user's inline policies 2,048 characters together, white space not counted [4], sent as a document
of 1 to 131,072 characters of IAM's pattern [5]; a page of users 1 to 1,000 [8]. Past a quota IAM
answers `LimitExceeded` (409), a name taken `EntityAlreadyExists`, an entity missing `NoSuchEntity`
(404), a user with keys held `DeleteConflict` (409), a malformed policy `MalformedPolicyDocument`
(400) [5]-[7]. An access key is an id and a secret as AWS's are, forty characters of base64 for the
secret (research/84) [1]; the secret is shown once, when the key is made, and never again [1], [6].

### §3 Identity policies and how a request is judged

An identity policy names no principal: "the principal is implicitly the identity where the policy
is attached" [10]. Its statements need a resource [11]; an action with no resource of its own,
`s3:ListAllMyBuckets` among S3's, is granted on `*` [11], and AWS's own policies grant it on `*`
[15] and on `arn:aws:s3:::*` [12], so it is judged here against S3's whole namespace, which both
patterns match and no bucket's or object's ARN does (DERIVED). `*` in a resource runs across `/`
[11]. Variables stand for the request's keys under `2012-10-17` [14]: a user's `aws:username` is
its name, `aws:userid` its unique ID, `aws:PrincipalType` `User`; the root has no `aws:username`
[14]. A home directory is granted as IAM's example grants it [15].

A request goes ahead only if a policy allows it and none denies it: "An explicit deny in any of the
policies overrides the allow" [12]. With no bucket policy served, a principal's identity policy is
the user context's grant that S3 requires [13]; the bucket context is the principal's own
account's, which owns every bucket of the cell. A bucket policy naming the account "delegate[s]
permissions to the account" [10], which combining with users' identity policies is for when bucket
policies are served beside principals.

### §4 Rotation and revocation

Rotation without interruption is two keys at once: the second made while the first is active,
clients moved to it, the first then deactivated and deleted [2]. IAM's changes are eventually
consistent and cached [16]; nothing in IAM makes a revoked key fail everywhere at a known moment.

### §5 Where a cell differs, and why

- *Revocation is at once, not eventual* [16]: a key's row is read by ReadIndex from the root range
  as each request signed with it arrives, so a key revoked by a committed command is refused by
  every node from then on (docs/design/principals.md §4).
- *No inactive state*: UpdateAccessKey's `Inactive` [2] is not offered; a key is revoked by
  deleting it, and the second key is the rotation's overlap. A key revoked by mistake is replaced by
  a new one, as a lost secret is [1].
- *Stricter policies*: a policy naming another service's action or resource is refused, since a
  cell serves S3 alone and the statement would grant nothing; IAM keeps such a statement.
- *Not offered*: paths, groups, managed policies, roles, federation and temporary credentials, and
  IAM's own Query API; principals are managed by the operator's commands over the native protocol.

### §6 Operations on more than one object

A copy reads one object and writes another, and is judged on both: `s3:PutObject` on its
destination and `s3:GetObject` on its source, CopyObject [17] and UploadPartCopy [18] alike; a
source named by its version asks `s3:GetObjectVersion` instead [20], so a principal let read only a
key's current version copies no older one. A copy that sets tags would ask `s3:PutObjectTagging`
too [20]; a cell keeps no tags, refuses a copy that sets them, and copies a source's empty set, so
none is asked. A DeleteObjects is each of its keys' deletes [19]: each judged as a delete of its own,
`s3:DeleteObjectVersion` where its entry names a version and `s3:DeleteObject` otherwise [20], and
a key its principal may not delete answered `AccessDenied` in its place, the others deleted, as
[19]'s own example answers. [19] says `s3:DeleteObject` is "always" needed and [20] that a
version's delete needs `s3:DeleteObjectVersion` in its place; a cell follows [20]'s per-operation
table, as its DeleteObject does (research/17), so a version's delete asks the version form alone.
