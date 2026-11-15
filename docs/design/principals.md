# Principals: keys of their own, permissions of their own

Status: built 2026-11-14 (branch `principals`). Sources: docs/research/91 (IAM users, access keys,
identity policies, cited as "91 §x"), research/17 (the policy language and its evaluation),
research/84 (an access key's forms), encryption.md §2 (the root key and key wrap), gateway.md §5
(validated caches), architecture §9 (the control plane's state in the root range).

A cell has one account. Until now it had one access key, the account's root's, which `mantle init`
writes into every founder's configuration (node.md §1.5). Services and kiln's runners need keys of
their own, each with its own permissions, made and revoked across the cell at once.

## 1. A principal

**Decision: a principal is an IAM user of the cell's account** (91 §1, §2). It has a name as IAM
names a user, 1 to 64 characters of letters, digits and `+=,.@_-`, unique without regard to case;
a unique ID of twenty base32 characters, 100 bits drawn from the OS when it is made, so a name
deleted and made again is another principal (n IDs drawn repeat one with a chance below
n²/2¹⁰¹, the birthday bound; a key id drawn again is refused as taken); one identity policy; and at
most two access keys, IAM's quota, which is what lets a key be replaced with no request refused
(91 §4). An account holds
at most IAM's 5,000 principals. What a principal makes is its account's, as an IAM user's is (91
§1): the host judges ownership by account exactly as before, and the cell's one account owns every
bucket.

The root's key stays what it was: in each founder's configuration, unrestricted, the key that S3
lets skip the user context (91 §1). An operator who wants no root key in use gives an
administrator principal `s3:*` on `*` and keeps the root's key for bootstrap; removing it from the
configurations is still one configuration at a time (§8).

## 2. Where they are kept

**Decision: in the cell's root range**, the control plane's store (architecture §9, item 5), which
every founder hosts a member of. Its rows (`mantle_meta::principal`; keys in `mantle_meta::key`):

| Row | Key | Value |
|---|---|---|
| a principal | `DATA`, 4, its name lowercase | its name as given, unique ID, creation, policy's compact form, its keys' ids |
| an access key | `DATA`, 5, its id | its principal's name, the root key's generation, the secret wrapped, its creation |
| the count of principals | `DATA`, 6 | a number, held to 5,000 |

Every row carries the format byte and CRC-32C every root-range row does (CLAUDE.md §6). The
commands are five (`principal::Command`): Create, Policy, Delete, Key and Revoke, each applied
deterministically and each holding IAM's bounds whatever it carries: a name or id out of form, or a
policy past its bound, is `Invalid` and changes nothing; a principal past 5,000 or a third key
is `Full`; a name taken is `Taken`; a delete of a principal holding keys is `Held`, IAM's
DeleteConflict (91 §2). A rotation is a Key command that holds only while the principal holds
exactly one key, which it names; with none it is `Unrotatable`. A command is a gateway's or a
node's to send (node.md §3.6); a client asks the node that takes its request, which checks and
commands.

## 3. Secrets

**Decision: a key's secret is drawn from the OS and wrapped with AES-256 key wrap under the root
key's current generation before anything is proposed**, as a file's data key is (encryption.md §2):
a secret is a key, the HMAC key SigV4 derives from, and KW is "approved for protecting keys" (SP
800-38F §5.4). The node that takes the operator's CreateKey draws the id and the secret
(`mantle_s3::account::draw_id`, `draw_secret`, the draw `mantle init` takes for the root's key),
wraps the secret's forty characters into 48 bytes (`mantle_s3::seal::wrap_secret`), and proposes the
row naming its generation; the Raft log, the engine store and every read's answer hold the wrapped
secret alone. The plaintext secret leaves the cell once, in the reply to the CreateKey that made it,
over the operator's mutually authenticated connection, as IAM shows a secret only at creation (91
§2). The command line writes it only to the file the operator names, as AWS's shared credentials
file under the profile `mantle`, owner-only, written whole and durable (`crate::files`), as `mantle
init` writes the root's; never to standard output or a log. No type that holds a secret prints it:
the reply's `Issued`, the door's `Signer` and the rows' `Debug` leave it out, and every secret in
memory is wiped when dropped.

## 4. How a request signed with a principal's key is judged

**Decision: each request signed with a principal's key reads that key's row from the root range by
ReadIndex as it arrives, and is judged by what the read returns.** This is gateway.md §5's rule for
every cache of what changes, "validated, not trusted and not invalidated": invalidation needs every
holder known and holds the change on the slowest, and leases wait for a stated clock bound
(replica.md §7). The listener (node.md §4.2, steps 3 and 4):

1. routes the request, and reads the access key its signature claims, its form checked before any
   secret is asked for (`sigv4::access_key`);
2. a root's key is the configuration's, as before; any other is asked of the node through the door
   (`door::KeyAsk`), whose host reads `Query::Signer` of the root range, a ReadIndex read asked after
   the request arrived, and unwraps the secret under the generation the row names;
3. checks the signature against that secret (`sigv4::verify`), an unknown key S3's `403
   InvalidAccessKeyId`;
4. judges the request by the principal's identity policy (§5), anything it does not allow `403
   AccessDenied`;
5. hands the operation to the host as the account's, as every request is.

A read sees every write committed before it was asked (replica.md §3), so a key revoked by a
command committed before a request arrives is refused by whichever node takes the request, and a
policy replaced is judged by from then on: a revocation takes effect at once across the cell, never
one configuration file at a time, where IAM's is eventually consistent (91 §4, §5). A request that
was judged before the revocation committed goes on, as one already authenticated does at S3. A node
that cannot reach a root-range leader within the range's budget answers `503 SlowDown` (node.md
§5.4) rather than judge by anything it holds. The lookups a node holds at once are its door's
requests, one a connection.

*Its cost:* one ReadIndex round of the root range per principal-signed request, the round a GET
already pays its Name range (gateway.md §5); rounds are shared by the reads that wait on one
(replica.md §3). The root's key pays nothing new. The round's added latency under load is not yet
measured (§8).

## 5. Permissions

**Decision: a principal's permissions are an IAM identity policy, read and judged by the bucket-policy
code** (`mantle_s3::policy`; research/17): `policy::identity` reads the document as IAM reads a
user's inline policy (91 §2, §3): at most 131,072 characters of IAM's pattern as sent, at most
2,048 characters of compact form; no `Principal`, IAM's "Policy document should not specify a
principal"; resources `*` or any S3 ARN; actions any of S3's in the Service Authorization
Reference's catalog, those on a bucket or an object and the fifteen on no resource
(`catalog::ACCOUNT_ACTIONS`, `s3:ListAllMyBuckets` among them); conditions as a bucket policy's. A
statement whose actions act on none of its resources is kept, granting nothing, as IAM keeps one. A
policy naming another service's action or resource is refused, since a cell serves S3 alone (91
§5). The cell keeps the compact form, which reads back as the same policy (`policy::kept`).

A request is judged (`policy::permits`) by IAM's single-account logic (91 §3): an applying deny
refuses it, an applying allow lets it, and nothing applying refuses it. The action is the one S3
authorizes its operation by (`policy::action`), ListBuckets's `s3:ListAllMyBuckets` on S3's whole
namespace, `arn:aws:s3:::*`, which `*` and `arn:aws:s3:::*` match and no bucket's ARN does
(`policy::ACCOUNT_RESOURCE`; 91 §3). The request's keys are the principal's, as IAM gives an IAM
user's (`aws:username`, `aws:userid`, `aws:PrincipalArn` `arn:aws:iam::ACCOUNT:user/NAME`,
`aws:PrincipalType` `User`, `aws:PrincipalAccount`), and the request's own: `aws:SecureTransport`,
`aws:SourceIp`, `aws:CurrentTime`, `aws:EpochTime`, and a listing's `s3:prefix`, `s3:delimiter` and
`s3:max-keys` where the catalog gives its action them (`policy::carries`): ListBucket's and
ListBucketVersions', never ListBucketMultipartUploads'. Bucket and prefix grants are IAM's own
forms: a bucket's ARN for its listing, under an `s3:prefix` condition for a prefix, and
`arn:aws:s3:::BUCKET/PREFIX*` for its objects, with `${aws:username}` for a home of each
principal's own (91 §3).

An operation on more than one object is judged on each, as S3 judges it (91 §6). A CopyObject or
an UploadPartCopy asks `s3:PutObject` on its destination and `s3:GetObject` on its source,
`s3:GetObjectVersion` where it names the source's version (`request::asks`), so a copy reads
nothing its principal may not read. A delete asks `s3:DeleteObject` always, and
`s3:DeleteObjectVersion` too where it names a version (`request::deleting`), as S3's pages for
DeleteObject and DeleteObjects both state. A DeleteObjects asks nothing of its own: once its document
is read, each object is judged as a delete of its own (`request::Judge::deletes`), and the host
answers those its principal may not delete `AccessDenied` in their places, as S3's own example does,
and deletes the rest (`door::Request::denied`).

Bucket policies are not yet served, so none combines with a principal's identity policy; a bucket
policy names no user yet (s3-protocol.md §10).

## 6. The operator's commands

**Decision: principals and keys are the operator's to manage with the binary**, asked of the cell
over the native protocol as the client of `--client`, which acts for the account's root (node.md
§3.6), as IAM lets the root manage users and their keys:

| Command | Native kind | IAM's |
|---|---|---|
| `mantle principal create NAME --policy FILE` | 25 CreatePrincipal | CreateUser, PutUserPolicy |
| `mantle principal policy NAME FILE` | 26 PutPolicy | PutUserPolicy |
| `mantle principal delete NAME` | 27 DeletePrincipal | DeleteUser |
| `mantle principal list`, `mantle key list` | 28 ListPrincipals | ListUsers, ListAccessKeys |
| `mantle principal show NAME` | 29 GetPrincipal | GetUser, GetUserPolicy |
| `mantle key create PRINCIPAL --out FILE` | 30 CreateKey | CreateAccessKey |
| `mantle key rotate PRINCIPAL --out FILE` | 30 CreateKey, rotating | CreateAccessKey, the second |
| `mantle key revoke ID` | 31 RevokeKey | DeleteAccessKey |

Refusals are IAM's codes (91 §2): `EntityAlreadyExists`, `NoSuchEntity`, `LimitExceeded`,
`DeleteConflict`, `MalformedPolicyDocument`, and `NothingToRotate` for a rotation of a principal
holding no key. The command line checks a policy as the node does before asking, so its refusal
names the rule. A key is made only into a file that does not exist; a rotation writes its new key
over the file of the key it replaces, which stays valid until it is revoked, and says both ids:
IAM's rotation (91 §4) is `rotate`, clients moving to the file's new key, then `revoke` of the old.
A secret made but not written is lost, as IAM's is, and the command names the key to revoke. An
issued key's reply travels as a listing of its own kind (8), so no earlier reply changes a byte.

## 7. Tests

- A copy's asks (`crates/node/src/listener/request.rs`): both its objects, the version form where
  its source names a version, and none of a batch delete's own; a delete's `s3:DeleteObject`, and
  `s3:DeleteObjectVersion` too where it names a version.
- Policies (`crates/s3/src/policy`): AWS's own identity policies read; what IAM refuses refused, a
  principal, another service, an action S3 does not define, IAM's bounds to the character; a
  prefix grant admitting exactly its prefix; a deny over an allow; a variable standing for the
  user's name; `s3:ListAllMyBuckets` granted by `*` and `arn:aws:s3:::*` alone.
- Rows and commands (`crates/meta/src/principal.rs`, wire and record golden vectors): made, read
  back, revoked and gone; two keys and a rotation's one; a delete only once no key is held; IAM's
  bounds; pages in order.
- Reads (`crates/node/src/read.rs`), the native messages (`crates/protocol`, golden vectors), the
  secret's wrap (`crates/s3/src/seal.rs`), SigV4's claimed key (`crates/s3/src/sigv4.rs`).
- Against a cell of three (`crates/mantle/tests/principals.rs`), the AWS CLI v2 and boto3 signing
  with keys the operator made: five principals under four policies let in exactly as granted, and
  refused `AccessDenied` otherwise, through every node; a copy, by CopyObject and by
  UploadPartCopy, refused whose source its principal may not read; a batch delete answering a key
  its principal may not delete `AccessDenied` in its place, verbose and quiet, and deleting the rest;
  in a versioned bucket, a named version's copy refused without `s3:GetObjectVersion` and its delete,
  by DeleteObject and DeleteObjects, without `s3:DeleteObject`, both let once the policy grants them;
  the operator's refusals; a policy replaced judged by every node from its answer on; a revoked
  key, and keys the cell never made, refused `InvalidAccessKeyId` by every node from the revoke's
  answer on, nothing waited for; and a key rotated under a boto3 client asking all along, through
  the nodes in turn, its credentials read afresh each request, none of its requests refused.

## 8. Open

- *The root's key* is still each founder's configuration's. Recording it in the root range at the
  cell's first start would let it be rotated and revoked as a principal's is.
- *Root key generations*: a key's secret is wrapped under the generation current when it was made;
  the rewrap pass encryption.md §2 plans for files' keys is owed these rows too, before a
  generation is retired.
- *Bucket policies* naming principals, and how they combine with identity policies (91 §3).
- *The lookup's cost*: one root-range ReadIndex round per principal request is not yet measured
  against the root's; a validation that sends the row's version and gets no row back when it is
  current (gateway.md §5) is the next step if it matters.
- *A key's last use* (IAM's GetAccessKeyLastUsed), which a rotation's step 3 reads (91 §4).
- *Veil*: linking principals' keys to Veil's credentials is its own piece of work, once Veil is
  built.
