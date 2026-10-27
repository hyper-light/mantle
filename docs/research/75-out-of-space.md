# 75. A cell out of space: the answer a full cell gives, on both doors

Status: research note, 2026-10-27. For the answer to an object no volume has room for (node.md
§4.4–§4.5, gateway.md §2; `crates/gateway/src/put.rs`, `crates/node/src/node/host.rs`,
`crates/node/src/listener/respond.rs`, `crates/protocol/src/object.rs`). Sources [1] to [4] were
read from the standard and from source code, each quoted where it was read.

## The question

A chunk a volume refuses goes to the next volume the placement offers, and a block left with no
volume failed its PUT `503 SlowDown` (gateway.md §2), whatever the volumes' reasons. node.md §4.5
says SlowDown sheds load only if its cause lasts no longer than clients retry, and that "a
condition that outlasts the retries, such as a cell out of space, is reported as what it is". A
cell whose volumes are full stays full until an operator frees space or adds a device, so an
answer that clients retry turns a full cell into a stream of retried uploads. The question is
what a full cell answers instead, on the native door and on the S3 listener.

## Sources

[1] RFC 4918, https://www.rfc-editor.org/rfc/rfc4918.txt (read 2026-10-27), §11.5 "507
Insufficient Storage": "The 507 (Insufficient Storage) status code means the method could not be
performed on the resource because the server is unable to store the representation needed to
successfully complete the request.  This condition is considered to be temporary.  If the request
that received this status code was the result of a user action, the request MUST NOT be repeated
until it is requested by a separate user action."

[2] botocore, `botocore/retries/standard.py` (develop, read 2026-10-27), the standard retry mode,
`class TransientRetryableChecker`: `_TRANSIENT_STATUS_CODES = [500, 502, 503, 504]`, checked in
`is_retryable` as `context.http_response.status_code in self._transient_status_codes`; 507 appears
nowhere in the file.

[3] botocore, `botocore/data/_retry.json` (develop, read 2026-10-27), the rules of its other retry
mode: the default policies apply to `"http_status_code": 500`, `502`, `503`, `504`, `509` and
`429`, to 400 with the throttling codes (`"ThrottlingException"`, `"ThrottledException"`,
`"RequestThrottledException"`, `"Throttling"`, `"ProvisionedThroughputExceededException"`), and
to `"GENERAL_CONNECTION_ERROR"`; S3's own add 400 with `"RequestTimeout"` and with `"BadDigest"`.
No policy names 507. `botocore/config.py` names the modes (`legacy`, "The pre-existing retry
behavior", `standard` and `adaptive`), so whichever a client runs, 507 is not among the codes it
retries. The AWS CLI and boto3 both retry through botocore (research/73 [5]).

[4] MinIO, `cmd/api-errors.go` (master, read 2026-10-21 in the data-layer plan's research), an
S3-compatible server's answer when its drives reach their free-space floor: code
`XMinioStorageFull`, "Storage backend has reached its minimum free drive threshold. Please delete
a few objects to proceed.", with HTTP status `http.StatusInsufficientStorage`, 507.

## Decisions

**A block every one of whose refusing volumes refused it as full fails `Full`.** A volume says why
it refused a chunk (`chunk::Status::Full`: "the volume has no room, and cleaning can make none"),
and the PUT's driver keeps, for each block, whether every refusal it met was that one. With no
volume left, the block fails `PutError::Full` if so, and `Unplaced` if any volume refused it for
another cause, busy, failed, closed or unreachable, each of which may pass within a client's
retries. A volume merely busy is never taken for a full cell.

**`Full` is `507 Insufficient Storage` on the S3 listener and `InsufficientStorage` natively.**
507 is the status HTTP defines for a server that cannot store what a request needs [1], an
S3-compatible server answers its full drives with it [4], and neither of botocore's retry modes
retries it [2][3], so a stock client's upload ends at the first answer rather than after its
retries' backoff, and the operator sees why. RFC 4918 calls the condition temporary and says a
user's request is not to be repeated until the user asks again [1], which is the behavior wanted:
space comes back by an operator's act. S3 names no code for a store out of space; the listener's
error document carries `InsufficientStorage`, the status's own name, where MinIO carries its own
`XMinioStorageFull` [4]. The native protocol's status is a code of its own, 26, beside the others,
which the client returns as the reply's status, as it does `SlowDown`, without asking again.

**Nothing of the refused object commits.** The refusal comes before the object's file is written
or its version committed; chunks of its blocks that some volumes took are left to the sweeps, as
every failed PUT's are (gateway.md §2). A multipart part and a native run are put by the same
driver and refused the same way; the upload stays open, its committed parts or runs kept.

## Open

- Space a deleted object frees comes back only after the collector's grace (three days by
  default), so a cell refusing `507` stays full through deletes until then; whether a cell near
  its floor should shorten the grace is a question for the collector's design.
- A volume full while its peers have room leaves a block of copies on fewer nodes than its scheme
  wants unplaced: placement by free space and device budgets is planned item 23.
