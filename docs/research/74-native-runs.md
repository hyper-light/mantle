# 74. Native uploads in runs: what this step builds of research note 30's design, and why

Status: research note, 2026-10-26. For the native upload in server-cut runs (gateway.md §2.1, §6;
metadata.md §2; crypto.md §10; `crates/meta/src/name.rs`, `crates/gateway/src/{put,complete}.rs`,
`crates/client`, `crates/mantle/src/{client,journal}.rs`). Research note 30 §3.5 designed the
upload; this note records what the first build of it decided where 30 left a choice, with the
sources each decision rests on, each quoted where it was read.

## Sources

[1] Research note 30, §3.5 and §4.6: the resumable-upload draft's model, "the offset MUST NOT
decrease", an append at the wrong offset refused "with the correct offset" [RESUME-12 §4]; runs
rather than an offset into one file, so no AES-GCM nonce is reused; the checkpoint interval's two
measured terms, `g/r_meta` where "the next run streams while the last one commits" and Young's
`g·sqrt(2h/λ)` where "a commit stalls the stream"; and its bounds, "`c ≥ S/10,000` for a known
size", "`5 MiB ≤ c ≤ 5 GiB` except the last run", "`c ≤` the client's buffer when its source cannot
be re-read", "checkpoints fall on 64 KiB plaintext boundaries".

[2] AWS CLI Command Reference, `aws s3 cp`,
https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html (read 2026-10-26), `--expected-size`:
"This argument specifies the expected size of a stream in terms of bytes. Note that this argument
is needed only when a stream is being uploaded to s3 and the size is larger than 50GB. Failure to
include this argument under these conditions may result in a failed upload due to too many parts in
upload." Its example 12: "The `--expected-size` option must be provided, or the upload may fail when
it reaches the default part limit of 10,000".

[3] Research note 73, source [10]: the AWS CLI's `multipart_chunksize`, "Default - `8MB`", "Minimum
For Uploads - `5MB`".

[4] Research note 05, §4.1: "Part size: 5 MiB to 5 GiB. There is no minimum size limit on the last
part", and 10,000 parts; §4.7: ListMultipartUploads lists at most 1,000 uploads a page.

[5] AWS-LC, `crypto/fipsmodule/digest/md32_common.h` (vendored, aws-lc-sys 0.45.0): "|Nl| and |Nh|
maintain the number of bits processed so far"; a context holds `num` bytes of a partial block
beside its chaining value, so its state is the chaining value and the length exactly when `num`
is 0. RFC 1321 §3.4 processes the message "in 16-word blocks", 64 bytes.

## Decisions

**A run's length is fixed at the Open, from the bounds alone.** Both measured terms of [1] need what
is not yet built: `r_meta` is admission's run-commit share per principal (note 27), and `λ` an
estimator of interruptions per upload with its exact Poisson bound. Until they are, the run is the
smallest the bounds of [1] allow, which [1] prescribes for "a first upload on a new path": the least
part, longer where 10,000 runs must hold a stated length, whole 64 KiB segments, at most the
client's buffer and the largest part (`name::run_length`). Being a function of the upload's row
alone, the range checks every run's length against it exactly, so the server cuts the runs in
fact, not by advice.

**A stream of no stated length runs at its buffer's length.** With no length, no run shorter than
the largest stream needs keeps the runs within 10,000: a stream of any length needs runs of 5 GiB.
The AWS CLI meets the same limit with a fixed chunk and asks the length of streams past what 10,000
chunks hold [2]; mantle cuts such a stream into runs as long as the client's buffer, the most a
source read once can resend, and the command line takes `--size` as the AWS CLI takes
`--expected-size`, but exactly: the last run must end at it. The command line's buffer is the AWS
CLI's 8 MiB chunk [3], so 10,000 runs hold 80 GiB of an unstated stream.

**One run per exchange, each sent once the last's frame is in.** Each run is a request whose body's
length is stated before it, which the transport requires of every exchange, and a frame answers it.
The next run therefore does not stream while the last commits: the stall [1]'s Young term weighs,
here unweighed, since the term is not built. Two exchanges in flight, the node holding a run's
commit behind the one before it, is the form [1] prefers; it is left for when a measurement says
what the stall costs.

**An object one run holds is put whole.** One run is the whole object either way, committed or
not, and a put carries it in one exchange where a native upload takes three, with the same bytes,
ETag (the MD5, at most 5 GiB) and full-object CRC; so the client puts it, and a copy of a small
object costs what it did.

**The MD5 across runs lives in the upload's row.** At a run's end on a 64 KiB boundary no byte waits
in MD5's buffer [5], so its chaining value and the run's offset are its whole state: the row keeps
the 16 bytes, the vendored aws-lc-rs reads them out and resumes from them (crypto.md §10). The
last run, which may end mid-block, finishes the digest instead. Past 5 GiB the object's ETag is the
multipart one and the running MD5 is dropped; a run's own MD5, its part's ETag, is taken only where
the object may pass 5 GiB, so a stated length within it hashes each byte once.

**A resumed upload hashes the bytes it does not resend.** After a restart the client reads the
source from its start and hashes the bytes below the node's offset without sending them, so the
Finish's CRC is of the source as it is now and a source changed below the offset ends in `BadDigest`
with nothing committed, where trusting the CRCs of the runs sent before would commit an object no
source ever held. Within one process the client combines the CRCs of the runs it sent, each the
trailer the node checked before it committed the run.

**The command line's journal is bounded by one listing page.** A record names an upload the client
left open, and the cell lists open uploads 1,000 a page [4]; holding at most that many keeps every
upload the journal names in one `mantle multipart uploads`, and a copy past them is refused rather
than leaving uploads no listing shows at once. The journal is the command line's, kept through the
library's `Watch`, which tells it the upload's ID before any run is sent; a journal in the library
itself, with the flush and power-cut tests of gateway.md §6, is STATUS item 12's.

**A native Finish's retry is matched by size and full-object checksum.** With the operation
identity's record not yet built (STATUS item 9), a Finish asked again once the upload is gone must
find the version its first made without a list of parts: the version keeps the object's size and
full-object CRC, which the client sent and cannot have sent for another object of the same upload,
so the range answers the retry from the version when both match, and `NoSuchUpload` otherwise.

## Open

- The checkpoint interval's measured terms, and two runs in flight, once a measurement of a run's
  commit stall on each supported target says what they would save (gateway.md §2.1).
- The operation identity's record, after which an Open asked again finds its upload.
- A journal in the library, flushed and power-cut tested at every write (gateway.md §6).
