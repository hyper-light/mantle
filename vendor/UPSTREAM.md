# Vendored crates

mantle's cryptography is AWS-LC, through its Rust binding aws-lc-rs (docs/design/crypto.md).
Both crates are vendored here so mantle owns the exact bytes it builds and can carry fixes
without waiting for an upstream release. The root `Cargo.toml` patches crates.io to these
copies, and `vendor/` is excluded from mantle's workspace: `vendor/Cargo.toml` is a workspace
of its own, in which each crate's suite runs against the other's vendored copy.

```
cargo test --manifest-path vendor/Cargo.toml --workspace --locked
```

That command is one of mantle's gates (CLAUDE.md) and runs on every CI target.

Each crate is its crates.io package as published, verified against the package's SHA-256 in
the crates.io index, plus the local changes listed below. In Rust and C sources, mantle's own
changes also carry a `mantle:` comment that points here; the backport is upstream's commit as
it stands.

## aws-lc-sys 0.45.0

| | |
|---|---|
| Package | `aws-lc-sys-0.45.0.crate`, SHA-256 `9bff6c3b54fad79a2e60b8102caf565819711497c1f5f092f49508e2f5c31b27` |
| aws-lc-rs commit | `7943223c99d909bc399bdf1b856821bb04f1f3c5` (`.cargo_vcs_info.json`) |
| AWS-LC commit | `02561621ffa4cf17c0c4f70bc11a82df36b42ae9` (`package.metadata.aws-lc-sys.commit-hash`) |

### Local changes

| Where | Change | Why |
|---|---|---|
| `builder/main.rs` | CPU jitter entropy is left out unless `AWS_LC_SYS_NO_JITTER_ENTROPY=0` asks for it; upstream builds it in unless asked not to. | The source costs every new process 17.6 ms before its first random bytes, and the cost is inherent to it. Without it, AWS-LC seeds from the operating system's generator (docs/measurements/2026-09-28-aws-lc-first-random.md; docs/design/crypto.md §2). |
| `builder/main.rs` | A system AWS-LC is used only when `AWS_LC_SYS_USE_SYSTEM=1` asks for one; upstream uses one whenever pkg-config finds it. | A system library would silently replace the patched sources here. |
| `aws-lc/crypto/fipsmodule/rand/asm/rndr-armv8.pl`, the three generated `rndr-armv8.S`, `entropy/entropy_sources.c`, `entropy/internal.h`, `entropy/entropy_source_test.cc` | Backport of AWS-LC commit `8d931575b042` (aws/aws-lc#3475): a failed read of RNDR is detected from the Z flag, and RNDR and RDRAND reads are retried up to 10 times (`RNDR_MAX_ATTEMPTS`, `RDRAND_MAX_ATTEMPTS`). The commit's edits to `crypto/libcrypto.map` and `libcrypto.txt` are left out: the package has neither file. | aws/aws-lc#3453: RNDR fails transiently in the wild, and the Arm ARM allows it (a read that cannot return a random number "in a reasonable period of time" sets NZCV to 0b0100 and returns 0). Before the fix a failure read as success. |
| `entropy/entropy_sources.c`, `entropy/internal.h` | A hardware rng read that fails all its attempts is replaced by a read from the operating system (`hw_rng_or_os_multiple8`), and the CPU entropy methods return success. `hw_rng_or_os_multiple8_FOR_TESTING` exposes the fallback to tests. | With the retry alone, ten consecutive failures still fail the method, and `rand.c` aborts the process. The hardware rng supplies only extra entropy or prediction resistance, input mixed into a DRBG seeded from another source. On a CPU without one, AWS-LC already takes that input from the operating system. |
| `generated-include/openssl/boringssl_prefix_symbols.h`, `_asm.h`, `_nasm.inc` | Prefix entries for `hw_rng_multiple8_with_retry_FOR_TESTING` and `hw_rng_or_os_multiple8_FOR_TESTING`. | Upstream regenerates these headers at release; the package predates both functions. |
| `tests/hw_rng_fallback.rs`, `Cargo.toml` | A test target: upstream's retry cases, carried over from `entropy_source_test.cc`, and fake hardware rngs that fail on demand. The fallback must deliver operating-system bytes after exactly the bounded attempts and never abort. | The C test suite is not in the package, and a real hardware rng cannot be made to fail. |

## aws-lc-rs 1.18.1

| | |
|---|---|
| Package | `aws-lc-rs-1.18.1.crate`, SHA-256 `b281d307588d634de920874890732659e2e7672f72b5e10e81badc1a8a83621e` |
| aws-lc-rs commit | `22e629d5c46276497a24ee3e575be4315940e7cb`, tag `v1.18.1` |

### Local changes

| Where | Change | Why |
|---|---|---|
| `src/digest.rs`, `src/digest/sha.rs` | `digest::MD5_FOR_LEGACY_USE_ONLY` over AWS-LC's `EVP_md5`, with RFC 1321's test suite. | S3 ETags and `Content-MD5` are MD5 (docs/research/05 §5.1). |
| `src/digest.rs` | `Context::try_new`, and `try_update` and `try_finish` made public: the fallible forms of `new`, `update` and `finish`, which panic on failure. Tested against the panicking forms and against input past the algorithm's maximum. | Production code never panics (CLAUDE.md §1). |
| `src/digest/state.rs`, `src/digest.rs` | `digest::State` and `Context::state` and `Context::resume`: a context's chaining value and length read out at a block boundary, and a context resumed from them, for MD5, SHA-1 and SHA-2 through AWS-LC's `MD5_CTX`, `SHA_CTX`, `SHA256_CTX` and `SHA512_CTX`. Both refuse a context with input waiting in its buffer, SHA-3, and a state that is not whole blocks, returning `Unspecified`. Tested: a digest resumed at every block boundary of generated inputs equals the one-shot digest, a state taken mid-block is refused, and a fresh state is RFC 1321's and FIPS 180-4's initial value. | A native upload keeps its object's MD5 running across the runs it is cut into and records the state with each run's commit (docs/design/crypto.md §10, gateway.md §2.1); aws-lc-rs exposed no way to read a state out or start from one. |
| `src/hmac.rs` | `hmac::sign_once`: HMAC with a key used once, through AWS-LC's one-shot `HMAC`, tested against RFC 4231 and against `sign` for every algorithm and key lengths either side of the block size. | Keying a `Key` and signing with a copy of its 1,224-byte context took 255 ns for a 170-byte message where the one-shot took 187 ns (docs/measurements/2026-09-28-aws-lc-crypto.md, finding 3). mantle computes every HMAC with it. |
| `src/aead/tls.rs`, `src/aead.rs`, `tests/tls13_vectored_seal.rs` | `aead::Tls13VectoredSealingKey`: AES-GCM sealing of a TLS 1.3 record whose plaintext is several borrowed slices, as one AES-GCM invocation with one nonce and one tag. It seals into a slice or into a `Vec`'s spare capacity, which it exposes only after success. It makes each nonce from the sequence number and the traffic IV, refuses sequence numbers that do not increase and `u64::MAX`, spends a sequence number before encrypting, and refuses every seal after one that fails partway. Built on AWS-LC's incremental `EVP_CIPHER` GCM, and not built with `fips`. Tested against RFC 8448's first encrypted server record, sealed from its four handshake messages, against `TlsRecordSealingKey` for both key sizes, and with slices that overrun, underrun or panic. | aws/aws-lc-rs#1241. A full 16 KiB record seals 8–11% faster from its pieces than gathered and sealed in one call; below about 1 KiB, gathering is as fast or faster (docs/measurements/2026-09-29-tls13-vectored-seal.md). |
| `src/aead/cbc_hmac.rs`, `src/aead.rs`, `src/hmac.rs`, `tests/jwe_rfc7516.rs` | `aead::cbc_hmac`: AES-CBC with HMAC-SHA-2, RFC 7518 §5.2's `A128CBC-HS256`, `A192CBC-HS384` and `A256CBC-HS512`. It seals under a random IV, or the caller's for known answers, and opens only after checking the tag in constant time. HMAC's fallible key copy, update and finish become crate-visible for it, with no change to the public API. Tested against RFC 7518 Appendix B for all three, with each input changed, and with RFC 7516 A.3's JWE decrypted from its compact serialization and encrypted again byte for byte. | aws/aws-lc-rs#617: JWE's content encryption, which aws-lc-rs lacked (research note 14 §10). |
| `src/key_wrap.rs`, `src/key_wrap/tests.rs` | `key_wrap::AES_192`, a 192-bit KEK for key wrap with and without padding, tested against RFC 3394 §4.2 and §4.4 and RFC 5649 §6. | aws/aws-lc-rs#617: JWE's A192KW, PBES2-HS384+A192KW and ECDH-ES+A192KW. |
| `src/aead/aead_ctx.rs`, `src/aead/unbound_key.rs`, `src/aead.rs` | `Clone` for `aead::LessSafeKey`, through `EVP_AEAD_CTX_copy`, with a test that clones are independent. | aws/aws-lc-rs#1165. The AES-GCM, AES-GCM-SIV and ChaCha20-Poly1305 contexts have copy hooks. |
| `Cargo.toml` | `autotests = true`. | Runs the integration tests below, which the published manifest turns off because the package leaves them out. |
| `src/aead/data`, `src/agreement/data`, `src/cipher/data`, `src/data`, `src/test`, `tests/`, `third_party/NIST` | Upstream's test data and integration tests, 119 files, from the `v1.18.1` tag's source archive (`aws-lc-rs-v1.18.1.tar.gz`, SHA-256 `aa5a8cf64b17e2e0bf758a5a5c12799502bc48bea68146eab09532a6c3d10fca`). | The package leaves them out, so its own suite could not run. |

## Upstream issues

| Issue | State here |
|---|---|
| aws/aws-lc-rs#935: GCC 15's `-Werror=unterminated-string-initialization` | Not present in this copy. aws-lc-sys builds with GCC 15.3.0 under both of its builders, including CMake with AWS-LC's `-Werror`, with no such diagnostic. |
| aws/aws-lc-rs#1165: `Clone` for `LessSafeKey` | Done (above). |
| aws/aws-lc-rs#1241: TLS 1.3 AES-GCM sealing from several borrowed slices | Done (above), meeting the issue's acceptance criteria. |
| aws/aws-lc-rs#617: JWE generation and validation | The primitives JWE needs that aws-lc-rs lacked are added (above). With RSA1_5 and RSA-OAEP, ECDH and SSKDF (the Concat KDF), AES-GCM, AES key wrap and PBKDF2 already present, every algorithm RFC 7518 §4 and §5 register can be built on this copy. The JOSE layer itself, headers and serializations, belongs to a JOSE library, as the issue's maintainers scoped it; mantle has no use for one. |
| aws/aws-lc-rs#1233, aws/aws-lc#3453, aws/aws-lc#3475: first-use latency of jitter entropy; RNDR failures | Jitter entropy off by default; retry backported; operating-system fallback added (above). |

## Verification

On macOS 26.4.1, Apple M5 Max, `cargo test --manifest-path vendor/Cargo.toml --workspace`
ran 850 tests with none failing: aws-lc-rs's unit tests, integration tests and doc tests, and
aws-lc-sys's binding layout tests, sanity tests and `hw_rng_fallback`. CI runs the same
command on the six targets.

## Updating

1. Download the new `.crate` files from crates.io, check each SHA-256 against the index, and
   unpack each over its directory here.
2. Re-apply each local change above. The `mantle:` comments mark where they go. Drop any that
   upstream has made, and move its row to Upstream issues.
3. Replace the test data from the new tag's source archive.
4. Run the vendored suites and mantle's gates. Update the versions, checksums, commits and
   test count in this file.

## hyper-raft (shared crates)

**Every crate at `0ad9275` (hyper-raft branch `mantle-frame-flush`, equal to `mantle-shard-port`:
`85bd35f` and thirty-one commits), with no local delta but hyper-transport's, hyper-log's and
hyper-durable's (Local changes, below)**, taken by `scripts/snapshot-hyper-raft.py`.
The branch is in the hyper-raft repository's own objects and not yet pushed to its origin. Its last
commit, `0ad9275`, changes hyper-rt alone (hyper-raft docs/runtime.md §6.2): a standard-output write
dropped before it is ready takes no buffer of the writer's pool. `Stdout::write_all` took a buffer, then
awaited its hand-off's send, which the turn charge (§3.14) may answer `Pending` with the value kept in
the send, so a write dropped there dropped its buffer. The client races each sink write against its
transport and drops a write the transport overtook (`mantle_client::Sink`), and the CLI's two buffers
went one by one on its slice of microseconds: on CI's one-CPU Linux leg every `cp s3://BUCKET/KEY -` of
`crates/mantle/tests/native.rs` then waited for good for a buffer no thread would hand back, its shard
parked and its writer thread idle (kiln r61). Each chunk now reserves room on the writer's channel, then
takes a buffer, then hands it over with no await between; hyper-rt's `tests/stdout_dropped_write.rs`
drops 500 writes part-way on a runtime shaped as the CLI's and was red at its second round before. The
one before it, `ae7d0f6`, changes hyper-tls alone (hyper-tls VENDORED.md §13): a connection's read
buffer grows with its capacity reserved exactly, to a record at its largest on the wire
(`MAX_WIRE_SIZE`, 18,437 bytes) and no more, and keeps it across records and idles, where upstream's
rule shrank it at every drain and Vec's doubling took it to 32 KiB, four reallocations for each PUT over
TLS whose body fills a record (`crates/node/tests/listener_memory.rs`). The one before it, `a575a44`,
changes hyper-liveness alone (hyper-raft docs/timing.md §2.4 and §2.8, "The
floor's measures in their usual range"): the default mode's sender floor `E[flush] + G` takes the
log writes' flush, the stream's own proof and `G` each as the mean of the samples within their tail
as each came, RFC 6298's bound over the samples before it, the rule the disk mode's floor already
took for its flushes (judged apart, `G` stays the mean of every wait). On CI's 1-CPU Linux leg a
node the membership test held 218 s in one poll after a restart put a sample of the hold into each
plain mean among a few, and heartbeated about every 157 s for hours, its peers and it judging each
other as slowly. A hold after any sample is now in no mean; a device that turns slow raises the
floor from its second slow flush and keeps it raised; the floor is never below the stream's proof
in its usual range. The one before it, `4c033bb`, changes hyper-rt alone (hyper-raft docs/runtime.md §3.17, hyper-rt ORIGIN.md): a
thread `Runtime::start` starts, a shard's worker or the watchdog, says it runs through a cell of
hyper-rt's process-wide table (`startup.rs`) in place of a `std::sync::mpsc::sync_channel`, which
whichever end dropped last freed: a started thread dropped its end after its send had woken the
starter, so a starter that ran first returned with the channel still allocated, and
`crates/node/tests/shard_memory.rs`, counting the bytes live as `start` returns, failed on CI's
one-CPU Linux leg (744 bytes over) and in 1–4% of serial runs on macOS. The cell allocates nothing,
so nothing of the hand-off is left to free whichever end lets go last; a refused build is the
worker's own return, which the rollback's join reads. The one before it, `20aa3a6`, changes
hyper-transport and hyper-quic (hyper-raft docs/transport.md, hyper-quic
VENDORED.md §53), for slice 1 of membership.md §5.1: a directory's refusal of an identity closes
with the TLS alert `access_denied` (RFC 8446 §6.2) as a QUIC transport close's CRYPTO_ERROR (RFC
9001 §4.8), which a peer reads in Handshake packets, where an application's close code is replaced
(RFC 9000 §10.2.3); a dialer refusing its server sends its own flight first, so the server
authenticates who refused it and is told `Closed { peer, refusal: Identity }`. A side that did not
dial is told `Identity` for that alert alone, a dialer for any alert of its peer, as before. The one
before it, `e9fa471`, changes hyper-rt alone (hyper-raft docs/runtime.md §3.14 and §8): a channel's
`Sender::reserve`, after Tokio's `mpsc::Sender::reserve` and `Permit`, waits for room before the
value is made, in a place of its own among the waiting senders, charged where it goes on; its
`SendPermit` sends without waiting, keeps a grant of room from every other waiting sender, and
hands the grant on, waking the next, when dropped unused. The room is looked at, not claimed, so a
channel's one sender always finds it: the listener's `stream` reserves the host's queue's room
before it copies a body's next piece. The one before it, `51335dc`, changes hyper-rt alone
(hyper-raft docs/runtime.md §3.16, hyper-rt ORIGIN.md): the compute pool, Kiln's, taken there. CPU-heavy jobs leave the shards for a pool of the CPUs the process
is granted less its shards (`compute::workers_for` over `Facts::cpus_at_once`), its threads below the
shards (Linux's batch policy at nice +10, macOS's utility QoS class, Windows' below-normal level, set
and read back in the OS-interface file `prioritysys.rs`); a job in pieces gives its thread to a job
waiting once its piece has run; admission refuses `Capacity` before anything is queued and hands the
job back; results return by one-shot, an unwind boundary around each job; and `Config::for_runtime`
takes every number from the runtime's own (room for a job of every task the shards hold, a piece of
their step quantum). It adds `crossbeam-deque` 0.8, with `crossbeam-epoch` and `crossbeam-utils`, to
hyper-rt's dependencies, by the owner's approval of 2026-11-18. Nothing in mantle starts the pool yet:
S6b moves the durability model onto it. Its new constants have their rows in
docs/design/constants.md. The one before that, `d4bd299`, changes hyper-quic and hyper-transport's
`Endpoint::new_seeded` doc: an endpoint
given a seed draws its connection IDs from it, each one's random part and the key they are hashed
under unless a `RestartKey` chose it, from a stream apart from the endpoint's own, so its other draws
are where they were; one given none draws them from the thread's generator as before (RFC 9000
§5.1). The IDs lay out the table a node finds its connections in, which in about one run in 16 of
`crates/node/tests/turn_allocs.rs`'s serving cell grew by an allocation (hyper-quic VENDORED.md §52).
The three before it are step 4c of mantle docs/design/durability.md §7.7. `a6a701f` changes hyper-log's
tests alone: a real direct file whose device is stated to write through, decision (B) taken in the
test, answers `Kept` where its writes reach the device and `Buffered` where not, and `Unmeasured`
under `AtFlush::DECIDED`, which is unchanged. `74242cd` changes hyper-durable's benches and the e2e
and comparison crates, which are not snapshotted: their wrapper files pass their file's
`writes_reach_device` through. `70e150f` changes hyper-block alone: `DeviceFile::writes_reach_device`
answers, for the handle as it was opened, whether a write it completes is the device's own: Linux's
`O_DIRECT` on a block device, or on a file of ext2-4 or XFS that statx(2) says takes direct I/O;
Windows' `FILE_FLAG_NO_BUFFERING`; never macOS's `F_NOCACHE`, a buffered handle or another
platform's; a duplicate answers as its original and a `SegmentedFile` its base's. No new constants.
The three before those are step 4b. `2b50efd` changes hyper-log alone: the
device reads the owner's words that another frame follows for a frame answered at its flush too, so
a word left unread never fills the room a later confirmed frame's word needs, which would cost that
frame a confirmation of its own. `f9248d5` changes hyper-log alone: the owner's
two decisions of §7.5 are one typed policy, `AtFlush` (decision (A) `WriteBackFrames`, decision (B)
`StatedCache`), whose `AtFlush::DECIDED` takes neither, so no device answers a frame at its flush; a
log is made or opened with it (`With::at_flush`, `DECIDED` by default); `Answers` gains `Kept`,
`Accepted` and `Buffered`; the create and the open of a log that answers at the flush set the mark's
bit and flush it before its first frame; and the writer answers a frame at its flush where its record
says so and the log answers so, writing no confirmation of it, under condition (a) only in a slot the
file holds written. `0f5dce2` changes hyper-sim and hyper-block: a simulated device that keeps every
completed write (`Device::keep_completed`, `SimFile::keeping`, its cache write-through from a new
`Source::Simulated`), and `BlockFile::writes_reach_device`, false for every real file. No new
constants. The two before those are step 4a. `abfca69` changes hyper-log alone: the open
and the create read the device's write cache where they read its atomic write unit, and `Laid`
carries it with `Answers`, how the log answers a frame's updates and why (`Whole`, `WriteBack`,
`Unmeasured`), every frame still answered once confirmed; and the mark of pieces says, in its
sequence field's next bit, that the log may hold frames answered at their flush (`Mark::at_flush`),
which a binary of `f3dae3b` or before reads as no mark and so refuses the log; this binary reads it
and every re-lay keeps it, and nothing sets it yet. New constants `MARK_AT_END` and `MARK_AT_FLUSH`,
with their rows in docs/design/constants.md. `a43ad9f` changes hyper-block alone: a wrapper or a
simulation states its own device's cache (`DeviceCache::new`). Its fifteenth commit, `f3dae3b`, and
the two before it are step 5 of mantle docs/design/durability.md §7.7. `33fc359` changes hyper-block alone: the macOS
registry search made its owned value before testing the result, so a key no entry had released
null, which traps in CoreFoundation; each value is now made only for a found object. `319ecde`
changes hyper-block alone: the device's write cache is read from the OS (`write_cache::write_cache`:
Linux's queue `write_cache` with `fua`, Windows' STORAGE_WRITE_CACHE_PROPERTY asked of the file's
volume with no access rights, macOS's "WriteCacheState" in the I/O Registry, else fcntl(2)'s
F_FULLFSYNC rule; write-back where the OS describes nothing), typed `WriteThrough`,
`PowerProtected` or `WriteBack` with its source and whether the OS stated it, and carried beside
the atomic write unit (`BlockFile::write_cache`: `DeviceFile`, `SegmentedFile`). `f3dae3b` changes
hyper-durable's benches and the e2e and comparison crates, which are not snapshotted: their wrapper
files pass it through. Nothing is answered differently yet. Its twelfth commit,
`537311c`, changes hyper-log alone (mantle docs/design/durability.md §7.7, step 3c): the writer's
persist record, and its confirmation, say the frame was answered at its flush (`format::AT_FLUSH`)
where the record is in pieces and every update in the frame allows it (`Update::at_flush`); frames
are still answered only once confirmed, so the bit only adds marks, an `Undetermined` frame so
recorded restored as a lost one. The equivalence test's updates allow no answer at the flush, so its
hashes stand. Its eleventh commit,
`e8c4bed`, changes hyper-log and hyper-durable (mantle docs/design/durability.md §7.7, step 3a): a
persist record in pieces says whether its frame was answered at its own flush, in the first byte its
pieces' header left as padding (`format::AT_FLUSH`), and pieces that differ in it are two writes; a
whole record cannot say it. The open reads it from the frame's own records only, so a crash between
two frames never marks the earlier, and restores an `Undetermined` frame so answered as a `Lost` one
is (`Tail::Undetermined::at_flush`). `Update::at_flush`, from hyper-durable's `Write::at_flush`,
which the replica sets where each half of its configuration has voters enough that the others are a
quorum, three or more. Every writer still writes the bit false, so a log's bytes are as before and
the equivalence test's hashes stand. New constant `AT_FLUSH`, and the header's padding read as one
byte, with their rows in docs/design/constants.md. Its tenth commit,
`b4c7230`, changes hyper-log alone (mantle docs/design/durability.md §7.7, step 2b-iii): a sealed
log's persist records are laid in pieces too (format 6), each piece's MAC under the log's
authentication key over the piece through its CRC, so a forged or altered piece, or an unsealed
piece in a sealed log, is `LogError::Tampered`; a sealed log of whole records (format 4) moves to
pieces at its open, and its mark is an unsealed record, which a binary before format 6 refuses as
tampering. The mark records the file's block the slots were laid by and whether they lie at the
area's end (`format::Mark`, which `Laid::pieces` now is), so the slots are found by the recorded
block where the file's block changed but takes them; a binary of format 5 from before refuses such
a mark, and its own marks are written again with the block. Where the device's unit is smaller
than the recorded one, or the file's block does not take the slots, the open lays the area again in
the device's unit and the file's block at the other end of the area, clear of the old slots and the
mark: each slot written whole, a flush, the mark, a flush, so a crash leaves the old area or the
new. The equivalence test's hashes are regenerated: each image differs only in the mark's
confirmation field and CRC, and no transcript line but the crash images' hashes. New decoder width
`piece_mac_at:take(4)`, with its row in docs/design/constants.md. Its ninth commit, `0a33647`,
changes the wrapper files that hyper-raft's benches, e2e and comparison run the log over, none
vendored here, to answer their file's atomic write unit. Its eighth commit, `e856e96`, changes
hyper-block alone: a `SegmentedFile` answers its base's device's atomic write unit rather than the
default. mantle's log runs on a `DeviceFile` (`crates/node` `open_log`), which asks the OS. Its
seventh commit, `3df5efa`, changes hyper-log alone (mantle docs/design/durability.md §7.7, step 2b-ii): an unsealed
log writes and reads its persist records in pieces of the device's atomic write unit (format 5), its
frame's record, its confirmation and a lost frame's copy (which fills its slot, so no later frame's
pieces hide it), each one write then a full flush, frames
still answered only once confirmed. The slots in pieces lie past the whole ones, at offsets the
larger of the unit and the file's block divides; the area's start holds the mark of pieces, a whole
record of the other kind that a binary before format 5 refuses; a log of whole records moves to
pieces at its open, carrying its records; `Recovery::laid` and `Log::laid` report the device's unit,
its source and whether the OS stated it, with the area's. Sealed logs keep format 4. The equivalence
test's hashes are regenerated: the images differ only in the persist area, and no transcript line
but the crash images' hashes. New constant `PIECES_MARK_LEN`, with its row and the mark decoder's
widths in docs/design/constants.md. Its sixth commit, `df77855`, changes hyper-block alone:
`BlockFile::atomic_unit`, a `DeviceFile` asking the OS, a simulated file stating its sector, any
other file `AtomicUnit::DEFAULT`. Its fifth commit, `e848f80`, changes a hyper-rt test alone: the
coop class test marks its trail with atomics, so hyper-raft's contracts check, which allows no
`RefCell` in hyper-rt, passes again. Before them, every crate was at `9580588`. Its third and fourth
commits, `79c74de` and `9580588`, change hyper-block alone (mantle docs/design/durability.md §7.2
and §7.7, step 2b-i): `atomic::atomic_unit(file, path)` reads the atomic write unit of the device
under a file from the OS, an `AtomicUnit` of the unit (an `Alignment`, as `Pieces::new` takes it)
and its `Source`. On Linux, the queue of the block device holding the file, found from its device
number at `/sys/dev/block/MAJOR:MINOR`: `atomic_write_unit_min_bytes` where non-zero and no larger
than the physical block, with `atomic_write_unit_max_bytes`, else `physical_block_size`, else
`logical_block_size`. On macOS, the "Physical Block Size" the storage device publishes in the I/O
Registry, found from the volume's BSD name without opening the device; a disk node opened directly
answers DKIOCGETPHYSICALBLOCKSIZE. On Windows, `FILE_STORAGE_INFO`'s
`PhysicalBytesPerSectorForAtomicity`. A device that states no physical block is taken at its
logical block, and one that states neither at 512 bytes, Linux's own rule (`blk_validate_limits`).
`AtomicUnit::stated` says whether the OS stated the unit or it is that default. Nothing writes in
it yet: the writer and the open take it in step 2b-ii. New constants `Alignment::SECTOR`,
`ATTRIBUTE_READ`, `LOGICAL_SECTOR_AT`, `ATOMICITY_AT`, the IOKit and CoreFoundation constants of
`atomic/macos.rs` and `PHYSICAL_BLOCK_SIZE` of `node/macos.rs`, with their rows in
docs/design/constants.md. Its second commit, `1cc3214`, changes hyper-log's `format.rs` alone (mantle docs/design/durability.md §7.2 and §7.7,
step 2a): `Pieces` lays a persist record in pieces of the device's atomic write unit, which its
caller passes, each piece naming its record (log, sequence, what it confirms, the CRC of all its
rows), its index, the count, the unit and a CRC of its own, format 5 and 6 sealed (format 4 is the
sealed log's); `Pieces::decode` reads a slot's newest record whole, torn with the rows of the
pieces that landed, or `LogError::Damaged` where a piece fails its own checksum. Nothing writes
the pieces yet: the writer and the open still take whole records of format 3 or 4. New constants
`FORMAT_PIECES`, `FORMAT_SEALED_PIECES` and `PIECE_HEADER_LEN`, with their rows in
docs/design/constants.md. Its first commit, `2dd3bde`, changes hyper-log alone (hyper-raft
docs/durable.md; mantle docs/design/durability.md §7.7, step 1): the open reports what the persist records left of the frame after the last that reads
(`Recovery::tail`), `Torn` where no record of it reads, `Undetermined` where its record reads and
no confirmation does, `Lost` where its confirmation reads; recovery is unchanged. No new constant.
Every other crate is as at `85bd35f`. Before them, every crate was at `85bd35f` (hyper-raft branch
`mantle-shard-port`: `land/mantle-general-4`'s `17371e2` and six commits), with no local delta. Its
sixth commit, `85bd35f`, changes hyper-rt alone (hyper-raft docs/runtime.md §3.15, hyper-rt ORIGIN.md,
"task classes"): tasks carry a class, Critical, Normal (the default) or Background, each with its
own lists in the run queue, made as a task of the shard first takes it; each class with work has an
equal share of the shard's time, and the most urgent goes first while it has run no more than the
shard's quantum past the least-run class ready, so a woken Critical task overtakes queued Normal
and Background work and no class is starved. A poll counts to its class at most the watchdog's
limit, a class that starts listing is raised to the shard's virtual time, and while classes are kept
a batch reads its inputs once a budget. A spawn takes the class it is given, Normal unless
`futures::spawn_detached_as` names another; `coop::set_class` changes a task's and `coop::give_way`
hands the shard to more urgent work. Taken from Kiln's runtime copy (its goal 6 step 3). No new
constant: the lead is the shard's quantum and the cap the watchdog's limit (§3.13). Every other
crate is as at `17371e2`. Before it, every crate was at `675a042` (the same branch: `17371e2` and
five commits), with no local delta. Its fifth commit, `675a042`, changes hyper-rt alone (hyper-raft docs/runtime.md §3.14, hyper-rt ORIGIN.md,
"turns by time, the sync primitives, joins, datagrams and signals"): the rest of Kiln's charges, each
made where the leaf goes on and never where it waits — a `Notify`'s notification, a one-shot's value,
a channel's receive and send, a watched word's change, a semaphore's permit (free, or granted to a
queued wait), a `Join`'s outcome and a `SignalStream`'s delivery, each looked at first with nothing
taken; `UdpSocket::recv_from` and `send_to_writable` as each tries. A permit freed while its acquirer
queued now goes to the oldest waiter as a release does, so a permit is taken only through a charge.
No new constant: the share is still the shard's quantum. Readiness groups' members and
hyper-rt-transport's datagram path do not charge yet. Every other crate is as at `17371e2`. Before
it, every crate was at `6a6b4d2` (the same branch: `17371e2` and four commits), with no local delta.
Its fourth commit, `6a6b4d2`, changes hyper-rt alone (hyper-raft docs/runtime.md §3.14, hyper-rt ORIGIN.md,
"turns by time"): a task's operations on sockets and timers charge its turn (`TcpStream`'s reads and
writes, and `LocalStream`'s through it; `TcpListener`'s and `LocalListener`'s accepts; a readiness
wait once it fired; a sleep once due), and once it has held a shard something else wants past its
share, the shard's own quantum (the one `wanted`, the long-poll rule and the watchdog use, so nothing
is picked), the leaf answers `Pending` with its task woken and nothing taken, so the task yields at
that await (`coop::consume_budget` is the charge alone; `Counters::coop_charges`, `coop_yields`).
Each poll's first charge goes on, and a leaf is charged only as it goes on, never as it waits: the
two livelocks Kiln's runtime copy, whose design this takes, found and fixed. Channels, joins,
datagrams and signals did not charge yet. Before it, every crate was at `c472c2e` (the same branch: `17371e2` and three commits), with no local delta, taken by
`scripts/snapshot-hyper-raft.py`. Its third commit, `c472c2e`, changes hyper-rt alone (hyper-raft docs/runtime.md §3.13, hyper-rt ORIGIN.md,
"the watchdog"): each OS runtime runs one watchdog thread that sees a shard held in one poll past
`batch` × twice its quantum (no step of tasks keeping the long-poll contract holds it longer) while
the poll still runs, and reports it once, with the task, its spawn site and label, the hold and whose
time it was (the shard thread's CPU clock over the hold, then its run state on Linux and macOS), on
a board of fixed size on the shard's registry entry that any thread reads with no lock
(`watch::stalled`, `stalls`, `report`: one report kept, numbered on); the thread starts once every
shard is built and is joined at the runtime's shutdown or drop. Every other crate is as at
`17371e2`. Before it, every crate was at `41ce087` (the same branch: `17371e2` and two commits),
with no local delta, taken by `scripts/snapshot-hyper-raft.py`. Those commits
change hyper-rt alone (`1783231`, and `41ce087`, which corrects hyper-rt ORIGIN.md's record of what
was taken from Kiln's `slow.rs` as written; hyper-raft docs/runtime.md §3.12, hyper-rt ORIGIN.md,
"what held a shard"): each shard counts its long polls, those held past twice its step quantum since it last let
them go on (§3.11), against the task's spawn site, recorded by `#[track_caller]` at every spawn, and
the label the task set (`futures::label`), with their hold, their longest and the attribution's
verdict, the task's or the host's, on a table of fixed size (the entries one 4 KiB page holds; a
pair past them counted in one unkeyed entry, nothing evicted) that any thread reads
(`slow::slow_polls`). The threshold is the long-poll rule's, derived from the quantum, where Kiln's
runtime copy, whose design this takes, counts polls past a picked 100 µs. Every other crate is as at
`17371e2`. Before it, every crate was at `17371e2` (hyper-raft branch `land/mantle-general-4`:
`land/mantle-general-3`'s `ba33c1e` and four topics, on `main`'s `045cf3f`), with no local delta,
taken by `scripts/snapshot-hyper-raft.py`. That branch is the landing candidate of mantle's general changes to the shared
crates, which goes to hyper-raft `main` once hyper-raft's gates and every consumer's suite pass on
it. Over `ba33c1e` it brings hyper-liveness's sender's floor never below the stream's own proof,
from the poll that asked a liveness write to the poll that takes its durability (`65cdd84`;
hyper-raft docs/timing.md, "The floor's flush"): with the floor below that proof, a stream whose
flushes return at once (tmpfs, a device with power-loss protection) asked a flush at every poll
and sent 3 heartbeats to 10,683 slots skipped, and hyper-liveness's member tests grew until the
OOM killer took them (adacachy2, /tmp a tmpfs). It brings hyper-quic's patch Q35,
`Connection::handshake_progress` (`e0613f5`, hyper-quic VENDORED.md §51), which changes no
behaviour and which mantle does not call: it is the one commit past `main` that Kiln, Hex and
Shards vendor, so every consumer's suite can run on this candidate. And it brings two changes of
no behaviour: hyper-quic's `tests/allocs.rs` making a connection's first key update before it
counts (`31a9ea7`), where a key update drawn inside the counted transfer failed the count in about
2% of runs, and hyper-transport's comment that the judge's unread-stream arm comes before a
progress note (`b3a12ca`, the review of `844cad0`, cherry-picked as `17371e2`). Before it, every
crate was at `ba33c1e` (hyper-raft branch `land/mantle-general-3`, on `main`'s `045cf3f`), with no
local delta: `doors-wire-bytes` (`33733b5`) and `doors-step8`
(`c97fb1a`), below; hyper-tls's certificates decoded by brotli-decompressor 6.1.1 behind an unwind
boundary and upstream rustls's fixes for RUSTSEC-2026-0346 to -0349 (`294678c`, `772d5b2`);
hyper-log's full log always having room for the frame that frees a segment, and a sweep that does
not fit waiting while a frame of nothing frees the emptied segments (`8ac0ca5`, `22f6bbb`,
`97b2a21`); hyper-block's one open file a segment whatever the handles, discovery leaving a segment
its own file is adding to that add (`93dc717`, `774c52f`), a device issuer's depth held by the
kernel on one thread with its lanes sharing one descriptor of each file attached (`81467a8`,
`5b4680e`, `e887733`), Linux's thread count leaving out an exiting thread (`3452770`), and a file
on tmpfs opened buffered whatever is asked, as tmpfs makes every transfer through the page cache
(`ba33c1e`); and hyper-transport's owner's end waiting for the head of a reply it gave whole
(`31aa800`). Before it, every crate was at `f923297`, the same but for `ba33c1e`, then at
`294678c`, and before that at `c97fb1a` (hyper-raft branch `doors-step8`:
`doors-wire-bytes`'s `33733b5` and one commit), with no local delta. That commit changes
hyper-transport alone: no endpoint offers to take QUIC DATAGRAM frames (`Limits::transport` sets
`datagram_receive_buffer_size` to `None`, so no connection sends `max_datagram_frame_size`, RFC
9221 §3), where hyper-quic's default offered every peer 1,250,000 bytes of them; hyper-raft
docs/transport.md §4aq, its test `tests/datagrams.rs` (Lean doors step 8). Main takes it once Ada
says. `33733b5` was `main`'s `fb74b59` and one commit, which changes
hyper-rt alone: a batched socket counts its datagrams' payload bytes, sent and received, beside the
datagrams (`IoStats::sent_bytes`, `received_bytes`; hyper-raft docs/runtime.md), which
mantle-client's `Client::wire` sums over its sockets for `mantle bench doors`' native wire bytes.
Before it, every crate was at `fb74b59` (hyper-raft `main`), with no local delta, taken by
`scripts/snapshot-hyper-raft.py`. Past `3d6640f` it brings a member's catch-up read from the log's
file off the device and off its owner's thread (`8203466`; `Answers` passes on the two methods a
wrapping store must, `LogStore::catch_up` and `LogStore::entries_or_catch_up`), the run read after
a member's kept until the member reaches it (`f45fc56`), and a driver reading what its sockets took
before it judges its timers (`fb74b59`, hyper-raft docs/transport.md §4ap; mantle
docs/measurements/2026-11-06-node-test-holds.md). Before it, every crate was at `3d6640f`
(hyper-raft `main`: its merge of `lean-doors`, the cuts of mantle's ledger of its native door,
docs/research/71 "The sites", on main's `d5d5f8f`), with no local delta, taken by
`scripts/snapshot-hyper-raft.py`. The cuts: hyper-durable's `Owner::pairs` tells
an unchanged configuration in place (`4a6ec51`); hyper-quic's queue of a stream's pieces is never
shrunk below its first growth, and a sent packet's credit frames are recorded inline and the
streams its MAX_STREAM_DATA and STREAM_DATA_BLOCKED name by its space (`6406ee5`, hyper-quic
VENDORED.md §49); hyper-transport's `Endpoint::write_body_bytes` writes a body the owner holds as
`Bytes` without copying it (`7541647`, docs/transport.md §4ao); hyper-rt's receive buffers are made
unwritten, the kernel writing them on Linux (`4fcdabc`). Main past `0a182ae` came with them:
hyper-transport's kept stream of a pooled connection (`57842d3`, §4aj; mantle's credit is
`StreamCredit::Fixed`, which keeps none), the reply acknowledgement (`b5edd11`, §4ak,
`Limits::acknowledgement`) and forward erasure correction (`c0d6ca8`, §4al, `Limits::fec_window`;
hyper-quic §47, with `6e7d184`, `6c375d3`, `62f568d`, `efc16f4`), BBR's recovery window never less
than the latest round's delivery (`efc16f4`, hyper-quic §48), hyper-raft's candidacy as long as its
grants take (`831bfd9`, `d804110`) and a leader's commits counted by the quorum that made each first
(`d5d5f8f`, R-8), hyper-rt's Windows reactor issuing one AFD poll a socket in
flight (`392407f`, `53a28a0`, `aee1ab6`) and a Windows datagram socket not reporting an earlier
datagram's ICMP port-unreachable as a reset (`6b5f176`, `22d5037`), and hyper-measure's and
hyper-rt's test fixes (`178f4e3`, `a66e1c0`). The two `Limits` fields are mantle's to state
(`crates/protocol/src/bounds.rs`): `Acknowledgement::Decimated` and no erasure code keep its wire as
it was at `0a182ae`; a member opens on 88 bytes more for `d804110`'s grants
(`derive::MEMBER_BYTES`), and a turn's allocations fell with the cuts (`tests/turn_allocs.rs`). Before it, every crate was at `0a182ae` (hyper-raft branch
`progress-notes-reachable`: `c68032a` and the progress notes, branch `progress-notes`' `844cad0` on `liveness-reachable`, for mantle's step 2 of
its waits), with no local delta**, taken by `scripts/snapshot-hyper-raft.py` (Python 3.11 or
later). Its commit changes hyper-transport alone: `Endpoint::working`, an owner's statement that its
work for an exchange it serves advanced, sent at most one note a beat a connection on a lane of its
own, and an asker's exchange named in a note waited on through the window the note starts
(hyper-raft docs/transport.md §4am; research/77 D1). Before it, every crate was at `c68032a`
(hyper-raft branch `liveness-reachable`: `f5a7dbd` and one commit, made for mantle's roll test),
with no local delta, taken by `scripts/snapshot-hyper-raft.py`. Its commit changes hyper-liveness
alone: `Liveness::reachable`, which the node
calls as it keys a peer's path, judges a peer never heard from then rather than from the pair's
attach, and has a stream backed off while nothing came send at its next flush rather than its
backed-off slot; founders started tens of seconds apart on one CPU suspected each other at once
and a survivor took the suspicion of its range's next leader into the term (hyper-raft
docs/timing.md §3, item 10; mantle CI on linux-x86_64, macos-aarch64 and linux-aarch64 at one
CPU). Before it, every crate was at `f5a7dbd` (hyper-raft branch `owner-holds-body`: `f2ad9c2` and
one commit, made for mantle's fill test on windows-x86_64), with no local delta, taken by
`scripts/snapshot-hyper-raft.py`. Its commit changes hyper-transport alone:
an owner that read part of a body, its last read wanting more than had arrived, and then holds the
rest unread while its peer fills the window, is no longer judged against the peer; a node holding a
PutObject's body while the chunks it carries go durable refused the exchange `Stalled` on its own
side (hyper-raft docs/transport.md §4al; mantle CI run 40612231375). Before it, every crate was at
`f2ad9c2` (hyper-raft `main`, its CI run 40466352588 green), with no local delta, taken by
`scripts/snapshot-hyper-raft.py`: main's merge of kiln's
CP-T2.2 disk mode (`cp/t2.2-mode`) over `degraded-suspicion-main`, which holds `e65afff`'s changes
as main took them (`fbaaa6c`), and the rest of the 124 commits main holds that `e65afff` does not,
since `2b13b15`, the base the two share. Mantle's code meets five of them, each kept at its behavior
before: hyper-durable's `Settings::disk`, which `crates/range` states as `Disk::Flushed`, a leader
applying only what a majority's disks hold with its own among them (`Disk::Apart`, a leader
committing on its followers' disks while its own lags, is a durability decision mantle has not
taken); hyper-transport's `Limits::retry_bytes`, `None` in `mantle_protocol::bounds`, a Retry once
half the pending places are taken (RFC 9000 §8.1.2), the rule before it; hyper-transport's
`Event::Degrading` and `Event::Progressing` (`c6baec2`), a dialer told its peer's path may be gone
and back, which the node takes as hints its plane's suspicion already judges; and hyper-log's
`HardState::held` and `Waits::Told`, core step R-8's promise and a simulation's writer. R-8 itself
(`5626e0b`), commits on entries held in memory across failure fates, changes nothing without
`Config::fates`, which mantle leaves unset (hyper-raft `docs/raft.md` §3.7), and
`apply_unpersisted` stays off. Beneath them: hyper-log's growth slots zeroed ahead of their first
frame and a frame's confirmation made durable the cheaper way the device measured (`16e9db2`,
`088acc1`); hyper-quic's BBR paced at its model's rate, loss thresholds that learn a reordering
path (`e911aab`, `cd90e5b`, `cf9849b`) and a token log (`a14c9ab`); hyper-transport's connection
state charged as its host holds it (`64acd86`, `c37c267`) and an exchange waiting on its owner
holding no stream credit (`d4e8447`); hyper-liveness's stop announced, answered and suspected at
its arrival (`8cc669a`, `a706c92`, `4367f7a`); hyper-rt's Windows console close held for the
owner's orderly stop (`04bbbf1`); hyper-block's zeros written only where a file system journals
the allocation (`e05956a`); hyper-seal's content-keyed sealed files (`5a22c8c`); and hyper-tls's
Brotli window held to the certificate (`e4bca91`). hyper-transport now names aws-lc-rs itself (its
restart key, `restart.rs`), which resolves to the copy vendored above. Before it, every crate was at
`e65afff` (hyper-raft branch `degraded-suspicion`: `313ada8` and fifteen commits, made for mantle's
roll and objects tests on CI's busy runners), with no local delta, taken by
`scripts/snapshot-hyper-raft.py`: hyper-raft's `d97153c`, a leader its detectors leave no
quorum leading on and handing over only to a voter that can be elected without it, and a
pre-candidate left no quorum no longer asking, so a false suspicion between the two live voters of
a group with one voter stopped costs no leadership (mantle's roll: terms 5 to 9 in 1.7 s, and a
livelock of terms 3 to 8 in 100 ms, on two CPUs beside two busy loops), model-checked
(`Suspicion.tla`, `d0b2b5d`, `378ea46`) with a test that such a leader answers no read without a
quorum's acknowledgement (`0c38cbb`); hyper-transport's `83f2047` (main's `1912194`), a stream its
peer reads in steps never refused as unread while it moves, where the unread rule judged a stream
by its state at each judgement alone (mantle's busy-runner trace: a gateway refused its client
`Stalled` after a window in which it took 312 of the client's MAX_DATA frames, the client reading
about 1.2 MB/s throughout), and `b095920` (main's `4da10dd`), a request smaller than a datagram
given its peer's period to answer; hyper-liveness's `1d35ad7`, an orderly stop announced to its
peers (`Liveness::stop`), whose MTBF evidence counts no failure at its restart and which send the
stopped run nothing more, and `85ab02b`, a run that has taken none of this node's heartbeats
having its ask taken once it has; hyper-timing's `5bccc5a`, stored failure evidence taken whole
(`Exposure::of`); and docs and tests (`2f7de21`, `49e54aa`, `808d1bd`, `adc0549`, `55d80cf`,
`e65afff`). Before it, every crate was at `313ada8` (hyper-raft branch `backoff-announced`:
`dd764a0` and four commits, CI run 40139950654, green on all 21 jobs; the same code is on hyper-raft
`main` at `af01c51`), with no local delta: hyper-liveness's `1088cfa`, a
stream that backs off doubling the gap it states and a peer it has not heard expecting each
doubling, where `f647365`'s back-off doubled the gap unannounced, so each doubling arrived a whole
gap past the receiver's freshness point and a restarted node that heard no peer was suspected by
both live peers at every doubling (49 to 91 times a simulated world, 0 to 2 after);
hyper-quic's `834ea20`, the warm-up tests' endpoints seeded, which raced BBR's plateau against
their budget on fresh random draws (2 of 1,000 runs failed unseeded, 0 of 1,000 seeded); and
`60f66fc`, a test bound taken from main. Before it, every crate was at `dd764a0` (hyper-raft
branch `windows-udp-reset-next`: `628111e` and three branches merged for mantle, CI run
40113149609, green on all 21 jobs), with no local delta: hyper-liveness's `f647365`, a heartbeat stream to a peer from
which nothing has come doubling its interval from the floor to one every 3 s (RFC 8085 §3.1.3) with
its flush requests, and answering the peer's first heartbeat at once, where a node that knew no
peer's address heartbeated at its stream's floor with a flush each (9,957 heartbeats a peer and
2.79 s of CPU a minute on fedora; 27 and 0.01 s after); hyper-rt's and hyper-rt-transport's
`ab2eb0a` (main's `0802a05`), a listener binding its port before it shares it, so a second
listener on a held port is refused `AddrInUse`; hyper-rt's `b0dc7a6` and `2921b36`, a console's
close or a shutdown that a subscriber took held for the owner's orderly stop until it exits or
the OS's time-out ends it (5 s by default; logoff not held, which only services receive and which
would hold back the shutdown after it, the OS running one handler call at a time); and tests
(`b0f6193`, `4397d16`, `0dfc6b3`, `c4be5db`). Before it, every crate was at `628111e` (hyper-raft
branch `windows-udp-reset`: `2b13b15` and five commits for mantle, CI run 40093470660), with no
local delta: two of
hyper-quic's patches on hyper-raft `main` taken to the line mantle vendors, Q25 (main's
`326ddd7`: a probe of the path's MTU waits for the congestion window and the pacer) and Q26 (main's
`0427988`: a probe the window and the pacer can carry goes before new data), which end the silences
of about 1.4 s that mantle's Windows CI found at a cell's first puts, a probe the window could not
carry holding the data behind it; and three tests: hyper-rt's of a Windows socket reading on past a
UDP port-unreachable report (measured on both Windows runners: the report neither stops the socket
nor loses a datagram behind it), hyper-durable's of an owner's wait for its oldest write, and
hyper-rt-transport's peer sockets' step (main's `c004deb`). Before it, every crate was at `2b13b15`
(hyper-raft `main`, 2026-10-20, green on its six targets and translated by Rosetta 2 in CI run
39548637128), with no local delta: `1b85eaf` and elections by suspicion that stay live when a leader
hears none of its voters and stay put when every node's flushes stall together, which the node's
Windows CI, its simulation and its cluster suite found. A leader that steps down suspecting every
other voter hands its term to one of them all the same, a round and a draw after its step-down if it
does not trust a quorum again by then (it campaigns itself if it does), and orders no second heir
within an expected election of the last, where it named no heir and its followers, which heard it,
trusted it for good: on a starved Windows runner a leader suspected both its followers and the cell
stayed leaderless. A judge whose own flush proof is outstanding judges no peer by its proof until
its own completes, and then judges the point it held, once, at its own wait plus the margin, where
the nodes of a cell on one volume, whose journal stalls every flush at once, suspected one another
and elected again (Lifeguard's local health, applied to durability); while it waits it shows those
peers trusted, as before, and asks no write of its own to judge a passed point, its next completion
of any write serving, so the rule adds no I/O. A node whose links all fell silent before it had
measured any evidence judges its peers once its pool starves, where a leader deafened at its
election never suspected anyone. A stream whose next slot is already due as it sends asks at once
for the flush that proves it, where a deaf node's stream waited for a peer's heartbeat to poll it.
Before it, every crate was at `1b85eaf` (hyper-raft `main`, 2026-10-20, green on its six targets and
translated by Rosetta 2 in CI run 39477492892): `e4a1434` and a value the kernel gives in its own
ticks converted at the kernel's rate (`hw.tbfrequency`), which the node's macOS x86_64 CI found: a
process Rosetta 2 translates is shown a timebase of 1/1, while XNU stamps a received datagram in 24
MHz ticks, so every datagram's arrival was wrong, no link was judged and no cell of x86_64 `mantle
node` processes elected; a thread's CPU clock and `proc_pid_rusage` times were read the same way. A
native process reads what it did, to the bit. Before it, every crate was at `e4a1434` (hyper-raft
`main`, green on its six targets in CI run 39436648371, focal's and slates' suites passing against
it): `a72352c` and a shared UDP socket given port 0 bound before it shares, which the node's process
tests found: Linux's search for a free port left out those another socket of the same user shared,
so two `mantle node` processes on one host could be handed one port. Before that, every crate was at
`a72352c` (hyper-raft branch `sim-node-ready`): `26b5af8`, a test's hold that returned before its
shard entered it (`e50ad17`), and whole nodes on the simulation runtime, which the node's simulation
found: `SimRuntime::run_until`, a bounded run of a world that always has a next deadline;
hyper-datagram's `Fence` `Sync`, so a node's task is `Send`; and the plane's peers flushed in the
order of their ids, where a random hasher's order made two runs of one seed differ. Before that,
every crate was at `26b5af8` (hyper-raft branch `dial-outcome`, its CI run 39410706279): `ba0c87b`
and every dial's end told to its owner, which the node's redials found (a peer cut off through two
failed redials was never dialed again). Before that, every crate was at `ba0c87b` (hyper-raft
`main`, 2026-10-20, green on its six targets, CI run 39369678300): `6df7b3e` and a page read from
`GroupStore` counted at what its entries take in a message, which the node's replication found (a
catch-up append past the core's bound). Before that, every crate was at `6df7b3e` (2026-10-17), with
the transport stack the node's replication runs on; before that, at `80c1f82`, the revision focal
and slates took on their branches, so every consumer was on one (hyper-raft `docs/STATUS.md`, "What
remains", item 2). Before that, every crate was at `5633487` but `hyper-rt` and `hyper-block`, at
`ccdbbd8` (the loop's redesign, since on `main`). Mantle's changes to `hyper-rt`, `hyper-block` and
`hyper-measure` went upstream (hyper-raft `17920a7`, the runtime work merged with `main`, and
`55d923e`, residency epochs and segmented files), and every public function the earlier merged
snapshots held is in `5633487`. The crates' `rust-version` is 1.98.1, which mantle's toolchain
follows: 1.98.0 miscompiles trait-object vtables, leaving a slot null in safe code (hyper-raft
`50d1adb`, `crates/hyper-raft/tests/toolchain.rs` red on 1.98.0). `hyper-sim`, the deterministic
world and the block device hyper-block's simulated file now runs on (hyper-raft `a22a1fb`), is
vendored beside them; `hyper-block`'s `sim` feature takes it.

The crates mantle shares with focal and slates come from github.com/hyper-light/hyper-raft:
- each is a snapshot of one crate taken from a commit's objects (`git archive`), its manifest made
  self-contained: the workspace's package fields and dependency specs written in, its benches and
  `[[bench]]` tables and its `[lints]` left out but for the declaration of the cfgs its interleaving
  tests select (`loom`, `shuttle`, `fuzzing`), kept where any `cfg` or `cfg_attr` predicate of its
  sources names one at any depth, `publish = false` added, and an empty `[workspace]` appended; its
  source revision is in `SNAPSHOT`. A file under `benches/` that a
  test includes by `#[path]` stays, since `cargo fmt --all` follows the include:
  `hyper-liveness`'s `benches/support/world.rs`;
- each is a workspace root of its own, excluded from this workspace;
- hyper-raft's CI runs their suites on all six targets, and mantle's own suites run against the
  snapshot;
- a change is made in hyper-raft and taken here by a new snapshot, never edited in place;
- snapshots that depend on each other by path sit side by side here, as they do under
  hyper-raft's `crates/`, so those paths resolve: `hyper-raft` on `hyper-timing`; `hyper-log`
  on `hyper-block`, `hyper-seal` and `hyper-timing`, and for its own tests `hyper-measure`;
  `hyper-durable` on `hyper-raft`, `hyper-log`, `hyper-block`, `hyper-timing` and
  `hyper-liveness`, and for its own tests `hyper-sim`; `hyper-liveness` on `hyper-timing`;
  the transport stack (node.md §3): `hyper-rt-transport` on `hyper-rt`, `hyper-transport` and
  `hyper-datagram`; `hyper-transport` on `hyper-quic` and `hyper-timing`; `hyper-quic` on
  `hyper-tls`, which it names `rustls`; `hyper-swim` on `hyper-timing`; `hyper-transport`,
  `hyper-quic`, `hyper-tls` and `hyper-datagram` on aws-lc-rs, which resolves to the copy vendored
  above;
  `hyper-block` with its `sim` feature on `hyper-sim`, which is std only; `hyper-seal` on aws-lc-rs (and `hyper-block` with its `files` feature), which
  resolves to the copy vendored above through the root `Cargo.toml`'s patch, and for its own
  tests `hyper-measure`; and `hyper-rt` on no other snapshot, and for its own tests
  `hyper-measure`. A build resolves no dev-dependency of
  a crate outside the workspace, but `cargo fmt --all` loads the manifest of every path
  dependency, dev-dependencies included, so a snapshot's manifest leaves out a path
  dev-dependency on a crate not vendored here, with a comment naming it: `hyper-raft`'s and
  `hyper-durable`'s on `hyper-check`, and `hyper-liveness`'s on `hyper-swim`, `hyper-datagram` and
  `hyper-tokio`, whose tests hyper-raft's CI runs and mantle does not.

The local changes the earlier snapshots carried (`hyper-rt`'s cooperative service admission and
cancellation, per-poll context guards, checked channel slot and ticket layouts, TCP setup and accept
ownership, cold-owned native retirement, parks with one kick and lock-free channels; `hyper-block`'s
numbered submission and completion lanes, terminal ownership and retirement, resident reads and
residency epochs) are hyper-raft's now, at `17920a7` and `55d923e`.

### Local changes

`hyper-transport` carries the changes below, made here against the rule above for the stall and
the sizing they close, to go to hyper-raft's `main` as they stand (they are not there yet). Its
sources carry `mantle:` comments that point here; a snapshot taken before hyper-raft has them
applies them again. `hyper-log` and `hyper-durable` carry the counts in the table's first two rows,
on the same terms.

| Where | Change | Why |
|---|---|---|
| hyper-log `src/stats.rs` (`LogStats::confirmations`, `Tally::confirmations`), `src/owner/mod.rs` (`Completion::Confirm`), `src/owner/read.rs` | The confirmations written on their own are counted apart, each also counted in `flushes` or `durable_writes` as before. | A node that only serves reads flushed its log dozens of times a second, and its owner could not tell a lone frame's confirmation from a frame (mantle docs/design/node.md §6.2, "What flushes while a node serves only reads"). |
| hyper-durable `src/replica.rs` (`Writes::entries`) | Of the `Ready`s whose write held something, those that carried entries, a snapshot or proposals are counted apart, so a write of a hard state alone is `readies − entries`. | The same: an owner counts its groups' writes by what they carried (mantle `node::FlushCauses`). |
| `src/endpoint.rs` (`Core::admit_prefix`, `Core::reply`, `Core::prefix`, `Endpoint::reply`), `src/exchange.rs` (`Outgoing::message`), `tests/exchange.rs`, `ORIGIN.md` | A peer's exchange takes, with its request's head, the room its reply's prefix and head take at their longest, `frame_grant(max_head)`: both or neither, a request the budget cannot fund both for refused `Budget` as it arrives, which its asker hears at once. The room is the exchange's outgoing message until the reply fills it, asking the budget for nothing, so a reply within its class's bounds is never refused `Budget`; it is given back once the reply's head is written (`Core::flush`), or at the exchange's end however it ends (`Core::finish_exchange`: its owner's end or refusal, its stream reset or stopped, its connection lost). Tests: `a_reply_given_once_the_budget_is_full_is_sent`, red before it (the reply refused `Budget`), and `each_end_gives_a_peers_exchange_its_reply_room_back` (answered, with and without a body; refused and ended unanswered by its owner; its request reset and its reply stopped by its asker; its connection lost), each leaving the budget's messages where they were before the request. | A reply's head was reserved as the reply was given, after other exchanges' bodies, windows and frames could have filled the budget; an owner that took that refusal as the exchange's end left it open, its asker waiting a whole period on nothing (mantle's uploads given up `Lost(Stalled)`; docs/design/node.md §3.3). An owner whose budget counts every exchange's head both ways, as mantle's does (`bounds::messages`), holds no more than it states. |
| `src/endpoint.rs` (`Core::progress`, `Core::next_visit`, `Core::demand_above`, `Core::check_visits`, `Core::check_passed`), `src/tally.rs` (`slot`, `Table::listed_after`), `src/lane.rs` (`Unsent`), `src/exchange.rs` (`Exchange::active`), `tests/exchange.rs`, `benches/progress.rs`, `benches/lookup.rs`, `Cargo.toml` (`[[bench]]`), `docs/benchmarks.md` | A connection's exchanges a progress pass visits are kept in lists by what moves them: those queued for a stream, then one list for each rank of those holding a stream, in the order they were made. A pass takes the next exchange made across the lists, a binary search of each, and passes over a rank's list while its class may take no credit (`Core::allowed` is 0): such an exchange's visit writes nothing of its head or trailer and wakes no owner, so what the pass does is what it did, in the same order. The lanes' unsent bytes by rank are running sums (`lane::Unsent`), kept as frames are queued, written and dropped, where `Core::demand_above` scanned every queued frame. The oracle compares the lists and the sums with their folds, and checks each exchange passed over would have waited on credit; `exchanges_waiting_for_credit_are_passed_over_and_move_once_it_comes_back` drives that path (red with the check inverted). The benches, which the snapshot leaves out, are added back to measure it. | A call on an exchange is followed by a pass over its connection (`Endpoint::with_exchange`), and the pass visited every exchange waiting for credit, each visit settling the tally and scanning every queued lane frame: on a range leader at 12,000 small operations a second, its first shard was 94–96% busy and the pass's frames about half of it (mantle 2026-11-19). `benches/progress.rs`, 1,023 heads waiting: 17.83 µs a call before, 0.11 µs after; with 512 lane frames queued as well, 427.52 µs before, 0.11 µs after (`docs/benchmarks.md`). |
| `src/budget.rs` (`Fixed`), `benches/budget.rs`, `Cargo.toml` (`[[bench]]`), `docs/benchmarks.md` | `Fixed` keeps its spare buffers in TLSF's classes (Masmano, Ripoll, Crespo and Real, ECRTS 2004), sixty-fourths of a power of two, in slots linked by class, with a bit for each class that holds one (a power of two's classes one word); a reservation takes the last given back of its size's class if that holds the grant, else one of the first later class that holds any, which does, else the largest class's, grown. At most `max_spare` are kept, as before. Tests: `a_reservation_takes_a_spare_of_the_first_class_that_holds_it`, `spares_are_bounded_and_every_buffer_holds_its_grant`. | A reservation searched every spare buffer for the smallest that held its grant, twice when none did, and a node keeps one for each peer and client it may serve: `Fixed::reserve` was 6.7% of a range leader's first shard (mantle 2026-11-19). `benches/budget.rs`, a reservation and its release beside 4,096 spares: 2,953 ns before, 8.4 ns after; 3.3 ns before and 7.7 ns after beside one (`docs/benchmarks.md`). The constants' rows are in docs/design/constants.md. |
| `src/endpoint.rs`, `src/lib.rs` | `state_charge_for::<C>(streams_per_connection)`, which `state_charge` now calls: a connection's state charge under limits whose connections take that many streams, the one bound it depends on. | An owner sizing its budget before its limits are whole counts its connections' state at what the endpoint charges each (mantle's `bounds::connections_state`, docs/design/node.md §3.3). |
| `src/deadlines.rs` (`defer`, `first_wanted`, `due_wanted`), `src/endpoint.rs` (`Endpoint::place`), `benches/transmit.rs`, `docs/benchmarks.md`; and in `hyper-quic`, `src/connection/spaces.rs` (`SentPackets::holds_flights`, `Retransmits::is_empty`) and `src/connection/mod.rs` (`fill_step`) | A connection's deadline that moves later is recorded and its heap entry kept at its earlier key until it reaches the top, where it moves once; one that moves earlier moves at once, so no key is later than its deadline and the earliest is exact (the oracle's `check_deadlines`). An unchanged entry is left alone. In hyper-quic, whether a space holds flights reads its first packet, packets being held in number order, not a search; an empty retransmit set is checked before its sets are walked; a space with no keys is passed over before its packet's room is worked out. | A turn re-placed every touched connection, and a connection that sends moves its loss timer later every packet (RFC 9002 §6.2.1), so each turn sifted every such entry; the searches ran on every transmission. Mantle's node, 2,048 client connections: the heap's sifts 13.1% of the transmit side before, 2.2% after; a turn 434 µs to 389 µs, 532 ns to 477 ns a datagram (mantle branch transport-transmit, 18b17a31). |
| `src/endpoint.rs` (`Endpoint::working`, `Endpoint::read_body`, `Core::note_due`, `NotesOut::pending`, `flush_notes`), `src/tally.rs` (`Table::set_vouch`), `benches/vouch.rs`, `docs/benchmarks.md`, `tests/exchange.rs` | A vouch sets its exchange's mark and its connection's note pending, from the first vouch's instant, without a progress pass and without marking the exchange touched (as `Table::place` does); the connection is placed again only where the vouch moves its due time. A note clears the marks it names the same way. A body read where no body is arriving returns at once. Test: `an_exchange_vouched_for_many_times_between_notes_is_named_once_when_the_note_is_due`. | Each vouch ran a progress pass over its connection: mantle's node vouches for every held and parked request a beat, and under a load of 2,048 clients the passes' settles were 23% of its busiest thread (mantle branch vouch-cheap, 9ec03b56). A vouch 32-35 ns before, 2.2-3.0 ns after, beside 1 to 1,023 open exchanges. |

Verified on macOS aarch64 (2026-11-18): hyper-transport's unit tests and its `body_bytes`,
`certificates`, `datagrams`, `e2e`, `exchange`, `responses`, `room` and `tls` suites, run from the
snapshot with hyper-raft's lock file, and its clippy under hyper-raft's `clippy.toml`.

The AWS-LC workspace command above excludes these standalone shared-crate roots. It does
not run their runtime, device or allocation tests. Their suites must also be run against
the exact development sources, including `hyper-block/tests/issuer_allocs.rs`.

| Snapshot | Revision | Used by |
|---|---|---|
| `hyper-raft` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`: since `1b85eaf`, a leader that steps down hands over at once to a voter it trusts, and, trusting none, a round and a draw later unless it trusts a quorum again; each hand-over names the next heir over every voter, none within an expected election of the last; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`: since `80c1f82`, its swarm harness's campaign strategies and what each setting reaches for its CPU, measured (`469f1c2`, `e9b60d6`), tests only; before that, `80c1f82`: since `5633487`, a candidate counting its vote for itself once the hard state naming it is durable (I9, `14a014f`), so a sole voter campaigns once a term and leads after the write that holds its vote, waiting for it with nothing armed (`5189926`); `Raft::reads_held` public; before that, `5633487`: since `f358210`, an append behind a follower's commit taken past it (`3d82bba`), a leader trusting a member again taking what it sent it while suspected for lost (`b66dc9e`), its judged schedules under swarm configurations (S-6, `3b5ca96`), and the toolchain test of `50d1adb`; before that, `f358210`: since `5d68252`, the core's two cycles made loops, a candidate's campaign and its poll, and committing and leaving a joint configuration (#19, mantle `CLAUDE.md` §10), and hyper-check's round search reaching reads and lagging writes (#9); before that, `5d68252`, unchanged since `f311ab6`: since `0c4a793`, a leader's quorum patience beyond its election timeout before it checks a quorum heard it (`Raft::set_quorum_patience`, hyper-raft `docs/raft.md` §3.6), zero by default; since `857a2ae`, an owner may defer a Ready's commit that moved alone, so the Ready vouches for no commit and its answers state only the durable one (`RawNode::defer_commit`); since `df54729`, elections and commitment counted by the newest configuration a member's log states, committed or not (Diss §4.1; hyper-raft `83f193a`, `docs/raft.md` §3.4), where a configuration took effect as it was applied before; the log's precedence the only election rule, raft-rs's kept for the differential tests behind a feature no consumer enables (`raft-rs-precedence`); a dropped proposal naming its cause; a member of a later term answering a leader of an earlier one whatever the settings; the rule for what arrives ahead of a hole changed on a running member (`set_ahead`); and the fast track's holdings kept until a classic commit covers them (`docs/raft.md` §3.5). Before, since `df5f8ad`, R-3, slates' enhancements: R6 and R7, a lease its member's own timer kept, the R4, R5, R20 and R21 tests; the window to a member one rule, what its path carries over a repair (R16); what arrives ahead of a hole kept and acknowledged with the write that holds it (R17), and a member that lost it probed after a beat (S-4); a learner caught up in rounds before it is promoted (R13); every bound derived from what the owner states (`Limits::derive`, which `crates/range` states from a range's settings); a read of a new leader waiting for the entry it began its term with (S-4); a pre-candidate deaf to a leader it suspects; and the planted defects of `mutants`, a feature no consumer enables. Since `df5f8ad` the tests take hyper-check, not vendored here; hyper-raft `docs/raft.md` §3, `docs/sim.md` §14, `ORIGIN.md`) | `hyper-durable`; `crates/range` (proto, `Config`, its errors) |
| `hyper-timing` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`: since `1b85eaf`, `LinkEstimator::first_measure` and `LinkEstimator::arrivals_most_correlated`, which change nothing it did; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, unchanged since `80c1f82`; before that, `80c1f82`, unchanged since `5633487`; before that, `5633487`: since `f358210`, a link its correlation refuses waiting at its interval for a longer level (`43aec4a`); before that, `f358210`: since `5d68252`, slates' timing asks (#17): the owner's detection budget flooring a judged period, a verdict's margin searched and a false condemnation priced, a pair's fit following its latest evidence; before that, `5d68252`, unchanged since `df54729`: the election law's draw, which the core's timer takes since L-2, and the detectors' estimator and configurator; since `df5f8ad`, a path's samples fresh and of one endpoint, `G` never below the owner's clock resolution, and a histogram of nanoseconds; hyper-raft `docs/timing.md`) | `hyper-raft`, `hyper-durable`, `hyper-liveness` |
| `hyper-log` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`: since `6df7b3e`, a fetch taking a `Page`, its bytes and a part each entry is counted at beside its payload (`Log::fetch`, `Log::fetch_waking`, `GroupLog::fetch`; `ba0c87b`); before that, `6df7b3e`, unchanged since `80c1f82`; before that, `80c1f82`, unchanged since `5633487`; before that, `5633487`: since `f358210`, a group's handle keeping what its replica has yet to apply (`0f0fb86`, `aa82ada`), a group claimed again waiting for the writes its dropped handle left out (`9fd8ead`), the seal walking a group's release (`7e6b1bc`), the caller's code behind unwind boundaries (`1f01792`), and the log sealed by default as measured (`5633487`, hyper-raft `docs/seal.md` §11; plain `create` and `open` remain); before that, `f358210`: since `5d68252`, the owner's loop flat and no type holding a channel of itself, its public API unchanged and its equivalence files byte-identical (#18, mantle `CLAUDE.md` §10); before that, `5d68252`, unchanged since `f311ab6`: mantle's `crates/log` at `147f035` with its history, L-1; one owner thread answering by ticket, L-2; a group's handle, `GroupLog`, answering its replica's reads and cutting its updates into parts on the replica's thread, a blocking writer flushing its own frame, and a waking submitter that does not wait for its admission; since `63cb65d`, a write its handle sent behind a refused one refused too, `LogError::Behind`, and the handle's depth, `GroupLog::depth` and `has_room`; since `df5f8ad`, the log's statistics, `LogStats`; since `df54729`, format 4 and a sealed log, every record sealed and every frame, header and persist record under a MAC (hyper-seal; hyper-raft `docs/seal.md` §5), and the fast track's proposals kept until an update releases them (the `Released` record); since `857a2ae`, a `LogOpener` that claims a log's groups from any thread without the log's threads (`Log::opener`), a submission's options sent as one value, and a configuration derived from a node's and device's facts, refused typed rather than clamped (`Config::derive`, `Facts`, `LogError::Unfit`); since `b483a13`, the log's id on the log and its opener (`Log::id`, `LogOpener::id`), and a log laid out in its file's layout block rather than its transfer alignment, so a file opened buffered (or on a file system that refuses direct I/O) holds a log; and the file growing only as its owner admits (`Growth`, `Log::create_with`, `Log::open_with`), a refusal answered `Full` as the bound reached, never a fence; since `bf12297`, a frame's confirmation as one durable write (a FUA write on Linux where the device has FUA, `BlockFile::write_durable_at`), a slot the file grows by written whole with zeros first where that takes the journal's flush off every later frame (`BlockFile::fills_new_space`; Linux direct files), and `LogOpener::stats`; hyper-raft `docs/benchmarks.md`, "hyper-log: the flushes an append costs"; since `51dd939`, an open whose keys do not match the log's named `SealedWithoutKeys` or `UnsealedWithKeys`, not `Foreign`; hyper-log `ORIGIN.md`) | `hyper-durable` (`GroupStore`), `crates/range` (the log a member's group is claimed on), `crates/mantle` (`mantle bench log`) |
| `hyper-block` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`: since `e4a1434`, a thread's CPU time (`thread_cpu`) read at the kernel's rate, as hyper-rt's; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`: since `80c1f82`, AIO reads on overlayfs going again without `RWF_NOWAIT` and a refused submission failing its batch, and a buffered handle asking as it opens whether it reads from memory (`f6a276f`); before that, `80c1f82`, unchanged since `ccdbbd8`; before that, `ccdbbd8`, no local delta, the same files as `2430d9e`: since `5633487`, its tests' runtimes stating no latency objective (`73affc5`); before that, `5633487`: since `f358210`, mantle's issuer, resident reads, residency epochs and segmented files upstream (`17920a7`, `55d923e`), unwind boundaries (`a7d1f31`), and the simulated file over hyper-sim's block device behind `sim` (`a22a1fb`); before that, `f358210` merged with mantle's local issuer changes and resident reads (above): since `5d68252`, #7, #11 (several `Send` handles on one simulated device), #13, #14, and the simulated device opening a handle on a queue the handle brings (#19, mantle `CLAUDE.md` §10); before that, `5d68252`: mantle-disk's block, buf, commit, file, issuer, thread budget, scratch and simulated device at `147f035`, each with one owner, L-2; since `63cb65d`, a record kept whole, the block size a file's system reports, and a transfer of no bytes taken as none; since `df54729`, group commit learning exactly from batches of up to 2^29 − 1 submitters; since `857a2ae`, a submitter keeping several batches out, each numbered and its answer taken when the submitter needs it (`Issuer::attach_deep`, `Attached::submit`, `answer`, `try_answer`), and an issuer's inbox sized for every batch its submitters may have out (`Issuer::start_for`); since `b483a13`, batches of reads as well as writes, answered with their buffers filled (`Attached::submit_reads`), and a file's layout block, its transfer alignment when direct and the device's write unit when buffered (`BlockFile::layout_block`; hyper-raft `docs/benchmarks.md`, "hyper-block: a batch of reads through the issuer": a batch halves four device reads and costs sixteen times four cached ones); since `7755f43` and `5d68252`, a direct file's reads, writes and flush issued and reaped by the submitter itself through Linux's native AIO, with no thread between it and the device (`aio::AioFile`; refused, typed, over a buffered file and on every other OS; hyper-raft `docs/research/issuer-completions.md` §8 and `docs/benchmarks.md`, "hyper-block: a batch of reads through native AIO": four direct reads 3.0 times faster than one after another, four durable page writes 1.4 times faster than in place, no allocation); hyper-block `ORIGIN.md`) | `hyper-log`, `hyper-durable`, `crates/chunk`, `crates/disk` (measurement and calibration through its files, buffers and thread budget), `crates/range`, `crates/mantle` |
| `hyper-measure` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`: since `e4a1434`, `proc_pid_rusage` times converted at the kernel's rate, which a translated process read at 3/125 of `getrusage(2)`'s; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, unchanged since `80c1f82`; before that, `80c1f82`, unchanged since `5633487`; before that, `5633487`: since `f358210`, mantle's runtime work upstream (`17920a7`) and an open loop's Poisson arrivals with exact quantile intervals (`136587a`); before that, `f358210`, unchanged since `857a2ae`: Windows through windows-sys since `63cb65d`; since `df5f8ad`, a process's account from its operating system, `usage`, and the costs of many pieces of work, `cost`; since `df54729`, the account's user time held to its unit) | `hyper-log`'s tests and benchmarks; `crates/range`'s tests (a waker over a channel) |
| `hyper-durable` | `SNAPSHOT` (`628111e`: since `2b13b15`, its test of an owner's wait for its oldest write counting the write's flushed parts from before the drive hands it to the log (`tests/hyperlog.rs`), tests only; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`: since `6df7b3e`, a page read from `GroupStore` (`entries` and `visit`) the core's own, each entry counted at what it takes in a message, its stored bytes and `IN_MESSAGE_BEYOND_STORED` (`ba0c87b`; a catch-up append was past the core's bound and the transport refused it); before that, `6df7b3e`: since `80c1f82`, its simulation on hyper-sim's decision tape (`5a3a6f2`) and a bench of many groups on their members' logs (`551f43a`), tests and benches only; before that, `80c1f82`: since `5633487`, a refused write's messages leaving with the write made again only while the log still holds what they acknowledge (`31575af`), the candidate's vote counted once durable (`14a014f`), and its simulation holding every read a leader took to an answer once its group settles (`0fe9c7d`); before that, `5633487`: since `f358210`, a store keeping what the replica will read back, `LogStore::keep_after` and `kept_bytes` (`aa82ada`, `0f0fb86`), a quiet group writing the commit its members applied past (`3edd614`), repair by entries against an image (`6798688`), unwind boundaries (`a7d1f31`), and its schedules in hyper-sim's world (S-6, `b4fe3f2`); before that, `f358210`, unchanged since `f311ab6`: the durable shell, D-1, at hyper-raft `173437b`'s contract: `Replica` over a `LogStore` and a `StateMachine`, the commit fence on R-6, readies to the store's depth, one page applied a drive; since `df5f8ad`, priority and windows set through the replica, a write made again never starting the log past the state machine, compaction due by the thesis's rule (which asks a machine its image's bytes, `StateMachine::image_bytes`) and a leader waiting for a member behind; since `df54729`, what a member does with an append ahead of a hole set by the owner (`Replica::set_ahead`), and the fast track's proposals held beside the log until a write releases them (`LogStore::released`) with a displaced one given back to its proposer (`Output::displaced`); since `756bfaa`, the owner's quorum patience passed to its core (`Replica::set_quorum_patience`); since `857a2ae`, `GroupStore::claim` and `remove` taking a `Log` or a `LogOpener` (`LogGroups`), and a commit that moved alone written by no write of its own, riding the next write that carries something or the fence needs (hyper-raft `docs/durable.md` §4.1); hyper-raft `docs/durable.md`, `crates/hyper-durable/ORIGIN.md`) | `crates/range` (a range's replica since D-1) |
| `hyper-liveness` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`: since `1b85eaf`, a judge whose own flush proof is outstanding judges no peer by it until it completes, a stream whose next slot is already due as it sends asks at once for the flush that proves it, and a node whose links all fell silent before it had evidence judges its peers once its pool starves; a judge that waits shows its peers trusted and asks no write of its own; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`: since `80c1f82`, its simulation of links young when every device slows for good (`cebb97c`), tests only; before that, `80c1f82`: since `5633487`, a pair's report stating its own heartbeats' interval and next due time to the peer (`PairReport::sending`, `sending_due_ns`, `87e67a6`); before that, `5633487`: since `f358210`, a sender following its floor down once its flushes doubled (`d06a949`) and a refused link waiting for a longer level (`43aec4a`); before that, `f358210`: since `5d68252`, its cost bench following hyper-timing's (#17); before that, `5d68252`, unchanged since `df54729`: the node-pair liveness stream, L-3; since `df5f8ad`, a heartbeat sent no earlier than the one it echoes came; hyper-raft `docs/timing.md` §2.8) | `hyper-durable` (its owner's stream for groups that elect by suspicion); no mantle crate, as a range elects on ticks until the node carries the stream, which takes what `docs/design/node.md` §2.4 lists |
| `hyper-seal` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, unchanged since `80c1f82`; before that, `80c1f82`, unchanged since `5633487`; before that, `5633487`, unchanged since `f358210`; before that, `f358210`, unchanged since `f311ab6`, first taken at `857a2ae`; since `51dd939`, a whole file sealed by STREAM through a `BlockFile` (`sealed_file::{SealedWriter, SealedReader}`, hyper-raft `docs/seal.md` §4.1), behind its `files` feature, the only one that links `hyper-block`, which mantle does not enable yet: sealing at rest, a hierarchy of random keys wrapped with AES-256-KW, files sealed by STREAM, the shared log sealed record by record under a key per writer session, keys to another machine by ML-KEM-1024 and names keyed per tenant, on aws-lc-rs; hyper-raft `docs/seal.md`) | `hyper-log` (a sealed log's records, session keys and framing MACs); no mantle crate seals yet |
| `hyper-rt` | `SNAPSHOT` (`628111e`: since `2b13b15`, a test of a Windows socket reading on past a UDP port-unreachable report (`tests/udp_astray_reads_on.rs`), tests only; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`: since `e4a1434`, a datagram's kernel stamp carried into the process's ticks at the kernel's rate before its age is taken, and a thread's CPU time read at the kernel's rate (`machine::clock::MachTicks`), where a process Rosetta 2 translates was given garbage arrivals and 3/125 of its threads' run time (`tests/udp_kernel_stamps.rs`); before that, `e4a1434`: since `a72352c`, a shared UDP socket given port 0 bound before it shares, so the kernel gives it a port no socket holds (`UdpSocket::bind_shared`; Linux's search skipped no port another socket of the user shared, `e4a1434`); before that, `a72352c`: since `26b5af8`, `SimRuntime::run_until`, a simulated world run to a deadline, as a world of nodes, which always has a next deadline, needs (`a72352c`), and its shutdown-drain admission test's hold waiting until its shard is inside it (`e50ad17`); before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`: since `80c1f82`, an idle turn leaving the driver to the park that follows (`6be6c2d`); a timed wait ending at its timeout, the OS's coalescing held to the shard's tick (`169f53d`; on Windows through a high-resolution timer, `70f8eb3`); a receive call that fills fewer places than it offers ending the drain (`77ed625`); each connection receiving on a socket of its own, and a socket's account read at an epoch's doublings (`b6e0101`, `73ca2b6`, `f91f35d`); a kernel stamp carried by the least offset it may have been taken on (`9d65509`); MTU discovery up to the path's MTU (`c7b7ea1`); a task reading its shard's counters (`0d49841`); before that, `80c1f82`: since `ccdbbd8`, the wake probe keeping a wake slower than its round's share, the round waiting for its answer within the probe's span (`492ef97`), and its overrun and blocked-poll tests' deadlines (`c9894d3`, `ea94289`); before that, `ccdbbd8`, no local delta: since `2430d9e`, the loop's redesign for the costs a consumer pays (hyper-raft `docs/research/event-loop.md`): a task asking whether its shard is wanted and yielding only then (`ShardContext::wanted`, `futures::maybe_yield`), the loop reading no inbox that nothing filled (`13702f2`), what a switch reads packed into the desk's first two lines and the loop's first (`71992ae`), and a long poll one held past twice the quantum since its shard let it go on (`e99834b`); its due-timer test bounded by a reading after the sleep armed (`b2b9a56`); before that, `2430d9e`: since `5633487`, a task that wakes itself listed apart until the loop next reads its inputs, a step's handoffs reading them again once its polls spend the budget, and that budget honouring a consumer's latency objective (`RuntimeConfig::latency_objective_ns`, `73affc5`); then `rt-2x`, the loop's own cost cut where it is the work (hyper-raft `docs/benchmarks.md`, "rt-2x"): a poll inlined into its batch, reading its task's slot once, with no divide, idle look or thread-local lookup (`0ff646b`, `366d8c7`, `3a42b5b`), a channel's hop without a claim on the head or a second fence (`629178c`), a task with no parent listed at its spawn and reaped by its join (`c5adcbc`), no per-task watchdog counts (`4dc0377`), a step copying no counters and asking its driver once (`295d5df`), a task spawned on its own shard in a frame the shard recycles (`ec1b27b`), a wake from another thread moving one line (`bfa5875`), the idle spin reading the counter (`8565bce`), and a task counted spawned at its admission (`2430d9e`); before that, `5633487`: since `f358210`, mantle's runtime work upstream (`17920a7`), a step's long polls judged at one reading at its end (`4cbea75`), the wake estimate outlasting a burst (`627c176`), a poll's end the step's reading carried by the counter (`9959b40`), an empty receive reading only the channel's own lines (`8deaf9b`), named futures for the combinators (`88db378`, `pin-project-lite` by the owner's approval, `bd7407f`), and every task behind the shard's unwind boundary (`0784bb2`); before that, `f358210` merged with mantle's local changes (above): since `3794819`, #12, every way of running a shard delivering readiness and losing the driver ending it, and #15; before that, `3794819`, first taken at `b483a13`; since `3794819` (hyper-raft #10), a blocking job gives its share back before its result is delivered, so a task that sees the job end can submit again under the same share and is never refused for a slot the pool is still about to free (hyper-raft `docs/runtime.md`); before that a wake that marks its word's summary itself when it reads it clear, so no wake waits on another waker's progress; with a shard that polls its driver only while a readiness wait is registered and spins with a driver poll once a quantum, so a thread's round trip to a task costs 0.54 µs at the median, not 12.3 (hyper-raft `docs/benchmarks.md`, "hyper-rt: a thread's round trip to a task"): the shared thread-per-core runtime, slates' runtime at slates `6b9ce5c` designed onward for the three consumers: shards with arena tasks and `Copy` wakers, a hierarchical timing wheel, a readiness driver per OS behind one seam (kqueue, epoll, IOCP with AFD), sockets, signals, stdio, synchronization without shared ownership, the machine's calibration, and a deterministic simulation driver; its loom dependency, which builds only under `--cfg loom` for hyper-raft's own interleaving runs, is left out, and the workspace's declaration of that cfg kept as the manifest's one lint; hyper-raft `docs/runtime.md`, `crates/hyper-rt/ORIGIN.md`) | `crates/engine` (a range replica's engine on one shard, step E2, docs/design/engine-structure.md §2) |
| `hyper-sim` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`: since `80c1f82`, a message lost to the capacity taking its arrival with it, and the world playing a decision tape (`5a3a6f2`, `b9cdc19`); before that, `80c1f82`, unchanged since `5633487`; before that, `5633487`, first taken: the deterministic world, S-1, and the block device, S-3, `a22a1fb`: a seeded generator with a named stream per source, virtual time, one event queue, the trace that replays a run, and the device's lost and misdirected writes and crash states; std only; hyper-raft `docs/sim.md`) | `hyper-block` with `sim`: every mantle suite that runs the simulated file |
| `hyper-rt-transport` | `SNAPSHOT` (`628111e`: since `2b13b15`, peer sockets' step alternating which driver it asks first (main's `c004deb`, `tests/peer_sockets.rs`), tests only; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`: since `26b5af8`, a plane's receive a `Send` future, so a node's task runs on the simulation runtime, with a test of whole nodes on it (`a72352c`); before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, first taken: hyper-transport's endpoint and hyper-datagram's plane driven on a hyper-rt shard, with hyper-tokio's driver API, each connection receiving on a socket of its own; hyper-raft `docs/runtime.md` §14) | the node's transport (node.md §3) |
| `hyper-transport` | `SNAPSHOT` and the local changes above (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`: since `ba0c87b`, a dial's end told to its owner even when it fails as the last one did (`Core::dial_failed`; found by mantle, whose node redials only on `Closed` and `Unreachable`); before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, first taken: focal-wire's core (T-1) with slates' credit law, sans-io: exchanges of a request and its reply, ordered lanes, classes with a credit reserve, typed refusals at every bound; hyper-raft `docs/transport.md`) | `hyper-rt-transport` |
| `hyper-quic` | `SNAPSHOT` (`628111e`: since `2b13b15`, patches Q25 and Q26 (main's `326ddd7` and `0427988`): a probe of the path's MTU waits for the congestion window and the pacer, and one they can carry goes before new data (`VENDORED.md`); before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, first taken: quinn-proto 0.11.18 conformed to the rules, no panic in shipped code and no `Arc` or `Mutex`, its path MTU searched up to what the kernel holds; `VENDORED.md`) | `hyper-transport` |
| `hyper-tls` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, first taken: rustls 0.23.45 on AWS-LC, conformed to the rules, configs lent per call; `VENDORED.md`) | `hyper-quic` |
| `hyper-datagram` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`: since `26b5af8`, `Fence` `Sync`, and a plane's peers kept and flushed in the order of their ids (`a72352c`); before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, first taken: the sealed datagram plane after slates' seal and node.md §3.4, AES-256-GCM under keys from the QUIC connection's TLS exporter, an epoch a connection, RFC 4303's replay window) | `hyper-rt-transport` |
| `hyper-swim` | `SNAPSHOT` (`628111e`, unchanged since `2b13b15`; before that, `2b13b15`, unchanged since `1b85eaf`; before that, `1b85eaf`, unchanged since `e4a1434`; before that, `e4a1434`, unchanged since `a72352c`; before that, `a72352c`, unchanged since `26b5af8`; before that, `26b5af8`, unchanged since `ba0c87b`; before that, `ba0c87b`, unchanged since `6df7b3e`; before that, `6df7b3e`, first taken: slates' detector with focal's extensions, each pair an NFD-E detector configured from measurement, its view bounded by placement; hyper-raft `docs/timing.md` §2.7) | membership on the datagram plane (node.md §3.5) |

mantle's `crates/log` and the parts of `crates/disk` that hyper-block took were deleted when
mantle moved onto these (research/32 §5.3); identification, calibration, measurement and the
benchmark rounds stay in `crates/disk`.
