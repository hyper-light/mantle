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

The crates mantle shares with focal and slates come from github.com/hyper-light/hyper-raft:
- each is a snapshot of one crate taken from a commit's objects (`git archive`), its manifest made
  self-contained: the workspace's package fields and dependency specs written in, its benches and
  `[[bench]]` tables and its `[lints]` left out, `publish = false` added, and an empty
  `[workspace]` appended; its source revision is in `SNAPSHOT`. A file under `benches/` that a
  test includes by `#[path]` stays, since `cargo fmt --all` follows the include:
  `hyper-liveness`'s `benches/support/world.rs`;
- each is a workspace root of its own, excluded from this workspace;
- hyper-raft's CI runs their suites on all six targets, and mantle's own suites run against the
  snapshot;
- a change is made in hyper-raft and taken here by a new snapshot, never edited in place;
- snapshots that depend on each other by path sit side by side here, as they do under
  hyper-raft's `crates/`, so those paths resolve: `hyper-raft` on `hyper-timing`; `hyper-log`
  on `hyper-block` and, for its own tests, `hyper-measure`; `hyper-durable` on `hyper-raft`,
  `hyper-log`, `hyper-block`, `hyper-timing` and `hyper-liveness`; and `hyper-liveness` on
  `hyper-timing`. A build resolves no dev-dependency of
  a crate outside the workspace, but `cargo fmt --all` loads the manifest of every path
  dependency, dev-dependencies included, so a snapshot's manifest leaves out a path
  dev-dependency on a crate not vendored here, with a comment naming it: `hyper-timing`'s on
  `hyper-sim`, and `hyper-liveness`'s on `hyper-swim`, `hyper-datagram`, `hyper-tokio` and
  `hyper-sim`, whose tests hyper-raft's CI runs and mantle does not.

| Snapshot | Revision | Used by |
|---|---|---|
| `hyper-raft` | `SNAPSHOT` (`df5f8ad`, hyper-raft branch `mantle-d1` from `687244f`: since `dce1daa`, readies taken ahead of their persistence, R-4; repair by the lost entries, R-5; the durable commit in a member's answers and the apply pause, R-6; a marked member's election, R-7; elections by suspicion beside the tick path, L-2; the `Ready` read in place; a new draw at every arming of the election timer; a change's record read in place, `wire::changes_stated`; and since `687244f`, found by D-1, a campaign superseding its own vote requests still waiting and a spare message queue kept for each ready in flight; hyper-raft `docs/raft.md` §3, `ORIGIN.md`) | `hyper-durable`; `crates/range` (proto, `Config`, its errors) |
| `hyper-timing` | `SNAPSHOT` (`df5f8ad`: the election law's draw, which the core's timer takes since L-2, and the detectors' estimator and configurator; hyper-raft `docs/timing.md`) | `hyper-raft`, `hyper-durable`, `hyper-liveness` |
| `hyper-log` | `SNAPSHOT` (`df5f8ad`: mantle's `crates/log` at `147f035` with its history, L-1; one owner thread answering by ticket, L-2; a group's handle, `GroupLog`, answering its replica's reads and cutting its updates into parts on the replica's thread, a blocking writer flushing its own frame, and a waking submitter that does not wait for its admission; since `63cb65d`, a write its handle sent behind a refused one refused too, `LogError::Behind`, and the handle's depth, `GroupLog::depth` and `has_room`; hyper-log `ORIGIN.md`) | `hyper-durable` (`GroupStore`), `crates/range` (the log a member's group is claimed on), `crates/mantle` (`mantle bench log`) |
| `hyper-block` | `SNAPSHOT` (`df5f8ad`: mantle-disk's block, buf, commit, file, issuer, thread budget, scratch and simulated device at `147f035`, each with one owner, L-2; since `63cb65d`, a record kept whole, the block size a file's system reports, and a transfer of no bytes taken as none; hyper-block `ORIGIN.md`) | `hyper-log`, `hyper-durable`, `crates/chunk`, `crates/disk` (measurement and calibration through its files, buffers and thread budget), `crates/range`, `crates/mantle` |
| `hyper-measure` | `SNAPSHOT` (`df5f8ad`: Windows through windows-sys since `63cb65d`) | `hyper-log`'s tests and benchmarks; `crates/range`'s tests (a waker over a channel) |
| `hyper-durable` | `SNAPSHOT` (`df5f8ad`: the durable shell, D-1, at hyper-raft `173437b`'s contract: `Replica` over a `LogStore` and a `StateMachine`, the commit fence on R-6, readies to the store's depth, one page applied a drive; hyper-raft `docs/durable.md`, `crates/hyper-durable/ORIGIN.md`) | `crates/range` (a range's replica since D-1) |
| `hyper-liveness` | `SNAPSHOT` (`df5f8ad`: the node-pair liveness stream, L-3; hyper-raft `docs/timing.md` §2.8) | `hyper-durable` (its owner's stream for groups that elect by suspicion); no mantle crate, as a range elects on ticks until the node carries the stream (`docs/design/node.md` §3.4) |

mantle's `crates/log` and the parts of `crates/disk` that hyper-block took were deleted when
mantle moved onto these (research/32 §5.3); identification, calibration, measurement and the
benchmark rounds stay in `crates/disk`.
