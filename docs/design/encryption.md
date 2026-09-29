# Encryption at rest

Status: design, 2026-09-29. Sources: docs/research/20 (server-side encryption, cited as
"20 §x"), docs/research/14 (AWS-LC, cited as "14 §x"); docs/design/crypto.md for the library
and its no-panic boundary.

mantle encrypts every object's data at rest. This record states what it encrypts, under which
keys, how the bytes are sealed, and what S3's encryption headers mean against that.

## 1. What is encrypted

**Decision: every object's data is sealed with AES-256-GCM before it is stored, whatever the
request asked for.** S3 has encrypted every new object since 5 January 2023, with SSE-S3 as
"the new base level of encryption", which a bucket cannot turn off (20 §1.3). An object that
answers `x-amz-server-side-encryption: AES256` must be stored under AES-256, not labelled so:
the header is a statement about the bytes on disk.

- **Data, not metadata.** "Server-side encryption encrypts only the object data, not the object
  metadata" (20 §1.5). Keys, user metadata, tags and checksums stay in the metadata service as
  they are.
- **SSE-S3** is the default and the only mode a bucket's default configuration can name.
- **SSE-C** encrypts under a key the client sends with every request, where the bucket allows
  it. S3 blocks SSE-C on new general purpose buckets since April 2026, answering a write with
  `403 AccessDenied` (20 §3.6); mantle blocks it the same way until PutBucketEncryption unblocks
  it.
- **SSE-KMS and DSSE-KMS** name keys in a key management service. mantle has none, so it answers
  them `501 NotImplemented`, as it answers every S3 feature it does not serve, rather than
  claim a key it cannot hold.

## 2. Keys

**Decision: each file is sealed under its own random 256-bit data key, stored wrapped in the
file's header row; the key that wraps it is the node's root key for SSE-S3 and the customer's
key for SSE-C.** This is envelope encryption as AWS describes S3, KMS and its Encryption SDK
doing it: "Each object is encrypted with a unique key. As an additional safeguard, SSE-S3
encrypts the key itself with a root key that it regularly rotates" (20 §1.5, §8.5).

- **A key per file.** A file, whether a PUT's object or an upload's part, is written once, whole
  (metadata.md §1). Its data key therefore seals exactly one sequence of bytes, and the
  deterministic nonce construction of SP 800-38D §8.2.1 applies: a nonce field that counts
  within one key never repeats under that key, where random 96-bit nonces would bound a key to
  2^32 seals (§8.3; 20 §8.1). A part uploaded again is a new file under a new key, so no two
  plaintexts ever meet under one key and one nonce, the failure that "an adversary ... then
  could easily construct a ciphertext forgery" from (SP 800-38D App. A).
- **Wrapped with AES key wrap.** A data key is wrapped with AES-256 KW (SP 800-38F; RFC 3394)
  into 40 bytes. KW is approved for protecting keys, deterministic, and has no limit on how many
  keys one wrapping key wraps (SP 800-38F §5.4; 20 §8.3). "The same plaintext key should not be
  encrypted twice under the same key-wrapping key" (NIST IR 8459 §9): random 256-bit data keys
  meet with probability about n²/2^257. The wrapping key is AES-256, since a 256-bit key
  "wrapped using AES-128 ... is reduced to 128 bits" (SP 800-57 §5.6.2).
- **SSE-C needs no stored hash.** S3 keeps "a randomly salted Hash-based Message Authentication
  Code (HMAC) value of the encryption key to validate future requests" (20 §3.4). mantle keeps
  the data key wrapped under the customer's key instead: unwrapping with any other key fails
  KW's integrity check, whose forgery probability is 1 in 2^64 (SP 800-38F App. A.3), and that
  failure is S3's `403 AccessDenied`, "must provide the correct secret key" (20 §3.5). Neither
  the customer's key nor anything derived from it alone is stored.
- **The root key has generations.** A wrapped key names the generation that wrapped it. New files
  take the current one; a background pass rewraps older files' data keys under it, changing 40
  bytes of each header and no data, as S3's UpdateObjectEncryption changes an object's key
  "without any data movement" (20 §1.6). A key-wrapping key's originator-usage period is at most
  two years (SP 800-57 Pt 1 Table 1; 20 §8.2), and "a wrapping operation shall not be performed
  using a key-wrapping key whose originator-usage period has expired" (§5.3.6); mantle starts a
  new generation when the current one is two years old.
- **Where the root key lives.** The root key is never on a data device: a node reads it from the
  key file its configuration names, 32 bytes created at initialization with owner-only
  permissions, or from an external key service through the same two calls, wrap and unwrap. A
  gateway holds data keys only while it seals or opens their file.
- **Keys are generated from the operating system.** Data keys and the root key are read with the
  `getrandom` crate, which returns a typed error where AWS-LC's generator would abort the
  process (crypto.md §2).

## 3. Sealing

**Decision: a file's bytes are sealed in 64 KiB segments, each an AES-256-GCM seal under the
file's data key with the segment's index as its nonce and the last segment marked, and the
file's ID as additional data.**

- **Segments of 64 KiB,** the chunk store's checksum block (chunk-store.md §3.1, after GFS
  [GGL03 §5.2]), so a byte range is opened segment by segment and a read touches the segments
  its range covers. Each adds a 16-byte tag: 0.024% of the bytes stored. The segment is far below
  GCM's limit of 2^39 − 256 bits a seal (SP 800-38D §5.2.1.1).
- **Nonces count.** The 96-bit nonce is the segment's index in its low 64 bits, with the top bit
  of the last segment's index set, and 32 bits of zero before it: SP 800-38D §8.2.1's fixed field
  and invocation field, one key per file making the fixed field constant. Marking the last
  segment is the STREAM construction of Hoang, Reyhanitabar, Rogaway and Vizár ("Online
  Authenticated-Encryption and its Nonce-Reuse Misuse-Resistance", CRYPTO 2015): a segment opens
  only at its own index, and a file cut short, reordered or extended fails to open.
- **The file's ID is additional data,** binding every segment to the file whose header holds its
  key.
- **Where.** The gateway seals before erasure coding and opens after decoding. Storage nodes and
  metadata ranges hold only ciphertext and wrapped keys. The chunk store's CRC-32C still checks
  every stored byte, as CLAUDE.md §6 requires of every record; GCM's tag authenticates the
  plaintext end to end.
- **Sizes.** A segment of `n` plaintext bytes is `n + 16` stored bytes, so a file's stored length
  and a byte's stored offset follow from its plaintext length and offset alone.
- **Cost.** Sealing runs at about 8 GB/s and opening at 8.4 GB/s on one core, a ninth of the
  MD5 an upload pays for its ETag, and a file's key 1.8 µs to make, wrap and unwrap
  (docs/measurements/2026-09-29-sealing-at-rest.md). `crates/s3/src/seal.rs` holds the keys,
  the wrap and the segments; RFC 3394's wrap vector and GCM's test cases 13 and 14 check them.
- **Not GCM-SIV.** AES-GCM-SIV survives a repeated nonce (RFC 8452), but it takes two passes and
  cannot stream, and it is not a NIST mode (20 §8.4). Counting nonces under one key per file
  never repeats one.

## 4. What the S3 headers mean here

- **SSE-S3** (`AES256`) is every object's state: responses carry `x-amz-server-side-encryption:
  AES256` on the operations S3 lists (20 §1.2), and its ETag is the MD5 of the plaintext, as S3's
  is (20 §5.1).
- **SSE-C** requires TLS: "Amazon S3 rejects any requests made over HTTP when using SSE-C" (20
  §3.3). The key is used for the request and dropped. An SSE-C object's ETag is not the MD5 of
  its data (20 §5.1); mantle's is the MD5 of its ciphertext.
- **The bucket's configuration** is S3's `ServerSideEncryptionConfiguration`, with SSE-C blocked
  by default and SSE-S3 the only default algorithm (s3-protocol.md §13).
