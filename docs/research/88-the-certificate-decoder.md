# 88. The certificate decoder: CVE-2026-111497, RUSTSEC-2026-0348 and a header's allocation

Status: research note, 2026-11-12. For the owner's decision on certificate compression's decoder
(node.md §3.6, "certificate compression's decoder"; Lean doors step 8). Sources [1] to [8]; [1] to
[3] are web pages read through a search's summaries, [4] to [8] were read on the box (crate
sources from the registry, the advisory database cargo deny fetched), each quoted where read.
Measurements are hyper-raft branch doors-step8-2's tests, run on the box.

## The question

Every node's QUIC server asks its peer for a client certificate and offers to take it compressed
(RFC 8879), with Brotli and zlib. So a node decodes what a stranger sends before the stranger
has authenticated. Google's Brotli releases of 2026-10-21 fix an out-of-bounds write in their C
decoder. mantle links the Rust port instead. What does the advisory mean for that decoder, what
else is open in the same path, and what does a peer's stream cost a node?

## Sources

[1] Red Hat, CVE-2026-111497, https://access.redhat.com/security/cve/CVE-2026-111497, as a search
summarised it: an out-of-bounds write in the block-type switch handling in `c/dec/decode.c`,
reached by a crafted Brotli stream, rated Important (8.1); Google Brotli 1.0.3 through 1.2.0
affected (Red Hat Bugzilla 2559470).

[2] Corgea, CVE-2026-111497, https://corgea.com/advisories/vulnerabilities/CVE-2026-111497, as a
search summarised it: fixed in Brotli 1.0.10, 1.1.1 and 1.2.1.

[3] CSO Online, https://www.csoonline.com/article/4241027/node2shell-puts-microsoft-and-node-js-on-brotli-patch-duty.html,
as a search summarised it: the weakness pairs an unchecked return value (CWE-252) with an
out-of-bounds write (CWE-787), the decoder's fast path going on with invalid state; Node.js fixed
it in v24.22.1. The box's Node is v24.21.0, its bundled Brotli `process.versions.brotli` 1.2.0, and
its `/usr/bin/brotli` 1.2.0: both affected.

[4] crates.io's index record for `brotli-decompressor`, read from the box's registry cache: 6.1.1
published `2026-10-21T08:45:48Z`; 6.2.0 on 10-25, 6.3.0 on 11-06, 6.3.1 on 11-09. For `brotli`:
9.0.0 (2026-09-02) depends on `brotli-decompressor` `~6.0`, 9.1.0 and 9.1.1 on `>=6.2, <7`, and
8.0.4 on `~5.0`. No `brotli` release takes 6.1.1.

[5] `brotli-decompressor` 6.1.1's source (`.cargo_vcs_info.json`: `fdfccff3`), against 6.0.1 and
5.0.3. `lib.rs`: `#![cfg_attr(not(any(feature="unsafe", feature="ffi-api")), forbid(unsafe_code))]`.
`decode.rs` `DecodeBlockTypeAndLength`: `let type_tree_offset = tree_type as usize *
huffman::BROTLI_HUFFMAN_MAX_SIZE_258 as usize; let len_tree_offset = tree_type as usize *
huffman::BROTLI_HUFFMAN_MAX_SIZE_26 as usize;`, where 6.0.1 and 5.0.3 read both trees at
`BROTLI_HUFFMAN_MAX_TABLE_SIZE` (1,080) within allocations of `3 * BROTLI_HUFFMAN_MAX_TABLE_SIZE`.
Its ring buffer "starts small and, whenever a metablock needs more room, grows to the smallest
power of two that holds everything decoded so far plus that metablock, up to the window size".
`BrotliEnsureRingBuffer`: "Nothing here can panic, and on failure the decoder keeps its current ring
buffer untouched, so the state stays consistent and droppable." `state.rs`: `BrotliState::new` and
`new_with_custom_dictionary`, which `BrotliDecompress` takes, set `large_window = true`;
`new_strict` sets it false.

[6] RUSTSEC-2026-0348, https://rustsec.org/advisories/RUSTSEC-2026-0348, from the advisory
database cargo deny 0.20.2 fetched: "The built-in Brotli certificate decompressor (enabled by the
non-default `brotli` crate feature) bounded the size of the decompressed output, but accepted a
peer-selected history window. Brotli's large-window extension was enabled, so a five-byte header
could cause an allocation of 1 GiB. … No trusted certificate is required by the attacker. Affected
configurations are: … servers that enable the `brotli` feature and request client certificates."
Solution: rustls 0.23.46 (rustls/rustls#3297).

[7] rustls 0.23.46's `src/compress.rs`, against 0.23.45 (hyper-tls's base): the Brotli decoder is
`BrotliState::new_strict` driven by `BrotliDecompressStream` straight into the output, `Ok` only
for `ResultSuccess if available_out == 0 && available_in == 0`; zlib's takes `Inflate` and checks
`total_in() == input.len()`; new tests `test_brotli_rejects_large_window` and
`test_decompress_trailing_data`. Its decoder is still `brotli-decompressor` 5.

[8] RFC 7932, https://www.rfc-editor.org/rfc/rfc7932: §9.1, WBITS 10 to 24, the window
`(1 << WBITS) - 16`; §9.2, a metablock's MLEN from MNIBBLES 4 to 6 nibbles, so up to 2^24 bytes,
declared before its data; §7.3, a literal context map of 64 contexts for each of NBLTYPESL block
types, at most 256. The large window is not RFC 7932. RFC 8879 §3 names RFC 7932's Brotli.

## What follows

- The C decoder's bug [1] is not in the code mantle links. The Rust port is safe Rust [5], so a
  logic bug of the same kind there is a checked index's panic, and nothing in hyper-tls,
  hyper-quic or hyper-transport caught an unwind: one hostile client could take a node's process
  down. 6.1.1, published the day of Google's fix [4], changes the same function's layout to the C
  decoder's [5]. The owner chose it ("Upgrade the decoder"); it is pinned (`=6.1.1`) and called
  directly, the encoder staying at `brotli` 8.0.4 since no `brotli` release takes 6.1.1 [4].
- hyper-tls conforms rustls 0.23.45, so RUSTSEC-2026-0348 [6] is open in it as well: the
  decoder took the large window. It takes upstream's fix [7].
- Strict decoding leaves a header's own claim. Measured (hyper-tls
  `tests/certificate_decoding.rs`, a certificate of 1 KiB): RFC 7932's largest window and a stored
  metablock of 2^24 bytes, four bytes `cf ff ff ff`, took 16,780,366 bytes, where a whole
  certificate takes 38,835. 6.1.1 answers a short allocation with its own error [5], so the
  decoder's byte allocator refuses any allocation past what a stream decoding to the
  certificate's length takes; those four bytes then take 2,584, and every stream a stock encoder
  makes, window 2^22 and the certificate's own, 1 to 65,535 bytes, still decodes.
- Every decoder runs behind an unwind boundary, a panic being the handshake's typed refusal
  (bad_certificate), as CLAUDE.md §1 asks of a dependency that can panic.
- cargo deny names more advisories against crates in hyper-raft's lock. hyper-tls takes upstream
  0.23.46's fixes for the three in its own code, on the goal 16 lead's word (2026-11-13; hyper-raft
  doors-step8-3, hyper-tls `VENDORED.md` §12): RUSTSEC-2026-0346 (session values' `Debug` printed
  the resumption PSK and master secret), -0347 (data written before an ECH rejection reached the
  outer server) and -0349 (a key-share hint kept from an unauthenticated ServerHello), each with a
  test that fails before it. hyper-raft's lock moves past -0382 (rustls-webpki 0.103.17, and
  hyper-tls requires 0.103.16), -0381 (webpki-roots 1.0.12), -0386 (tokio 1.54.1) and -0486 (rustix
  1.1.6). Its oracle, upstream rustls pinned at 0.23.45, a test dependency, keeps its four.
- An independent decoder, one not from the encoder's crate, has not decoded mantle's
  certificate at its window: every Google decoder on the box is 1.2.0 [3], and another is a new
  tool, the owner's to allow.
