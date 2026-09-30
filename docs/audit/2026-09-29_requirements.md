# 2026-09-29 audit: requirement checklist

Source: `docs/audit/2026-09-29_audit.md` (§§2–17) against the ledger `docs/audit/2026-09-29_resolution.md`.
Status is judged from the ledger. "Ledger: X" names the ledger row cited. Where the ledger is silent, the row says so.
A requirement that is a prohibition ("do not X") and currently holds because nothing violating it was built is marked DONE with "constraint holds".

## §2–§4 residuals not closed by the ledger (and the named examples)

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| S01a | Log regression breadth: damage every header field and payload/CRC field at interior and final boundaries, with valid later frames, reused slots, reopen after attempted recovery | log | DONE | Ledger S01 (40a6ef5) and "S01, the last frame": `damage_to_any_field_of_an_acknowledged_frame_is_reported`, `stale_frames_in_a_reused_slot_prove_nothing`, `damage_to_the_last_acknowledged_frame_is_restored_from_its_persist_record` |
| S01b | Confirmation tracked against an absolute obligation, not input arrival; no ack before prefix authority is durable | log | DONE | Ledger "S01, round three": answered only once confirmed; idle timer removed; refused frame followed by confirmation |
| S01c | Quarantine acknowledged damage and recover from peers before rejoining | log, range/replica | PARTIAL | Fence done (ledger S01 last-frame row, S16, R17: group served only for removal, "repairs from its peers"); no ledger test of end-to-end removal and re-replication of a damaged member |
| S03a | Log admission covers combined queued+held+executing **encoded bytes** (incl. container/record overhead), bounded gathering, fair group scheduling so one hot range cannot take the device backlog | log | PARTIAL | Count bound and two-per-group cap done (ledger S03); byte budget and device-level fairness open (ledger 5.2 row) |
| S04a | Demonstrate fencing of the replica after an actual durability (write/flush) failure, distinct from recoverable refusals | range/replica | PARTIAL | Log fences on commit errors (ledger R08); "Fatal log errors still stop the replica" (ledger S04) but no replica-level fencing test cited |
| S07a | Streaming reads for large ranges; account queued output, checksum-read buffers and fragment lists together | chunk | PARTIAL | Admission before allocation done (ledger S07); integrated streaming read path and combined accounting open |
| S10a | Expand panic-path review beyond named panic/unwrap calls (e.g. std calls that panic internally) | tooling/benchmarks | OPEN | Not in ledger |
| B04a | Differential test of weak/strong `If-Match`/`If-None-Match` edge responses against real S3 | s3 | OPEN | Ledger B04 fixes RFC comparison; AWS behaviour unverified |
| B07a | Verify S3's behaviour for a valid-but-absent version-ID marker and align contract | s3, meta | OPEN | Ledger B07: "S3's answer is unrecorded" |
| B08a | Test legal maximum parts under delayed renewals/cancellation; emit bounded renewal batches with funded progress | gateway | PARTIAL | Length refusal and ordered renewal index done (ledger B08); batching waits on gateway driver |
| B09a | Fleet failure model qualification (field rates, correlated events) for durability claims | placement/repair | OPEN | Audit status table; ledger B09 fixes model state only |
| B09b | Conservative numerical error bounds for the durability computation; qualify the annual exponential approximation | placement/repair | PARTIAL | Ledger B09: exponential law only under a rare-loss test, uniformization otherwise; no stated numerical error bound |
| B10a | Physical raw-device qualification on Linux/macOS/Windows; real zoned device | disk | PARTIAL | macOS disk image and Linux loop device pass (ledger 6.2); Windows host and zoned hardware not run (ledger B10, 6.2) |
| B11a | Test mixed/retried/replaced multipart parts' ETags before claiming endpoint conformance | gateway, s3 | PARTIAL | Ledger B11 covers single PUT, empty part and combination rule; replaced/retried part cases not cited |
| P01a | Range-read strategy benchmarks: HDD seek/transfer comparison, head/middle/tail, full reads, mixed writes, cold/warm, p99 | chunk, tooling/benchmarks | PARTIAL | Gap derived from measured rates; release runs on this Mac (ledger P01); HDD and mixed/cold runs not recorded |
| P02a | Release size-sweep benchmark of completion at 1/100/1k/10k parts | meta, tooling/benchmarks | DONE | Ledger P02: `mantle bench meta`, linear 1–10,000 parts |
| P02b | Also benchmark rejected/retried completions, reply/session overhead, realistic engine row sizes, interference with heartbeats and other ranges | meta, tooling/benchmarks | OPEN | Ledger P02 measures successful completion on the model engine only |
| P04a | Settle session-rule values (answer count and byte budget) | meta | OPEN | Ledger P04: value "waits on the session rules replica.md §7 leaves open" |
| P05a | Measure sweep/collector contention with foreground work | meta, node runtime | OPEN | Ledger P05: "measured with the node runtime, when it runs both" |
| P06a | Victim selection without a full segment scan (bounded candidate index with generation validation) | chunk | PARTIAL | Ledger P06: aggregates incremental; "Choosing a victim still passes over the table" |
| P06b | Benchmark 1/10/100 TiB geometries, fragmentation, near-full foreground p99, cleaner work per reclaimed byte | chunk, tooling/benchmarks | PARTIAL | Ledger P06: 62,403-segment RAM-disk fill only |
| P07a | Admission for synchronous cold reads and aggregate recovery-window memory | log, chunk, node runtime | OPEN | Audit status table; ledger P07 silent |
| P07b | Coalesce interleaved many-group cold entries; measure by entry size and medium | log | PARTIAL | Ledger P07: interleaved entries still read one at a time; measured on this SSD only |
| P08a | Reusable admitted EC workspaces; benchmarks of degraded GET, single-shard repair, mixed PUT/repair with allocations and memory bandwidth | gateway, tooling/benchmarks | PARTIAL | Ledger P08: copies removed; workspace retention "is the gateway's memory admission to decide" |
| P09a | Calibration: compare reusable/native async backend; measure mixed read/durable write, background interference, near-full; derive foreground QD and background budgets | disk | OPEN | Ledger P09: "backend and mixed workloads open" |

## §5 Raft and metadata service

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 5.1a | Staged Ready: emit eligible leader messages before local flush, submit persistence, suspend Ready, emit durability-dependent messages after; follower acks never before durability; one Ready per group | range/replica | DONE | Ledger 5.1: `Replica::begin`, `a_leader_sends_while_it_flushes_and_a_follower_acknowledges_after` |
| 5.1b | Device/node scheduler staging many ranges before a shared flush, reserving time for heartbeats, reads, apply; slow disk op never stalls unrelated groups | node runtime | OPEN | Ledger 5.1: node scheduler open |
| 5.1c | Replace count-only 64-Ready drive budget with time/bytes/flush bounds | range/replica, node runtime | OPEN | Ledger 12.6: replica drive budget among 14 open constants |
| 5.1d | Measure commit p50/p99, flush size, messages per commit, scheduler delay, election churn under mixed groups/RTTs before claiming gains | tooling/benchmarks | OPEN | Ledger 5.1: waits on node process |
| 5.1e | Batching/transport changes preserve durable term/vote and ordered-log rules | range/replica | DONE | Ledger 5.1 and 5.9: simulation linearizable with staged readies |
| 5.2a | Publish and enforce per-range bounds: proposal/append bytes, command count, retained log bytes, outstanding reads, reply bytes | range/replica, meta | PARTIAL | Ledger 5.2: entry bytes, merge bytes, session answer bytes, reads-waiting bytes; not yet a published full set |
| 5.2b | Per-device bounds on queued/held/executing persistence bytes, flush concurrency, fg/bg work | log, node runtime | PARTIAL | Count bound from S03; bytes and device-level budget open (ledger 5.2) |
| 5.2c | Per-node sums: range state, engine caches, sessions, snapshots, network/TLS buffers, coding workspaces | node runtime | OPEN | Ledger 5.2 |
| 5.2d | Per-tenant shares, weighted fairness, typed overload responses | node runtime, gateway | OPEN | Ledger 5.2 |
| 5.2e | Snapshot, merge and reply buffers hold budget ownership through completion/cancellation | range/replica, meta | PARTIAL | Merge byte bound (ledger 5.2); snapshot/reply budgets open |
| 5.2f | Bound actual bytes/work before cloning, encoding or proposing | range/replica, meta | PARTIAL | Merge `max_bytes`, entry refused past `max_entry_bytes`; `propose` still encodes before checking (ledger 12.2 row) |
| 5.3a | Production engine recovers rows, session outcomes, membership/descriptor state and applied index coherently | engine | OPEN | Ledger 5.3 |
| 5.3b | Prove ordering among Raft append, engine batch publication, engine flush, snapshot preparation, log truncation | engine, docs/research | OPEN | Ledger 5.3 |
| 5.3c | Failed apply fences consistently; command refusal stays a replicated outcome | engine, range/replica | OPEN | Ledger 5.3 |
| 5.3d | Engine compaction and WAL/checkpoint work get foreground-latency and disk-space budgets | engine | OPEN | Ledger 5.3 |
| 5.3e | Real-process kill/restart and corruption tests with the real engine and filesystem, incl. full disk during compaction/snapshot | engine, tooling/benchmarks | OPEN | Ledger 5.3 |
| 5.4a | Streamed out-of-band snapshots: bounded concurrent streams, chunk integrity, resumability, stale term/generation fencing, crash-safe publication, membership-aware catch-up | range/replica, engine | OPEN | Ledger 5.4 |
| 5.4b | Snapshot transfer and learner rebuild share repair/foreground bandwidth budgets | node runtime | OPEN | Ledger 5.4 |
| 5.4c | Test interrupted install, concurrent writes, two replacements, large ranges, near-full disks | range/replica | OPEN | Ledger 5.4 |
| 5.4d | Never publish engine state from a snapshot whose log state cannot be made durable | engine, range/replica | OPEN | Ledger 5.4 |
| 5.5a | Batch compatible ReadIndex confirmations | range/replica | DONE | Ledger 5.5 |
| 5.5b | Follower reads establish current-leader authority and wait for local apply to reach the confirmed index; cache only at a proven term/applied boundary | range/replica | OPEN | Ledger 5.5 covers leader rounds only |
| 5.5c | No lease without explicit clock/pause model and fallback | range/replica | DONE | Ledger 5.5: "No lease is used" |
| 5.5d | Benchmark hot read ranges and election/partition boundaries with the linearizability checker | tooling/benchmarks | PARTIAL | Checker runs in simulation (ledger 5.9); no hot-read benchmark |
| 5.6a | Fast track: durable group-mode identity, public proposer/displaced handling, durable-tail authority; never enabled by a boolean | range/replica | OPEN | Ledger 5.6 |
| 5.6b | Account for every admitted proposal; measure real storage/tail/CPU/bandwidth under the chosen application contract | range/replica, tooling/benchmarks | OPEN | Ledger 5.6 |
| 5.7a | Failure-domain independence for metadata voters, incl. devices and shared logs | placement/repair | OPEN | Ledger 5.7 |
| 5.7b | Throttle simultaneous moves/replacements; bound learner lag and bytes | range/replica, node runtime | OPEN | Ledger 5.7 |
| 5.7c | Prevent removal while the new replica lacks required durable state | range/replica | OPEN | Ledger 5.7 |
| 5.7d | Test loss during joint configuration, repeated leader transfer, multi-voter recovery, shared-log device failure | range/replica | PARTIAL | Five voters with two down and member replacement (ledger 5.9); joint-config loss and shared-log device failure open |
| 5.7e | Define metadata quorum availability and data durability independently | docs/research | OPEN | Ledger 5.7 |
| 5.8a | Transport: message authentication, node identity/authorization, replay handling, size limits, MTU strategy, retransmission, duplicate suppression | transport | OPEN | Ledger 5.8 |
| 5.8b | Large AppendEntries never become unbounded fragmentation/reassembly work | transport | OPEN | Ledger 5.8 |
| 5.8c | Transport queues partitioned by urgency and budget; snapshots cannot starve votes/heartbeats; stale-reply floods bounded | transport | OPEN | Ledger 5.8 |
| 5.8d | Use measured loss/RTT/bandwidth incl. slow receivers | transport, tooling/benchmarks | OPEN | Ledger 5.8 |
| 5.9a | Retain header faults, same-group overflow order, recoverable Ready refusals, overlapping collectors, multipart adoption graphs in simulation | log, range/replica, meta | DONE | Ledger S01, S02, S04, B01, B02 |
| 5.9b | Simulate 1/3/5 voters | range/replica | PARTIAL | 3 and 5 in simulation (ledger 5.9); one-voter case only in unit tests (S04) |
| 5.9c | Thousands of concurrently driven ranges | range/replica, node runtime | OPEN | Ledger 5.9 |
| 5.9d | Long partitions | range/replica | PARTIAL | Two of five down for a quarter of steps (ledger 5.9); long partitions not stated |
| 5.9e | Duplicate/delayed messages | range/replica | DONE | Ledger 5.9 |
| 5.9f | Clock skew and steps | range/replica | DONE | Ledger 5.9: clocks step ±2 s, 5,000 seeds |
| 5.9g | Slow apply | range/replica | OPEN | Ledger 5.9 |
| 5.9h | Snapshot pressure | range/replica | OPEN | Ledger 5.9 |
| 5.9i | Multiple replacements | range/replica | DONE | Ledger 5.9: runs "replaced its lost members" with two down |
| 5.9j | Full-disk and shared-device failures | range/replica, log | OPEN | Ledger 5.9 |
| 5.9k | Safety checked after every action, liveness after faults stop | range/replica | DONE | Ledger 5.9, R19 ("all linearizable and live") |
| 5.9l | Keep failure seeds and mutation tests showing each invariant observable | range/replica | DONE | Ledger 5.9 exact seeds; per-row removal checks |

## §6 Storage adaptation

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 6.1a | Persisted, versioned device profile: resolved identity, capabilities, alignment, measured workload curves, confidence/caps, selected I/O/admission policy | disk | OPEN | Ledger 6.1 |
| 6.1b | Invalidate/revalidate profile on device/firmware/backend/filesystem change | disk | OPEN | Ledger 6.1 |
| 6.1c | Separate immutable format choices from mutable scheduling; runtime changes never reinterpret old records | disk, chunk | OPEN | Ledger 6.1 |
| 6.1d | Translate capabilities plus representative measurements into every writer/cleaner/scrubber choice | chunk | OPEN | Ledger 6.1 |
| 6.1e | Treat NVMe as interconnect and SSD as medium | disk | DONE | Constraint holds: audit §14.2 finds identity already separates them; no ledger row |
| 6.2a | Resolve file to backing storage and device node as device (st_rdev, OS capacity, device flush) | disk | DONE | Ledger 6.2 |
| 6.2b | Discover/enforce offset and buffer alignment, transfer ceilings, file-vs-device allocation, flush/barrier semantics, topology, zone geometry | disk | PARTIAL | Capacity and flush done (ledger 6.2); rest open |
| 6.2c | Report the caching mode actually selected after direct-I/O fallback | disk | OPEN | Not in ledger |
| 6.2d | Composite/virtual/network paths keep Unknown and are measured | disk | PARTIAL | Linux probe leaves medium unknown (ledger 12.6); measurement open |
| 6.2e | Typed refusal before format for unsupported durability/write constraints | disk | PARTIAL | Host-managed zoned refused (ledger B10); other unsupported backends not enumerated |
| 6.2f | Windows raw-device flush semantics validated on a Windows host | disk | OPEN | Ledger 6.2 |
| 6.3a | NVMe SSD evidence: mixed read/group-commit curves, flush p99, CPU/byte, RSS, thermal/near-full | disk, tooling/benchmarks | OPEN | Ledger 6.3 |
| 6.3b | SATA/SAS SSD evidence: sustained, fresh/overwrite, GC-state | disk, tooling/benchmarks | OPEN | Ledger 6.3 |
| 6.3c | HDD evidence: seek/transfer break-even, fg p99 under cleaning, multi-hour steady state | disk, tooling/benchmarks | OPEN | Ledger 6.3 |
| 6.3d | Host-managed SMR/ZNS evidence (if supported): zoned emulator and hardware tests of interrupted append/reset/recovery | disk | OPEN | Currently refused (B10) |
| 6.3e | USB/virtual/composite/network: correctness under disconnect/cache/server failure; no independent-domain claims from logical volumes | disk, placement/repair | OPEN | Ledger 6.3 |
| 6.4a | Reject unsupported host-managed paths before writes | disk | DONE | Ledger B10 |
| 6.4b | Any zoned backend: zone report/capacity/write pointer/append/reset/open-active limits, proven metadata placement and crash protocol, fault injection | disk, chunk | OPEN | Not built |
| 6.4c | "Append-oriented" and "works on ZNS/SMR" stated as separate claims | docs/research | DONE | Ledger B10: design no longer maps segments onto zones |
| 6.5a | Retain full platform flush and parent-directory durability | disk | DONE | Constraint holds (audit found no defect); ledger 6.2 device flush |
| 6.5b | Validate Windows directory/volume persistence on a real target | disk | OPEN | Not in ledger |
| 6.5c | Never skip a flush on an identity heuristic (write cache/PLP label) | disk | DONE | Constraint holds; no skip path exists; no ledger row |
| 6.5d | Data and index/log on different devices: persist each and publish dependency order | chunk, node runtime | OPEN | Not built |
| 6.5e | Derive batching from measured flush latency, throughput, fg SLO; account small-object and first-write cost | chunk | OPEN | Ledger 12.6: chunk writer batch shape open |
| 6.5f | Prewriting chosen by computed break-even from actual allocation behaviour | disk | OPEN | Not in ledger |
| 6.6a | Per backend record identity, firmware/driver/fs, caching mode, alignment, capacity, profile version, calibration conditions | disk, tooling/benchmarks | OPEN | Ledger 6.6 |
| 6.6b | Exercise fresh/warm/steady/near-full with mixed fg/bg; measure throughput, p50/95/99, CPU, memory, physical amplification, flush distributions | tooling/benchmarks | OPEN | Ledger 6.6 |
| 6.6c | Correctness matrix per backend: power loss, failed/short I/O, corrupt/stale headers, reboot/reopen, directory publication, device disappearance | disk, chunk, log | PARTIAL | SimFile power-loss and corruption suites (ledger S01, S05, R-rows); per-backend and device disappearance open |
| 6.6d | Measure Linux HDD, Windows SSD, remote NVMe (Mac results do not certify them) | tooling/benchmarks | OPEN | Ledger 6.6 |

## §7 S3 compatibility

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 7.0a | Explicit compatibility profile: wire helpers vs metadata behaviour vs integrated endpoints vs unsupported vs verified AWS differences | docs/research | OPEN | Not in ledger |
| 7.1a | Preserve exact signed host/path/query through proxies, redirects, cell routing; test duplicate headers/params and unusual UTF-8 keys | s3, gateway | OPEN | Ledger 7.1: needs HTTP server |
| 7.1b | Payload hash, chunk signatures, trailers validated before Name publication; key revocation/rotation, skew/expiry agree across gateways | s3, gateway | OPEN | Ledger 7.1 |
| 7.1c | Bound total HTTP/decoder/coding/metadata memory; disconnect, partial-body, checksum-failure, late-trailer cleanup | gateway, s3 | OPEN | Ledger 7.1 |
| 7.1d | Admission enforces operation length, 5 GiB part, minimum nonfinal part, 10,000 parts | gateway, meta | DONE | Ledger 7.1 part-number/size row, B08 |
| 7.1e | Integrated multipart completion: conditional failure, retries, abort/replacement, ordering, composite/full-object checksum, no premature child reclamation | gateway, meta | PARTIAL | Library done (ledger B02, R12, R13, §16 completion driver); integrated endpoint open |
| 7.1f | Race PUT/DELETE/COPY/Complete against concurrent changes; assert status, ETag, unchanged data | gateway, meta | OPEN | Ledger 7.1 |
| 7.1g | GET/HEAD: current/versioned/deleted, suffix/invalid ranges, HEAD errors, degraded reads, segment-tag failure, Content-Length, stream termination | gateway, s3 | PARTIAL | GET library (ledger §16); HTTP checks open |
| 7.1h | Version listing across deleted markers and split/merge/migration; UTF-8 order, delimiters, max keys, flags | meta, s3 | PARTIAL | Deleted marker between pages (ledger B07); split/merge/migration open |
| 7.1i | Correct canonical owner on delete markers; decide stored vs derived bucket-owner authority | meta | DONE | Ledger 7.1 delete-marker owner |
| 7.1j | Cross-account owner/expected-owner behaviour and actual authorization | s3, gateway | OPEN | Ledger 7.1 |
| 7.1k | Bucket policy context from authenticated facts; explicit deny, anonymous, public-access blocks, version actions | s3, gateway | OPEN | Ledger 7.1 |
| 7.1l | Object Lock concurrency with delete/overwrite/retention change, default retention, governance bypass authorization, clock boundaries | meta, s3 | OPEN | Ledger 7.1 |
| 7.1m | Lifecycle: durable paced workers, version-aware expiry, multipart abort, continuation/retry, locked data | meta, node runtime | OPEN | Ledger 7.1 |
| 7.1n | Encryption: cross-node unwrap, generation rotation, mixed multipart keys, copy SSE-C headers, corruption, lost-key recovery | gateway, s3 | OPEN | Ledger 7.1 |
| 7.1o | Stable NotImplemented mapping for unsupported modes throughout routing, no partial side effects | s3, gateway | OPEN | Ledger 7.1 |
| 7.1p | Error protocol: request IDs, headers, HEAD/body, multi-delete partial errors, Complete HTTP-200 error body | s3, gateway | OPEN | Ledger 7.1 |
| 7.1q | AWS CLI plus Python/Go/Java/JS SDKs and an S3 conformance suite against the integrated server | tooling/benchmarks | OPEN | Ledger 7.1 |
| 7.1r | Multi-gateway: read-after-write across gateways, reads during partitions, retries through leader/cell changes | gateway, cells/fleet | OPEN | Ledger 7.1 |
| 7.1s | State and test the operation-specific admission policy from current AWS limits (48.8 TiB, 10,000 parts, 5 MiB–5 GiB, 1,000-entry ListParts/ListMultipartUploads pages) | s3, gateway | PARTIAL | Part/PUT limits done; list page limits and stated policy not ledgered |
| 7.2a | Measure object-size/access distributions; compare replicated append/packing for small objects against EC | gateway, tooling/benchmarks | OPEN | Ledger 7.1–7.4 "the rest" |
| 7.2b | Cold-data re-encoding (if built): atomic layout-generation transition, ownership-safe retirement, reader fencing | gateway, placement/repair | OPEN | Not built |
| 7.2c | Packing preserves single ownership (no B02-style sharing), independent encryption, deletion/retention | gateway, meta | DONE | Constraint holds: no packing built |
| 7.2d | Range GET reads only required authenticated segments and healthy systematic chunks; reconstruct only on verified failure | gateway | PARTIAL | GET seeks by byte (ledger §16), decode passes present data chunks (ledger P08); degraded path under HTTP open |
| 7.2e | Budget hedged reads; cancel losers without unbounded repair traffic | gateway | OPEN | Not in ledger |
| 7.2f | Verify block/chunk/segment checksums then AEAD before returning plaintext | gateway, chunk | PARTIAL | Chunk verifies checksum blocks (ledger P01); GET-level AEAD failure test not cited |
| 7.3a | CopyObject reads/reseals/recopies under fresh file/block identities | gateway, meta | OPEN | No ledger row; COPY driver not built |
| 7.3b | Zero-copy/reflink/dedup only after multi-owner references, fenced adoption/release, retention, crash-safe cleanup are designed and benchmarked | meta, docs/research | DONE | Constraint holds: none built |
| 7.4a | Capacity planning states single-hot-key serialization; bounded caching/read confirmation or per-key admission | meta, docs/research | OPEN | Ledger 7.1–7.4 |
| 7.4b | Large listings stream bounded pages across ordered ranges | meta, gateway | OPEN | Ledger 7.1–7.4 |
| 7.4c | Continuation tokens: stable logical position, bounded, opaque, tamper-protected | s3, gateway | OPEN | Ledger 7.1–7.4 |
| 7.4d | Range/cell generation changes force correct rerouting of listing | meta, cells/fleet | OPEN | Ledger 7.1–7.4 |
| 7.4e | Specify pagination behaviour under concurrent mutation | meta, docs/research | PARTIAL | Version-ID resumption contract (ledger B07); key listing contract not ledgered |

## §8 Laptop → region → fleet

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 8.1a | Choose a geo model; define per-operation consistency, RPO/RTO, partition behaviour, authority transfer | cells/fleet, docs/research | OPEN | Not in ledger |
| 8.1b | Build the chosen model's mechanism (replication cursor + fencing, remote durable barrier, or global protocol) | cells/fleet | OPEN | Not in ledger |
| 8.1c | Keep cell-local consensus bounded; no planet-spanning quorum per range | cells/fleet | DONE | Constraint holds (architecture) |
| 8.2a | Record measured limits: ranges, placement entries, fragments, sessions/replies, cache bytes, repair queue, placement solve time | cells/fleet, tooling/benchmarks | OPEN | Not in ledger |
| 8.2b | Validate the accounting equations (placement rows, node memory, map distribution bandwidth, recovery time) | cells/fleet, docs/research | OPEN | Not in ledger |
| 8.2c | Measured root-map size/receiver ceiling and dissemination strategy; persisted last-good routing and epoch fencing | cells/fleet | OPEN | Not in ledger |
| 8.2d | Resource ceilings trigger split, cell addition, admission or tenant isolation before saturation | cells/fleet, node runtime | OPEN | Not in ledger |
| 8.2e | Scale tests include metadata/index memory for small objects | tooling/benchmarks | OPEN | Not in ledger |
| 8.3a | Measure failure detection plus rebuild under real contention | placement/repair | OPEN | Not in ledger |
| 8.3b | Reserve capacity and bandwidth to recover the largest failure domain with another failure plausible | placement/repair | OPEN | Not in ledger |
| 8.3c | Repair: prioritize by remaining redundancy, verify every source, install placements by generation, retire old chunks only when safe | placement/repair | OPEN | Not in ledger |
| 8.3d | Block reverse index for bounded per-volume discovery; persist/restart repair tasks | placement/repair, meta | OPEN | Not in ledger |
| 8.3e | Reject impossible topology; never place several chunks of a stripe on one physical domain (volumes/namespaces on one device) | placement/repair | OPEN | Not in ledger |
| 8.3f | Derive block/size policy or qualify service class for large-object durability (per-block 10⁻¹¹ union bound) | placement/repair, docs/research | OPEN | Not in ledger |
| 8.3g | Operational durability story includes metadata/key loss, correlated events, software deletion | docs/research | OPEN | Not in ledger |
| 8.4a | Release/move fence before reading chunk places for destruction (reclaimer vs block::Move), or proven generation protocol | meta, placement/repair | OPEN | Not in ledger |
| 8.4b | Reconcile abandoned writes promptly (orphan-chunk reconciliation) | placement/repair, chunk | OPEN | Not in ledger |
| 8.4c | Test move-versus-delete in both orders with crashes at every step, no sleeps | meta, placement/repair | OPEN | Not in ledger |
| 8.5a | Migration protocol covers objects, versions, parts, bucket settings, dispositions, keys, in-flight cleanup | cells/fleet | OPEN | Not in ledger |
| 8.5b | Fault-test every migration boundary (partial copy … source reclamation) with loss, duplicate movers, failed flush, old gateways, retries | cells/fleet | OPEN | Not in ledger |
| 8.5c | Old-epoch mutations refused after ownership change | cells/fleet, meta | OPEN | Not in ledger |
| 8.5d | Destination readiness by verifiable inventory/checkpoint and replay frontier | cells/fleet | OPEN | Not in ledger |
| 8.6a | Size headroom for max ingestion/deletion debt, read grace, worst repair, cleaner runway | node runtime, placement/repair | OPEN | Not in ledger |
| 8.6b | Bound release/sweep/repair queues in bytes and work; expose their age | meta, placement/repair | OPEN | Not in ledger |
| 8.6c | Inactive tenants cannot leave unlimited multipart or abandoned-write debt | meta, gateway | OPEN | Not in ledger |
| 8.6d | Separate fair shares for foreground, consensus control, snapshot/repair, cleanup under one device/node budget | node runtime | OPEN | Not in ledger |
| 8.6e | Overload surfaces as typed retryable responses, never OOM or stranded Ready | node runtime, range/replica, meta | PARTIAL | `Stalled`, `SessionsFull`, `MessagesHeld`, S04 retained Ready (ledger); node-level overload open |
| 8.7a | Persist and negotiate wire/disk/engine versions; refuse unknown fields/versions | node runtime, meta, log, chunk | PARTIAL | Versioned row/entry/log formats (ledger R07, R13, R16, R20, R21); negotiation open |
| 8.7b | Staged activation of new semantics across mixed versions; rollback boundaries; interrupted migration handling | cells/fleet, node runtime | OPEN | Not in ledger |
| 8.7c | Rollout and replica placement limit a bad shared-log upgrade's blast radius | cells/fleet, placement/repair | OPEN | Not in ledger |
| 8.7d | Backup/DR: coherent metadata/data/key recovery point, restore tooling, regular drills | cells/fleet | OPEN | Not in ledger |
| 8.8a | Propagate disk histograms, read refusals/high-water, scrub findings, writer failure state into telemetry | node runtime | OPEN | Not in ledger |
| 8.8b | Telemetry for queue/held/executing bytes, apply/flush/ReadIndex latency, leader churn, snapshot lag, repair debt, route/migration epochs, checksum/AEAD failures, causes and denied work | node runtime | OPEN | Not in ledger |
| 8.8c | Milestone laptop serving: real S3 PUT/GET/HEAD/list/delete/multipart, production engine, bounded RSS, restart/disk-full safety, honest redundancy report | node runtime, gateway, engine | OPEN | |
| 8.8d | Milestone multi-disk node: proven device identity/capacity, domain-aware placement, degraded reads/repair, disk removal/replacement, HDD/SSD/NVMe profiles | disk, placement/repair | OPEN | |
| 8.8e | Milestone regional cluster: authenticated transport, 3/5-voter histories, topology-aware placement, node/rack/zone failures, slow receivers, learner/snapshot pressure, near-full load | cells/fleet, transport | OPEN | |
| 8.8f | Milestone multi-cell region: root map recovery, stale-route fencing, live migration, full-cell relief, one-cell failure isolation | cells/fleet | OPEN | |
| 8.8g | Milestone global fleet: geo contract, region failover fencing, measured map/placement/repair limits, staged upgrades, recovery drills | cells/fleet | OPEN | |
| 8.8h | Meta-scale claim: repeatable tests at largest supported cardinalities with published resource/SLO limits | cells/fleet, tooling/benchmarks | OPEN | |

## §9 Security, structure, operations

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 9a | Policy context, owner identity, SSE-C authorization, bypass privileges come from authenticated server facts, never caller command fields | gateway, s3 | OPEN | Not in ledger (delete-marker owner named by gateway, ledger 7.1, is one instance) |
| 9b | HTTP/header/body/decoded-byte admission before allocation; consistent duplicate header/query resolution; request-smuggling tests at server/proxy boundary | s3, gateway | PARTIAL | Bounded XML/JSON readers, `xml::Compact` (ledger 12.6, R10); HTTP server absent |
| 9c | No Name publication until body hash, signed chunks, trailer checksum and length validate; uncommitted chunks recoverably reclaimed | gateway | PARTIAL | Length and Content-MD5 checks (ledger B08, B11); signed chunks/trailers in HTTP path open |
| 9d | Authenticated node roles and credentials; bounded replay/failed-auth work | transport | OPEN | Not in ledger |
| 9e | Root-key generation as one shared authority across gateways/cells; persisted/backed up; old unwrap retained; rotation tested under node/region loss | gateway, cells/fleet | OPEN | Not in ledger |
| 9f | Consider zeroizing SigV4 secrets, signing-key intermediates, Chain key storage | s3 | OPEN | Not in ledger |
| 9g | Stateful sealing API (or enforced one-key/one-file/monotonic index) to prevent caller-induced nonce reuse | s3, gateway | OPEN | Not in ledger |
| 9h | Preserve patched RNG fallback/fallible crypto on every target; keep auditing abort modes catch_unwind cannot contain | s3, tooling/benchmarks | OPEN | Ongoing; not in ledger |
| 9i | Dependency hygiene: checksums/provenance, minimal features, fresh advisories, reproducible builds, patch regressions on all platforms | tooling/benchmarks | PARTIAL | `cargo deny` and vendor suites in gates (CLAUDE.md); reproducible builds and six-platform patch runs not ledgered |
| 9j | Ownership authority: prepared/adopted/released/reclaimed states persistent; reclamation authorized by that authority | meta | DONE | Ledger B01, B02, R16, R20 |
| 9k | Resource ownership: budgets reserved queued→deferred→executing→completed/cancelled; one centralized lifetime rule | log, chunk, node runtime | PARTIAL | Log (ledger S03) and chunk read gate (S07); not centralized |
| 9l | Persistence staging: immediately-sendable vs durability-dependent messages; pending Ready retained | range/replica | DONE | Ledger 5.1, S04 |
| 9m | Backend capabilities (identity, capacity, alignment, caching, durability) validated at open/format | disk, chunk | PARTIAL | Ledger 6.2, S08, S13 (`Geometry::holds`); alignment/caching open |
| 9n | Derive/cross-check protocol limits and wire budgets; tie spec claims to named mutation/regression tests | tooling/benchmarks, docs/research | PARTIAL | Ledger 12.6 inventory gate, `largest_entry_bytes`; not complete |

## §11 Slates/Focal adoption

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 11.1a | Build Mantle's own durable shell, transport, authentication, memory budgets and fleet scheduler (none arrive with focal-raft) | node runtime, transport | OPEN | Not in ledger |
| 11.2a | Preserve Append pipelining through byte-bounded peer queues and async persistence; windows from path and receiver resources | transport, range/replica | OPEN | Not in ledger |
| 11.2b | One owned Ready per group; overlap independent groups first | range/replica, node runtime | PARTIAL | One Ready enforced (ledger 5.1); cross-group overlap needs node scheduler |
| 11.2c | Nonblocking shared-WAL receipts in Mantle's driver and shared-device owner | range/replica, log, node runtime | PARTIAL | `begin`/`persisting` (ledger 5.1); device owner open |
| 11.2d | Leader messages sent before local write via a phased driver | range/replica | DONE | Ledger 5.1 |
| 11.2e | No all-log barriers (multi-log sync) per range without demonstrated gain | range/replica | DONE | Constraint holds |
| 11.2f | Load/media-aware leader placement, damped timing, graceful handoff, keeping quorum/vote rules | range/replica, placement/repair | OPEN | Not in ledger |
| 11.2g | Choose an explicit internal transport boundary; keep public S3/HTTP interoperability | transport | OPEN | Not in ledger |
| 11.3a | Range actor owns Ready, ordered parts, generation and disk ticket; nonblocking submission gets room or retains the part for a capacity notification | range/replica, node runtime | PARTIAL | Refused Ready retained with unwritten parts (ledger S04); actor/ticket open |
| 11.3b | Completion wakes the correct actor without scanning every range | node runtime | OPEN | Not in ledger |
| 11.3c | Actor yields after a byte/time budget; inputs meanwhile held in bounded owner queues | range/replica, node runtime | PARTIAL | Held messages/ticks bounded (ledger R19); byte/time yield open |
| 11.3d | Fatal flush/write errors fence the whole affected durability domain | log, node runtime | PARTIAL | Log fences (ledger R08); domain-wide fence open |
| 11.3e | Snapshot and Engine completion use the same ownership discipline | range/replica, engine | OPEN | Not in ledger |
| 11.3f | Keep leader-send overlap and Mantle's commit-recovery model; never copy/delete fences without proof of which durable state covers an answer | range/replica | DONE | Ledger 5.1 |
| 11.3g | Ticket contract: cancellation removes interest not the admitted write; ambiguous timeout is not no-effect; late completion checked by generation with cleanup ownership | node runtime, log | OPEN | Not in ledger |
| 11.3h | Service fairness and aggregate completion capacity across thousands of ranges | node runtime | OPEN | Not in ledger |
| 11.4a | Bound inflight bytes, retained unstable/durable entries and receiver work together | range/replica, transport | PARTIAL | Held appends within one flow-control window (ledger R19); byte-level peer bounds open |
| 11.4b | Initial replication windows from durable-ack latency and sink rate, adapting within node/device budgets | range/replica, transport | OPEN | Not in ledger |
| 11.4c | Measure min(leader CPU, WAL durability, follower durable-ack, network/window, engine apply) | tooling/benchmarks | OPEN | Not in ledger |
| 11.4d | Confirmed progress never moves back on a late reply; stale range/term/incarnation callbacks rejected | range/replica, transport | OPEN | Not in ledger |
| 11.4e | Conflict-hint repair and snapshot feedback preserved while a local Ready is out | range/replica | DONE | Ledger 5.9 (snapshot report kept), R19 (messages held) |
| 11.5a | Fast-track test accounts every admitted request (committed once / displaced / unknown) across holes, losing proposals, persistence rejection, compaction, removed voters, session rows, split barriers, leader loss | range/replica, meta | OPEN | Not in ledger |
| 11.5b | Keep the chosen fast algorithm's full invariants (durable approvals, no transplanted Slates pruning) | range/replica | OPEN | Not in ledger |
| 11.5c | Crossover comparison with real flush, load, CPU and wire bytes; no blanket enablement | tooling/benchmarks | OPEN | Not in ledger |
| 11.6a | No MLRaft barrier model across Name/File/Block | meta | DONE | Constraint holds |
| 11.6b | Speculative in-range apply only with full dependency tracking, ordered atomic Engine prefix, differential histories | engine, meta | OPEN | Not built |
| 11.6c | Cross-range parallelism and removal of full scans before speculative apply | meta, node runtime | PARTIAL | Scans removed (ledger P02–P06); cross-range parallelism needs node runtime |
| 11.7a | Leadership chooses using durable-ack and owner scheduling/flush delay alongside RTT | range/replica, placement/repair | OPEN | Not in ledger |
| 11.7b | Hysteresis, bounded transfer rate per node/cell, staggered elections, drained handoffs | range/replica, node runtime | OPEN | Not in ledger |
| 11.7c | No indefinite timer stretching under overload; progress policy for unknown paths and startup | range/replica | OPEN | Not in ledger |
| 11.7d | Learner catch-up before joint promotion; final voter set learns committed configuration | range/replica | DONE | Audit §5.7/§11.7 notes it implemented and simulated; ledger 5.9 replacement runs |
| 11.7e | Bound simultaneous catch-up bytes and repair/snapshot pressure across domains | node runtime, placement/repair | OPEN | Not in ledger |
| 11.7f | Graceful shutdown waits for a successor's demonstrated authority | range/replica, node runtime | OPEN | Not in ledger |
| 11.7g | ReadIndex tied to range generation and applied prefix; bound contexts, waiters, response bytes; safe cancellation | range/replica | PARTIAL | Contexts bounded, unconfirmed reads returned (ledger 5.5, S14); generation correlation and waiter/response bytes not ledgered |
| 11.7h | Clock leases need an explicit deployment clock bound | range/replica | DONE | No lease (ledger 5.5) |
| 11.8a | Streaming source/sink API and aggregate application-buffer admission for bulk/snapshot transport | transport | OPEN | Not in ledger |
| 11.8b | Separate useful bytes from congestion-accounted packet bytes; ACK-only exemptions and probe handling (RFC 9002) | transport | OPEN | Not in ledger |
| 11.8c | Aggregate transport credits derived from node/device/tenant memory; small independent control allowances | transport, node runtime | OPEN | Not in ledger |
| 11.8d | Reserve raw-frame and decoder capacity before reading; carry the reservation through worker/output/cancellation | transport | OPEN | Not in ledger |
| 11.8e | Node identity authenticated separately from range/tenant authority; bind sender, group, epoch, operation; no replayable early data | transport | OPEN | Not in ledger |
| 11.8f | Durable nonce-counter reservation or fresh key/incarnation before first send after restart; bounded key overlap/revocation | transport | OPEN | Not in ledger |
| 11.8g | HTTP/3, if offered, uses standard HTTP/QUIC integration separate from the internal protocol | s3, transport | OPEN | Conditional; not built |

## §12 Allocation, copies, page faults, constants

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 12.1a | Fix ownership and asymptotic work before any allocator change; no swap on a microbenchmark | tooling/benchmarks | DONE | Constraint holds: no allocator change |
| 12.1b | Count alloc/realloc calls, requested/retained capacity, copied bytes, touched pages, peak live ownership separately; charge shared backing once | tooling/benchmarks | OPEN | Not in ledger |
| 12.2a | Gateway: owned block/segment slots with tag space, seal into final positions, recycle after last user | gateway | OPEN | Not in ledger |
| 12.2b | EC: reuse admitted workspaces, borrow systematic shards, reconstruct only needed outputs, measure source+parity+scratch peak | gateway | PARTIAL | Ledger P08 (borrowed spans, targeted rebuild); workspace reuse and peak measurement open |
| 12.2c | Chunk ingestion: accept owned payloads, encode header/table into final buffers, compare gather vs contiguous | chunk | OPEN | Not in ledger |
| 12.2d | Small/range GET: admission before allocation, caller-owned outputs, direct systematic reads, targeted rebuild, in-place decrypt, bounded streaming | gateway, chunk | PARTIAL | Ledger S07, P01, P08, §16 GET window; caller-owned outputs and in-place decrypt not ledgered |
| 12.2e | Ready/commands: admit encoded size before encoding, one immutable backing allocation, return Update ownership on refusal, retain Ready through receipts | range/replica | PARTIAL | Ready retained (ledger S04); `update_of`/`propose` copies remain (ledger 12.2) |
| 12.2f | Shared-log frame encoded into one admitted reusable aligned frame | log | PARTIAL | Ledger 12.2: frame copy and zeroed allocation removed; payload still encoded into a vector |
| 12.2g | Sessions/manifests/directories: answer byte retention, bounded directory updates, dense slots, streaming extent/list plans, indexed expiry | meta, gateway | PARTIAL | Ledger P03, P04, P05, B08; dense slots and indexed session expiry not ledgered |
| 12.2h | Snapshot/install: consistent streamed range view, verified chunks, bounded staging, atomic prefix publication, catch-up traffic reserved separately | range/replica, engine | OPEN | Ledger 5.4 open |
| 12.2i | Internal peer frames: streaming bodies, borrowed fixed-header parse, one admitted payload per stage, capacity tokens across handoffs | transport | OPEN | Not in ledger |
| 12.2j | Log writer: borrowed descriptors, reused bounded scratch, evaluate generation-tagged completion slots; refusal returns ownership | log | OPEN | Not in ledger |
| 12.2k | Cold `Storage::entries`: bounded prefetch/cache admission or a proven async storage/core interaction | range/replica, log | OPEN | Not in ledger |
| 12.2l | Snapshot delivery: pinned immutable checkpoint and bounded chunks instead of d×S cloned vectors | range/replica | OPEN | Ledger 5.4 open |
| 12.3a | Bounded size classes for frames, segments, chunks, coding workspaces; live and cached capacity charged separately; per-shard caps summing to node cap | node runtime, disk | OPEN | Not in ledger |
| 12.3b | Generation-tagged handles so late callbacks cannot use recycled slots | node runtime, disk | OPEN | Not in ledger |
| 12.3c | Shrink idle caches under pressure by measured break-even; bounded refill; allocation refusal leaves state retryable | disk, node runtime | OPEN | Not in ledger |
| 12.3d | Never checksum/encrypt/send/persist uninitialized memory; zero padding once; distinct reuse/zeroization rules for ciphertext, plaintext, key buffers | disk, gateway, log | OPEN | Not ledgered (buffer reuse in log writer, ledger 12.2, makes this live) |
| 12.3e | Node-wide pressure and live-byte permits govern the sum of per-volume pools | node runtime, chunk | OPEN | Not in ledger |
| 12.3f | Typed admission bound sets maximum capacity rather than preallocating worst case | node runtime | OPEN | Not in ledger |
| 12.4a | Record base page size; distinguish minor/major faults, compressed/swapped pages, page-cache misses, IO stalls | tooling/benchmarks | OPEN | Not in ledger |
| 12.4b | Bounded prefault scheduling for a measured active working set only | node runtime | OPEN | Not in ledger |
| 12.4c | NUMA-aware shard/first-touch/worker/queue placement; measure remote memory; cap local reserves; pinned IO buffers live until kernel completion | node runtime | OPEN | Not in ledger |
| 12.4d | Huge-page use by per-arena measured policy with pressure behaviour | node runtime | OPEN | Not in ledger |
| 12.4e | Measure dirty-writeback throttling, cold restart/page cache, near-RAM operation; report direct vs buffered fallback; cold behaviour without privileged cache drops | disk, tooling/benchmarks | OPEN | Not in ledger |
| 12.4f | Track cgroup memory.high and PSI; shed bulk admission and trim idle buffers; test trimming and refill latency | node runtime | OPEN | Not in ledger |
| 12.4g | Measure Mantle's own faults under network/device pressure | tooling/benchmarks | OPEN | Not in ledger |
| 12.5a | Benchmarks cover fresh/warm, session churn, cancellation, slow readers, stalled storage, cold restart, near-RAM; report the listed alloc/RSS/fault/CPU/tail/thread metrics incl. socket/TLS/kernel memory | tooling/benchmarks | OPEN | Not in ledger |
| 12.5b | Measure all participating workers or an isolated process; measurement epoch across owners; include refused/retried/cancelled ops | tooling/benchmarks | OPEN | Not in ledger |
| 12.5c | Add same-session batch sweeps, cached/cold entry batches, stalled snapshot recipients, 1/16 warmed volumes, many dormant ranges | tooling/benchmarks | PARTIAL | 256-command session (ledger P04), cold entry runs (ledger P07); rest open |
| 12.5d | Fund infallible map/Arc/clone owners before admitting work | node runtime, meta, range/replica | OPEN | Not in ledger |
| 12.5e | Zero-allocation gates for warmed bounded ops; no post-admission payload-proportional realloc on streaming; measured copy/peak bound per coding scheme; before/after data with regression limits | tooling/benchmarks | OPEN | Not in ledger |
| 12.6a | Inventory every numeric operating constant with its derivation; gate fails an unsupported one | tooling/benchmarks, docs/research | DONE | Ledger 12.6: constants.md, `check-contracts.py` |
| 12.6b | Resolve the constants still open (14 per ledger) | tooling/benchmarks | PARTIAL | Ledger 12.6: batch shape, index size, calibration statistics, backend depth, log per-group credit, drive budget, XML/JSON allowance |
| 12.6c | Per policy value record purpose/units, constraints, objectives, inputs, candidates, selection rule, uncertainty, fallback, rollback; expose selected value and binding constraint | docs/research, node runtime | PARTIAL | Kind and basis recorded (ledger 12.6); full record and runtime exposure open |
| 12.6d | Adaptive-controller parameters (confidence, tolerance, samples, exploration, margins, hysteresis) derived | disk, docs/research | OPEN | Not in ledger |
| 12.6e | Validate sensitivity and candidate coverage; test scale/cancellation/pressure and persisted-layout compatibility | tooling/benchmarks | OPEN | Not in ledger |
| 12.6f | 8 MiB chunk replaced by per-upload constrained sizing with persisted geometry | gateway | OPEN | See 16.2 |
| 12.6g | 64 KiB AEAD segment kept; any new version justified by measured range/tag/crypto costs | s3, docs/research | DONE | Constraint holds: format unchanged |
| 12.6h | 256 MiB segment: read real zone geometry where supported; measure conventional-media tradeoffs; preserve persisted geometry | chunk, disk | OPEN | Not in ledger |
| 12.6i | 32 MiB batch / 1,024 requests / 4,096 fragments per chunk derived from RAM/work/SLO and measured T(bytes, records, depth) | chunk | OPEN | Ledger 12.6: batch shape open |
| 12.6j | 2²² indexed fragments derived from capacity/fragment distribution, heap, volumes, checkpoint bandwidth, restart deadline | chunk | OPEN | Ledger 12.6: index size open |
| 12.6k | 4 MiB frame ceiling and 8 KiB allowance: name header/alignment terms, prove every generated frame fits, gate config before IO | chunk | DONE | Ledger S13: one `MAX_FRAME_BYTES`, allowance removed |
| 12.6l | Pool sizes and reuse fit chosen node-wide from measured reuse/fault/pressure costs | chunk, disk, node runtime | OPEN | Not in ledger |
| 12.6m | Two queued batches/submissions: derive queue credit from service/deadline/memory incl. oversized singletons | log, chunk | OPEN | Ledger 12.6: log per-group credit open |
| 12.6n | 64-Ready quantum replaced by measured byte/work/time slices; each blocking substep bounded | range/replica | OPEN | Ledger 12.6: drive budget open |
| 12.6o | Quarter-lease renewal derived from control-delay/outage distributions, clock/fencing semantics, accepted expiry risk | gateway | OPEN | Not in ledger |
| 12.6p | Calibration defaults (span, step time, ladder, durable count) fitted to confidence, steady state and budgets; censored uncertainty reported | disk | OPEN | Ledger 12.6: calibration statistics open |
| 12.6q | Thread backend depth 256 derived from CPU/RAM/thread allowances; capped search reported | disk | PARTIAL | Capped search reported (ledger S09); derivation open (ledger 12.6) |
| 12.6r | 10,000 parts/5 MiB kept as external bounds; internal manifest fanout/byte/recovery limits derived independently | gateway, meta | PARTIAL | Ledger B08 derives 646 blocks per PUT file; recovery limits not derived |
| 12.6s | 65,536 commands: keep encoding bound, add byte/apply-work budgets | meta | PARTIAL | Byte bound via `max_entry_bytes` (ledger 5.2); apply-work budget open |
| 12.6t | Identifier reservations parameterized by measured rate/fence cost and explicit overhead goal | chunk | OPEN | Not in ledger |

## §13 Constrained networks

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 13.1a | PMTU is a ceiling; operating bulk packet/burst size derived from path rate, latency budget, receiver resources | transport | OPEN | Not in ledger |
| 13.1b | Keep QUIC minima (1,200-byte Initial padding) while allowing smaller established packets; refuse paths below the minimum | transport | OPEN | Not in ledger |
| 13.1c | Airtime/fresh-control test before adopting sibling pacer choices | transport, tooling/benchmarks | OPEN | Not in ledger |
| 13.2a | Choose congestion control on Mantle's own control/append/snapshot/chunk/repair mix over asymmetric, competing, incast, variable links; keep rejected records | transport | OPEN | Not in ledger |
| 13.2b | Validate wire-byte inflight/pacing, ACK-delay correction, monotonic clocks, app-limited detection, bandwidth transitions, idle restart, blackouts | transport | OPEN | Not in ledger |
| 13.2c | Bounded reordering adaptation; PTO still recovers tail loss; reordered retransmits never duplicate application effects | transport | OPEN | Not in ledger |
| 13.2d | PMTU probes bounded in airtime/CPU/memory; probe loss not treated as congestion | transport | OPEN | Not in ledger |
| 13.3a | Separate traffic classes; authorize message kind; bound each class by bytes, work, age, tenant | transport | OPEN | Not in ledger |
| 13.3b | Reserved control opportunities plus minimum repair/cleaning progress and per-tenant fairness (no starvation under strict priority) | transport, node runtime | OPEN | Not in ledger |
| 13.3c | Test competing high-priority clients, tiny-message floods, cancellation storms, worst replication fanout | transport, tooling/benchmarks | OPEN | Not in ledger |
| 13.3d | Rate-limit resends at app and transport layers; no unbounded Raft retry over QUIC retransmit; coalesce obsolete heartbeats only where safe | transport, range/replica | OPEN | Not in ledger |
| 13.3e | Reserve egress/NIC/kernel queue capacity, not only stream slots | transport | OPEN | Not in ledger |
| 13.3f | Classify by semantics/work; large snapshot evidence out of band in bounded slices; integrity work never dropped | transport, range/replica | OPEN | Not in ledger |
| 13.4a | Window ≤ min(stream budget, connection share, node remaining budget, admitted sink capacity) | transport | OPEN | Not in ledger |
| 13.4b | Receiver credit tracks movement into a budgeted stage; bounded whole-chunk assembly slot or durable streaming fragments; Stored only after durable fence | transport, chunk | OPEN | Not in ledger |
| 13.4c | Slow readers: count source, retransmit, response bytes and retained parents; bound streams/dials by live bytes; reduce credit on collapse while funding control; S3 error/retry contract under overload | transport, gateway | OPEN | Not in ledger |
| 13.5a | Progress deadlines derived from bytes remaining, measured rate/tail, caller budget; admission wait, queueing, remote work, disk fence, RTT separated | transport | OPEN | Not in ledger |
| 13.5b | Bound append bytes by rate/deadline policy; test Raft append/snapshot at 8/16/64 kbit/s | transport, range/replica | OPEN | Not in ledger |
| 13.5c | Bounded backoff+jitter; revalidate cert/range/route epochs; resume with authenticated bounded offsets; hedging with measured delay, shared budget, exact identity, loser cleanup; retries keep operation identity | transport, gateway | OPEN | Not in ledger |
| 13.5d | Raft safe under asymmetric partitions, minority reachability, lagging storage, leader moves; qualification states infeasible rates | range/replica, transport | OPEN | Not in ledger |
| 13.6a | Qualification axis: rates 8 kbit/s–100 Gbit/s, asymmetric, PMTU vs latency-optimal size | transport, tooling/benchmarks | OPEN | Not run |
| 13.6b | Qualification axis: LAN to 1,000+ ms RTT, jitter, ACK compression, delayed ACKs, buffer sizes, incast, bufferbloat, competing flows | transport, tooling/benchmarks | OPEN | Not run |
| 13.6c | Qualification axis: 0–10% random and burst loss, duplication, reordering, MTU shrink/black holes/send refusal | transport, tooling/benchmarks | OPEN | Not run |
| 13.6d | Qualification axis: capacity steps/collapse, 5–120 s outages, one-way failures, NAT/address change, route retirement, cert rotation, reconnect storms | transport, tooling/benchmarks | OPEN | Not run |
| 13.6e | Qualification axis: real application mix (cold handshake, tiny durable ops, Raft traffic, large streaming, snapshot, repair, stalled disk, slow HTTP readers) | transport, tooling/benchmarks | OPEN | Not run |
| 13.6f | Qualification axis: scale/authority (one voter to many cells, malicious priority use, old-epoch callbacks, quorum loss/recovery) | transport, tooling/benchmarks | OPEN | Not run |
| 13.6g | Report delivered/durable bytes, amplification, dispositions, p50/p99/p999, repair progress, election/recovery time, memory, allocs/faults, CPU, queue age, cancellation cleanup, first-byte and tail stalls | tooling/benchmarks | OPEN | Not run |

## §14 Media adaptation and the object path

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 14.1a | Bounded reusable/native-async issue population instead of per-volume threads and per-batch scoped threads, keeping the flush guarantee | chunk, node runtime | OPEN | Not in ledger |
| 14.1b | One scheduler/admission authority per resolved bottleneck with a storage graph of shared ancestors; child budgets for volumes, WAL, Engine, fg, cleaner, scrub, repair | node runtime, disk | OPEN | Not in ledger |
| 14.1c | Account IO count/bytes, service time, flushes, live buffers, CPU, reserved extents, oldest work; acquire before allocating/coding; atomic credit transfer; reserve WAL/renewal and background progress | node runtime | OPEN | Not in ledger |
| 14.1d | Cleaner, scrub and cold-log reads go through device admission (they bypass the client Gate today) | chunk, log, node runtime | OPEN | Not in ledger |
| 14.1e | Flush fence acknowledges only writes complete before it; one file's flush certifies no other; failed/ambiguous flush fences the domain; late completions cannot publish success | chunk, log | PARTIAL | Log fences and answers only confirmed frames (ledger S01 round three, R08); cross-file/domain fencing open |
| 14.1f | One active owner per opened volume/log incl. alias paths and incarnations | chunk, log, disk | OPEN | Not in ledger |
| 14.1g | Stalled device calls do not stop healthy devices or make an indefinite shutdown look successful | node runtime, chunk | OPEN | Not in ledger |
| 14.2a | Extend identity: stable backend identity, raw capacity, file-specific direct-IO alignment, topology completeness, firmware/caching generation, persistence semantics, provenance | disk | PARTIAL | Raw capacity/identity (ledger 6.2); rest open |
| 14.2b | Bound composite-graph walk, dedupe members/aliases, detect cycles, report truncated/unknown topology | disk | PARTIAL | Each composite member described once (ledger 12.6); walk bound/cycle/truncation not ledgered |
| 14.2c | Files on one SSD never counted as independent redundancy; write-cache/FUA hints never taken as power-loss durability | placement/repair, disk | OPEN | Not in ledger |
| 14.2d | Reject known unsupported host-managed zones before formatting | disk | DONE | Ledger B10 |
| 14.2e | Persisted versioned DevicePlan deriving widths, batch bytes/deadlines, direct/buffered/preallocation, cleanup/repair shares, reserve from capabilities and mixed-load measurements; invalidated on change | disk, chunk | OPEN | Not in ledger |
| 14.2f | Online adaptation: bounded probes, confidence, hysteresis, last valid safe plan; never weaken durability by label | disk | OPEN | Not in ledger |
| 14.2g | Laptop NVMe/Apple SSD plan measurements (tiny durable objects, QD mixes with WAL flushes, first allocation, energy, sleep/wake, pressure) | disk, tooling/benchmarks | OPEN | Not in ledger |
| 14.2h | Server NVMe/PLP plan measurements (shared controller, NUMA/coding load, batch/deadline/width sweep, near-full/GC/thermal, failed/late completions) | disk, tooling/benchmarks | OPEN | Not in ledger |
| 14.2i | SATA/SAS SSD plan measurements (queue mixes, overwrite/GC, discard/prewrite break-even, endurance, cache and power-loss semantics) | disk, tooling/benchmarks | OPEN | Not in ledger |
| 14.2j | HDD plan measurements (seek/transfer crossover, ranged-read coalescing, distant WAL/index writes, bg sequential under random fg, slow sectors) | disk, tooling/benchmarks | OPEN | Not in ledger |
| 14.2k | RAID/virtual/network/FUSE plan measurements (shared saturation, remote cache/RTT, direct fallback, detach/reconnect, incomplete topology) | disk, tooling/benchmarks | OPEN | Not in ledger |
| 14.2l | ZNS/host-managed SMR plan measurements (zone size/capacity/limits, append/write pointers, reset/recovery, refusal before unsupported writes) | disk | PARTIAL | Refusal done (ledger B10); zone measurements need a zoned backend |
| 14.3a | Cleaner runway extended to concurrent WAL/Engine/scrub/repair work and degraded throughput | chunk, node runtime | OPEN | Not in ledger |
| 14.3b | Measure actual cleaning amplification; refuse/throttle workloads without sustainable reserve | chunk | PARTIAL | Unprofitable victims excluded, futile pass answers `Full` (ledger P06, R06); amplification measurement open |
| 14.3c | Incremental aggregates and bounded candidate indexes; revalidate victim incarnation/live data before relocation; keep empty-device pacing | chunk | PARTIAL | Ledger P06; victim selection still scans |
| 14.3d | Aggregate urgency/fairness for damage-triggered scrub across many devices | chunk, node runtime | OPEN | Not in ledger |
| 14.3e | Qualify largest node/rack/zone loss and a second failure near capacity before feeding a repair rate into the durability model; repair placement matches B09 model | placement/repair | OPEN | Not in ledger |
| 14.4a | Range GET uses bounded iteration over pieces/spans (no complete-vector planning) and only necessary segments | gateway | DONE | Ledger §16 (16.7): GET seeks one extent at a time, holds two blocks at most |
| 14.4b | Measure tiny-object replication/append packing against wide EC | gateway, tooling/benchmarks | OPEN | Same measurement as 7.2a |
| 14.4c | No packing/recoding/dedup/zero-copy COPY before adoption/release/retention/reader fences; recode needs new layout identity, verified chunks, atomic Block publication, fenced retirement | meta, gateway | DONE | Constraint holds: none built |
| 14.4d | Preflight each PUT/part's permitted length and manifest/renewal work | gateway | DONE | Ledger B08, 7.1 part-number row |
| 14.4e | Backups: consistent metadata/data/key cut with protected references during export; restore key generations, ownership/placement, versions/retention, chunk integrity without exposing root keys; test old keys after rotation and partial-cell loss | cells/fleet, gateway | OPEN | Not in ledger |
| 14.4f | Ciphertext migration without resealing only when file/key/AEAD identities and policy stay valid | cells/fleet | OPEN | Not in ledger |

## §15 Capacity and qualification

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 15.1a | Laptop runs the same ack/checksum/encryption/ownership/cleanup contracts with one member/device and honestly reports single-device durability | node runtime | OPEN | Not in ledger |
| 15.1b | Regional cell states max groups/objects/placement entries/connections, live bytes, fg SLO, admitted failure size | cells/fleet | OPEN | Not in ledger |
| 15.1c | Validate the M_node memory model against actual RSS | node runtime, tooling/benchmarks | OPEN | Not in ledger |
| 15.1d | Per-peer network ceiling is a share of the node budget; kernel socket queues and pinned pages accounted | transport, node runtime | OPEN | Not in ledger |
| 15.1e | Account leader heartbeat traffic L(r−1)/h at target densities | node runtime, range/replica | OPEN | Not in ledger |
| 15.1f | Event-driven deadlines and safe quiescence instead of ticking every group, with a per-group quorum wake/recovery protocol | node runtime, range/replica | OPEN | Not in ledger |
| 15.1g | Bound cell neighborhoods and use hierarchical control dissemination (no full-fleet mesh) | cells/fleet, transport | OPEN | Not in ledger |
| 15.1h | Router-map refresh via bounded versioned deltas/snapshots with stale-route fencing, incl. restart/admission storms | cells/fleet | OPEN | Not in ledger |
| 15.1i | Reserve disk/NIC/CPU/memory/space at node/rack/cell scope before launching repair or migration | placement/repair, cells/fleet | OPEN | Not in ledger |
| 15.1j | Bound total and active groups separately; compare shared vs per-range Engine/cache; fixed capacities refuse with admission and clean up through restart | node runtime, engine | OPEN | Not in ledger |
| 15.2a | Deterministic many-group simulation host with the real phased owner, shared-device scheduler and chosen transport | range/replica, node runtime | OPEN | Ledger 5.9: needs node process |
| 15.2b | Inject delayed/failed/ambiguous flushes, completion loss, stopped workers, credit starvation, stale callbacks, cancelled clients, corrupt snapshots, receiver pressure | range/replica, node runtime | OPEN | Single-group faults only (ledger 5.9) |
| 15.2c | Check per event: aggregate budgets, every admitted request's disposition, ownership, applied-prefix reads, durable voting, repair progress, token return | range/replica, node runtime | OPEN | Not in ledger |
| 15.2d | Include hot/cold groups, abusive tenant, correlated elections, slow HDD, healthy device continuing while another stalls | range/replica, node runtime | OPEN | Not in ledger |
| 15.2e | Fast-path tests assert actual use (ahead batches, shared flush coverage, early sends, reused slots, fewer copies, prefault slices, reorder adaptation, chunked snapshots) | tooling/benchmarks | PARTIAL | Simulation asserts staged readies and duplicates exercised (ledger 5.1, 5.9); other mechanisms not built |
| 15.2f | Mutation tests break each fence/accounting/ownership rule; keep minimal crash histories and rejected experiments; no oracle that repeats the implementation's assumptions | tooling/benchmarks | PARTIAL | Per-fix removal checks throughout ledger; many-group composition open |
| 15.3a | Readiness stage one laptop: full S3 profile, restart/durability, single-device semantics, bounded memory/threads/energy, full-disk/pressure/sleep-wake, key restore | node runtime, tooling/benchmarks | OPEN | |
| 15.3b | Readiness stage one device, many volumes/ranges: shared WAL/chunk/Engine/clean/scrub/repair, aggregate QD/live bytes, funded completion/renewal, flush fence accuracy, stalled-device isolation | node runtime, tooling/benchmarks | OPEN | |
| 15.3c | Readiness stage regional cell: group densities, real engines/processes/sockets, tiny/large/10K-part objects, lane fairness, catch-up storms, node/rack/zone loss near capacity | cells/fleet, tooling/benchmarks | OPEN | |
| 15.3d | Readiness stage harsh network: §13 matrix with real packets, cold handshakes, durable traffic, collapse, outage, asymmetry | transport, tooling/benchmarks | OPEN | |
| 15.3e | Readiness stage global fleet: control-plane cardinality/convergence, stale routes/epochs, migration/retirement, repair headroom, key authority, rolling upgrades, geo RPO/RTO | cells/fleet, tooling/benchmarks | OPEN | |
| 15.3f | Readiness stage allocation/media portability: Linux/macOS/Windows backends, fresh/warm/churn/pressure curves, alloc/copy/fault/RSS, media-specific mixed-load SLOs | disk, tooling/benchmarks | OPEN | |
| 15.3g | Open-loop tests with admission/refusal measurement and coordinated-omission correction; report samples, failures, rejected settings, identity, throughput and tails; separate virtual, real and harness time | tooling/benchmarks | OPEN | Not in ledger |
| 15.4a | Run the full current-tree gates across all six targets rather than citing historical suites | tooling/benchmarks | PARTIAL | Gates per commit (CLAUDE.md); six-target execution not ledgered |

## §16 Massive objects

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 16.1a | Large objects go through multipart; a single PUT beyond the supported size is refused before writing | gateway | DONE | Ledger B08 |
| 16.1b | Objects past 48.8 TiB use a separate multi-object dataset manifest with its own publication/retention semantics; no promise of atomic multi-object publication | gateway, meta, docs/research | OPEN | Not in ledger |
| 16.1c | For known size S choose a legal part size with ceil(S/p) ≤ 10,000 and reserve completion capacity before transfer | gateway | OPEN | Ledger §16: per-upload part sizing open |
| 16.1d | Unknown final size: declared upper bound, conditional growth policy or multi-object representation | gateway | OPEN | Not in ledger |
| 16.1e | Renewals apply only to the uncommitted part; completed parts rely on durable Name ownership | gateway, meta | DONE | Ledger B02, R16, R20 (part marks, adoption) |
| 16.2a | A large part streams through the same bounded block/segment slots as a small one | gateway | DONE | Ledger §16 PUT window (`Holding::window`), B08 |
| 16.2b | Node-wide byte/work allowance for concurrent parts | node runtime, gateway | OPEN | Ledger §16: "the windows a node admits" open |
| 16.2c | Part size and internal transfer/checkpoint size chosen separately from rate, loss/outage, seek cost, replay budget; no short whole-request deadline | gateway | OPEN | Not in ledger |
| 16.2d | Adaptive bounded concurrent parts/block flights covering BDP and durable latency, capped by aggregate bytes/coding/sink, backing off under congestion | gateway, node runtime | PARTIAL | Windows exist (ledger §16); size from memory and measured latency open |
| 16.2e | Per-upload optimizer: explicit objective, measured inputs (goodput, RTT, outages, per-part overhead, CPU, MTU, curves, credits), derived part count, independent block/shard/frame counts | gateway | OPEN | Ledger §16 |
| 16.2f | Optimizer hard constraints (legal sizes/counts, manifest/Entry bound, replay duration, peak ownership, coding/key limits, sink rate, funded control/repair); infeasible returns reason and degraded/alternate plan | gateway | OPEN | Not in ledger |
| 16.2g | Bounded candidate search with confidence/error bounds and optimality gap; fit on real progress, held-out traces; switch only when benefit exceeds cost; derived horizon/confidence/hysteresis/exploration/margin; record inputs, plan, predicted/observed cost, reason | gateway | OPEN | Not in ledger |
| 16.2h | Optimize the discrete count over aligned partitions incl. final part; deterministic for the same state; overflow or missing evidence gives explicit uncertain/infeasible | gateway | OPEN | Not in ledger |
| 16.2i | Expose part-size recommendations/transfer-manager policy; never renumber or resize an accepted part | gateway, s3 | OPEN | Not in ledger |
| 16.2j | Adaptive geometry uses authoritative extent-driven addressing and immutable per-block recorded layout; readers/repair/GC compatible; committed bytes never reinterpreted | gateway, meta | PARTIAL | GET addresses by extents and part plaintext lengths (ledger §16); per-block layout epoch not ledgered |
| 16.3a | Generalize the one Going block into an admitted bounded flight window with per-block ownership, bounded completion order, placement prefetch, paced transfers | gateway | DONE | Ledger §16: `Holding::window`, `blocks_go_down_together_as_the_window_admits` |
| 16.3b | Window sized as ceil(target rate × chain latency / B), capped by reservations | gateway, node runtime | OPEN | Ledger §16: node-admitted windows open |
| 16.3c | Batch independent Block metadata updates through shared WAL receipts | gateway, meta | OPEN | Not in ledger |
| 16.3d | Name publication stays behind validated body/checksum and durable File ownership; all prescribed chunks before Block publication unless a proven policy | gateway | DONE | Constraint holds (ledger B11 checks; existing order) |
| 16.3e | Bulk bytes never routed through Raft; NIC, coding/crypto, DRAM and disk bandwidth fit the 100 Gbit/s expansion | gateway, tooling/benchmarks | OPEN | Not measured |
| 16.4a | Durable client/transfer-manager record (upload ID, source identity/length, part ranges, ETags/checksums, completion); ListParts reconciliation with pagination | gateway, s3 | OPEN | Ledger §16: resumption records open |
| 16.4b | Internal resumable gateway→storage protocol: bounded attempt manifest, immutable identities, verified offsets, received/durable/published states | gateway, chunk | OPEN | Not in ledger |
| 16.4c | Uncertain replies retried as the same immutable operation or reconciled; fresh file/key per replaced part; resumed ciphertext identical; nonce/index/last-segment binding kept | gateway, meta | PARTIAL | Copies answered as their first delivery (ledger R16, R20); resumption not built |
| 16.4d | Download resume pins version/generation across ranges | gateway | OPEN | Not in ledger |
| 16.4e | Upload source identity/length checked; changed source fails or starts a new attempt | gateway | OPEN | Not in ledger |
| 16.4f | Credential/cert/key-generation renewal, route retirement, gateway restart during long transfers without discarding completed parts | gateway, cells/fleet | OPEN | Not in ledger |
| 16.5a | Indexed deadline scheduler for renewals, no per-turn scan | gateway | DONE | Ledger B08 |
| 16.5b | Bounded/jittered renewal batches and reserved durable renewal/control capacity | gateway, node runtime | OPEN | Ledger B08, §16: batched renewals open |
| 16.5c | Expiry returns a recoverable refusal and cleans orphan data safely, never publishes a damaged part | gateway, meta | PARTIAL | Property test accepts only `Expired`/`Released` with no version published (`crates/gateway/tests/object_path.rs`); orphan cleanup after expiry not asserted; not ledgered |
| 16.5d | max_entry_bytes, WAL frame/retention and command-work bounds admit the worst supported Complete; encoded bounds checked before finalization | gateway, range/replica, log | PARTIAL | `largest_entry_bytes` 561,804 and gateway keeps Complete within it (ledger §16); node check of each range's bound open |
| 16.5e | Completion driver derives size/ETag/checksum from verified parts, validates sum and aggregate limit, reconciles replacement/abort/conditional | gateway, meta | DONE | Ledger §16 (`a_complete_is_checked_against_its_parts`, `a_completed_upload_is_its_parts`), R12, R13 |
| 16.5f | Part number/size enforced on every route incl. internal callers | meta, gateway | DONE | Ledger 7.1 part row (`parts_outside_s3s_limits_are_refused`) |
| 16.5g | Keep the initiation checksum algorithm/type; validate mode-specific consecutive numbering, part values and aggregate | gateway, meta | PARTIAL | Full-object CRC or composite derived from part rows (ledger §16); mode-specific numbering validation not ledgered |
| 16.6a | Full-object CRC combined from part values, no reread | gateway, s3 | DONE | Ledger §16 completion |
| 16.6b | Validate exact length, part order, algorithm/type, header/trailer semantics before Name publication | gateway, s3 | PARTIAL | Length/order checked (ledger 7.1, §16); trailers need HTTP path |
| 16.6c | Fuse mandatory passes (hash while streaming, in-place encrypt, checksums during coding); measure MD5/AES-GCM/RS/CRC together | gateway, tooling/benchmarks | PARTIAL | SSE-C one MD5 pass (ledger B11); joint cost measurement open |
| 16.6d | Mode-aware ETag semantics resolved | gateway | DONE | Ledger B11 |
| 16.6e | Reviewed per-key byte/invocation AES-GCM security budget | s3, docs/research | OPEN | Not in ledger |
| 16.6f | Persist key generations; rotation/recovery without losing old multipart keys | gateway, cells/fleet | OPEN | Not in ledger |
| 16.6g | Range responses verify every returned segment's AEAD and stored checksums | gateway | PARTIAL | GET library reads by segment (ledger §16); no ledger test of AEAD/checksum failure on a range |
| 16.7a | Seekable bounded cursor through part and block extents; no whole-plan materialization; open only active part keys | gateway | DONE | Ledger §16 (`a_get_holds_two_blocks_at_most`) |
| 16.7b | Object plaintext prefixes indexed separately from child stored offsets; translate within the child | gateway | DONE | Ledger §16 (`an_object_of_parts_reads_by_its_parts_plaintext`) |
| 16.7c | Qualify video (first byte, playback, seeks, tail metadata, many viewers) and datasets (footer/range scans, row groups, shuffled, export) | gateway, tooling/benchmarks | OPEN | Not in ledger |
| 16.7d | Prefetch from observed access/media behaviour; cancel obsolete seeks; HDD sequential runs vs NVMe parallel spans | gateway, chunk | OPEN | Not in ledger |
| 16.7e | Parallel range GETs share disk/network/tenant/coding budgets; slow reader holds only its window; cancellation releases after kernel/network ownership ends | gateway, node runtime | PARTIAL | Per-GET window (ledger §16); shared budgets open |
| 16.8a | Sustained ingest past burst caches, thermal steady state, GC, near-full; count physical writes incl. parity, index, relocation, repair | chunk, tooling/benchmarks | OPEN | Not in ledger |
| 16.8b | Free-space and funded cleaning/repair reserves before admitting another large part; cell-full transfer fails/recovers per reservation policy | gateway, node runtime | OPEN | Not in ledger |
| 16.8c | Qualify cold restart, index rebuild and scrub at physical fragment cardinality under foreground bulk; no eager cloning of huge objects/snapshots | chunk, tooling/benchmarks | OPEN | Not in ledger |
| 16.8d | Fragment slots, index heap/checkpoints, reverse rows and cleanup/repair fit the envelope of a 10,000 × 5 GiB object | chunk, placement/repair | OPEN | Not in ledger |
| 16.8e | Derive max_fragments from volume bytes/minimum fragment size and heap/recovery/checkpoint budgets; network frames never become durable fragments | chunk | OPEN | Ledger 12.6: index size open |
| 16.8f | Checkpoint streams an immutable view through bounded frames | chunk | DONE | Ledger S13, §16 (16.8) |
| 16.8g | Measure the writer's foreground pause and recovery at declared cardinality (2²²) | chunk, tooling/benchmarks | OPEN | Ledger §16: "yet to be measured" |
| 16.8h | State the supported object-size/service-class durability envelope from measured repair bandwidth and occupancy; uploads cannot consume repair bandwidth | placement/repair, docs/research | OPEN | Not in ledger |
| 16.9a | Experiment: 100/500 GiB video, 2/5/10 TiB dataset, max manifest — exact length/content/checksum, bounded memory/work, fits metadata limits | tooling/benchmarks | OPEN | Ledger §16: qualification runs open |
| 16.9b | Experiment: single part vs multipart/block concurrency — durable throughput, amplification, alloc/copies/faults, p99 interference, bottleneck | tooling/benchmarks | PARTIAL | Round-trip counts by window (ledger §16 `mantle bench gateway`); full experiment open |
| 16.9c | Experiment: adaptive size/count/concurrency vs fixed and trace-optimal baselines | tooling/benchmarks | OPEN | Optimizer not built |
| 16.9d | Experiment: 8/64/256 kbit/s and 100 Mbit/s with outages — durable progress, replay, renewal load, bounded state | tooling/benchmarks | OPEN | Not in ledger |
| 16.9e | Experiment: gateway/client restart after each publication step — exact disposition reconciled, completed parts survive | gateway, tooling/benchmarks | OPEN | Not in ledger |
| 16.9f | Experiment: part replacement, abort, Complete timeout/retry, conditional failure, lifecycle expiry — winner keeps children, losers freed, late workers fenced | gateway, meta | PARTIAL | Model/property coverage (ledger B02, R16, R20, orphan simulation); lifecycle expiry and real-process runs open |
| 16.9g | Experiment: seeks/scans during ingest, cleaning and disk/node/rack loss — bounded latency, repair progress, no mixed-version concatenation | tooling/benchmarks | OPEN | Not in ledger |
| 16.9h | Experiment: sustained near-full SSD/HDD/RAID/cloud volumes and cold restart — reported device mode and plan, bounded RAM/recovery, no durability downgrade or cleanup deadlock | tooling/benchmarks | OPEN | Not in ledger |

## §17 Remediation stages and exit criteria

| ID | Requirement | Subsystem | Status | Evidence / what remains |
|---|---|---|---|---|
| 17.1a | Stage 1: keep S02–S11/B01–B07 fixes; resolve S01 final-frame ambiguity; close S12; fix S13 | log, chunk, meta | DONE | Ledger S01 (three rows), S12, S13, plus S14–S17 |
| 17.1b | Stage 1: continue crash/corruption/reclamation mutation schedules through real persistent metadata/chunk graphs | meta, engine, chunk | PARTIAL | Chunk graphs real; metadata runs on model engine (production engine open, ledger 5.3) |
| 17.1x | Exit 1: every reported loss/deletion schedule preserves reachable acknowledged data or returns typed damage/refusal; repeated reopen/retry/takeover safe | log, chunk, meta | DONE | All S/B/R rows fixed with regressions |
| 17.2a | Stage 2: resolve B08 and B11, correct B10, qualify B09 claims | gateway, disk, placement/repair | PARTIAL | B08/B10/B11 fixed; B09 qualification open (see B09a/B09b) |
| 17.2b | Stage 2: enforce aggregate byte/work admission | node runtime | PARTIAL | See 5.2 |
| 17.2c | Stage 2: exact upload/manifest feasibility and cancellation cleanup | gateway | PARTIAL | Feasibility bound (ledger B08, §16); cancellation cleanup not ledgered |
| 17.2d | Stage 2: reconcile gateway expiry tests with the handover/retry contract | gateway | PARTIAL | See 16.5c: property test now accepts `Expired`; cleanup not asserted; not ledgered |
| 17.2e | Stage 2: inventory and derive every operating constant | tooling/benchmarks | PARTIAL | Ledger 12.6: inventory done, 14 open |
| 17.2f | Stage 2: implement and validate the per-upload size/count/concurrency optimizer | gateway | OPEN | See 16.2 |
| 17.2x | Exit 2: bounded allocations/work shown under sustained overload; a freed resource lets pending work proceed; overload never changes safety | node runtime, log, range/replica | PARTIAL | Freed room resumes a Ready (ledger S04), queue bound (S03); sustained-overload demonstration open |
| 17.3a | Stage 3: integrate production engine, authenticated HTTP S3 path, key authority, chunk reconciliation | engine, s3, gateway, placement/repair | OPEN | |
| 17.3b | Stage 3: measure idle/busy memory, tiny/large objects, restart time, disk-full cleanup, real laptop storage | tooling/benchmarks | OPEN | |
| 17.3c | Stage 3: integrated S3 conformance and real-process linearizability/durability failures | tooling/benchmarks | OPEN | |
| 17.3d | Stage 3: required gates complete on the actual tree | tooling/benchmarks | PARTIAL | See 15.4a |
| 17.3x | Exit 3: a complete supported API profile serves durable objects on one laptop with stated limits and recoverable failures | node runtime | OPEN | |
| 17.4a | Stage 4: receipt-driven multi-range persistence | node runtime, range/replica | PARTIAL | `begin` (ledger 5.1); scheduler open |
| 17.4b | Stage 4: authenticated bounded transport | transport | OPEN | |
| 17.4c | Stage 4: streaming snapshots | range/replica, engine | OPEN | |
| 17.4d | Stage 4: topology-aware placement | placement/repair | OPEN | |
| 17.4e | Stage 4: repair/rebalance fences | placement/repair, meta | OPEN | See 8.4 |
| 17.4f | Stage 4: physical-device adaptive profiles | disk | OPEN | See 6.1, 14.2 |
| 17.4g | Stage 4: remove whole-buffer/materialization and full-scan costs of §§11–14 and measure alloc/copies/faults/throughput/tails | chunk, log, meta, gateway | PARTIAL | P01–P08 and 12.2 frame copy done; many 12.2 rows and resource measurements open |
| 17.4h | Stage 4: finish the bounded resumable multipart/GET/completion pipeline; qualify massive transfers | gateway | PARTIAL | GET, completion, windows done (ledger §16); resumption, batching, qualification open |
| 17.4i | Stage 4: validate adaptive sizing against measured objectives and trace-optimal baselines, preserving layout/crypto identity | gateway | OPEN | |
| 17.4j | Stage 4: qualify extremely slow/congested/unstable paths | transport | OPEN | See 13.6 |
| 17.4k | Stage 4: fast track and speculative apply evaluated only with their own safety and crossover evidence | range/replica | OPEN | See 5.6, 11.5, 11.6b |
| 17.4l | Stage 4: device/node/rack/zone failure and near-full tests on supported OSes and backends | tooling/benchmarks | OPEN | |
| 17.4x | Exit 4: the largest supported regional cell meets declared latency/resource/rebuild limits in declared failures | cells/fleet | OPEN | |
| 17.5a | Stage 5: finish root map, migration, rollout/recovery and geo contract | cells/fleet | OPEN | |
| 17.5b | Stage 5: load-test control-plane cardinalities, router dissemination, placement and repair at declared limits | cells/fleet, tooling/benchmarks | OPEN | |
| 17.5c | Stage 5: publish measured bounds and failure semantics | docs/research | OPEN | |
| 17.5x | Exit 5: growth, retirement, stale routes and region failover preserve ownership guarantees with measured RPO/RTO and isolation envelope | cells/fleet | OPEN | |
| 17.6a | Release qualification proves persistence, ownership transfer, cleanup, admission and distributed drivers together; adaptive decisions carry the same guarantees and evidence | tooling/benchmarks | OPEN | |
