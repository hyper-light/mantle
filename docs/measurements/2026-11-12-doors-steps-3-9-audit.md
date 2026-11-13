# The Lean doors plan, steps 3 to 9: each acceptance criterion against the evidence on the branch

**Question.** For each of steps 3 to 9 of the Lean doors plan (kiln plan
01a170a2cb0176d4ae9329235025dea0), which acceptance criteria does origin/lean-doors at e511ae15
show met, and which are gaps?

**How.** Each criterion as the plan states it, beside the file and section, run or test that bears
on it. Met means the evidence measures the criterion as the criterion states it: by its own
instrument, on its machines, against its stated baseline. Anything less is a gap, and the gap says
what is missing. Read on 2026-11-12 from docs/measurements/2026-11-06-doors-baseline.md (below,
"the baseline"), docs/research/71-the-doors-costs.md ("research/71") and its "Step N, after"
sections, the commits `git log origin/main..origin/lean-doors`, the vendored hyper-raft crates at
33733b5, and the raw outputs under ~/.cache/claude-scratch/doors/ on the box.

Two facts shape most of the rows:

- **The baseline is not yet step 3's baseline.** It ran the harness as it stood at 4711a46a.
  The harness has changed since: it now counts refused, unanswered, skipped and differed operations
  apart, it reads only acknowledged objects, and it reports wire bytes, datagrams and peak memory
  per request and per connection. Every mantle case of that run also failed at the controls.
  Steps 5 to 7's "before" counts come from b39a1173 and 44c6d21e builds run beside each step. They
  were not compared against the baseline's counting arm. So no "rises by no more than 0% compared with step
  3's recorded baseline" criterion can be met until the baseline arm is re-run. That arm is the
  graft branch doors-baseline-graft: 4711a46a with the final harness.
- **Ada's sequencing.** Timed before/after matrices are step 10's: implement, then merge, then test.
  Where a criterion is timed (A/A bands, p99.9 under busy), the row says "step 10" and the gap
  stays open until step 10 runs it.

## Step 3: baselines recorded beside the comparators, before any change

| criterion | evidence | state |
|---|---|---|
| Expected cases (door × operation × size × condition × machine × arm × round) not accounted is 0 | The baseline, "Accounting": the box's 78 planned cases, 36 completed, 18 failed, 24 excluded, 0 not run (trial-report.md). | **gap.** The box alone is accounted. The Mac and its VM lane are "to come", so their cases are neither planned nor accounted. All 18 mantle cases failed at the controls, so mantle's slope and handshake lines come only from six short control cases. |
| Transfers behind each reported p99.9 ≥ 3,688 | Sized phases asked 368 operations at most, so their p99.9 is a dash. Only the 10,000 set's phases (5,000 a door) carry one. The harness dashes any quantile its samples do not resolve. | **met as reported, gap as intended.** No p99.9 is reported on fewer than 3,688. But no sized cell has one, and the 100,000 set (the plan's default) was cut. |
| Each comparator's revision, configuration and durability setting is recorded beside its numbers | The baseline, "Comparators and their durability"; mantle-compare/COMPARATORS.md, copied into tables.md. | **met** for the box: Garage, SeaweedFS, RustFS, secnetperf and quinn-perf. MinIO runs on the Mac, which has not run. |
| Each cell's A/A band is recorded | The baseline, "A/A band": per condition, the median and p90 of \|A2 − A1\|/A1. | **gap.** The bands are aggregated per condition, not recorded per cell. There is one round, so one pair per cell. Effects under about 30% are not resolved (the baseline's own words). |
| The raw outputs of every run, slow ones included, are kept | ~/.cache/claude-scratch/doors/baseline-2026-11-06/raw (397 files), controls/raw, bin/ with SHA256SUMS and COMMIT. | **met** for the box run. |
| Test: Williams batches on the VM lane, the box and the Mac under every condition, comparators interleaved; tables regenerate from raw | scripts/doors-trials.sh and doors-tables.py, box only. One Williams cycle. The native door's 500 ms relay was excluded (not in the harness). | **gap.** The Mac and the VM lane have not run. The relay condition has not run. The tables do not yet read the harness's new fields (wire bytes, datagrams, peak per request and per connection, harness peak and threads, refused, unanswered, skipped, differed). |

## Step 4: every counted cost attributed to a site, with the first two questions answered

| criterion | evidence | state |
|---|---|---|
| Share of the recorded steady-state operation's allocations and reallocations attributed to a named site is 100%, every cell | research/71, "Attribution checks". In 5,130 windows, recorded + recorder = `Counting`'s count, exactly (runs/s-*/attrib.txt). | **met** on the box, for the 8 sizes × 11 operations × 2 doors and the listing. Some sizes were reused from the build before the rebase, as research/71 states. |
| Page faults in each sampled window (Linux, period 1) with no attributed call stack is 0 | research/71, "Attribution checks". Of the user-mode faults, 99.4% resolve to a mantle or dependency frame. 12,895 end in glibc's `sysmalloc`, where the frame-pointer chain stops in libc. The 13,807 kernel-mode faults have no stack (`perf_event_paranoid` 2), attributed by elimination. In 14 cells perf's count fell short of the kernel's. | **gap.** 13,807 kernel-mode faults are unstacked. 12,895 stop in libc with no caller. The criterion's instrument is DWARF stacks; the run used frame pointers because perf's libdw unwinder stopped at Rust frames. The box still runs `perf_event_paranoid` 2 (read 2026-11-12), which keeps perf to user mode; stacking kernel-mode faults needs 1 or less, a root setting that is the owner's. |
| Requests above the busy run's p99.9 in the traced run with no attributed stage is 0 | research/71, "Tails": 14 requests above p99.9 across two busy runs, 0 unattributed. | **met** on the box. |
| Every ledger site carries its two answers with measurements; no product code changed | research/71, "The sites" (22 site rows, each with "needed at all?" and "how much?"), "The candidates" (8 verdicts). Instruments on diag-doors fd1aac8d, never merged. | **met.** |
| Test: the diagnostic build's profiling runs on the box and the VM lane, busy and quiet; raw profiles kept and named | The box: ~/.cache/claude-scratch/doors/ledger/, named in research/71, "What the raw data is". | **gap.** The VM lane has no profile. |

## Step 5: native door, a body crosses the client, the host and the transport without a copy or growth it does not need

| criterion | evidence | state |
|---|---|---|
| Allocations per steady-state native PUT and GET, every size, counting build, box and VM lane, rise by no more than 0% against step 3's baseline | research/71, "Step 5, after": a6d1ad29 against b39a1173, the box, sizes 1 B, 64 KiB, 8 MiB, 64 MiB. Native PUT 64 MiB rose 445,050 → 614,720 (n 4, with SlowDown asks again). | **gap.** The comparison was not made against step 3's baseline. It covers 4 of 8 sizes, the box only. The 64 MiB PUT rose. |
| Counted events per operation at each native site step 4 marked unneeded is 0 | Cut in mantle (a6d1ad29): the events Vec, `drive`'s lists, plane copies, the scheme, status lines, the chunk copies, `Get::finish`. Each has a counting test at 0 or its floor (research/71's table). Cut in hyper-raft 3d6640f (7dec16b3): `Owner::pairs`, the stream's piece queue, a packet's credit record. Still in vendor at 33733b5: `ByteSlice::pop_chunk`'s `to_owned` copy (vendor/hyper-quic/src/connection/streams/send.rs:245), the retransmits `Box` (`get_or_create`, spaces.rs:944), the receive ring's growth (`Ring::resize`, vendor/hyper-rt/src/udp/batched.rs:376), and per-datagram header splits (packet.rs `split_to(..).freeze()`). | **gap.** The QUIC copy per write and the per-packet records remain. Research/71 says 8 MiB PUT "keeps its per-datagram vendor sites (about 10,500 each)". No stacks run on the current build shows which are 0. |
| Reallocations per native operation of buffers whose final length is known when made is 0 | Step 7's counting tests (encoding_allocs.rs) cover the codec's messages; steps 6–7 measured left 3–115 reallocations a request "outside the door (the node's and hyper-quic's tables)". | **gap.** No call-stack record of reallocations on the current build sorts known-length buffers from growing tables. |
| A node's peak live heap above idle during a 10 GB native PUT and GET, minus the stated bound, is ≤ 0 | none | **gap.** Not measured. The bound "window of blocks plus one piece" is not stated as a number. |
| Minor page faults per steady-state native operation, every cell, rise by no more than 0% against step 3 | research/71, step 5: native GET 8 MiB faults rose 4,645 → 5,582 (run B) and 6,463 → 7,214 (run A). Research/71 says the step "does not explain" this. | **gap.** A rise, unexplained, and not against step 3's baseline. |
| Timed A/B: no native cell worse beyond its A/A band; targeted cells better beyond it | research/71, step 5's latency paragraph: "No latency change is claimed either way." | **gap, step 10** (Ada's sequencing). |
| Test: counting tests per changed native operation, red before and green after, on every target | turn_allocs.rs, host.rs `the_scheme_is_chosen_again_only_when_the_tables_domains_change`, process.rs `a_turns_lines_are_written_into_the_kept_buffer_as_each_was_said`, volume_memory.rs `a_chunk_put_from_shared_bytes_copies_none_of_them`, object_path.rs `a_get_copies_each_byte_it_reads_once`, each "red on the code before, run once against it" (research/71). | **met** for mantle's sites. Vendor sites have none in mantle; hyper-raft 3d6640f carries its own. |
| Test: end to end, a node SIGKILLed mid-put and resumed; client tests; transfer smoke | research/71, step 9 "Correctness": `mantle bench transfer --small 1000 --large 64M` with each founder SIGKILLed, 0 wrong, 0 missing. | **met** on the box, at step 9's build. Other targets are CI's, step 11. |
| Test: hyper-raft changed crates against slates', focal's and mantle's suites | hyper-raft main 3d6640f carries the native door's cuts (vendored 7dec16b3). | **met** for 3d6640f, as landed on hyper-raft main. |

## Step 6: HTTP door, a request parsed, verified and answered without allocating per request

| criterion | evidence | state |
|---|---|---|
| Allocations per request head parsed at steady state is 0 | crates/http/tests/allocations.rs `every_head_shape_is_parsed_with_no_allocation`, `a_mutated_head_is_parsed_with_no_allocation` (e565b1d5): 1 → 0. | **met.** |
| Allocations per steady-state HTTP PUT, GET, HEAD and DELETE, every size, counting build, rise by no more than 0% against step 3 | research/71, "Steps 6 and 7, measured" (77542df9 against 44c6d21e): every row at 1 B and 64 KiB falls. At 8 MiB a PUT rose 31,153 → 32,947 and a GET 9,114 → 9,603 in run B. Research/71 says this is within the per-datagram sites' run-to-run spread, with "no stacks taken to split them". | **gap.** Two cells rose, unattributed. Not against step 3's baseline. 3 of 8 sizes. |
| Reallocations per steady-state HTTP request is 0 | Same table: 3 to 115 a request remain (HEAD 3.0, GET 9.3, PUT 75.5, PUT 8 MiB 112). | **gap.** Research/71 attributes them "outside the door (the node's and hyper-quic's tables)". Not cut. |
| A request's peak live heap above its stated bound (one 8 KB head, one body segment, one TLS record) is ≤ 0 | Research/71: "the door's own bound … is not what it measures, and no test isolates it." | **gap.** |
| RFC 9112 rule tests and oracle property tests unchanged; TE+CL refused and closed; bare CR refused; a refused request sends no body; timed A/B | crates/http/tests/rules.rs and properties.rs pass unchanged (research/71, step 6). The timed A/B was not run. | **met** for correctness. **Gap, step 10** for the timed A/B. |
| Test: AWS CLI v2, boto3 and kiln's Blobs suite against a cell of three, every target | scripts/stock-clients.sh exists. No run of it on the step 6 build is recorded. | **gap** (needs a recorded run). |
| Test: serve.rs smuggling shapes, stopped clients, small requests answered meanwhile | crates/mantle/tests/serve.rs. Its run on the step 6 build is not recorded beside the step. | **gap** (needs a recorded run). |

## Step 7: metadata operations, messages built once at their known lengths

| criterion | evidence | state |
|---|---|---|
| Reallocations while encoding any message whose length is known first is 0, by counting tests over every message kind | crates/meta/tests/encoding_allocs.rs: an entry, a row and an answer, keys, the overlay, each at 0 reallocations (20f64d90). The native protocol's requests and replies (crates/protocol) have no counting test of their own. | **gap.** The meta messages are covered; the native protocol's message kinds are not. |
| Allocations per steady-state HEAD, DELETE, ListObjectsV2 page of 1,000 keys and CompleteMultipartUpload of 10,000 parts, both doors, rise by no more than 0% against step 3 (`mantle bench doors` and `mantle bench meta`) | Research/71, "Steps 6 and 7, measured". HTTP rows all fall. Native rose: HEAD 8 MiB 52.0 → 76.8, DELETE 64 KiB 357 → 361, DELETE 8 MiB 321 → 327, MPU-LIST-PARTS 8 MiB 59.4 → 82.1, MPU-ABORT 8 MiB 325 → 334. A completion of 10,000 parts was not measured: the bench's completion has one part. `mantle bench meta` was not run. | **gap.** |
| Bytes of any wire message or range command differing from its golden vector is 0 | crates/meta/tests/golden/{wire,record}.txt, crates/protocol/tests/golden/object.txt: 216 vectors from 44c6d21e, 0 bytes differ (research/71, step 7). | **met.** |
| S3 semantics hold through every node; timed A/B | crates/node/tests/cluster.rs and crates/mantle/tests/serve.rs in the gates. The timed A/B was not run. | **met** for semantics, as far as the gates run them. **Gap, step 10** for timing. |

## Step 8: connections, what a handshake and a held connection cost

Not done as a step. Research/71 has no "Step 8, after", and no commit names it. Part of the
work predates the plan: hyper-raft e4bca91 (2026-10-21, on hyper-raft main and vendored here)
sizes brotli's window to the certificate (`window_bits`, vendor/hyper-tls/src/compress.rs:261).
Mantle's statement was never updated to match: crates/protocol/src/bounds.rs still says
`CERTIFICATE_COMPRESSION_BYTES = 34_267_200` ("a 4 MiB window, measured"). Research/71 dropped
both connection candidates as allocation sites ("The candidates"): brotli at 1.82 MB at a server's
first window, and 0 allocations under hyper-quic's datagram code.

| criterion | evidence | state |
|---|---|---|
| A server's peak transient at its first TLS handshake, minus the brotli encoder state measured for a window the certificate's size, is ≤ 0 (tests/transport_memory.rs, the peak as the server takes the ClientHello) | crates/node/tests/transport_memory.rs measures the transports' peak against their budget. It does not take the first handshake's transient apart, and the bound it checks still carries 34,267,200. | **gap.** |
| QUIC DATAGRAM receive allowance per connection on nodes and clients is 0 bytes, read from the transport parameters each endpoint sends | vendor/hyper-transport/src/endpoint.rs `transport()` leaves hyper-quic's default `datagram_receive_buffer_size: Some(STREAM_RWND)` (1,250,000), so every endpoint advertises max_datagram_frame_size. No mantle, hyper-raft, focal or slates code sends or reads a DATAGRAM frame. | **gap.** |
| Allocations per full and resumed QUIC handshake and per TLS-over-TCP handshake rise by no more than 0% against step 3 (hyper-quic tests/handshake.rs, hyper-tls tests/alloc_per_handshake.rs, `mantle bench doors`) | The tests exist in vendor (vendor/hyper-quic/tests/handshake.rs, vendor/hyper-tls/tests/alloc_per_handshake.rs). The baseline has handshake counts only from the six control cases (3,598 native and 157 HTTP allocations a server-side handshake). | **gap** until a change exists to compare and step 3's counts are re-taken. |
| Bytes per idle and per active connection (the slope) rise by no more than 0% against step 3 | The baseline's slope: from the control cases only. | **gap.** |
| Stock clients complete TLS handshakes; compressed certificates decode with an independent decoder; hyper-tls's handshake allocations still equal upstream's | vendor/hyper-tls/tests/alloc_per_handshake.rs states the equality with upstream. No independent decoder test exists for the cert-sized window. | **gap** for the independent decoder. |

## Step 9: tails under load, traced and their causes removed

| criterion | evidence | state |
|---|---|---|
| Cells whose p99 or p99.9 under busy, one-CPU or two-CPU rose beyond their A/A band against step 3, box and VM lane, is 0 | Research/71, "Step 9, after": B1/B2 against N1/N2 on the box, busy2x, cpu2 and quiet, n 368 (32 at 8 MiB). "A p99.9 needs 3,688 samples: no cell has them." p99 bands 5–555%. | **gap, step 10.** No p99.9 is resolved. The VM lane, cpu1 and busy1x were not run. |
| Cells whose traced cause was removed and whose p99.9 did not fall beyond its A/A band is 0 | Same. | **gap, step 10.** |
| Requests above p99.9 in a re-traced busy run attributed to a cause this step removed is 0 | Research/71, "Re-traced": the flushes per operation and commands per entry under load before and after. Requests above p99.9 are not counted by cause. | **gap.** Needs the re-traced run's requests above p99.9 tallied by attributed stage. |
| Medians and throughput no worse beyond A/A; engine release bench at 100M and 10M within its A/A band; overhead above physics at 500 ms recorded per door and not raised; node simulations and process suites pass | The small-write ladder rose beyond both A/A spreads (research/71). Simulations: sim.rs `a_batched_entry_is_answered_only_once_durable_and_a_power_cut_at_any_moment_keeps_it_whole` and the kill tests pass. | **met** for correctness. **Gap** for the engine bench (not run after 83aaa47e) and the 500 ms relay (not built, so no overhead is recorded). **Step 10** for medians. |
| Test: the engine's ranges bench at 100M and 10M with RocksDB and SplinterDB, Williams order | none after 83aaa47e | **gap.** |

## What closes each gap, and in what order

1. **Step 3.** Run the graft (doors-baseline-graft: 4711a46a, the baseline's product, with the
   harness as e511ae15 has it) as A1, A2 and C on the box, the Mac and the VM lane, every condition,
   comparators interleaved. Sized phases at 3,688 operations wherever a p99.9 is to be reported.
   Per-cell A/A bands. The tables read the harness's new fields. This waits on the full-run volume
   fix and on the heavy-run lock, and the Mac and the VM lane wait on their step 2 runs.
2. **Step 8.** In hyper-raft, on a branch: a transport that does not advertise DATAGRAM support,
   with a test reading the parameters it sends. In mantle: the compression bound measured at the
   certificate's window, with transport_memory.rs taking the first handshake's transient apart, and
   an independent decoder test.
3. **Steps 5 to 7.** Re-run the counting arm C of the current lean-doors beside the graft's C, and a
   stacks run (diag-doors rebased) on the cells that rose: native PUT 64 MiB, GET 8 MiB faults,
   HTTP PUT and GET 8 MiB, native HEAD, DELETE, LIST-PARTS and ABORT. Each rise is then cut at its
   cause or shown to be the harness's. A counting test for the native protocol's messages. A
   completion of 10,000 parts through both doors, and `mantle bench meta`. The 10 GB stream's peak
   against a stated bound. The vendor sites (QUIC's copy per write, the per-packet records, the ring)
   in hyper-raft.
4. **Step 4.** Kernel-mode faults stacked, which needs `perf_event_paranoid` at 1 for the run (the
   owner's to allow). The libc-terminated stacks resolved. The VM lane's profile.
5. **Step 9.** The re-traced run's requests above p99.9, tallied by cause. The relay and the engine
   bench go to step 10's batch.

## Closed since, on doors-3-9 (2026-11-12)

| step | criterion | evidence | state |
|---|---|---|---|
| 3 | the baseline arm | doors-baseline-graft d3be078c: 4711a46a's product, the harness as e511ae15 has it. The harness commits picked are 1878a7e0, 6cb5eb21, 8d6d9ae6 and 8569637f, plus 0ecfd1d5's hyper-rt counting alone; f2326636 without its change to the response writers; and bench_doors.rs from e511ae15. Box arms: A1 = A2 sha256 9cfa6af9…, C 56d94837…; the C smoke exits 0 with 6 of 6 controls caught. | built; runs wait on the volume fix and the heavy-run lock |
| 3 | every case accounted on three machines; tables from raw | scripts/doors-trials.sh takes MACHINE=box, mac or vm. The Mac's cpu1, cpu2 and memfloor are excluded with their reasons; the tail case runs 3,688 operations a phase. scripts/doors-tables.py reads the new fields, records the A/A band cell by cell, and checks the transfers behind every p99.9. The 2026-11-06 raw outputs regenerate 78 = 36 + 18 + 24. | ready; the Mac and the VM lane are untested until they are free |
| 7 | 0 reallocations encoding any message of known length | protocol 9e4bda98: every native request and reply, and a 10,000-part completion, at 2 allocations and 0 reallocations (`every_request_and_reply_is_written_at_its_length`, red before at 5 reallocations for a PUT). Golden vectors unchanged. | met for the native protocol. **Gap** for the node's read path (`read.rs` `Query::encode`, `Read::encode`), range `conf`, `image` and `machine::encode_term`, and mantle's `journal::Record::encode`, which still start empty. `Read::encode` nests records that allocate, so it needs the meta records' write-in-place forms. |
| 8 | first handshake's transient minus brotli at the certificate's window ≤ 0 | 0aa07643, transport_memory.rs `a_servers_first_handshake_takes_brotlis_state_at_its_certificates_window_and_no_more`. Brotli at quality 11 takes 720,874 bytes for a 326-byte certificate, and 721,474 for the longest a node carries (365 bytes). A server counted alone peaks at 758,414 through its first handshake and 83,183 through its second: 675,231 apart, within brotli's state. `CERTIFICATE_COMPRESSION_BYTES` is now 721,474 (was 34,267,200). | **met** |
| 8 | DATAGRAM receive allowance 0 on nodes and clients, read from the parameters sent | hyper-raft doors-step8 c97fb1a (hyper-transport `Limits::transport`, `datagram_receive_buffer_size(None)`). Its tests/datagrams.rs reads the parameter at a bare hyper-quic peer: Some(1414) before, None after, with a positive control. Vendored by e4d8a768; the vendored suite passes. | **met** on the branch; hyper-raft main waits on Ada's word |
| 8 | handshake allocations no higher than step 3's | hyper-tls alloc_per_handshake: equal to upstream rustls 0.23.45 (passes). hyper-quic handshake.rs: full 506 allocations, resumed 509 (hyper-quic's default config, which this step does not change). | **gap** until the graft's and the final build's `doors … handshake` lines are compared |
| 8 | the slope per connection no higher than step 3's | none yet | **gap** (the graft's slope line) |
| 8 | compressed certificates decode with an independent decoder | The only Brotli decoder in the tree is the `brotli` crate, the encoder's own. | **gap**: an independent decoder is a new tool, which is Ada's to allow |
