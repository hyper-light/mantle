# 80. The doors' costs: every counted cost of both doors at its site, and whether it is needed

Status: research note, 2026-11-06. Step 4 of the doors plan: each allocation, reallocation, page
fault and instruction that `mantle bench doors` (research/79, measurement.md §12) counts on the
native door and the S3 door is attributed to the call stack that made it, and each site is asked
two questions with a measurement behind each answer: is this work needed at all, and is this much
of it needed. No product code changes here; the instruments live on the scratch branch `diag-doors`
(fd1aac8d, never merged) and the fixes are later steps.

## How it was measured

**Instruments (diag-doors fd1aac8d, built in a 16G scope on CPUs 0-7, `-j 6`, line tables, unstripped).**

- `mantle-stacks` (feature `diag-stacks`): a global allocator that counts through hyper-measure's
  `Counting` and, under one gate, records each allocation's and reallocation's return addresses
  (glibc `backtrace`, which allocates nothing through Rust's allocator) aggregated by stack. A
  window runs from one `counts` answer to the next on each server, and from one phase mark to the
  next in the harness; the switch reads and restarts `Counting`'s process count under the same
  gate, so recorded + recorder (the recorder's own HashMap growth) equals `Counting`'s total
  exactly, and every window file states both. Stacks are symbolized offline with llvm-symbolizer
  (inlined frames included). A site is the first frame past the allocator, libc and the standard
  library; the standard-library frame above it says how (`to_vec`, `collect`, `push`).
- `mantle-plain` (release, system allocator): `perf stat -e instructions:u,cycles:u` per process.
- `mantle-fp` (release, `-C force-frame-pointers=yes`): `perf record -k CLOCK_MONOTONIC -e
  page-faults/period=1/u -e instructions/period=100000/u --call-graph fp` on the three servers and
  the harness: every user-mode fault and an instruction sample per 100,000. DWARF unwinding
  (`--call-graph dwarf,16384` and `65528`) was tried first: perf's libdw unwinder stopped at the
  first Rust frame in every sample and lost samples, so frame pointers it is.
- The stall trace (`mantle_node::diag_trace`): each request's stages through both doors (head
  parsed, authorized, submitted to the door, the host's take with the jobs' depth, native exchange
  arrived and op decoded, each host ask and answer by kind, reply head, response written, end), the
  node loop's turns with their phases and thread CPU time, and the harness's intended start, send
  and end, all on CLOCK_MONOTONIC.

**Steady windows.** `MANTLE_DIAG_STEADY` runs each phase as a warm-up half, then an idle window as
long as the warm-up (the servers' background: what they do with no requests), then the measured
half. `--ops 8 --clients 2 --load open:100`: four measured operations a cell (two for multipart
complete and abort, two and one at 64 MiB), each of the 8 sizes × 11 operations × 2 doors, and the
listing: a set of 4,000 objects, three measured pages of 1,000 keys. Per operation, a site's count
is (measured − idle × measured span ÷ idle span) ÷ ops: what the requests caused. With four
operations the background share is noisy where an idle window caught a burst (a compaction, a
durability recompute, below); the table gives both. Every cell's data was deleted after it ran.

**The rebase.** The coordinator rebased both branches onto origin/main cc720c05 (readiness lock,
peer and client re-resolution). All three binaries were rebuilt from fd1aac8d and re-run: every
perf cell (faults, instructions, stat; the pre-rebase perf runs had been cut off and held no data),
the stacks cells at 1 B, 65,537 B and 8 MiB and the listing, and the stall trace. The stacks cells
at 0 B, 65,535 B, 64 KiB, 128 KiB and 64 MiB are reused from the pre-rebase build (2026-11-04,
same scripts): the rebased re-runs added no allocation site of 1 per op or more that the pre-rebase runs lack
except a read-serving closure (`serve_reads`, 3–4 per op), and removed none (QUIC key updates at the
confidentiality limit, `force_key_update`, are traffic-driven: they appear at 65,537 B and 8 MiB
rebased and at 64 MiB before). What moved between the two builds moved by run-to-run timing, not by the new code:
per-packet sites (with the datagrams a run took, up to 4.5× at 8 MiB), per-turn sites (with the
loop's turns), and the volume table's durability recompute (with which window a root entry landed
in). compare.py's output for each re-run size is kept (runs/s-*/compare.txt).

## Attribution checks

- **Allocations and reallocations: 100%.** In every window of every process (servers and harness,
  every stacks run, rebased and reused), recorded + recorder = `Counting`'s count for allocations,
  reallocations, moved reallocations and bytes, exactly: 5,130 windows, 3,876 of them in the runs this ledger uses, 0 inexact
  (runs/s-*/attrib.txt). The first try's three inexact windows were `mantle serve` restarting the
  process count outside the gate at its start; the diag build now begins it inside the gate.
- **Faults: every user-mode fault has a stack.** Over 204 measured windows (and their idle
  windows), perf recorded 2,032,768 fault samples against the kernel's 2,046,541 minor faults over
  the same CLOCK_MONOTONIC bounds; in every window perf's count is at least the kernel's
  (perf ≥ kernel by up to 2 where a fault fell between the stamp and the `/proc` read) except in
  14 cells. 2,019,873 samples (99.4%) resolve to a frame of mantle or a dependency; the other
  12,895 end in glibc's heap growth (`sysmalloc`, `mmap_set_chunk` under `malloc`/`calloc`), whose
  frame-pointer chain stops in libc: they are first touches of fresh heap pages by malloc itself,
  attributed to that stack and to no caller.
- **Kernel-mode faults: 13,807 (0.67%), not stacked.** `perf_event_paranoid` 2 restricts perf to
  user mode, so a fault the kernel takes while writing into a user page (a read or receive into a
  fresh buffer) has no sample. They occur only where a large buffer is first written by the
  kernel: 8 MiB and 64 MiB GET and GET-RANGE (the chunk reader's 8 MiB buffer,
  `crates/chunk/src/reader.rs` `check`, allocated by `try_reserve_exact` and filled by the read
  before user code touches it), PUT-STREAM (receive rings, `hyper-rt/src/udp/batched.rs` `Ring::resize`),
  and 8+30 in the listing set's PUT/GET. Attributed by elimination, not by stack: by cell, native GET 8 MiB 2,537, native GET-RANGE 8 MiB 2,117, S3 GET 8 MiB 1,004, S3 GET-RANGE 8 MiB 901, S3 PUT-STREAM 8 MiB 651, native PUT-STREAM 8 MiB 12, S3 and native GET-RANGE 64 MiB 2,826 each, S3 GET 64 MiB 339, native GET 64 MiB 257, native MPU-PART 64 MiB 298, S3 PUT-STREAM 64 MiB 1.
- **Requests above p99.9: 0 unattributed.** All 14 (7 per busy run) were matched to their server jobs and attributed to a stage, and so were the 12 above each door's p99.9 of service time (below).

## Background and per-request work

An idle server (no request for the window's length) allocates about 240 times and executes about
6.2 million user instructions and 26 million user cycles a second (stat-1, stat-65537, stat-8MiB idle
windows, servers summed by server-second; IPC 0.24: the loop polls clocks and timers), and takes
no page fault. The allocations at rest are per-turn: hyper-durable's `Owner` collecting each
replica's `peers()` into a `Vec` (vendor/hyper-durable/src/owner.rs:224, 64 a second),
the plane's messages copied into the inbox (crates/node/src/node.rs, `inbox.push(.. message.to_vec())`, 36 a
second), the node's `status` lines formatted and handed to the stdout task with a oneshot each
(crates/node/src/process.rs `status` format and `say`, 9 + 17 + 17 a second), `drive`'s
`(Handle, group)` list and its `driven`/`messages` vectors (node.rs `drive`, 20 + 14 + 4 a second),
hyper-durable's `measure` and the liveness heartbeat. Each of these runs every loop turn whether or not a request
is in flight, and under requests the turns multiply, so the same sites reappear as per-request
costs that scale with packets (a turn per batch of datagrams): at 8 MiB `drive` and the plane copy
alone are 2,900–5,900 allocations per operation.

Across every measured window, the background (the idle rate times the measured span) is this share
of the servers' allocations: 0 B 14%, 1 B 4%, 65,535 B 6%, 64 KiB 30% (an idle window that caught a
compaction), 65,537 B 14%, 128 KiB 9%, 8 MiB 12%, 64 MiB 3%. Per-request numbers below have it
subtracted. The client side has no background of its own worth counting: its windows are the
measured phase only, and the harness's own work (its generator, checks, signing of what it sends,
and the cell's reader threads) is set aside by hyper-measure's flag and by stack.

## The ledger

Per door, operation and size: the servers' allocations, reallocations, moved reallocations and
bytes asked per operation with the background subtracted, the background itself, the client's
(mantle-client or the S3 request writer in the harness, harness work set aside), page faults per
operation (net of background; gross), user instructions per operation from the samples
(net) and from `perf stat` (net), and the top three sites of each kind. Every site of every cell,
with its stack, is in the raw files (below); this table names the top ones.

| door | op | size | srv allocs/op | reallocs/op | moved/op | bytes/op | bg allocs/op | client allocs/op | faults/op (net; gross) | instr/op sampled | instr/op stat | top allocation sites (allocs/reallocs per op) | top fault sites | top instruction sites |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| http | PUT | 0B | 511 | 192 | 96 | 125,582 | 91 | 52 | 0.0; 0.0 | 2,906,468 | 1,791,351 | crates/meta/src/overlay.rs:115 apply 38/0; crates/codec/src/lib.rs:48 u64 0/36; crates/meta/src/key.rs:620 escape 3/28 |  | crates/ec/src/durability.rs:786 {closure#0} 350k; crates/ec/src/durability.rs:333 down 200k |
| http | PUT | 1B | 1,456 | 540 | 283 | 289,427 | 390 | 52 | 4.5; 4.5 | 4,693,728 | 6,707,984 | crates/meta/src/overlay.rs:115 apply 153/0; crates/codec/src/lib.rs:48 u64 0/112; crates/codec/src/lib.rs:36 u8 100/6 | crates/engine/src/memtable/hashed.rs:1866 hot_add 1.5; [libc.so.6 _int_malloc] (frame-pointer chain ends in the library) 0.8 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 137k; crates/node/src/process.rs:2143 {closure#0} 100k |
| http | PUT | 65535B | 1,799 | 541 | 284 | 1,062,573 | 91 | 52 | 48.2; 48.2 | 22,985,461 | 9,232,785 | crates/meta/src/overlay.rs:115 apply 150/0; vendor/hyper-durable/src/owner.rs:224 ... 133/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 129/0 | crates/node/src/node/host.rs:822 step_host 34.2; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 4.5 | vendor/hyper-log/src/ticket.rs:211 poll 2965k; vendor/hyper-raft/src/configuration.rs:238 {closure#0} 1075k |
| http | PUT | 64KiB | 11,987 | 539 | 281 | 3,155,552 | 3,798 | 52 | 60.5; 60.5 | 8,179,654 | 8,643,438 | vendor/hyper-durable/src/owner.rs:224 ... 5138/0; crates/node/src/node.rs:2147 {closure#1} 4300/-4; crates/node/src/node.rs:2130 drive 1031/0 | crates/node/src/node/host.rs:822 step_host 45.0; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 5.5 | crates/engine/src/memtable/hashed.rs:188 start_pass 150k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 140k |
| http | PUT | 65537B | 1,699 | 551 | 293 | 1,384,058 | 221 | 52 | 45.2; 45.2 | 7,323,794 | 7,894,013 | crates/meta/src/overlay.rs:115 apply 153/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 112/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 112/0 | crates/node/src/node/host.rs:822 step_host 37.5; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 4.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 268k; crates/engine/src/shard_db.rs:2925 scan_inner 150k |
| http | PUT | 128KiB | 2,041 | 558 | 297 | 1,871,187 | 152 | 52 | 161.2; 161.2 | 12,988,658 | 11,876,196 | bytes-1.12.1/src/bytes_mut.rs:1373 from 183/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 183/0; crates/meta/src/overlay.rs:115 apply 153/0 | crates/node/src/node/host.rs:822 step_host 72.0; crates/gateway/src/put.rs:656 take 32.0 | crates/ec/src/durability.rs:786 {closure#0} 225k; crates/ec/src/durability.rs:333 down 200k |
| http | PUT | 8MiB | 45,825 | 843 | 444 | 120,395,837 | 2,028 | 64 | 11,762.8; 11,762.8 | 330,350,773 | 392,958,071 | bytes-1.12.1/src/bytes_mut.rs:1373 from 12197/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 12197/0; crates/ec/src/durability.rs:779 {closure#0} 3484/0 | crates/node/src/node/host.rs:822 step_host 5986.2; crates/node/src/node.rs:0 on_event 3936.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 6854k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 4825k |
| http | PUT | 64MiB | 270,205 | 2,436 | 1,298 | 768,321,005 | 2,307 | 52 | 64,201.0; 64,201.0 | 3,838,271,546 | 4,720,023,563 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 78028/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 78028/0; crates/node/src/node.rs:2130 drive 22581/0 | crates/node/src/node/host.rs:822 step_host 31706.0; crates/node/src/node.rs:0 on_event 22244.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 84431k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 55200k |
| http | PUT-STREAM | 0B | 2,969 | 188 | 101 | 254,972 | 129 | 73 | 5.2; 5.2 | 1,628,906 | 1,820,036 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; crates/meta/src/overlay.rs:115 apply 34/0 | vendor/hyper-log/src/writer.rs:1304 {closure#2} 1.0; crates/engine/src/shard_db.rs:1277 metadata_full 0.5 | crates/range/src/stored.rs:130 next 75k; vendor/hyper-quic/src/connection/timer.rs:103 next_timeout 50k |
| http | PUT-STREAM | 1B | 3,894 | 522 | 277 | 487,941 | 520 | 73 | 2.8; 2.8 | 5,111,419 | 5,390,346 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; crates/meta/src/overlay.rs:115 apply 145/0 | vendor/hyper-log/src/writer.rs:1304 {closure#2} 1.2; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 0.2 | crates/engine/src/util/coding.rs:315 get_varint32_ptr 125k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 113k |
| http | PUT-STREAM | 65535B | 4,244 | 527 | 286 | 1,276,860 | 112 | 73 | 39.5; 39.5 | 21,407,767 | 8,350,976 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; crates/meta/src/overlay.rs:115 apply 143/0 | crates/node/src/node/host.rs:822 step_host 26.8; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 5.2 | vendor/hyper-log/src/ticket.rs:211 poll 1875k; crates/node/src/node.rs:2171 drive 1300k |
| http | PUT-STREAM | 64KiB | 3,872 | 551 | 295 | 1,490,475 | 1,333 | 73 | 43.0; 43.0 | 8,368,255 | 8,650,555 | vendor/hyper-durable/src/owner.rs:224 ... 1477/0; crates/node/src/node.rs:2147 {closure#1} 950/-2; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 160/0 | crates/node/src/node/host.rs:822 step_host 33.8; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 4.8 | vendor/hyper-raft/src/raft.rs:2693 Held> 125k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 125k |
| http | PUT-STREAM | 65537B | 1,828 | 571 | 308 | 1,421,441 | 438 | 90 | 41.8; 41.8 | 7,545,346 | 7,866,728 | crates/meta/src/overlay.rs:115 apply 150/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 146/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 146/0 | crates/node/src/node/host.rs:822 step_host 27.0; crates/node/src/node.rs:0 on_event 7.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 213k; crates/node/src/node.rs:1187 status 175k |
| http | PUT-STREAM | 128KiB | 2,074 | 558 | 296 | 1,816,110 | 153 | 73 | 178.5; 178.8 | 12,127,953 | 12,687,546 | bytes-1.12.1/src/bytes_mut.rs:1373 from 187/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 187/0; crates/meta/src/overlay.rs:115 apply 153/0 | crates/node/src/node/host.rs:822 step_host 72.0; crates/node/src/node.rs:0 on_event 48.0 | vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 150k; crates/node/src/node.rs:1187 status 125k |
| http | PUT-STREAM | 8MiB | 50,696 | 851 | 456 | 143,362,122 | 1,736 | 108 | 12,986.5; 12,986.5 | 435,742,638 | 397,479,309 | bytes-1.12.1/src/bytes_mut.rs:1373 from 14945/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 14945/0; crates/node/src/node.rs:2130 drive 4120/0 | crates/node/src/node/host.rs:822 step_host 6585.5; crates/node/src/node.rs:0 on_event 3930.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 10309k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 5825k |
| http | PUT-STREAM | 64MiB | 259,630 | 2,625 | 1,453 | 779,437,831 | 2,678 | 74 | 51,341.0; 51,341.0 | 3,012,469,747 | 4,207,143,139 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 77154/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 77154/0; crates/node/src/node.rs:2130 drive 20484/0 | crates/node/src/node/host.rs:822 step_host 23602.0; crates/node/src/node.rs:0 on_event 19993.0 | vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 47750k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/b 41200k |
| http | GET | 0B | 131 | 44 | 25 | 14,492 | 38 | 48 | 0.0; 0.0 | 449,046 | 375,183 | crates/node/src/node.rs:2130 drive 6/0; crates/s3/src/sigv4.rs:596 collapse 6/0; crates/s3/src/time.rs:77 http_date 2/4 |  | vendor/hyper-durable/src/replica.rs:762 Rang 25k; vendor/hyper-liveness/src/lib.rs:1297 report 25k |
| http | GET | 1B | 416 | 87 | 52 | 60,964 | 83 | 48 | 0.0; 0.0 | 1,270,518 | 1,399,785 | crates/node/src/node.rs:1588 {closure#5} 35/1; crates/node/src/node.rs:2130 drive 31/0; vendor/hyper-durable/src/owner.rs:224 ... 26/0 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 75k; vendor/hyper-rt/src/runtime.rs:875 block_on 50k |
| http | GET | 65535B | 528 | 85 | 51 | 491,425 | 22 | 48 | 9.5; 9.5 | 2,154,928 | 2,972,565 | bytes-1.12.1/src/bytes_mut.rs:1373 from 58/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 58/0; crates/node/src/node.rs:2130 drive 41/0 | crates/gateway/src/get.rs:755 span 7.5; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 1.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 1514k; vendor/hyper-rt/src/udp/batched.rs:1157 {closure_env#0}< 245k |
| http | GET | 64KiB | 410 | 86 | 51 | 428,159 | 453 | 48 | 16.0; 16.0 | 2,429,612 | 2,385,901 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 39/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 39/0; crates/node/src/node.rs:1588 {closure#5} 28/-0 | crates/gateway/src/get.rs:755 span 15.0; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 1.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 150k; vendor/hyper-quic/src/connection/streams/mod.rs:65 open 50k |
| http | GET | 65537B | 518 | 85 | 50 | 556,523 | 26 | 48 | 8.0; 8.0 | 1,939,312 | 1,912,213 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 58/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 58/0; crates/node/src/node.rs:2130 drive 35/0 | crates/gateway/src/get.rs:912 finish 7.5; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 0.5 | crates/node/src/node.rs:1187 status 100k; vendor/hyper-quic/src/connection/mod.rs:911 fill_step 50k |
| http | GET | 128KiB | 470 | 85 | 51 | 683,221 | 49 | 48 | 78.0; 78.0 | 3,522,293 | 4,412,662 | bytes-1.12.1/src/bytes_mut.rs:1373 from 39/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 39/0; crates/node/src/node.rs:2130 drive 34/0 | crates/gateway/src/get.rs:755 span 32.0; crates/gateway/src/get.rs:912 finish 32.0 | vendor/hyper-transport/src/tally.rs:366 settle 75k; vendor/hyper-raft/src/raft.rs:2693 Held> 75k |
| http | GET | 8MiB | 18,419 | 112 | 62 | 54,157,299 | 294 | 48 | 2,032.0; 2,032.0 | 79,640,543 | 99,845,833 | bytes-1.12.1/src/bytes_mut.rs:1373 from 6422/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 6422/0; crates/node/src/node.rs:2130 drive 1202/0 | crates/gateway/src/get.rs:755 span 641.8; crates/node/src/node.rs:2953 {closure#0} 631.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 2613k; [libc.so.6 __libc_malloc2] (frame-pointer chain ends in the library) 1275k |
| http | GET | 64MiB | 80,151 | 333 | 204 | 405,307,448 | 739 | 48 | 52,955.5; 52,955.5 | 541,611,471 | 829,792,932 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 22001/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 22001/0; crates/node/src/node.rs:2130 drive 8154/0 | crates/gateway/src/get.rs:755 span 15625.0; crates/gateway/src/get.rs:912 finish 15577.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 12550k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 7750k |
| http | GET-RANGE | 1B | 63 | 13 | -21 | -33,647 | 358 | 52 | 0.0; 0.0 | 1,309,402 | 1,524,448 | vendor/hyper-raft/src/wire.rs:624 take_body 16/0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 15/0; crates/node/src/process.rs:2143 {closure#0} 4/8 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/c 75k; vendor/hyper-log/src/ticket.rs:211 poll 50k |
| http | GET-RANGE | 65535B | 540 | 87 | 53 | 476,595 | 8 | 52 | 11.5; 11.5 | 2,990,981 | 3,014,820 | bytes-1.12.1/src/bytes_mut.rs:1373 from 57/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 57/0; crates/node/src/node.rs:2130 drive 40/0 | crates/gateway/src/get.rs:755 span 7.5; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 3.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 983k; crates/node/src/node.rs:156 now_ns 237k |
| http | GET-RANGE | 64KiB | 409 | 87 | 52 | 410,664 | 390 | 52 | 16.0; 16.0 | 3,841,274 | 2,106,692 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 39/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 39/0; crates/node/src/node.rs:1588 {closure#5} 25/-0 | crates/gateway/src/get.rs:755 span 15.0; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 1.0 | crates/ec/src/durability.rs:786 {closure#0} 325k; [libc.so.6 __libc_calloc2] (frame-pointer chain ends in the library) 225k |
| http | GET-RANGE | 65537B | 558 | 87 | 53 | 496,424 | 7 | 52 | 8.0; 8.0 | 1,908,534 | 1,870,995 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 57/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 57/0; crates/node/src/node.rs:2130 drive 41/0 | crates/gateway/src/get.rs:755 span 7.5; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 0.5 | crates/node/src/node.rs:1187 status 91k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 66k |
| http | GET-RANGE | 128KiB | 477 | 87 | 53 | 683,729 | 41 | 52 | 45.0; 45.0 | 3,240,019 | 4,262,788 | bytes-1.12.1/src/bytes_mut.rs:1373 from 39/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 39/0; crates/node/src/node.rs:2130 drive 34/0 | crates/gateway/src/get.rs:755 span 32.0; crates/gateway/src/get.rs:912 finish 10.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 144k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/b 75k |
| http | GET-RANGE | 8MiB | 4,575 | 88 | 52 | 19,177,396 | 79 | 52 | 2,062.8; 2,062.8 | 28,031,110 | 50,109,178 | bytes-1.12.1/src/bytes_mut.rs:1373 from 1491/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 1491/0; crates/node/src/node.rs:2130 drive 313/0 | crates/gateway/src/get.rs:755 span 704.0; crates/node/src/node.rs:2953 {closure#0} 576.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 1032k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/b 475k |
| http | GET-RANGE | 64MiB | 42,621 | 174 | 105 | 162,336,234 | 460 | 52 | 21,387.0; 21,387.0 | 339,660,145 | 325,115,985 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 12487/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 12487/0; crates/node/src/node.rs:2130 drive 4007/0 | crates/node/src/node.rs:0 on_event 5472.0; crates/gateway/src/get.rs:912 finish 5460.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 14600k; crates/node/src/node.rs:1187 status 5450k |
| http | HEAD | 0B | 137 | 47 | 27 | 17,092 | 48 | 48 | 0.0; 0.0 | 387,723 | 514,124 | crates/node/src/node.rs:1588 {closure#5} 9/1; crates/node/src/node.rs:2130 drive 6/0; crates/s3/src/sigv4.rs:596 collapse 6/0 |  | crates/node/src/listener/respond.rs:420 quoted 25k; vendor/hyper-raft/src/raft.rs:4449 file: 25k |
| http | HEAD | 1B | 158 | 49 | 30 | 19,440 | 6 | 48 | 0.0; 0.0 | 387,335 | 504,417 | crates/node/src/node.rs:1588 {closure#5} 11/1; crates/node/src/node.rs:2130 drive 9/0; vendor/hyper-durable/src/owner.rs:224 ... 7/0 |  | crates/node/src/diag_trace.rs:54 on 25k; vendor/hyper-transport/src/endpoint.rs:1178 take_back_windows 25k |
| http | HEAD | 65535B | 150 | 49 | 30 | 17,364 | 1 | 48 | 0.0; 0.0 | -101,723 | 477,854 | crates/node/src/node.rs:2130 drive 9/0; crates/node/src/node.rs:1588 {closure#5} 7/0; crates/node/src/listener/respond.rs:0 resource 2/4 |  | crates/node/src/node.rs:1427 {closure#0} 106k; vendor/hyper-quic/src/connection/pacing.rs:0 delay 91k |
| http | HEAD | 64KiB | 133 | 49 | 30 | 14,714 | 76 | 48 | 0.0; 0.0 | 447,116 | 759,254 | crates/node/src/node.rs:1588 {closure#5} 8/-0; vendor/hyper-durable/src/owner.rs:224 ... 6/0; crates/s3/src/time.rs:77 http_date 2/4 |  | vendor/hyper-timing/src/link.rs:606 next_interval 25k; crates/meta/src/key.rs:634 take_string 25k |
| http | HEAD | 65537B | 155 | 49 | 30 | 82,549 | 9 | 48 | 0.0; 0.0 | 316,776 | 585,243 | crates/node/src/node.rs:2130 drive 7/0; crates/node/src/node.rs:1588 {closure#5} 6/0; crates/s3/src/sigv4.rs:596 collapse 6/0 |  | vendor/hyper-raft/src/raft.rs:2693 Held> 50k; vendor/hyper-durable/src/replica.rs:1939 machine: 25k |
| http | HEAD | 128KiB | 151 | 48 | 29 | 17,375 | 7 | 48 | 0.0; 0.0 | 378,217 | 561,466 | crates/node/src/node.rs:2130 drive 9/0; crates/node/src/node.rs:1588 {closure#5} 6/-0; crates/s3/src/sigv4.rs:631 percent_decode 3/3 |  | crates/node/src/node.rs:1725 step_frame 25k; crates/engine/src/memtable/hashed.rs:249 cmp (.llvm.142675451258216764 25k |
| http | HEAD | 8MiB | 159 | 50 | 31 | 19,866 | 7 | 48 | 0.0; 0.0 | 359,264 | 445,527 | crates/node/src/node.rs:1588 {closure#5} 11/1; crates/node/src/node.rs:2130 drive 10/0; vendor/hyper-durable/src/owner.rs:224 ... 8/0 |  | vendor/hyper-raft/src/raft.rs:2693 Held> 50k; vendor/hyper-liveness/src/lib.rs:1297 report 44k |
| http | HEAD | 64MiB | 150 | 49 | 31 | 16,857 | 8 | 48 | 0.0; 0.0 | 467,775 | 532,561 | crates/node/src/node.rs:2130 drive 8/0; crates/s3/src/sigv4.rs:631 percent_decode 3/3; crates/s3/src/time.rs:77 http_date 2/4 |  | vendor/hyper-raft/src/raft.rs:2693 Held> 50k; crates/http/src/head.rs:268 field 50k |
| http | DELETE | 0B | 443 | 126 | 59 | 53,196 | 103 | 46 | 0.0; 0.0 | 1,704,251 | 1,877,521 | vendor/hyper-durable/src/owner.rs:224 ... 30/0; crates/meta/src/key.rs:620 escape 3/24; crates/engine/src/memtable/hashed.rs:1007 get_hashed 25/0 |  | vendor/hyper-timing/src/qos.rs:385 binomial_tail 50k; vendor/hyper-timing/src/link.rs:606 next_interval 50k |
| http | DELETE | 1B | 474 | 128 | 62 | 73,523 | 207 | 46 | 0.0; 0.0 | 1,766,707 | 1,761,434 | vendor/hyper-durable/src/owner.rs:224 ... 41/0; crates/node/src/node.rs:2147 {closure#1} 27/2; crates/node/src/node.rs:2130 drive 24/0 |  | crates/node/src/process.rs:2143 {closure#0} 75k; vendor/hyper-rt/src/epoll.rs:211 now_ns 50k |
| http | DELETE | 65535B | 477 | 149 | 76 | 71,080 | 31 | 46 | 7.2; 7.2 | 4,604,346 | 2,060,027 | crates/meta/src/key.rs:620 escape 3/41; vendor/hyper-durable/src/owner.rs:224 ... 24/0; crates/node/src/node.rs:2130 drive 23/0 | crates/engine/src/store/mod.rs:1402 allocate_extent 0.8; crates/engine/src/fst/bits.rs:179 index 0.8 | vendor/hyper-log/src/ticket.rs:211 poll 825k; vendor/hyper-rt/src/udp/batched.rs:1157 {closure_e 405k |
| http | DELETE | 64KiB | 1,705 | 157 | 79 | 354,171 | 1,081 | 46 | 4.0; 4.0 | 1,783,433 | 2,113,322 | vendor/hyper-durable/src/owner.rs:224 ... 695/0; crates/node/src/node.rs:2147 {closure#1} 686/1; crates/meta/src/key.rs:620 escape 3/45 | crates/engine/src/store/mod.rs:1402 allocate_extent 0.8; crates/engine/src/store/alloc.rs:241 allocate 0.8 | vendor/hyper-durable/src/replica.rs:765 Rang 50k; vendor/hyper-liveness/src/lib.rs:1297 report 50k |
| http | DELETE | 65537B | 433 | 144 | 76 | 115,521 | 106 | 46 | 0.8; 0.8 | 1,817,133 | 1,901,161 | crates/meta/src/key.rs:620 escape 3/40; crates/meta/src/record.rs:1604 take_bytes 21/0; crates/node/src/process.rs:2143 {closure#0} 6/12 | crates/engine/src/branch/mod.rs:1270 0.2; crates/engine/src/shard_db.rs:2415 apply 0.2 | crates/engine/src/util/coding.rs:315 get_varint32_ptr 100k; vendor/hyper-raft/src/raft.rs:2693 Held> 60k |
| http | DELETE | 128KiB | 451 | 155 | 74 | 149,077 | 73 | 46 | 4.2; 4.2 | 1,747,504 | 1,725,322 | crates/meta/src/key.rs:620 escape 3/45; crates/meta/src/record.rs:1604 take_bytes 24/0; crates/node/src/process.rs:1972 {closure#0} 8/16 | crates/engine/src/store/mod.rs:3294 span_with_floor 2.8; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 1.2 | crates/engine/src/memtable/hashed.rs:186 start_pass 75k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/c 75k |
| http | DELETE | 8MiB | 414 | 134 | 70 | 50,041 | 139 | 46 | 0.2; 0.2 | 1,894,811 | 1,904,248 | crates/meta/src/key.rs:620 escape 2/35; crates/node/src/process.rs:2143 {closure#0} 7/14; vendor/hyper-durable/src/owner.rs:224 ... 20/0 | crates/engine/src/memtable/hashed.rs:791 take_buf 0.2 | crates/engine/src/util/coding.rs:315 get_varint32_ptr 100k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 75k |
| http | DELETE | 64MiB | 451 | 154 | 73 | 51,584 | 83 | 46 | 0.0; 0.0 | 1,745,844 | 2,258,904 | crates/meta/src/key.rs:620 escape 3/40; crates/node/src/node.rs:2130 drive 23/0; crates/node/src/process.rs:1972 {closure#0} 7/15 |  | crates/engine/src/memtable/hashed.rs:242 cmp (.llvm.142675451258216764 100k; vendor/hyper-log/src/lib.rs:1449 ask 100k |
| http | LIST | page of 1,000 | 15,191 | 7,407 | 2,520 | 1,348,100 | 235 | 52 | 86.1; 86.3 | 82,728,230 | 114,241,816 | crates/meta/src/key.rs:634 take_string 2668/1334; crates/range/src/stored.rs:132 {closure#0} 2668/0; crates/meta/src/key.rs:620 escape 0/2003 | crates/node/src/listener/conn.rs:487 {closure#0} 19.7; crates/s3/src/xml.rs:782 escape 15.0 | crates/engine/src/fst/trie.rs:1353 leftmost 3867k; crates/engine/src/fst/bits.rs:232 {closure#0} 3667k |
| http | MPU-CREATE | 0B | 429 | 124 | 62 | 66,942 | 89 | 55 | -0.7; 0.0 | 1,428,619 | 1,848,517 | vendor/hyper-durable/src/owner.rs:224 ... 24/0; crates/meta/src/overlay.rs:115 apply 22/0; crates/codec/src/lib.rs:48 u64 0/22 |  | vendor/hyper-raft/src/raft.rs:2693 Held> 50k; crates/engine/src/util/coding.rs:315 get_varint32_ptr 50k |
| http | MPU-CREATE | 1B | 481 | 135 | 69 | 59,774 | 136 | 55 | 0.8; 0.8 | 1,477,300 | 1,791,145 | vendor/hyper-durable/src/owner.rs:224 ... 39/0; crates/node/src/node.rs:2147 {closure#1} 26/2; crates/node/src/node.rs:2130 drive 22/0 | crates/node/src/node.rs:2138 {closure#1} 0.2; crates/node/src/node.rs:2147 {closure#1} 0.2 | crates/engine/src/shard_db.rs:2943 scan_inner 75k; crates/engine/src/shard_db.rs:2925 scan_inner 75k |
| http | MPU-CREATE | 65535B | 376 | 124 | 65 | 44,502 | 100 | 55 | 0.0; 0.0 | -107,390 | 1,528,529 | crates/codec/src/lib.rs:48 u64 0/21; crates/meta/src/overlay.rs:115 apply 20/0; crates/node/src/process.rs:1972 {closure#0} 7/13 |  | vendor/hyper-log/src/ticket.rs:211 poll 675k; crates/node/src/node.rs:3929 {closure#0} 254k |
| http | MPU-CREATE | 64KiB | 1,388 | 142 | 76 | 279,185 | 860 | 55 | 0.0; 0.0 | 1,576,786 | 1,773,982 | vendor/hyper-durable/src/owner.rs:224 ... 601/0; crates/node/src/node.rs:2147 {closure#1} 590/1; crates/meta/src/overlay.rs:115 apply 24/0 |  | vendor/hyper-timing/src/qos.rs:383 binomial_tail 50k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 50k |
| http | MPU-CREATE | 65537B | 442 | 141 | 73 | 71,167 | 33 | 55 | 0.0; 0.0 | 1,423,205 | 1,685,610 | crates/meta/src/overlay.rs:115 apply 24/0; vendor/hyper-durable/src/owner.rs:224 ... 23/0; crates/codec/src/lib.rs:48 u64 0/23 |  | crates/range/src/stored.rs:130 next 75k; crates/engine/src/shard_db.rs:2925 scan_inner 75k |
| http | MPU-CREATE | 128KiB | 420 | 127 | 69 | 20,304 | 71 | 55 | 0.2; 0.2 | 1,505,719 | 1,506,955 | vendor/hyper-durable/src/owner.rs:224 ... 30/0; crates/node/src/node.rs:2147 {closure#1} 21/1; crates/node/src/node.rs:2130 drive 22/0 | crates/engine/src/shard_db.rs:2415 apply 0.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 81k; crates/engine/src/memtable/hashed.rs:2135 {closure#1} 50k |
| http | MPU-CREATE | 8MiB | 361 | 105 | 41 | 40,375 | 532 | 55 | 3.8; 3.8 | 1,807,834 | 1,667,922 | crates/codec/src/lib.rs:48 u64 0/22; crates/meta/src/overlay.rs:115 apply 22/0; crates/meta/src/key.rs:620 escape 3/16 | crates/engine/src/store/mod.rs:3294 span_with_floor 2.8; crates/engine/src/shard_db.rs:2415 apply 0.8 | vendor/hyper-durable/src/owner.rs:230 Answers, mantle 100k; vendor/hyper-timing/src/qos.rs:416 election_time 75k |
| http | MPU-CREATE | 64MiB | 424 | 131 | 63 | 81,927 | 69 | 56 | 0.5; 0.5 | 1,773,698 | 1,567,120 | crates/meta/src/overlay.rs:115 apply 24/0; crates/codec/src/lib.rs:48 u64 0/23; crates/node/src/node.rs:2130 drive 23/0 | crates/engine/src/shard_db.rs:2415 apply 0.5 | crates/node/src/node.rs:1187 status 100k; vendor/hyper-liveness/src/lib.rs:1297 report 100k |
| http | MPU-PART | 0B | 1,448 | 511 | 275 | 383,697 | 276 | 69 | 2.8; 2.8 | 4,468,795 | 5,424,288 | crates/meta/src/overlay.rs:115 apply 139/0; vendor/hyper-durable/src/owner.rs:224 ... 105/0; crates/codec/src/lib.rs:48 u64 0/101 | crates/engine/src/memtable/hashed.rs:1866 hot_add 1.0; [libc.so.6 _int_malloc] (frame-pointer chain ends in the library) 0.8 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 251k; crates/meta/src/apply.rs:83 apply_entry 75k |
| http | MPU-PART | 1B | 1,508 | 527 | 278 | 421,630 | 305 | 69 | 19.8; 19.8 | 5,150,211 | 5,575,696 | crates/meta/src/overlay.rs:115 apply 141/0; vendor/hyper-durable/src/owner.rs:224 ... 126/0; crates/codec/src/lib.rs:48 u64 0/102 | crates/engine/src/store/mod.rs:1958 run 8.2; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 4.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 253k; crates/range/src/stored.rs:130 next 175k |
| http | MPU-PART | 65535B | 1,811 | 525 | 288 | 1,165,522 | 78 | 69 | 49.8; 49.8 | 49,084,191 | 8,091,820 | crates/meta/src/overlay.rs:115 apply 139/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 133/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 133/0 | crates/node/src/node/host.rs:822 step_host 34.0; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 6.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 4061k; vendor/hyper-log/src/ticket.rs:211 poll 2909k |
| http | MPU-PART | 64KiB | 9,871 | 552 | 293 | 3,125,160 | 4,706 | 84 | 63.2; 63.2 | 8,443,178 | 8,758,869 | crates/node/src/node.rs:2147 {closure#1} 3792/-4; vendor/hyper-durable/src/owner.rs:224 ... 3777/0; crates/node/src/node.rs:2130 drive 530/0 | crates/node/src/node/host.rs:822 step_host 36.8; crates/node/src/node.rs:0 on_event 9.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 193k; vendor/hyper-quic/src/connection/timer.rs:103 next_timeout 125k |
| http | MPU-PART | 65537B | 1,939 | 557 | 293 | 1,695,142 | 221 | 84 | 50.5; 50.5 | 7,447,847 | 7,892,229 | bytes-1.12.1/src/bytes_mut.rs:1373 from 152/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 152/0; crates/meta/src/overlay.rs:115 apply 141/0 | crates/node/src/node/host.rs:822 step_host 33.8; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 5.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 235k; crates/node/src/process.rs:2143 {closure#0} 225k |
| http | MPU-PART | 128KiB | 2,086 | 540 | 293 | 1,914,451 | 183 | 69 | 177.0; 177.0 | 11,977,776 | 13,087,724 | bytes-1.12.1/src/bytes_mut.rs:1373 from 188/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 188/0; crates/meta/src/overlay.rs:115 apply 141/0 | crates/node/src/node/host.rs:822 step_host 72.0; crates/node/src/node.rs:0 on_event 48.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/b 175k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 173k |
| http | MPU-PART | 8MiB | 33,153 | 824 | 427 | 95,635,511 | 544 | 69 | 3,979.2; 3,979.2 | 325,464,274 | 394,965,367 | bytes-1.12.1/src/bytes_mut.rs:1373 from 9556/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 9556/0; crates/node/src/node.rs:2130 drive 2504/0 | crates/node/src/node/host.rs:822 step_host 2035.8; crates/node/src/node.rs:0 on_event 1253.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 9068k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 4800k |
| http | MPU-PART | 64MiB | 447,414 | 214 | -1,747 | 1,157,621,248 | 41,417 | 101 | 145,866.0; 145,866.0 | 5,047,003,673 | 3,198,563,830 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 121809/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 121809/0; crates/node/src/node.rs:1604 {async_fn#0} 37118/0 | crates/node/src/node/host.rs:822 step_host 73231.5; crates/node/src/node.rs:0 on_event 47875.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 91619k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 86650k |
| http | MPU-COMPLETE | 0B | 700 | 252 | 140 | 110,203 | 390 | 66 | 0.5; 0.5 | 2,727,924 | 3,329,009 | crates/meta/src/key.rs:620 escape 2/44; crates/meta/src/key.rs:614 put_string 0/36; crates/meta/src/key.rs:554 objects 32/0 | crates/engine/src/memtable/hashed.rs:816 give_buf 0.5 | crates/engine/src/shard_db.rs:2925 scan_inner 150k; crates/node/src/node.rs:1187 status 100k |
| http | MPU-COMPLETE | 1B | 1,132 | 380 | 202 | 173,120 | 265 | 68 | 2.5; 2.5 | 3,835,725 | 4,007,871 | vendor/hyper-durable/src/owner.rs:224 ... 91/0; crates/codec/src/lib.rs:48 u64 0/66; crates/meta/src/overlay.rs:115 apply 66/0 | [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 1.0; crates/engine/src/branch/mod.rs:970 leaf_push 0.5 | vendor/hyper-timing/src/qos.rs:383 binomial_tail 131k; vendor/hyper-rt/src/timer.rs:182 next_event_tick 100k |
| http | MPU-COMPLETE | 65535B | 1,188 | 415 | 223 | 141,065 | 35 | 66 | 1.5; 1.5 | 13,744,680 | 4,555,918 | crates/meta/src/key.rs:620 escape 2/104; vendor/hyper-durable/src/owner.rs:224 ... 77/0; crates/meta/src/overlay.rs:115 apply 71/0 | crates/engine/src/branch/mod.rs:970 leaf_push 1.5 | vendor/hyper-log/src/ticket.rs:211 poll 2300k; crates/node/src/node.rs:2171 drive 1350k |
| http | MPU-COMPLETE | 64KiB | 5,868 | 450 | 235 | 1,171,654 | 3,022 | 66 | 2.5; 2.5 | 4,050,000 | 4,508,184 | vendor/hyper-durable/src/owner.rs:224 ... 2606/0; crates/node/src/node.rs:2147 {closure#1} 2442/-3; crates/meta/src/key.rs:620 escape 3/122 | crates/engine/src/branch/mod.rs:970 leaf_push 1.5; crates/engine/src/branch/mod.rs:496 push 0.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 150k; vendor/hyper-timing/src/qos.rs:383 binomial_tail 100k |
| http | MPU-COMPLETE | 65537B | 1,198 | 451 | 234 | 172,702 | 62 | 68 | 2.5; 2.5 | 3,794,175 | 4,036,930 | crates/meta/src/key.rs:620 escape 3/122; crates/meta/src/overlay.rs:115 apply 78/0; crates/codec/src/lib.rs:48 u64 0/74 | crates/engine/src/branch/mod.rs:970 leaf_push 1.5; crates/engine/src/shard_db.rs:2415 apply 0.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 164k; crates/meta/src/key.rs:620 escape 150k |
| http | MPU-COMPLETE | 128KiB | 1,072 | 415 | 210 | 125,271 | 184 | 66 | 3.0; 3.0 | 3,969,467 | 4,010,835 | crates/meta/src/key.rs:620 escape 3/108; crates/meta/src/overlay.rs:115 apply 73/0; crates/codec/src/lib.rs:48 u64 0/71 | crates/engine/src/branch/mod.rs:970 leaf_push 1.5; crates/engine/src/memtable/hashed.rs:791 take_buf 0.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 200k; vendor/hyper-liveness/src/pair.rs:590 {closure#1} 150k |
| http | MPU-COMPLETE | 8MiB | 1,164 | 447 | 233 | 280,503 | 78 | 68 | 19.5; 19.5 | 3,856,069 | 4,134,411 | crates/meta/src/key.rs:620 escape 3/122; crates/meta/src/overlay.rs:115 apply 78/0; crates/codec/src/lib.rs:48 u64 0/74 | crates/engine/src/store/mod.rs:3294 span_with_floor 16.5; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 1.5 | crates/engine/src/shard_db.rs:2925 scan_inner 100k; crates/range/src/stored.rs:130 next 100k |
| http | MPU-COMPLETE | 64MiB | 1,343 | 468 | 248 | 274,112 | 168 | 68 | 7.0; 7.0 | 4,200,920 | 3,921,117 | crates/meta/src/key.rs:620 escape 3/122; crates/codec/src/lib.rs:48 u64 0/81; crates/engine/src/fst/bits.rs:42 push 81/0 | crates/engine/src/branch/mod.rs:970 leaf_push 3.0; crates/node/src/node/host.rs:2765 reply_job 1.0 | crates/engine/src/memtable/hashed.rs:236 {closure#0} 200k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 175k |
| http | MPU-ABORT | 0B | 333 | 95 | 45 | 72,527 | 565 | 56 | -2.4; 0.5 | 2,143,738 | 1,815,210 | vendor/hyper-durable/src/owner.rs:224 ... 31/0; crates/node/src/node.rs:2147 {closure#1} 23/2; crates/node/src/node.rs:1588 {closure#5} 19/-1 | crates/engine/src/shard_db.rs:2415 apply 0.5 | crates/range/src/stored.rs:130 next 100k; vendor/hyper-liveness/src/pair.rs:594 {closure#2} 100k |
| http | MPU-ABORT | 1B | 570 | 148 | 77 | 251,867 | 237 | 56 | 22.5; 22.5 | 1,854,861 | 2,114,480 | vendor/hyper-durable/src/owner.rs:224 ... 63/0; crates/node/src/node.rs:2147 {closure#1} 36/4; crates/node/src/node.rs:2130 drive 27/0 | crates/engine/src/store/mod.rs:3294 span_with_floor 16.0; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 5.0 | vendor/hyper-timing/src/election.rs:278 span 100k; crates/engine/src/branch/mod.rs:2554 load 100k |
| http | MPU-ABORT | 65535B | 469 | 159 | 84 | 57,763 | 59 | 56 | 1.0; 1.0 | 3,061,531 | 2,172,281 | crates/meta/src/key.rs:620 escape 3/38; vendor/hyper-durable/src/owner.rs:224 ... 31/0; crates/node/src/node.rs:1588 {closure#5} 23/2 | crates/engine/src/branch/mod.rs:496 push 0.5; crates/engine/src/branch/mod.rs:970 leaf_push 0.5 | vendor/hyper-log/src/ticket.rs:211 poll 850k; crates/node/src/node.rs:2171 drive 300k |
| http | MPU-ABORT | 64KiB | 1,017 | 150 | 77 | 187,904 | 827 | 56 | 0.0; 0.0 | 1,729,937 | 2,404,569 | crates/node/src/node.rs:2147 {closure#1} 371/-1; vendor/hyper-durable/src/owner.rs:224 ... 360/0; crates/meta/src/key.rs:620 escape 2/38 |  | crates/engine/src/shard_db.rs:2925 scan_inner 100k; crates/engine/src/memtable/hashed.rs:2161 sort_step (.llvm.14267545125 50k |
| http | MPU-ABORT | 65537B | 415 | 145 | 75 | 46,177 | 80 | 56 | 0.0; 0.0 | 1,878,483 | 2,705,172 | crates/meta/src/key.rs:620 escape 2/38; crates/s3/src/sigv4.rs:631 percent_decode 11/7; crates/meta/src/record.rs:1604 take_bytes 18/0 |  | crates/engine/src/shard_db.rs:2925 scan_inner 100k; crates/node/src/node.rs:1256 patience 100k |
| http | MPU-ABORT | 128KiB | 506 | 166 | 86 | 56,950 | 66 | 56 | 1.5; 1.5 | 1,812,994 | 2,044,374 | crates/meta/src/key.rs:620 escape 3/45; vendor/hyper-durable/src/owner.rs:224 ... 31/0; crates/node/src/node.rs:2130 drive 26/0 | crates/engine/src/memtable/hashed.rs:1866 hot_add 0.5; crates/engine/src/shard_db.rs:2415 apply 0.5 | crates/node/src/node.rs:1447 {closure#0} 150k; vendor/hyper-raft/src/raft.rs:2693 Held> 100k |
| http | MPU-ABORT | 8MiB | 501 | 169 | 89 | 58,428 | 67 | 56 | 0.0; 0.0 | 1,839,062 | 2,312,464 | crates/meta/src/key.rs:620 escape 3/45; vendor/hyper-durable/src/owner.rs:224 ... 25/0; crates/node/src/node.rs:2147 {closure#1} 20/1 |  | vendor/hyper-durable/src/replica.rs:765 Rang 113k; crates/engine/src/shard_db.rs:2925 scan_inner 100k |
| http | MPU-ABORT | 64MiB | 354 | 121 | 47 | 35,079 | 137 | 58 | 1.0; 1.0 | 1,808,776 | 2,958,161 | crates/meta/src/key.rs:620 escape 2/30; crates/s3/src/sigv4.rs:631 percent_decode 11/7; crates/meta/src/record.rs:1604 take_bytes 17/0 | crates/engine/src/shard_db.rs:2415 apply 1.0 | crates/engine/src/memtable/hashed.rs:1992 heapify (.llvm.1426754512582 100k; vendor/hyper-durable/src/replica.rs:622 machine 100k |
| http | MPU-LIST-PARTS | 0B | 170 | 61 | 34 | 16,196 | 22 | 58 | 0.0; 0.0 | 339,992 | 577,065 | crates/s3/src/sigv4.rs:631 percent_decode 11/6; crates/node/src/node.rs:2130 drive 8/0; crates/s3/src/sigv4.rs:486 canonical_request 1/5 |  | crates/node/src/node.rs:1187 status 50k; vendor/hyper-liveness/src/lib.rs:1297 report 25k |
| http | MPU-LIST-PARTS | 1B | 173 | 63 | 35 | 18,844 | 73 | 58 | 0.0; 0.0 | 424,746 | 549,592 | crates/s3/src/sigv4.rs:631 percent_decode 11/6; crates/node/src/node.rs:1588 {closure#5} 8/0; crates/node/src/node.rs:2130 drive 7/0 |  | vendor/hyper-durable/src/replica.rs:765 Rang 50k; crates/node/src/node.rs:2159 drive 25k |
| http | MPU-LIST-PARTS | 65535B | 172 | 65 | 38 | 18,260 | 12 | 58 | 0.0; 0.0 | -294,183 | 545,028 | crates/s3/src/sigv4.rs:631 percent_decode 11/7; crates/meta/src/key.rs:620 escape 0/10; crates/node/src/node.rs:2130 drive 8/0 |  | vendor/hyper-rt/src/udp/linux.rs:604 160k; vendor/hyper-timing/src/folds.rs:39 mean 133k |
| http | MPU-LIST-PARTS | 64KiB | 155 | 64 | 37 | 16,023 | 363 | 58 | 0.0; 0.0 | 423,354 | 872,151 | crates/s3/src/sigv4.rs:631 percent_decode 11/7; crates/node/src/node.rs:1588 {closure#5} 12/0; crates/meta/src/key.rs:620 escape 0/10 |  | vendor/hyper-timing/src/link.rs:606 next_interval 25k; crates/node/src/node.rs:0 patience 25k |
| http | MPU-LIST-PARTS | 65537B | 190 | 65 | 37 | 36,912 | 9 | 58 | 0.0; 0.0 | 496,862 | 552,312 | crates/s3/src/sigv4.rs:631 percent_decode 11/7; crates/node/src/node.rs:1588 {closure#5} 13/0; crates/meta/src/key.rs:620 escape 0/10 |  | crates/node/src/node.rs:4040 send_plane 50k; vendor/hyper-rt/src/registry.rs:786 {im 25k |
| http | MPU-LIST-PARTS | 128KiB | 195 | 68 | 40 | 23,546 | 23 | 58 | 0.0; 0.0 | 523,171 | 600,154 | crates/s3/src/sigv4.rs:631 percent_decode 11/7; crates/node/src/node.rs:1588 {closure#5} 14/1; vendor/hyper-durable/src/owner.rs:224 ... 12/0 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 39k; crates/node/src/process.rs:2158 {closure#0} 25k |
| http | MPU-LIST-PARTS | 8MiB | 163 | 62 | 33 | 15,763 | 50 | 58 | 0.0; 0.0 | 369,467 | 273,585 | crates/s3/src/sigv4.rs:631 percent_decode 11/7; crates/meta/src/key.rs:620 escape 0/10; crates/node/src/node.rs:2130 drive 7/0 |  | crates/s3/src/time.rs:77 http_date 50k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 25k |
| http | MPU-LIST-PARTS | 64MiB | 202 | 70 | 44 | 26,085 | 19 | 58 | 1.0; 1.0 | 583,457 | 929,535 | crates/node/src/node.rs:1588 {closure#5} 20/2; crates/s3/src/sigv4.rs:631 percent_decode 11/7; vendor/hyper-durable/src/owner.rs:224 ... 15/0 | crates/engine/src/memtable/hashed.rs:1866 hot_add 1.0 | crates/s3/src/xml.rs:747 end 50k; vendor/hyper-rt/src/cells.rs:83 pop 50k |
| native | PUT | 0B | 504 | 192 | 100 | 127,540 | 121 | 28 | 0.2; 0.2 | 2,779,324 | 2,941,519 | crates/meta/src/key.rs:620 escape 3/57; crates/meta/src/overlay.rs:115 apply 42/0; crates/codec/src/lib.rs:48 u64 0/38 | crates/engine/src/shard_db.rs:2415 apply 0.2 | crates/ec/src/durability.rs:333 down 200k; crates/ec/src/durability.rs:877 transient_loss 175k |
| native | PUT | 1B | 3,786 | 498 | 272 | 490,457 | 641 | 28 | 1.4; 1.8 | 4,771,713 | 4,882,102 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; crates/meta/src/overlay.rs:115 apply 142/0 | crates/node/src/node.rs:2147 {closure#1} 0.5; crates/engine/src/shard_db.rs:2415 apply 0.5 | crates/ec/src/durability.rs:786 {closure#0} 325k; crates/ec/src/durability.rs:333 down 200k |
| native | PUT | 65535B | 4,634 | 578 | 324 | 2,152,919 | 269 | 218 | 50.2; 50.2 | 53,410,654 | 11,206,001 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 206/0 | crates/node/src/node/host.rs:822 step_host 41.2; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 4.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 2908k; vendor/hyper-log/src/ticket.rs:211 poll 1701k |
| native | PUT | 64KiB | 10,524 | 538 | 264 | 2,629,258 | 1,063 | 70 | 73.2; 73.2 | 10,226,099 | 9,589,105 | vendor/hyper-durable/src/owner.rs:224 ... 2948/0; crates/node/src/node.rs:2147 {closure#1} 1940/-2; crates/ec/src/durability.rs:779 {closure#0} 1742/0 | crates/node/src/node/host.rs:822 step_host 48.8; crates/gateway/src/put.rs:656 take 8.0 | vendor/hyper-timing/src/election.rs:278 span 262k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 162k |
| native | PUT | 65537B | 4,409 | 509 | 268 | 1,779,691 | 408 | 73 | 31.0; 31.0 | 8,099,405 | 7,682,760 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 201/0 | crates/node/src/node/host.rs:822 step_host 19.2; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 7.0 | crates/ec/src/durability.rs:333 down 275k; crates/ec/src/durability.rs:786 {closure#0} 275k |
| native | PUT | 128KiB | 4,761 | 571 | 324 | 2,561,990 | 211 | 125 | 176.8; 176.8 | 14,779,894 | 16,103,525 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 258/0 | crates/node/src/node/host.rs:822 step_host 96.0; crates/node/src/node.rs:0 on_event 56.0 | crates/ec/src/durability.rs:333 down 275k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 250k |
| native | PUT | 8MiB | 39,460 | 745 | 407 | 96,769,417 | 2,299 | 3,657 | 8,012.0; 8,012.0 | 501,090,529 | 494,138,886 | bytes-1.12.1/src/bytes_mut.rs:1373 from 10498/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 10498/0; crates/node/src/node.rs:1604 {async_fn#0} 2862/0 | crates/node/src/node/host.rs:822 step_host 4067.0; crates/node/src/node.rs:0 on_event 2790.8 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 9344k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 9175k |
| native | PUT | 64MiB | 374,987 | 2,595 | 1,314 | 898,864,409 | 5,466 | 43,474 | 135,210.7; 135,211.0 | 5,176,545,728 | 5,069,485,577 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 102311/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 102311/0; crates/node/src/node.rs:2130 drive 36186/0 | crates/node/src/node/host.rs:822 step_host 67184.5; crates/node/src/node.rs:0 on_event 44449.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 119632k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 104750k |
| native | PUT-STREAM | 0B | 2,975 | 190 | 103 | 261,710 | 100 | 25 | 1.0; 1.0 | 4,309,148 | 4,858,971 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; crates/meta/src/key.rs:620 escape 3/52 | crates/engine/src/memtable/hashed.rs:1866 hot_add 0.5; vendor/hyper-log/src/writer.rs:1304 {closure#2} 0.2 | crates/ec/src/durability.rs:786 {closure#0} 475k; [libc.so.6 __libc_calloc2] (frame-pointer chain ends in the library) 275k |
| native | PUT-STREAM | 1B | 1,449 | 531 | 284 | 367,124 | 309 | 28 | 4.5; 5.0 | 6,373,265 | 3,189,284 | crates/meta/src/overlay.rs:115 apply 153/0; vendor/hyper-durable/src/owner.rs:224 ... 115/0; crates/codec/src/lib.rs:48 u64 0/112 | vendor/hyper-rt-transport/src/driver.rs:489 size_ring 3.2; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 0.8 | crates/ec/src/durability.rs:786 {closure#0} 375k; crates/ec/src/durability.rs:333 down 150k |
| native | PUT-STREAM | 65535B | 4,296 | 514 | 283 | 1,370,419 | 260 | 144 | 70.5; 70.5 | 45,943,832 | 10,179,832 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 168/0 | crates/node/src/node/host.rs:822 step_host 42.0; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 11.0 | vendor/hyper-log/src/ticket.rs:211 poll 3911k; vendor/hyper-raft/src/node.rs:983 Held 1300k |
| native | PUT-STREAM | 64KiB | 10,429 | 513 | 275 | 2,816,849 | 4,175 | 95 | 80.8; 81.5 | 10,602,363 | 9,952,602 | vendor/hyper-durable/src/owner.rs:224 ... 4237/0; crates/node/src/node.rs:2147 {closure#1} 2678/-1; crates/node/src/node.rs:2130 drive 1889/0 | crates/node/src/node/host.rs:822 step_host 41.5; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 17.8 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 284k; crates/ec/src/durability.rs:786 {closure#0} 225k |
| native | PUT-STREAM | 65537B | 4,713 | 568 | 301 | 1,894,721 | 645 | 113 | 46.0; 46.0 | 8,969,933 | 10,746,026 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 244/0 | crates/node/src/node/host.rs:822 step_host 33.8; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 3.8 | crates/ec/src/durability.rs:333 down 350k; crates/ec/src/durability.rs:786 {closure#0} 275k |
| native | PUT-STREAM | 128KiB | 2,152 | 543 | 294 | 1,963,224 | 302 | 116 | 181.5; 181.5 | 14,155,309 | 191,889 | bytes-1.12.1/src/bytes_mut.rs:1373 from 208/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 208/0; crates/meta/src/overlay.rs:115 apply 153/0 | crates/node/src/node/host.rs:822 step_host 96.0; crates/node/src/node.rs:0 on_event 48.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 257k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/b 225k |
| native | PUT-STREAM | 8MiB | 52,975 | 1,626 | 840 | 98,404,483 | 2,708 | 3,890 | 10,223.5; 10,223.5 | 492,763,835 | 491,944,156 | bytes-1.12.1/src/bytes_mut.rs:1373 from 15695/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 15695/0; crates/node/src/node.rs:1604 {async_fn#0} 2767/0 | crates/node/src/node/host.rs:822 step_host 5113.5; crates/node/src/node.rs:0 on_event 3688.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 9684k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 8525k |
| native | PUT-STREAM | 64MiB | 417,685 | 5,657 | 2,978 | 776,725,871 | 6,092 | 29,386 | 96,090.0; 96,090.0 | 3,974,331,160 | 3,886,224,026 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 138831/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 138831/0; crates/node/src/node.rs:2130 drive 28240/0 | crates/node/src/node/host.rs:822 step_host 48387.5; crates/node/src/node.rs:0 on_event 31746.0 | vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 80950k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 73291k |
| native | GET | 0B | 99 | 14 | 6 | 14,372 | 30 | 19 | 4.0; 4.0 | 411,297 | 12,833 | crates/node/src/node.rs:1588 {closure#5} 12/1; crates/node/src/node.rs:2130 drive 7/0; vendor/hyper-durable/src/owner.rs:224 ... 6/0 | vendor/hyper-rt-transport/src/driver.rs:489 size_ring 3.2; vendor/hyper-log/src/owner/write.rs:922 frame_bytes 0.5 | vendor/hyper-raft/src/raft.rs:2693 Held> 50k; vendor/hyper-timing/src/qos.rs:164 golden 44k |
| native | GET | 1B | 356 | 52 | 29 | 53,818 | 68 | 21 | 0.2; 0.2 | 1,195,360 | 3,304,951 | crates/node/src/node.rs:2130 drive 29/0; crates/node/src/node.rs:1588 {closure#5} 26/-1; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 20/0 | vendor/hyper-rt-transport/src/driver.rs:489 size_ring 0.2 | vendor/hyper-liveness/src/lib.rs:1297 report 50k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 50k |
| native | GET | 65535B | 557 | 52 | 30 | 644,392 | 2 | 154 | 4.5; 4.5 | 14,599,645 | 3,831,255 | bytes-1.12.1/src/bytes_mut.rs:1373 from 63/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 63/0; crates/node/src/node.rs:2130 drive 58/0 | crates/gateway/src/get.rs:755 span 3.8; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 0.5 | vendor/hyper-quic/src/connection/timer.rs:103 next_timeout 2958k; vendor/hyper-rt/src/udp/batched.rs:1157 {closure_env#0}< 1708k |
| native | GET | 64KiB | 517 | 57 | 33 | 604,438 | 216 | 141 | 10.0; 10.0 | 4,954,502 | 3,276,886 | bytes-1.12.1/src/bytes_mut.rs:1373 from 72/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 72/0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 31/0 | crates/gateway/src/get.rs:755 span 3.8; crates/gateway/src/get.rs:912 finish 3.2 | crates/ec/src/durability.rs:786 {closure#0} 300k; crates/ec/src/durability.rs:785 {closure#0} 150k |
| native | GET | 65537B | 730 | 51 | 30 | 660,349 | 87 | 154 | 20.0; 20.0 | 2,556,422 | 2,300,402 | crates/node/src/node.rs:2130 drive 283/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 52/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 52/0 | crates/gateway/src/get.rs:755 span 11.2; crates/gateway/src/get.rs:912 finish 7.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 107k; vendor/hyper-raft/src/raft.rs:2693 Held> 103k |
| native | GET | 128KiB | 855 | 57 | 33 | 1,220,880 | 27 | 135 | 87.2; 87.2 | 2,733,317 | 5,372,720 | bytes-1.12.1/src/bytes_mut.rs:1373 from 164/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 164/0; crates/node/src/node.rs:2130 drive 76/0 | crates/gateway/src/get.rs:755 span 35.8; crates/gateway/src/get.rs:912 finish 32.0 | crates/ec/src/durability.rs:786 {closure#0} 275k; crates/ec/src/durability.rs:177 loss_within 225k |
| native | GET | 8MiB | 30,167 | 64 | 38 | 66,526,008 | 1,250 | 10,518 | 6,379.8; 6,379.8 | 143,622,533 | 153,538,130 | bytes-1.12.1/src/bytes_mut.rs:1373 from 7409/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 7409/0; crates/node/src/node.rs:2130 drive 5896/0 | crates/node/src/node.rs:2953 {closure#0} 2032.5; crates/gateway/src/get.rs:912 finish 1919.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 5163k; vendor/hyper-quic/src/connection/mod.rs:911 fill_step 1425k |
| native | GET | 64MiB | 141,497 | 297 | 184 | 528,679,161 | 1,336 | 27,966 | 52,583.5; 52,583.5 | 1,409,645,836 | 1,340,568,199 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 32799/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 32799/0; crates/node/src/node.rs:2130 drive 17745/0 | crates/gateway/src/get.rs:912 finish 13573.5; crates/gateway/src/get.rs:755 span 13573.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 52101k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 21600k |
| native | GET-RANGE | 1B | 379 | 58 | 35 | 57,831 | 63 | 21 | 0.0; 0.0 | 2,494,419 | 1,238,331 | crates/node/src/node.rs:1588 {closure#5} 36/1; crates/node/src/node.rs:2130 drive 30/0; vendor/hyper-durable/src/owner.rs:224 ... 24/0 |  | crates/ec/src/durability.rs:786 {closure#0} 400k; crates/ec/src/durability.rs:333 down 200k |
| native | GET-RANGE | 65535B | 81 | 43 | 21 | 540,082 | 417 | 52 | 8.0; 8.0 | 1,779,477 | 2,470,618 | bytes-1.12.1/src/bytes_mut.rs:1373 from 41/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 41/0; crates/node/src/node.rs:1588 {closure#5} 35/1 | crates/gateway/src/get.rs:755 span 7.5; [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 0.5 | vendor/hyper-rt/src/timer.rs:248 link 243k; crates/node/src/node.rs:1447 {closure#0} 237k |
| native | GET-RANGE | 64KiB | 521 | 56 | 33 | 513,913 | 319 | 62 | 5.8; 5.8 | 2,405,802 | 5,350,151 | bytes-1.12.1/src/bytes_mut.rs:1373 from 61/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 61/0; crates/node/src/node.rs:1588 {closure#5} 37/0 | crates/gateway/src/get.rs:755 span 3.8; crates/gateway/src/get.rs:912 finish 1.5 | vendor/hyper-rt/src/udp/linux.rs:604 75k; /aws-lc/crypto/fipsmodule/cipher/e_aes.c:1128 aead_aes_gcm_seal_scatte 75k |
| native | GET-RANGE | 65537B | 410 | 55 | 32 | 660,535 | 180 | 60 | 11.0; 11.0 | 3,070,309 | 2,148,816 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 52/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 52/0; crates/node/src/node.rs:1588 {closure#5} 28/0 | crates/gateway/src/get.rs:755 span 7.5; crates/gateway/src/get.rs:912 finish 2.5 | crates/ec/src/durability.rs:786 {closure#0} 425k; crates/ec/src/durability.rs:333 down 100k |
| native | GET-RANGE | 128KiB | 776 | 57 | 34 | 1,092,163 | 46 | 58 | 68.5; 68.5 | 3,049,267 | 4,108,602 | bytes-1.12.1/src/bytes_mut.rs:1373 from 151/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 151/0; crates/node/src/node.rs:2130 drive 63/0 | crates/gateway/src/get.rs:755 span 39.5; crates/node/src/node.rs:2953 {closure#0} 16.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 122k; /aws-lc/crypto/fipsmodule/cipher/e_aes.c:1128 aead_aes_gcm_seal_scatte 100k |
| native | GET-RANGE | 8MiB | 5,684 | 58 | 35 | 26,610,689 | 6,627 | 3,511 | 2,446.2; 2,446.2 | 58,328,260 | 77,949,119 | bytes-1.12.1/src/bytes_mut.rs:1373 from 3287/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 3287/0; crates/node/src/node.rs:1604 {async_fn#0} 818/0 | crates/gateway/src/get.rs:755 span 704.0; crates/node/src/node.rs:2953 {closure#0} 704.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 1526k; vendor/hyper-quic/src/connection/mod.rs:911 fill_step 750k |
| native | GET-RANGE | 64MiB | 35,898 | 155 | 96 | 150,825,494 | 364 | 9,317 | 21,517.5; 21,517.5 | 474,638,614 | 374,143,012 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 8822/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 8822/0; crates/node/src/node.rs:2130 drive 4305/0 | crates/node/src/node.rs:2953 {closure#0} 5472.0; crates/gateway/src/get.rs:755 span 5472.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 19717k; crates/node/src/node.rs:1187 status 9050k |
| native | HEAD | 0B | 117 | 19 | 11 | 15,583 | 27 | 18 | 0.0; 0.0 | 193,481 | -1,194,922 | crates/node/src/node.rs:2130 drive 9/0; crates/node/src/node.rs:1588 {closure#5} 8/-0; vendor/hyper-durable/src/owner.rs:224 ... 8/0 |  | vendor/hyper-rt/src/udp/batched.rs:1157 {cl 25k; vendor/hyper-rt-transport/src/plane.rs:71 Stored, hyper_log 25k |
| native | HEAD | 1B | 146 | 30 | 13 | 35,801 | 61 | 20 | 0.0; 0.0 | 403,166 | 1,372,257 | crates/node/src/node.rs:1588 {closure#5} 14/-0; crates/node/src/node.rs:2130 drive 11/0; vendor/hyper-durable/src/owner.rs:224 ... 7/0 |  | crates/node/src/node.rs:1187 status 50k; vendor/hyper-liveness/src/lib.rs:1297 report 25k |
| native | HEAD | 65535B | 112 | 17 | 9 | 14,698 | 8 | 18 | 0.0; 0.0 | -337,960 | 615,620 | crates/node/src/node.rs:2130 drive 9/0; crates/node/src/node.rs:1588 {closure#5} 8/0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0 |  | vendor/hyper-timing/src/link.rs:901 margin 181k; crates/node/src/node.rs:1374 step_replacements 112k |
| native | HEAD | 64KiB | 290 | 18 | 10 | 59,152 | 12 | 23 | 0.0; 0.0 | 593,163 | 486,908 | crates/node/src/node.rs:2130 drive 187/0; crates/node/src/node.rs:1588 {closure#5} 7/-0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0 |  | vendor/hyper-rt/src/shard_loop.rs:1356 park_with_cleanup (.llvm.389730 25k; vendor/hyper-transport/src/endpoint.rs:1229 queue_sending 25k |
| native | HEAD | 65537B | 121 | 16 | 9 | 16,707 | 55 | 21 | 0.0; 0.0 | 633,706 | -604,795 | crates/node/src/node.rs:1588 {closure#5} 13/0; vendor/hyper-durable/src/owner.rs:224 ... 9/0; crates/node/src/node.rs:2147 {closure#1} 7/0 |  | vendor/hyper-timing/src/link.rs:901 margin 25k; crates/engine/src/memtable/hashed.rs:1651 walk_from_into 25k |
| native | HEAD | 128KiB | 107 | 14 | 7 | 12,956 | 22 | 23 | 0.0; 0.0 | 441,908 | 471,457 | crates/node/src/node.rs:2130 drive 9/0; vendor/hyper-durable/src/owner.rs:224 ... 7/0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0 |  | vendor/hyper-timing/src/qos.rs:385 binomial_tail 38k; crates/node/src/node.rs:2147 {closure#1} 25k |
| native | HEAD | 8MiB | 42 | 17 | 10 | 35,535 | 158 | 22 | 0.0; 0.0 | 425,034 | 706,932 | vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0; crates/meta/src/key.rs:620 escape 0/5; crates/node/src/node.rs:1588 {closure#5} 5/-0 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 25k; crates/engine/src/memtable/hashed.rs:2023 seek_run 25k |
| native | HEAD | 64MiB | 114 | 19 | 11 | 80,328 | 10 | 24 | 0.0; 0.0 | 331,985 | 262,538 | crates/node/src/node.rs:2130 drive 8/0; crates/node/src/node.rs:1588 {closure#5} 8/0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0 |  | vendor/hyper-quic/src/connection/mod.rs:5520 predict_1rtt_overhead 50k; vendor/hyper-rt/src/runtime.rs:865 {cl 50k |
| native | DELETE | 0B | 384 | 93 | 45 | 47,992 | 91 | 16 | 0.2; 0.2 | 1,623,791 | 1,819,544 | crates/meta/src/key.rs:620 escape 2/36; vendor/hyper-durable/src/owner.rs:224 ... 29/0; crates/node/src/node.rs:2147 {closure#1} 22/1 | crates/engine/src/shard_db.rs:2415 apply 0.2 | vendor/hyper-timing/src/histogram.rs:114 quantile 100k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 74k |
| native | DELETE | 1B | 416 | 109 | 55 | 70,188 | 185 | 19 | 0.2; 0.2 | 1,395,292 | 2,233,039 | crates/meta/src/key.rs:620 escape 2/37; vendor/hyper-durable/src/owner.rs:224 ... 33/0; crates/node/src/node.rs:2147 {closure#1} 25/1 | crates/engine/src/shard_db.rs:2415 apply 0.2 | crates/node/src/node.rs:1447 {closure#0} 52k; vendor/hyper-rt/src/shard_loop.rs:537 now_ns 50k |
| native | DELETE | 65535B | 458 | 135 | 70 | 77,923 | 30 | 22 | 0.0; 0.0 | 12,659,449 | 1,920,497 | crates/meta/src/key.rs:620 escape 3/45; vendor/hyper-durable/src/owner.rs:224 ... 32/0; crates/node/src/node.rs:2147 {closure#1} 26/1 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 1085k; vendor/hyper-log/src/ticket.rs:211 poll 803k |
| native | DELETE | 64KiB | 1,441 | 121 | 62 | 283,429 | 742 | 20 | 0.2; 0.2 | 1,566,279 | 2,115,239 | vendor/hyper-durable/src/owner.rs:224 ... 550/0; crates/node/src/node.rs:2147 {closure#1} 540/1; crates/meta/src/key.rs:620 escape 3/41 | crates/engine/src/util/coding.rs:168 put_varint32s 0.2 | vendor/hyper-durable/src/replica.rs:765 Rang 50k; vendor/hyper-liveness/src/lib.rs:1297 report 50k |
| native | DELETE | 65537B | 383 | 108 | 52 | 83,272 | 142 | 18 | 0.0; 0.0 | 1,996,311 | 1,806,148 | crates/meta/src/key.rs:620 escape 2/36; vendor/hyper-durable/src/owner.rs:224 ... 32/0; crates/node/src/node.rs:2147 {closure#1} 29/0 |  | vendor/hyper-timing/src/qos.rs:416 election_time 125k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 97k |
| native | DELETE | 128KiB | 414 | 126 | 68 | 53,539 | 118 | 18 | 0.2; 0.2 | 1,561,085 | 1,753,171 | crates/meta/src/key.rs:620 escape 3/39; vendor/hyper-durable/src/owner.rs:224 ... 26/0; crates/node/src/node.rs:2130 drive 23/0 | crates/engine/src/shard_db.rs:2415 apply 0.2 | vendor/hyper-timing/src/qos.rs:385 binomial_tail 75k; crates/node/src/process.rs:2143 {closure#0} 75k |
| native | DELETE | 8MiB | 378 | 114 | 60 | 45,008 | 96 | 16 | 0.2; 0.2 | 1,811,994 | 1,572,979 | crates/meta/src/key.rs:620 escape 3/38; crates/engine/src/memtable/hashed.rs:1007 get_hashed 21/0; crates/meta/src/record.rs:1604 take_bytes 20/0 | crates/engine/src/shard_db.rs:2415 apply 0.2 | crates/engine/src/shard_db.rs:2925 scan_inner 100k; vendor/hyper-liveness/src/lib.rs:1297 report 100k |
| native | DELETE | 64MiB | 399 | 116 | 62 | 49,707 | 84 | 16 | 0.5; 0.5 | 1,457,887 | 1,633,289 | crates/meta/src/key.rs:620 escape 2/38; vendor/hyper-durable/src/owner.rs:224 ... 23/0; crates/node/src/node.rs:2130 drive 22/0 | crates/engine/src/shard_db.rs:2415 apply 0.5 | vendor/hyper-durable/src/owner.rs:158 100k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/c 100k |
| native | LIST | page of 1,000 | 11,957 | 4,717 | 1,331 | 1,130,090 | 223 | 1,536 | 45.0; 45.0 | 96,563,903 | 118,289,365 | crates/meta/src/key.rs:634 take_string 2669/1335; crates/range/src/stored.rs:132 {closure#0} 2669/0; crates/meta/src/key.rs:620 escape 0/2003 | crates/codec/src/lib.rs:56 bytes 14.0; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 5.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/c 5033k; crates/engine/src/fst/bits.rs:239 {closure#2} 3967k |
| native | MPU-CREATE | 0B | 380 | 101 | 50 | 28,809 | 163 | 20 | 0.5; 0.5 | 1,308,344 | 1,798,927 | vendor/hyper-durable/src/owner.rs:224 ... 32/0; crates/codec/src/lib.rs:48 u64 0/22; crates/node/src/process.rs:1972 {closure#0} 7/15 | crates/engine/src/shard_db.rs:2415 apply 0.5 | crates/engine/src/memtable/hashed.rs:186 start_pass 50k; crates/node/src/process.rs:2143 {closure#0} 50k |
| native | MPU-CREATE | 1B | 434 | 110 | 53 | 90,447 | 161 | 19 | 0.2; 0.2 | 1,515,357 | 660,282 | vendor/hyper-durable/src/owner.rs:224 ... 44/0; crates/node/src/node.rs:2147 {closure#1} 31/3; crates/codec/src/lib.rs:48 u64 0/24 | crates/engine/src/shard_db.rs:2415 apply 0.2 | vendor/hyper-timing/src/election.rs:278 span 82k; crates/range/src/stored.rs:130 next 75k |
| native | MPU-CREATE | 65535B | 426 | 116 | 60 | 74,057 | 16 | 26 | 0.2; 0.2 | 10,528,571 | 1,459,538 | vendor/hyper-durable/src/owner.rs:224 ... 31/0; crates/node/src/node.rs:2147 {closure#1} 24/1; crates/codec/src/lib.rs:48 u64 0/24 | crates/engine/src/shard_db.rs:2415 apply 0.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 1226k; vendor/hyper-log/src/ticket.rs:211 poll 1223k |
| native | MPU-CREATE | 64KiB | 667 | 86 | 39 | 247,150 | 4,101 | 20 | 0.2; 0.2 | 1,662,550 | 3,308,801 | vendor/hyper-durable/src/owner.rs:224 ... 1517/0; crates/node/src/node.rs:2147 {closure#1} 1303/-6; crates/codec/src/lib.rs:48 u64 0/23 | crates/engine/src/shard_db.rs:2415 apply 0.2 | crates/node/src/process.rs:2143 {closure#0} 75k; vendor/hyper-timing/src/election.rs:278 span 65k |
| native | MPU-CREATE | 65537B | 464 | 116 | 57 | 102,774 | 164 | 21 | 0.5; 0.5 | 2,195,799 | 1,405,216 | vendor/hyper-durable/src/owner.rs:224 ... 74/0; crates/node/src/node.rs:2147 {closure#1} 50/3; crates/meta/src/overlay.rs:115 apply 25/0 | crates/engine/src/shard_db.rs:2415 apply 0.5 | crates/node/src/node.rs:1447 {closure#0} 150k; crates/node/src/node.rs:2163 drive 100k |
| native | MPU-CREATE | 128KiB | 387 | 101 | 47 | 67,167 | 80 | 22 | 0.2; 0.2 | 1,283,700 | 1,653,708 | vendor/hyper-durable/src/owner.rs:224 ... 28/0; crates/codec/src/lib.rs:48 u64 0/23; crates/node/src/node.rs:2147 {closure#1} 23/0 | crates/engine/src/shard_db.rs:2415 apply 0.2 | vendor/hyper-rt/src/shard_loop.rs:1037 receive_completions 50k; crates/meta/src/session.rs:291 Stored<hyper_bloc 50k |
| native | MPU-CREATE | 8MiB | 401 | 100 | 49 | 75,955 | 146 | 20 | 0.0; 0.0 | 1,552,067 | 1,683,546 | vendor/hyper-durable/src/owner.rs:224 ... 56/0; crates/node/src/node.rs:2147 {closure#1} 49/1; crates/codec/src/lib.rs:48 u64 0/23 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 75k; crates/node/src/process.rs:2143 {closure#0} 75k |
| native | MPU-CREATE | 64MiB | 316 | 86 | 40 | -7,237 | 198 | 18 | 0.0; 0.0 | 1,597,257 | 1,974,015 | crates/codec/src/lib.rs:48 u64 0/21; crates/meta/src/overlay.rs:115 apply 19/0; crates/node/src/process.rs:1972 {closure#0} 6/12 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/t 50k; crates/node/src/node.rs:1559 {async_fn#0} 50k |
| native | MPU-PART | 0B | 1,364 | 482 | 271 | 326,825 | 232 | 25 | 3.2; 3.2 | 5,987,668 | 5,227,071 | crates/meta/src/overlay.rs:115 apply 138/0; vendor/hyper-durable/src/owner.rs:224 ... 102/0; crates/codec/src/lib.rs:48 u64 0/101 | [libc.so.6 sysmalloc] (frame-pointer chain ends in the library) 1.0; crates/engine/src/shard_db.rs:2415 apply 0.8 | crates/ec/src/durability.rs:177 loss_within 375k; [libc.so.6 __libc_calloc2] (frame-pointer chain ends in the library) 250k |
| native | MPU-PART | 1B | 1,382 | 477 | 263 | 313,549 | 385 | 24 | 4.8; 4.8 | 4,739,431 | 5,113,033 | crates/meta/src/overlay.rs:115 apply 141/0; vendor/hyper-durable/src/owner.rs:224 ... 110/0; crates/codec/src/lib.rs:48 u64 0/104 | vendor/hyper-rt-transport/src/driver.rs:489 size_ring 3.5; crates/engine/src/shard_db.rs:2415 apply 0.5 | vendor/hyper-rt/src/shard_loop.rs:1209 poll_batch 125k; crates/engine/src/util/coding.rs:315 get_varint32_ptr 125k |
| native | MPU-PART | 65535B | 2,118 | 481 | 263 | 1,281,075 | 94 | 88 | 56.8; 56.8 | 144,181,907 | 11,006,423 | vendor/hyper-durable/src/owner.rs:224 ... 252/0; crates/node/src/node.rs:2147 {closure#1} 188/-1; crates/node/src/node.rs:2130 drive 180/0 | crates/node/src/node/host.rs:822 step_host 33.8; vendor/hyper-rt-transport/src/driver.rs:489 size_ring 12.5 | vendor/hyper-quic/src/connection/timer.rs:103 next_timeout 16905k; vendor/hyper-log/src/ticket.rs:211 poll 9484k |
| native | MPU-PART | 64KiB | 9,623 | 490 | 274 | 2,675,721 | 2,990 | 93 | 56.0; 56.0 | 9,932,358 | 8,147,503 | vendor/hyper-durable/src/owner.rs:224 ... 3307/0; crates/node/src/node.rs:2147 {closure#1} 2379/-2; crates/node/src/node.rs:2130 drive 2323/0 | crates/node/src/node/host.rs:822 step_host 37.5; crates/gateway/src/put.rs:656 take 7.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 183k; vendor/hyper-rt/src/udp/linux.rs:604 111k |
| native | MPU-PART | 65537B | 3,071 | 511 | 284 | 1,711,618 | 2,212 | 98 | 66.8; 66.8 | 8,395,758 | 9,059,894 | crates/ec/src/durability.rs:779 {closure#0} 871/0; vendor/hyper-durable/src/owner.rs:224 ... 408/0; crates/node/src/node.rs:2147 {closure#1} 379/5 | crates/node/src/node/host.rs:822 step_host 45.0; crates/gateway/src/put.rs:656 take 11.5 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 208k; vendor/hyper-raft/src/raft.rs:2693 Held> 186k |
| native | MPU-PART | 128KiB | 4,424 | 445 | 251 | 2,137,834 | 447 | 120 | 237.2; 237.2 | 18,752,161 | 13,470,736 | crates/ec/src/durability.rs:779 {closure#0} 1742/0; crates/ec/src/durability.rs:792 product 567/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 209/0 | crates/node/src/node/host.rs:822 step_host 120.0; crates/node/src/node.rs:0 on_event 48.0 | crates/ec/src/durability.rs:786 {closure#0} 300k; vendor/hyper-timing/src/qos.rs:383 binomial_tail 225k |
| native | MPU-PART | 8MiB | 37,429 | 782 | 371 | 120,139,556 | 17,376 | 4,716 | 11,972.0; 11,972.0 | 458,480,076 | 485,240,328 | bytes-1.12.1/src/bytes_mut.rs:1373 from 13847/0; bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 13847/0; crates/node/src/node.rs:1604 {async_fn#0} 4499/0 | crates/node/src/node/host.rs:822 step_host 5990.8; crates/node/src/node.rs:0 on_event 4068.2 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 10304k; vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 7425k |
| native | MPU-PART | 64MiB | 410,199 | 2,629 | 1,368 | 987,203,373 | 4,091 | 43,484 | 141,863.0; 141,863.0 | 5,777,771,652 | 3,761,649,861 | bytes-1.12.1/src/bytes_mut.rs:1127 promote_to_shared (.llvm.1807562128 113777/0; bytes-1.12.1/src/bytes_mut.rs:1373 from 113777/0; crates/node/src/node.rs:2130 drive 36143/0 | crates/node/src/node/host.rs:822 step_host 70084.0; crates/node/src/node.rs:0 on_event 46736.5 | vendor/hyper-quic/src/connection/mod.rs:1751 handle_event 120000k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 101659k |
| native | MPU-COMPLETE | 0B | 820 | 284 | 152 | 94,653 | 100 | 24 | 1.0; 1.0 | 2,153,171 | 1,187,352 | crates/meta/src/key.rs:620 escape 3/116; crates/meta/src/overlay.rs:115 apply 48/0; vendor/hyper-durable/src/owner.rs:224 ... 41/0 | crates/engine/src/shard_db.rs:2415 apply 1.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 300k; vendor/hyper-timing/src/folds.rs:42 mean 100k |
| native | MPU-COMPLETE | 1B | 1,085 | 371 | 201 | 139,146 | 271 | 24 | 0.5; 0.5 | 3,403,462 | 1,575,745 | crates/meta/src/key.rs:620 escape 2/104; vendor/hyper-durable/src/owner.rs:224 ... 86/0; crates/meta/src/overlay.rs:115 apply 71/0 | crates/engine/src/shard_db.rs:2415 apply 0.5 | crates/node/src/node.rs:1187 status 135k; crates/codec/src/lib.rs:48 u64 100k |
| native | MPU-COMPLETE | 65535B | 1,051 | 403 | 218 | 124,225 | 91 | 34 | 1.0; 1.0 | 11,866,930 | 3,594,689 | crates/meta/src/key.rs:620 escape 3/122; crates/meta/src/overlay.rs:115 apply 78/0; crates/codec/src/lib.rs:48 u64 0/75 | crates/engine/src/shard_db.rs:2415 apply 1.0 | crates/node/src/node.rs:1187 status 1696k; vendor/hyper-log/src/ticket.rs:211 poll 1629k |
| native | MPU-COMPLETE | 64KiB | 3,121 | 406 | 220 | 620,387 | 1,645 | 30 | 0.0; 0.0 | 5,622,592 | 5,058,769 | vendor/hyper-durable/src/owner.rs:224 ... 1261/0; crates/node/src/node.rs:2147 {closure#1} 1126/0; crates/meta/src/key.rs:620 escape 3/122 |  | vendor/hyper-timing/src/election.rs:278 span 200k; crates/node/src/node.rs:1187 status 150k |
| native | MPU-COMPLETE | 65537B | 1,039 | 337 | 187 | 162,876 | 250 | 28 | 1.0; 1.0 | 4,147,065 | 4,103,289 | vendor/hyper-durable/src/owner.rs:224 ... 99/0; crates/meta/src/key.rs:620 escape 2/85; crates/codec/src/lib.rs:48 u64 0/66 | crates/engine/src/shard_db.rs:2415 apply 1.0 | vendor/hyper-durable/src/replica.rs:765 Rang 150k; crates/node/src/node.rs:1268 patience 150k |
| native | MPU-COMPLETE | 128KiB | 1,069 | 403 | 220 | 163,668 | 302 | 36 | 0.5; 0.5 | 4,031,797 | 3,721,742 | crates/meta/src/key.rs:620 escape 3/122; crates/meta/src/overlay.rs:115 apply 78/0; crates/codec/src/lib.rs:48 u64 0/75 | crates/engine/src/shard_db.rs:2415 apply 0.5 | vendor/hyper-timing/src/folds.rs:39 mean 100k; vendor/hyper-timing/src/election.rs:278 span 100k |
| native | MPU-COMPLETE | 8MiB | 1,130 | 417 | 230 | 170,902 | 118 | 26 | 0.0; 0.0 | 3,640,954 | 3,596,782 | crates/meta/src/key.rs:620 escape 3/122; crates/meta/src/overlay.rs:115 apply 78/0; vendor/hyper-durable/src/owner.rs:224 ... 77/0 |  | crates/node/src/node.rs:1236 status 100k; crates/node/src/node.rs:1268 patience 100k |
| native | MPU-COMPLETE | 64MiB | 1,046 | 401 | 206 | 123,463 | 191 | 40 | 1.0; 1.0 | 4,317,380 | 3,804,204 | crates/meta/src/key.rs:620 escape 3/122; crates/meta/src/overlay.rs:115 apply 78/0; crates/codec/src/lib.rs:48 u64 0/68 | crates/engine/src/shard_db.rs:2415 apply 1.0 | vendor/hyper-rt/src/timer.rs:315 level_shift 200k; vendor/hyper-rt/src/shard_loop.rs:961 apply_interest 100k |
| native | MPU-ABORT | 0B | 421 | 114 | 61 | 56,869 | 82 | 18 | 0.0; 0.0 | 1,953,422 | 1,695,859 | crates/meta/src/key.rs:620 escape 2/38; vendor/hyper-durable/src/owner.rs:224 ... 34/0; crates/node/src/node.rs:2147 {closure#1} 24/2 |  | vendor/hyper-timing/src/link.rs:606 next_interval 100k; vendor/hyper-log/src/lib.rs:258 clear 50k |
| native | MPU-ABORT | 1B | 454 | 95 | 54 | 94,135 | 184 | 22 | 7.5; 7.5 | 1,989,353 | 1,010,910 | vendor/hyper-durable/src/owner.rs:224 ... 62/0; crates/node/src/node.rs:2147 {closure#1} 34/6; crates/meta/src/key.rs:620 escape 2/29 | crates/node/src/node.rs:2138 {closure#1} 2.5; vendor/hyper-raft/src/raft.rs:868 push_counted 2.0 | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/c 200k; vendor/hyper-raft/src/wire.rs:677 take_body 150k |
| native | MPU-ABORT | 65535B | 491 | 140 | 75 | 98,601 | 43 | 26 | 0.0; 0.0 | -18,549,683 | 1,501,721 | crates/meta/src/key.rs:620 escape 3/45; vendor/hyper-durable/src/owner.rs:224 ... 46/0; crates/node/src/node.rs:2147 {closure#1} 35/2 |  | vendor/hyper-raft/src/raft.rs:2693 Held> 408k; vendor/hyper-durable/src/replica.rs:765 Rang 200k |
| native | MPU-ABORT | 64KiB | 1,933 | 110 | 59 | 435,283 | 1,150 | 24 | 0.0; 0.0 | 1,806,857 | 2,161,073 | vendor/hyper-durable/src/owner.rs:224 ... 871/0; crates/node/src/node.rs:2147 {closure#1} 858/-1; crates/meta/src/key.rs:620 escape 2/36 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 100k; crates/engine/src/shard_db.rs:2782 scan 50k |
| native | MPU-ABORT | 65537B | 436 | 139 | 75 | 55,369 | 109 | 21 | 0.0; 0.0 | 1,741,851 | 453,242 | crates/meta/src/key.rs:620 escape 3/45; vendor/hyper-durable/src/owner.rs:224 ... 25/0; crates/node/src/node.rs:2147 {closure#1} 23/1 |  | vendor/hyper-durable/src/replica.rs:1951 Answers, ma 100k; crates/node/src/node.rs:2163 drive 100k |
| native | MPU-ABORT | 128KiB | 443 | 125 | 69 | 55,142 | 83 | 19 | 0.0; 0.0 | 1,511,392 | 2,263,086 | crates/meta/src/key.rs:620 escape 3/39; vendor/hyper-durable/src/owner.rs:224 ... 35/0; crates/node/src/node.rs:2130 drive 25/0 |  | crates/engine/src/shard_db.rs:2925 scan_inner 150k; crates/node/src/node.rs:1447 {closure#0} 100k |
| native | MPU-ABORT | 8MiB | 463 | 142 | 76 | 60,821 | 61 | 18 | 0.0; 0.0 | 1,986,855 | 2,443,060 | crates/meta/src/key.rs:620 escape 3/45; vendor/hyper-durable/src/owner.rs:224 ... 28/0; crates/node/src/node.rs:2130 drive 26/0 |  | vendor/hyper-raft/src/raft.rs:2693 Held> 100k; vendor/hyper-timing/src/election.rs:278 span 100k |
| native | MPU-ABORT | 64MiB | 444 | 146 | 80 | 58,502 | 71 | 20 | 0.0; 0.0 | 1,714,126 | 1,973,456 | crates/meta/src/key.rs:620 escape 3/45; crates/node/src/process.rs:1972 {closure#0} 10/20; crates/node/src/node.rs:1588 {closure#5} 24/0 |  | vendor/hyper-raft/src/configuration.rs:189 voters 100k; crates/range/src/stored.rs:130 next 100k |
| native | MPU-LIST-PARTS | 0B | 102 | 21 | 12 | 11,575 | 38 | 18 | 3.0; 3.0 | 437,880 | 477,260 | crates/meta/src/key.rs:620 escape 0/10; crates/node/src/node.rs:2130 drive 8/0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0 | vendor/hyper-rt-transport/src/driver.rs:489 size_ring 3.0 | crates/node/src/node.rs:1187 status 25k; vendor/hyper-durable/src/replica.rs:622 machine 25k |
| native | MPU-LIST-PARTS | 1B | 158 | 40 | 21 | 20,204 | 46 | 18 | 0.0; 0.0 | 340,455 | 377,821 | crates/node/src/node.rs:1588 {closure#5} 13/0; vendor/hyper-durable/src/owner.rs:224 ... 11/0; crates/meta/src/key.rs:620 escape 0/10 |  | crates/range/src/stored.rs:130 next 25k; vendor/hyper-transport/src/tally.rs:385 settle 25k |
| native | MPU-LIST-PARTS | 65535B | 118 | 25 | 14 | 47,653 | 7 | 21 | 0.0; 0.0 | 775,139 | 249,740 | crates/meta/src/key.rs:620 escape 0/10; crates/node/src/node.rs:2130 drive 9/0; crates/node/src/node.rs:1588 {closure#5} 8/0 |  | crates/node/src/node.rs:1187 status 226k; vendor/hyper-rt/src/udp/batched.rs:1157 {closure_env#0}< 141k |
| native | MPU-LIST-PARTS | 64KiB | 106 | 25 | 16 | 29,283 | 145 | 21 | 0.0; 0.0 | 368,091 | -134,468 | crates/meta/src/key.rs:620 escape 0/10; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0; crates/node/src/node.rs:1588 {closure#5} 5/-0 |  | vendor/hyper-timing/src/link.rs:606 next_interval 25k; vendor/hyper-rt-transport/src/driver.rs:687 take_fired 25k |
| native | MPU-LIST-PARTS | 65537B | 117 | 26 | 16 | 14,374 | 12 | 20 | 0.0; 0.0 | 341,700 | 208,558 | crates/meta/src/key.rs:620 escape 0/10; crates/node/src/node.rs:2130 drive 7/0; crates/node/src/node.rs:1588 {closure#5} 7/0 |  | vendor/hyper-raft/src/raft.rs:4936 file: 25k; vendor/hyper-rt/src/timer.rs:128 arm 25k |
| native | MPU-LIST-PARTS | 128KiB | 99 | 22 | 12 | 11,142 | 20 | 22 | 0.0; 0.0 | 261,397 | 227,279 | crates/meta/src/key.rs:620 escape 0/10; crates/node/src/node.rs:2130 drive 8/0; vendor/hyper-quic/src/connection/streams/send.rs:245 pop_chunk 6/0 |  | vendor/hyper-transport/src/endpoint.rs:1279 : 25k; /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 25k |
| native | MPU-LIST-PARTS | 8MiB | 117 | 25 | 17 | 31,015 | 22 | 20 | 0.0; 0.0 | 291,905 | 267,904 | crates/meta/src/key.rs:620 escape 0/10; crates/node/src/node.rs:2130 drive 8/0; crates/node/src/node.rs:1588 {closure#5} 7/-0 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/r 41k; /aws-lc/crypto/fipsmodule/cipher/e_aes.c:1128 aead_aes_gcm_seal_scatte 25k |
| native | MPU-LIST-PARTS | 64MiB | 138 | 41 | 23 | 51,008 | 34 | 22 | 0.0; 0.0 | 436,994 | 808,805 | crates/meta/src/key.rs:620 escape 0/16; crates/node/src/node.rs:1588 {closure#5} 12/0; crates/node/src/node.rs:2130 drive 6/0 |  | /home/adalundhe/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/b 50k; vendor/hyper-rt/src/handoff.rs:103 register_task 50k |

## The sites: is the work needed, and how much of it

Each family below is one site or a few sites with one cause; the counts are per operation, servers
summed, from the rebased runs (1 B, 8 MiB) unless said. "Needed" is answered from what the
operation must produce (bytes on the wire, a durable record, a reply); "how much" from the same
measurement with what the operation must produce as the floor.

| site (where) | what the measurement shows | needed at all? | how much is needed? |
|---|---|---|---|
| transport receive copy, `hyper_transport::receive::Receive::take` (vendor/hyper-transport/src/receive.rs:79, `Bytes::from(BytesMut)` → `to_vec`) | one allocation and one copy of every received stream byte, per datagram: native PUT 8 MiB 10,498 allocs and 25.6 MB/op on the servers (the two replica streams and the client's body), native GET 8 MiB 4,368 allocs and 8.7 MB on the client; scales with datagrams, not bytes asked | no: the datagram buffer already holds the bytes; the copy exists because the slice is converted out of a shared `BytesMut` | 0 copies, 0 allocations per datagram (hand the frozen slice on) |
| QUIC packet split, `PartialDecode::finish` (vendor/hyper-quic/src/packet.rs:195, `split_to` → `promote_to_shared`) | one 40-byte shared header per received datagram: identical counts to the row above (10,498 per native PUT 8 MiB) | partly: a received buffer split into packets needs a shared owner | one per receive batch, not per datagram |
| QUIC copy per write, `ByteSlice::pop_chunk` (vendor/hyper-quic/src/connection/streams/send.rs:245, `taken.to_owned()`) | every byte written to a stream copied into a fresh `Vec`: native PUT 8 MiB 16.8 MB/op on the servers (both replica streams), 8.39 MB on the client; 1,771 server allocs/op; each exchange takes 3 writes with no body (native GET: 3.0 client allocs/op) and 5 with a 1-byte body | no: `SendStream::write_chunks` takes `Bytes` by ownership | 0 copies; one `Bytes` per written chunk |
| QUIC send-buffer entry, `SendBuffer::write` (vendor/hyper-quic/src/connection/send_buffer.rs:52) | one allocation per write: 1,745/op on native PUT 8 MiB servers, 863 on the client | the entry is needed; its allocation is not (the queue is emptied and its storage dropped between writes) | 0 allocations in steady state |
| QUIC retransmittable-frames box, `get_or_create` (vendor/hyper-quic/src/connection/spaces.rs:637 via `write_max_data`) | a `Box` for each packet that carries a MAX_DATA frame: 2,608/op native PUT 8 MiB servers, 883 on the native GET 8 MiB client | the record of what a packet carried is needed for loss recovery | 0 allocations: inline or pooled records |
| node events, `turn` (crates/node/src/node.rs, `self.events.push`) | the events `Vec` is `mem::take`n each turn, so each turn regrows it: 19/op at 1 B, 2,862/op at native PUT 8 MiB | no | 0: keep the buffer across turns |
| `drive`'s per-turn collections (node.rs `drive`: `groups`, `driven`, `messages`; vendor/hyper-durable/src/owner.rs:224 `peers().collect()`) | every turn, busy or idle: 154 (native PUT 1 B) to 5,874 (native GET 8 MiB) allocs/op; 64 + 20 a second at rest | no | 0: reused buffers or iteration in place |
| plane message copy (node.rs, `inbox.push((sender, at, message.to_vec()))`) | one allocation per heartbeat/plane message: 54/op at 1 B, 2,877/op at 8 MiB | the message must outlive the receive buffer only until it is handled in the same turn | 0 |
| volume table's durability model, `scheme` → `durability::choose` (crates/node/src/node/host.rs `Table`, crates/ec/src/durability.rs:779/792) | 2,448.5 allocations whenever it runs: in the rebased runs it fell in native PUT 1 B, native PUT 8 MiB and native GET 8 MiB alike (the same count to the half allocation), in other runs elsewhere; it runs once per entry the root range applies, not per request | not per entry: the scheme depends on the table's failure domains and the gateway's rates, which change only when the volume table does | once per change of the volume table: 0 per request |
| `status` lines and `say` (crates/node/src/process.rs `status` format, hyper-rt sync ring oneshot) | a formatted `String` and a oneshot per range status line, each time a commit moves: 45 allocs/op at native PUT 1 B, 100 at http PUT 8 MiB; 43 a second at rest | the line is how `mantle serve` reports a range's leader to its operator and the cell; its allocations are not | 0 allocations (write into a reused line buffer) |
| codec `Writer` growth (crates/codec/src/lib.rs `u8`/`u32`/`u64`/`u128`/`bytes`, from mantle_meta's record encoders) | 115 allocs and 225 reallocations per native PUT 1 B, 198 and 343 at 8 MiB: buffers start empty and double | the encoded bytes are needed; the growth is not | 0 reallocations (size the writer from the record) |
| meta key encoding (crates/meta/src/key.rs `escape`, `put_string`, `objects`) | 73 allocs and 147 reallocations per native PUT 1 B; a listing page 2,669 + 2,003 reallocations | the keys are needed | 0 reallocations; one buffer per command |
| meta overlay row clone (crates/meta/src/overlay.rs:115 `apply`) | 167 allocs/op (native PUT 1 B), 264 at 8 MiB | a staged row must be owned until the entry applies | at most one per row written (about a dozen per PUT) |
| host chunk copy per replica (crates/node/src/node/host.rs `ask_put`, `bytes.to_vec()` per `put::Request::Chunk`) | three copies of every sealed byte: 25.2 MB/op at native PUT 8 MiB (6 allocations), 226 MB at 64 MiB | no: the three replicas carry the same bytes | 0 copies (share one `Bytes`) |
| gateway GET, `Get::finish` (crates/gateway/src/get.rs:870 gather, :899 per-segment `to_vec`, :883 output) | three copies of every byte read: 25.2 MB/op at 8 MiB (2 + 128 + 2 allocs), 201 MB at 64 MiB; the 64 KiB segment copy is 128 allocations per 8 MiB | one buffer is needed for the opened plaintext; the gather is needed only where spans arrive apart | 1 copy (open each segment in place in the gathered buffer and hand out slices) |
| gateway PUT seal (crates/gateway/src/put.rs `seal_segment`) | one ciphertext buffer per block: 8.39 MB at 8 MiB | yes: the ciphertext must be written somewhere | as measured (or 0 extra if sealed in place in the received buffer) |
| node receive of a chunk body (node.rs `on_event`, `Receiving.bytes` with capacity) | 16.8 MB/op at 8 MiB (the two remote replicas each assemble the chunk) | the chunk must be contiguous to check its CRC and write it: one buffer, yes | 1 copy, but it follows the transport's copy above, so the bytes are copied twice on the receiver |
| S3 body decode (crates/node/src/listener/conn.rs `decode`) | 10.5 MB/op at http PUT 8 MiB; aws-chunked `feed` 67 MB at 64 MiB stream | a body piece handed to the host must be owned; plain bodies need no decoding | ≤1 copy (plain: hand the input buffer's slice on) |
| chunk reader buffer (crates/chunk/src/reader.rs `check`) | 8.39 MB per 8 MiB GET, filled by the kernel, so its first touches are kernel-mode faults | yes | as measured |
| UDP receive ring (vendor/hyper-rt/src/udp/batched.rs:479 `Ring::resize`, zeroed) | 15 allocs, 1 MB/op on the servers at 8 MiB, 4.4 MB at 64 MiB: the ring grows during a transfer and is zeroed each time | the ring is needed | 0 allocations in steady state (size once from the measured burst) |
| hashbrown growth (several maps: `mantle_chunk::index`, hyper-quic's sent-packet maps) | 2,629/op at native PUT 8 MiB, 27 at 1 B | tables are needed, re-growing per operation is not | 0 in steady state |
| listing page (crates/meta/src/key.rs `take_string`, crates/range/src/stored.rs:132 `to_vec`, crates/s3 `iso8601`/`quoted`, crates/protocol `take_text`) | 11,957 (native) and 15,191 (S3) allocations per page of 1,000 keys, about 12–15 a key; 97 and 83 million instructions a page, most in the engine's scan (fst trie seeks, CRC of blocks read, `read_exact_at`) | the keys and the scan are needed | 0 allocations per key (write the page straight into the reply); the instructions are the engine's (steps 1–3), not the door's |

Faults follow the copies: a fault is the first touch of a fresh page, so the large-size faults are
the large copies above (e.g. native PUT 64 MiB: 135,211 faults/op, 528 MB of first touches, in
`step_host`'s chunk copies 67,184, `on_event`'s receive buffers 44,450 and the seal 23,240; GET
64 MiB: `Get::finish` 27,148 and the chunk read 11,995). Small operations take few (0–23 a 1 B
operation, most of them 0–5). Instructions at 8–64 MiB are AES-GCM (aws-lc: QUIC packet protection and data
sealing, 17–33%), hyper-quic (24–28%), the transport and its driver (13–19%), and the loop's clock
and timer polling (4–7%); at 1 B they are the engine and range (18–34%), the durability model where
it ran (25% of native PUT 1 B), the node and the transport.

## Tails

The stall trace ran at 64 KiB, 400 operations per phase, 4 clients, open loop, with 16 busy loops
pinned to the cell's CPUs 0-7 (2× the cores), twice: at 300 requests a second (8,000 requests) and
at 60 (8,000). Every request above the run's p99.9 (7 in each) was matched to its server job by key
and time and attributed: **0 unattributed**.

- **Above the run's p99.9 (25.9 s at 300/s, 14.0 s at 60/s), every request waited for a free
  client**: the writes (PUT, PUT-STREAM, MPU-PART) under 16 busy loops take about 65 ms each at the
  median, so 4 clients carry at most ~60 a second and an open loop at or above that queues in the
  generator. Above the p99, 98.2% and 98.8% of latency is that wait (intended start to send); the
  servers' admission depth was 0 or 1 job at every take, so no queue formed inside a node.
- **Service time above each door's p99.9** (send to done; 3.9 s native and 1.7 s S3 at 300/s,
  1.1 s and 1.6 s at 60/s): native DELETE and S3 PUT waited on a **command** (the range's commit)
  for 0.7–4.2 s, S3 PUT and native MPU-PART on a **chunk** write to a remote volume for 0.2–1.8 s,
  and two native PUTs on the gap **between one request's exchanges** (0.9–1.0 s, the client side:
  the harness's runtime thread starved by the busy loops). During each commit or chunk wait every
  node's loop was busy for under 8% of it (the busiest node 1–143 ms of a 0.2–4.2 s wait), and the
  longest turn was 0.3–4.9 ms: **the stall is outside the node loops**, in what the commit and the
  chunk write wait on (the log's and the volume's flush under CPU contention, and the asks' timers),
  which the trace's stages do not split further.
- **The node loop's turns**: p50 9–16 µs, p99 0.8–1.8 ms, p99.9 3.1–4.4 ms, max 8–53 ms; the longest
  turn on each node (8.3–53 ms) was 92–97% wall time less thread CPU time: preempted by the busy
  loops, not working.

## The candidates

| candidate | verdict | the number |
|---|---|---|
| The transport's three writes per body and QUIC's copy of each (`write` vs `write_chunks`) | **confirmed** | an exchange takes 3 writes with no body (native GET client: 3.0 `pop_chunk` allocs/op) and 5 with a 1-byte body; each write copies its bytes (`pop_chunk` `to_owned`): 8,388,715 B copied per 8 MiB native PUT on the client, 16.8 MB on the servers; plus a send-buffer entry allocated per write (863 and 1,745/op) |
| Allocations that scale with packets inside hyper-quic | **confirmed** | per received datagram one shared header (`promote_to_shared`, 10,498/op native PUT 8 MiB) and one copy (`receive.rs` `take`, same count, 25.6 MB); per MAX_DATA packet one `Box` (2,608/op); 102,311 of each at native PUT 64 MiB |
| The client's reply gathered then split | **dropped** | mantle-client allocates 1 time per operation at every size (264 B); its reply path is one pass over the body, 3.0 million instructions per 8 MiB native GET (0.36 instructions a byte, 2.9% of the client's instructions with the harness's checks set aside); the client's per-packet costs are the transport's rows above |
| The gateway host's carry copy, per-chunk `to_vec`, range command clones (crates/gateway, host) | **confirmed** for the copies, **small** for the clones | host `ask_put` copies each sealed block per replica (3 copies, 25.2 MB per 8 MiB PUT); `Get::finish` copies each byte read 3 times (25.2 MB per 8 MiB GET, 128 per-segment `to_vec`s); the native carry (`read_fields`' `split_off`) and the command clones (`Command::Name(command.clone())` and kin) are within the host's 49.5 allocs/op at 1 B, 73 at 8 MiB |
| `mantle_codec::Writer` growing by doubling | **confirmed** | 225 reallocations (and 115 allocations) per native PUT 1 B, 343 per 8 MiB, from record encoders writing into writers that start empty |
| The 34.3 MB brotli window at a server's first TLS handshake (hyper-tls cert compression) | **dropped as measured** | brotli allocates once per server process, in its first window (start and the cell's own handshakes): 1.82 MB asked by one server and 0.96 MB by each other, 164 and 99 allocations; never again in any window of any run; the 34.3 MB is not what the allocator is asked for |
| The 1.25 MB QUIC DATAGRAM receive allowance per connection | **dropped** | 0 allocations under hyper-quic's datagram code in every window of every run: the allowance is a bound on buffered DATAGRAM frames, which the doors do not send, not memory a connection holds |
| The node's ranges and transport sharing one shard (inline engine writes holding the loop; f18) | **dropped as a tail cause at these sizes, confirmed as a structure** | both doors, every range and the transport run on the node thread's one shard (process.rs `runtime`, `shards = 1`); under 16 busy loops the slowest requests waited on commits and chunk writes while every node's loop was busy for under 8% of the wait, and the longest turns were 92–97% preemption |
| QUIC receive-side ACK processing | **confirmed, moderate** | sampled instructions under hyper-quic's ACK handling (`on_ack_received`, `on_packets_acked`, `SentPackets`, loss detection, ACK frequency): 4.7–7.5% of the servers' instructions at 8–64 MiB (23 million per native PUT 8 MiB, 354 million per 64 MiB), 1% at 1 B; hyper-quic as a whole is 24–28% |

## What the raw data is, and where

Kept under `~/.cache/claude-scratch/doors/ledger/` (not in the repository):

- `bin/` the three rebased diag binaries (mantle-stacks, mantle-plain, mantle-fp) the perf and
  stacks data symbolize against; `bin-old/` the pre-rebase stacks binary.
- `runs/s-{1,65537,8388608,list}` rebased stacks runs; `runs/s-{0,65535,65536,131072,67108864}`
  pre-rebase stacks runs (reused, above); `runs-old/` the pre-rebase runs the rebased ones replace.
  Each holds `out.txt` (the bench's lines and the window map), `stacks/stacks-<pid>-<window>.txt`
  (every window's sites and the three totals), `attrib.json`/`attrib.txt` (symbolized, checked),
  `perop.json` (per-operation sites), `compare.txt` (rebased against pre-rebase).
- `runs/fault-*`: perf records per window (`perf/*.data`, faults and instruction samples with
  frame-pointer stacks, the harness included), `perf/windows.txt` (bounds and minflt),
  `faults.json`/`faults.txt`.
- `runs/stat-*`: `perf stat` per process per window, `stat.json`/`stat.txt`.
- `runs/tail-64k`, `runs/tail-64k-r60`: the stall traces (`trace/`), `stall.json`.
- `scripts/`: cell.sh, hook.sh, batch2.sh, tail.sh, attrib.py, perop.py, compare.py, faults.py,
  stat.py, stall.py, cand.py, cats.py, gen.py; `ledger-table.md.faultcheck` holds each cell's kernel
  and perf fault counts.

The bench exits non-zero at the end of every run on its controls ("a control's object was not put:
slowed or unanswered"), in the diag and the plain build alike and before any diagnostic code ran in
the first try; every measured phase completed with 0 failed operations.

## Step 5, after

Step 5 cut seven of the sites above at their cause (commits 6cb5eb21 bench, a6d1ad29 node and
gateway, ea55821d docs, 8d6d9ae6 bench): the turn's events, `drive`'s lists and the plane's
message copies; the volume table's scheme, carried while its volumes' domains hold; `mantle
serve`'s lines, written into one kept buffer and said once a turn; a put's chunk, one `Bytes` for
every copy; and `Get::finish`, each segment opened where it was gathered (one copy of each byte
read). The vendor sites (hyper-quic, hyper-transport, hyper-durable's `peers().collect()`, 64 a
second at rest) are hyper-raft's and untouched.

**Counted exactly, before and after** (each test red on the code before, run once against it):

| site | test | before | after |
|---|---|---:|---:|
| events, `drive`'s lists, plane copies: a cell of three at rest, 10 s simulated | `crates/node/tests/turn_allocs.rs` | 477,128 allocs (`drive` 189,808, plane 119,301, events 0) | 168,019 |
| the same serving three clients, 40 ops each | same | 77,323 (events 395, `drive` 19,870, plane 11,970) | 45,088–45,089 (clients' random ids) |
| scheme over an unchanged table of three domains | `host::tests::the_scheme_is_chosen_again_only_when_the_tables_domains_change` | 4,670 allocs an entry applied | 0 (1,833 when a domain changes) |
| a turn's two link lines and a status line, buffer grown | `process::tests::a_turns_lines_are_written_into_the_kept_buffer_as_each_was_said` | 4 allocs, 2 reallocs (the link lines' `String`s) and a flush's oneshot a line | 0, one flush a turn, byte-identical |
| a 60,000-byte chunk put from bytes its copies share | `crates/chunk/tests/volume_memory.rs` `a_chunk_put_from_shared_bytes_copies_none_of_them` | 4 allocs, 60,780 B | 3, 780 B |
| an 8 MiB GET's own work | `crates/gateway/tests/object_path.rs` `a_get_copies_each_byte_it_reads_once` | 146 allocs, 25,177,456 B | 16, 8,396,144 B |

**`mantle bench doors`, count-allocs release builds** (before b39a1173, after a6d1ad29), CPUs 0-7 in
a 16G scope, the baseline batch on 8-15. Run A: `--sizes 1,64K,8M,64M --objects 0 --ops 368
--clients 2 --load open:100` (n 368, 32 at 8 MiB and 4 at 64 MiB by the 256 MB step); run B: `--sizes
8M --ops 368 --bytes 3G --load open:20 --volume 40G`. Background subtracted as above, the idle rate
measured on the same cell, servers summed, 60 s after 30 s of settling (`counts` twice): 2,802
allocations and 638 KB a second before, 1,042 and 55 KB after. Native door, per operation:

| op | size | allocs before → after | bytes before → after | faults before → after |
|---|---|---:|---:|---:|
| PUT | 1 B | 1,901 → 1,320 | 367,917 → 273,710 | 8.4 → 8.2 |
| PUT | 64 KiB | 2,022 → 1,689 | 1,108,162 → 829,204 | 39.6 → 9.9 |
| PUT | 8 MiB (B, n 368) | 52,114 → 51,175 | 101,905,865 → 75,424,334 | 8,079 → 6,063 |
| PUT | 64 MiB (n 4) | 445,050 → 614,720 (3 SlowDown asks again after, 1 before) | 815,768,932 → 788,729,408 | 100,769 → 65,280 |
| GET | 1 B | 390 → 273 | 61,462 → 34,915 | 0.0 → 0.1 |
| GET | 64 KiB | 542 → 371 | 564,953 → 388,626 | 8.4 → 0.4 |
| GET | 8 MiB (B, n 368) | 17,964 → 15,660 | 63,167,458 → 45,602,489 | 4,645 → 5,582 |
| GET | 64 MiB (n 4) | 139,961 → 104,395 | 542,754,146 → 433,443,793 | 74,999 → 58,757 |
| HEAD | 1 B / 8 MiB (B) | 110 → 81 / 135 → 57 | 18,965 → 12,622 / 28,057 → 11,749 | 0 → 0 |
| DELETE | 1 B / 8 MiB (B) | 453 → 360 / 487 → 325 | 59,154 → 43,622 / 71,772 → 39,570 | 1.1 → 1.6 / 0.1 → 0.1 |

The 8 MiB PUT keeps its per-datagram vendor sites (the transport's receive copy and QUIC's
header, about 10,500 each), so its allocations move little; its bytes fall 26.5 MB, the three
chunk copies. GET 8 MiB falls 17.6 MB, the two copies `Get::finish` no longer makes; its faults did
not fall in either run (6,463 → 7,214 in A, 4,645 → 5,582 in B), which this step does not explain.

**Latency** (µs, from intended start; open loop past capacity, so writes queue in the generator):
native GET 1 B p50/p99 754/3,108 → 1,606/4,588 in A, but 2,228/21,496 → 819/3,801 and 999/6,029 →
1,032/8,651 in two runs with the order reversed (C, D: after first): the harness's own CPU a
GET (73,873 → 131,007 ns in A; 169,186 → 80,717 in C) moved with it, so the box, not the change,
set these. Native PUT 1 B p50 8.46 s → 9.66 s (A), 10.2 s → 8.86 s (C): the generator's queue.
GET 8 MiB (B, n 368) p50/p99 73/478 ms → 115/411 ms; PUT 8 MiB 25.2/54.8 s → 28.5/53.7 s (queued).
No latency change is claimed either way from these runs.

**The bench's exit.** Every run of the build before ended exit 1 on its controls, "a control's
object was not put", in A, B, C and D alike; every run of the build after reported the six
controls as failures (6cb5eb21: a native client held past its keep-alive is opened again). Runs A
and B after still exit 1, and B before had too, on SlowDown asked three times under the open
loop's overload (native MPU-PART 8 MiB in both B runs; 157 asks again before, 161 after), which
step 2's acceptance does not cover.

## Step 6, after

Step 6 cut the HTTP door's sites at their cause (commits e565b1d5 http, cc64cd66 s3 and node),
every RFC 9112 verdict, refusal, signature and checksum kept and every byte on the wire as it was:

- **The head** is parsed where it lies with no allocation: a head keeps the span of its field
  section and its count of lines, each judged a field line when the head was parsed, and
  `Head::fields` reads them from there. The verdicts are the same ones in the same order (the
  head's end found first, every line judged, then its fields); the rule tests and the oracle
  property tests pass unchanged.
- **SigV4**: the canonical request is written into its SHA-256 as it is formed, its path and
  query decoded and re-encoded byte by byte (a query's parameters sorted where they lie, one list
  of them, only for a request with a query); the string to sign is one buffer at its length, and
  a chunked body's chain keeps one, made at its bound (`CHAIN_REST`), for every chunk's and the
  trailer's string.
- **aws-chunked** is decoded in place (`Decoder::step`): each run of payload is a span of the
  connection's buffer, its framing line held in an array of its own, every chunk's signature and
  the trailing checksum checked as before. Each run goes to the host as a piece of exactly its
  bytes, one copy, as a plain body's does; a document is read into one buffer at the length its
  request stated within its limit.
- **The response**: its field lines are written once, checked as written, into a buffer the
  connection keeps; its head and its document, or a GET's head and its first bytes, go in one
  vectored write (`Io::write_pair`). The request id, host id and Date are written where they go
  (`time::Stamp`); an error's resource only when an error names it.
- **XML** documents are made at their length (`xml::Writer::document` runs the body once to
  count), their numbers, quoted ETags and times written in place.

Not done, and why: a plain body's pieces are still copied once out of the connection's buffer,
not handed on as slices of it. hyper-rt reads only into initialized memory, so a buffer shared
with the host would have to be made, and zeroed for the read, for every piece it gave away: an
allocation and a write of every byte per piece where the copy is one of each, so allocations per
PUT would rise past the ledger's (reasoned from the code, not measured). TLS's records: the
bench's cells serve plain HTTP on loopback and the ledger attributes no copy to hyper-tls's
buffered API, so it is unchanged. JSON (bucket policies) has no site in the ledger and is
unchanged.

**Counted exactly, before and after** (each test red on the code before, run once against it):

| site | test | before | after |
|---|---|---:|---:|
| a head parsed (codec), every head shape of the rule tests cut at every length, the 8 KiB head and one octet more, 256 mutated | `crates/http/tests/allocations.rs` `every_head_shape_is_parsed_with_no_allocation`, `a_mutated_head_is_parsed_with_no_allocation` | 1 allocation a head with a field | 0 |
| SigV4, AWS's GetObject example verified | `crates/s3/tests/allocations.rs` `a_signature_is_checked_in_three_buffers` | 33 allocs, 20 reallocs | 3, 0 (signing key, string to sign, access key) |
| aws-chunked, AWS's 66,560-byte, three-chunk example decoded and its signatures checked | same, `an_aws_chunked_body_is_decoded_in_place_with_no_allocation` | 11, 32 | 0, 0 |
| a ListObjectsV2 document | same, `a_listings_document_allocates_nothing_a_key` | 7, 8 at 1 key | 2, 0 at 1, 10 and 1,000 keys |

## Step 7, after

Step 7 made the metadata's messages at their lengths (commits 20f64d90 codec, meta and range,
77542df9 gateway and node, cc64cd66 for the listing's documents):

- **`mantle_codec::encode`** runs a message's encoder once on a writer that only counts, then
  into a buffer of exactly that length: every row value, entry and answer is one allocation and
  never grows. A record inside an entry is written in place behind its length
  (`record::put_record`), its CRC-32C over what it wrote, with no buffer of its own.
- **Keys** are made at their lengths, computed from their parts; a key read back is unescaped
  into one buffer at its length, and the Name layer's scans pass over the bucket they never read
  (`key::decode_object_row`).
- **The overlay** keeps the rows it is given, moved: `Rows::apply` takes an entry's writes by
  value, and the first entry of a term adds its row to its batch instead of copying the batch.
- **Range commands** go to their ranges moved once the node has room (`Node::command_room`);
  without it the request waits whole, as before. A completion asks its Name command once, its list
  of up to 10,000 parts moved into it, the parts' ETags moved from the request's list.
- **The listing page**: the keys a scan passes are made and read at their lengths, the listener
  moves a page's keys, ETags and owners into its document's page, and the document allocates
  nothing a key.

Not done: a listing still allocates per key in the range's scan (`Stored::next` copies each row
it returns out of the engine's buffer, two a key), in the Name layer's decode of the version
(its ETag and owner), in the page's own key and its positions, and on the gateway's decode of the
page (`take_text`): writing the page straight into the reply needs the engine to lend a row and
the page to be written into the reply as it is scanned, a change of `Rows` and of the native
page's encoding this step did not make.

**Golden vectors.** `tests/golden/{wire,record,object}.txt` hold the bytes the code before
wrote of every range command (alone and together), every answer, one row of each kind and every
native request and reply this suite builds: 216 vectors, generated from 44c6d21e; 0 bytes differ
(`wire::tests::every_command_and_answer_is_its_golden_vector`,
`record::tests::every_row_is_its_golden_vector`,
`object::tests::every_request_and_reply_is_its_golden_vector`).

| site | test | before | after |
|---|---|---:|---:|
| an entry of a put's File and Block commands (extents, referrer, block header, chunks) | `crates/meta/tests/encoding_allocs.rs` `an_entry_is_one_buffer_made_at_its_length` | 9 allocs, 27 reallocs | 1, 0 |
| a file header, an extent, an answer | same, `a_row_and_an_answer_are_each_one_buffer` | file header 1, 4 | 1, 0 each |
| 16 row keys, 3 key spans, a Name key read back | same, `a_key_is_one_buffer_made_at_its_length` | Name null key 1, 2 | 1, 0 each; spans 2, 0; read back 1, 0 |
| an overlay given three puts and a delete | same, `an_overlay_keeps_its_rows_by_move` | 8, 0 | 1, 0 (its map's node) |
| a cell of three serving its clients 40 ops each | `crates/node/tests/turn_allocs.rs` | 45,088–45,089 allocs, 6,372–6,377 reallocs | 41,182 in 7 of 7 runs, 2,303–2,307 |

## Steps 6 and 7, measured

**`mantle bench doors`, count-allocs release builds** (before 44c6d21e, after 77542df9), CPUs 0-7
in a 16G scope. Run A: `--sizes 1,64K,8M --objects 4000 --ops 368 --clients 2 --load open:100`;
run B: `--sizes 8M --objects 0 --ops 368 --bytes 3G --clients 2 --load open:20 --volume 40G`.
Background subtracted as in step 5, the idle rate measured on one smoke cell for each build (60 s
after 30 s of settling, servers summed): 840.6 allocations and 59,312 bytes a second before,
642.9 and 42,901 after; reallocations are not subtracted. Servers per operation, before → after:

| door | op | size (run, n) | allocs | reallocs | bytes | faults |
|---|---|---|---:|---:|---:|---:|
| http | PUT | 1B (A, n 368) | 1,307 → 1,189 | 493 → 75.5 | 364,741 → 259,674 | 1.8 → 1.5 |
| http | PUT | 64KiB (A, n 368) | 1,670 → 1,414 | 497 → 79.8 | 867,550 → 801,816 | 4.9 → 5.2 |
| http | PUT | 8MiB (B, n 368) | 31,153 → 32,947 | 726 → 112 | 73,836,174 → 76,577,902 | 5950.6 → 6189.6 |
| http | PUT-STREAM | 1B (A, n 368) | 1,346 → 1,205 | 497 → 76.5 | 346,987 → 332,867 | 1.5 → 2.9 |
| http | PUT-STREAM | 64KiB (A, n 368) | 1,721 → 1,429 | 499 → 77.5 | 917,388 → 807,621 | 5.7 → 2.0 |
| http | PUT-STREAM | 8MiB (B, n 368) | 30,351 → 31,465 | 736 → 110 | 72,308,845 → 73,133,625 | 5802.5 → 6039.8 |
| http | GET | 1B (A, n 368) | 280 → 242 | 73.4 → 9.3 | 40,290 → 41,093 | 0.0 → 0.1 |
| http | GET | 64KiB (A, n 368) | 383 → 330 | 73.7 → 9.4 | 305,167 → 295,528 | 0.1 → 0.1 |
| http | GET | 8MiB (B, n 368) | 9,114 → 9,603 | 101 → 27.8 | 40,017,847 → 36,916,798 | 5562.3 → 4455.3 |
| http | GET-RANGE | 1B (A, n 368) | 283 → 244 | 73.7 → 9.4 | 36,000 → 40,111 | 0.0 → 0.1 |
| http | GET-RANGE | 64KiB (A, n 368) | 378 → 333 | 74.9 → 9.5 | 293,114 → 303,705 | 0.1 → 0.0 |
| http | GET-RANGE | 8MiB (B, n 368) | 3,423 → 3,433 | 77.6 → 17.7 | 12,715,197 → 13,497,189 | 1987.1 → 2117.4 |
| http | HEAD | 1B (A, n 368) | 118 → 80.7 | 45.0 → 3.0 | 10,553 → 9,531 | 0.0 → 0.0 |
| http | HEAD | 64KiB (A, n 368) | 118 → 79.8 | 45.1 → 3.0 | 10,583 → 7,015 | 0.0 → 0.0 |
| http | HEAD | 8MiB (B, n 368) | 96.7 → 79.4 | 47.7 → 5.7 | 8,915 → 6,295 | 0.0 → 0.0 |
| http | DELETE | 1B (A, n 368) | 385 → 346 | 131 → 25.3 | 51,464 → 31,012 | 0.1 → 0.1 |
| http | DELETE | 64KiB (A, n 368) | 392 → 357 | 135 → 26.4 | 53,127 → 76,863 | 0.2 → 0.6 |
| http | DELETE | 8MiB (B, n 368) | 354 → 328 | 143 → 25.9 | 37,457 → 33,080 | 0.2 → 0.0 |
| http | MPU-CREATE | 1B (A, n 368) | 365 → 326 | 118 → 25.6 | 53,738 → 49,707 | 0.0 → 0.1 |
| http | MPU-CREATE | 64KiB (A, n 368) | 378 → 334 | 118 → 25.9 | 61,412 → 35,690 | 0.1 → 0.1 |
| http | MPU-CREATE | 8MiB (B, n 368) | 331 → 304 | 119 → 26.8 | 38,373 → 34,361 | 0.2 → 0.4 |
| http | MPU-PART | 1B (A, n 368) | 1,304 → 1,127 | 468 → 77.4 | 323,262 → 252,251 | 3.5 → 2.1 |
| http | MPU-PART | 64KiB (A, n 368) | 1,671 → 1,386 | 470 → 80.5 | 1,020,868 → 804,067 | 14.0 → 3.8 |
| http | MPU-PART | 8MiB (B, n 368) | 33,408 → 32,273 | 699 → 115 | 76,204,867 → 74,570,048 | 4885.7 → 5891.7 |
| http | MPU-LIST-PARTS | 1B (A, n 368) | 152 → 103 | 63.0 → 7.5 | 12,007 → 8,578 | 0.0 → 0.0 |
| http | MPU-LIST-PARTS | 64KiB (A, n 368) | 152 → 103 | 62.1 → 7.5 | 17,623 → 8,621 | 0.0 → 0.0 |
| http | MPU-LIST-PARTS | 8MiB (B, n 368) | 126 → 102 | 63.7 → 10.7 | 9,448 → 8,264 | 0.0 → 0.0 |
| http | MPU-COMPLETE | 1B (A, n 184) | 1,035 → 906 | 400 → 70.3 | 111,879 → 91,858 | 0.2 → 0.1 |
| http | MPU-COMPLETE | 64KiB (A, n 184) | 1,021 → 897 | 402 → 71.6 | 193,973 → 89,651 | 0.2 → 0.2 |
| http | MPU-COMPLETE | 8MiB (B, n 184) | 955 → 870 | 408 → 76.7 | 94,202 → 120,034 | 0.2 → 0.6 |
| http | MPU-ABORT | 1B (A, n 184) | 422 → 388 | 146 → 31.2 | 72,513 → 34,763 | 0.1 → 0.1 |
| http | MPU-ABORT | 64KiB (A, n 184) | 423 → 383 | 151 → 32.8 | 53,658 → 48,393 | 0.0 → 0.1 |
| http | MPU-ABORT | 8MiB (B, n 184) | 381 → 346 | 154 → 32.0 | 31,763 → 40,199 | 0.0 → 0.0 |
| native | HEAD | 1B (A, n 368) | 76.8 → 76.1 | 13.0 → 5.0 | 20,973 → 12,816 | 0.0 → 0.0 |
| native | HEAD | 64KiB (A, n 368) | 76.0 → 76.2 | 14.7 → 5.0 | 13,335 → 19,833 | 0.0 → 0.0 |
| native | HEAD | 8MiB (B, n 368) | 52.0 → 76.8 | 15.1 → 7.0 | 12,332 → 26,963 | 0.2 → 0.0 |
| native | DELETE | 1B (A, n 368) | 354 → 351 | 103 → 26.3 | 44,425 → 37,453 | 1.7 → 1.3 |
| native | DELETE | 64KiB (A, n 368) | 357 → 361 | 114 → 26.2 | 48,833 → 46,936 | 0.3 → 0.3 |
| native | DELETE | 8MiB (B, n 368) | 321 → 327 | 114 → 26.2 | 41,750 → 33,886 | 0.1 → 0.1 |
| native | MPU-CREATE | 1B (A, n 368) | 314 → 308 | 88.3 → 27.8 | 72,853 → 30,931 | 1.0 → 0.3 |
| native | MPU-CREATE | 64KiB (A, n 368) | 318 → 311 | 88.0 → 27.5 | 49,473 → 33,297 | 0.1 → 0.1 |
| native | MPU-CREATE | 8MiB (B, n 368) | 283 → 288 | 88.1 → 28.1 | 37,503 → 30,530 | 0.4 → 0.4 |
| native | MPU-PART | 1B (A, n 368) | 1,186 → 1,055 | 422 → 71.7 | 252,519 → 237,905 | 3.4 → 2.9 |
| native | MPU-PART | 64KiB (A, n 368) | 1,736 → 1,389 | 427 → 76.0 | 936,377 → 812,710 | 8.5 → 3.9 |
| native | MPU-PART | 8MiB (B, n 368) | 55,241 → 50,003 | 656 → 111 | 78,958,993 → 75,044,612 | 6064.3 → 6184.1 |
| native | MPU-LIST-PARTS | 1B (A, n 368) | 83.3 → 82.6 | 21.1 → 7.5 | 17,357 → 11,950 | 0.0 → 0.0 |
| native | MPU-LIST-PARTS | 64KiB (A, n 368) | 82.2 → 82.0 | 21.7 → 7.5 | 14,265 → 11,601 | 0.0 → 0.0 |
| native | MPU-LIST-PARTS | 8MiB (B, n 368) | 59.4 → 82.1 | 21.9 → 10.0 | 23,259 → 25,190 | 0.0 → 0.0 |
| native | MPU-COMPLETE | 1B (A, n 184) | 937 → 840 | 358 → 65.5 | 115,033 → 101,414 | 0.8 → 0.9 |
| native | MPU-COMPLETE | 64KiB (A, n 184) | 913 → 865 | 364 → 65.0 | 109,717 → 83,993 | 0.5 → 0.3 |
| native | MPU-COMPLETE | 8MiB (B, n 184) | 874 → 832 | 367 → 70.4 | 93,700 → 82,373 | 0.4 → 0.2 |
| native | MPU-ABORT | 1B (A, n 184) | 355 → 344 | 111 → 29.2 | 39,126 → 31,248 | 0.4 → 0.3 |
| native | MPU-ABORT | 64KiB (A, n 184) | 358 → 346 | 118 → 29.2 | 64,500 → 28,566 | 0.8 → 0.1 |
| native | MPU-ABORT | 8MiB (B, n 184) | 325 → 334 | 117 → 29.2 | 53,833 → 40,824 | 0.2 → 0.3 |
| http | LIST | page of 1,000 (A, n 5) | 18,170 → 10,894 | 8,883 → 17.8 | 1,406,161 → 644,632 | 79.2 → 61.0 |
| native | LIST | page of 1,000 (A, n 5) | 14,160 → 10,934 | 5,638 → 18.8 | 1,222,883 → 712,184 | 31.8 → 21.6 |

Every HTTP row's reallocations fall 77–99% (a HEAD 45 → 3, a GET 73 → 9, a PUT 493 → 76, a
listing page 8,883 → 18); its allocations fall at 1 B and 64 KiB (a HEAD 118 → 81, a GET 280 →
242, a PUT 1,307 → 1,189, a page of 1,000 keys 18,170 → 10,894). At 8 MiB a PUT's and a GET's
allocations rose 6% and 5% in run B (31,153 → 32,947; 9,114 → 9,603), within the run-to-run
spread of the per-datagram sites inside the cell that the ledger shows moving with timing (run A
had them at 41,843 → 35,116 and 10,033 → 11,117); no stacks were taken to split them, and both
stay below the ledger's (45,825 and 18,419). 0 reallocations per HTTP request is not reached
through the cell: 3 to 115 a request remain, outside the door (the node's and hyper-quic's
tables, the vendor sites step 5 left).

**Latency** (ms, from intended start, open loop past capacity): http PUT 1 B p50/p99 11,006 /
22,549 → 33,286 / 54,761 (A); GET 1 B 2.6 / 13.9 → 2.4 / 34.6 (A); PUT 8 MiB 44,023 / 68,720 →
32,212 / 106,811 (B); GET 8 MiB 56.6 / 255.9 → 54.5 / 167.8 (B). The box ran other sessions' builds
and tests through both runs (load average 37–41 on 32 CPUs, kswapd active), and writes queue in the
generator, so no latency change is claimed either way; the servers' CPU a write fell (http PUT 1 B:
user 9.0 → 8.8 ms, system 7.3 → 5.1 ms an operation, A).

**Peak live heap** per request as the bench reads it is the servers' whole (an HTTP HEAD 72 KB →
4.8 KB, an 8 MiB PUT 26.3 MB either side, B): the door's own bound (one 8 KB head, one body piece,
one TLS record) is not what it measures, and no test isolates it.

**The bench's exit.** Run A before ended exit 1, as step 5's runs did; A after exit 0. Both B runs
exit 1 on the open loop's overload: one 8 MiB put asked SlowDown three times (http PUT after,
http PUT-STREAM and native MPU-PART before) and the reads of its object failed after (404 or
NoSuchKey; 4 failed of 7,362 after, 11 before); the six controls were caught in every run.

