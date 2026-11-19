# hyper-transport benchmarks (mantle's snapshot)

The benches hyper-raft keeps for this crate (`benches/lookup.rs`; hyper-raft `docs/benchmarks.md`,
"Exchange lookup by stream") and the ones mantle added with the changes below
(`benches/progress.rs`, `benches/budget.rs`), recorded here until the change and its bench go upstream
(vendor/UPSTREAM.md, "Local changes"). Each runs from this directory against hyper-raft's lock file
(vendor/UPSTREAM.md): `cp <hyper-raft>/Cargo.lock vendor/hyper-transport/` and then
`cargo bench --manifest-path vendor/hyper-transport/Cargo.toml --bench <name> --offline`.

## A call beside exchanges waiting for credit (2026-11-19)

`benches/progress.rs`: one connection whose credit a bulk body spent, its peer never reading it, so
no credit comes back; then `n` requests asked beside it, each holding a stream (the stream limit
is 1,024), their heads waiting for credit; and, in the second table, the four lanes the limits
allow each filled to its window of 128 frames, which wait for credit too (a few more frames are
counted for small `n`: the class reserve still took them). The timed call is the bulk body's write,
refused for credit (`write_body` takes nothing), 2,000 calls after 200 warm-up calls: it changes
nothing, so each is the same call on the same state, and its cost is the progress pass that follows
every call on an exchange (`Endpoint::with_exchange` → `Core::progress`). Simulated network, real
CPU time. The shape is a range leader's busiest shard under `mantle bench doors --load open:12000`
(mantle, 2026-11-19), where `hyper-rt-shard-0` was 94–96% busy and the pass's frames
(`tally::Table::settle` 27%, `Core::flush` 6.1%, `Core::allowed` 5.0%, `Core::progress` 3.8%,
`Core::start` 3.6%, `Core::flush_trailer` 1.1%) were about half of it.

Apple M5 Max (18 cores), macOS 26.4.1, rustc 1.98.1, release profile, one run each; load average
11.5 before the run (the machine is shared; another session was building).

Before, perf-10k at `518e78b8` with the bench: every pass visited every exchange whose head waited,
and each visit settled the tally and scanned every queued lane frame to find what the classes above
had left (`Core::allowed` → `demand_above`).

| Waiting beside it | Lane frames queued | Allocations | Bytes | Time per call |
|---|---|---|---|---|
| 1 | 0 | 0.00 | 0 | 0.06 µs |
| 32 | 0 | 0.00 | 0 | 0.08 µs |
| 64 | 0 | 0.00 | 0 | 0.65 µs |
| 128 | 0 | 0.00 | 0 | 1.79 µs |
| 256 | 0 | 0.00 | 0 | 4.00 µs |
| 512 | 0 | 0.00 | 0 | 8.71 µs |
| 1023 | 0 | 0.00 | 0 | 17.83 µs |
| 1 | 524 | 0.00 | 0 | 3.22 µs |
| 32 | 512 | 0.00 | 0 | 3.44 µs |
| 64 | 512 | 0.00 | 0 | 15.41 µs |
| 128 | 512 | 0.00 | 0 | 39.78 µs |
| 256 | 512 | 0.00 | 0 | 87.26 µs |
| 512 | 512 | 0.00 | 0 | 189.33 µs |
| 1023 | 512 | 0.00 | 0 | 427.52 µs |

Up to 32 requests the credit the class reserve leaves their class takes their heads, so none waits;
past it each call costs
about 17 ns for each head waiting, and with the lanes full about 420 ns, the lane scan's 512 frames
read for each visit.

After: the lanes' unsent bytes are running sums by rank (`lane::Unsent`), and a pass passes over a
class's exchanges holding a stream while the class may take no credit (`Core::next_visit`), each
visit list's next exchange found by a binary search of that list alone.

| Waiting beside it | Lane frames queued | Allocations | Bytes | Time per call |
|---|---|---|---|---|
| 1 | 0 | 0.00 | 0 | 0.10 µs |
| 32 | 0 | 0.00 | 0 | 0.07 µs |
| 64 | 0 | 0.00 | 0 | 0.07 µs |
| 128 | 0 | 0.00 | 0 | 0.07 µs |
| 256 | 0 | 0.00 | 0 | 0.11 µs |
| 512 | 0 | 0.00 | 0 | 0.12 µs |
| 1023 | 0 | 0.00 | 0 | 0.11 µs |
| 1 | 524 | 0.00 | 0 | 0.17 µs |
| 32 | 512 | 0.00 | 0 | 0.16 µs |
| 64 | 512 | 0.00 | 0 | 0.12 µs |
| 128 | 512 | 0.00 | 0 | 0.12 µs |
| 256 | 512 | 0.00 | 0 | 0.11 µs |
| 512 | 512 | 0.00 | 0 | 0.12 µs |
| 1023 | 512 | 0.00 | 0 | 0.11 µs |

A second run of the committed tree measured 0.05 µs at every `n` without lanes and 0.11 µs with
them. A call costs the same beside 1,023 heads waiting and 512 frames as beside one: 162 times less than
before at 1,023 without lanes, 3,900 times less with them. Neither allocates.

## A reservation beside spare buffers (2026-11-19)

`benches/budget.rs`: a `Fixed` budget keeping `n` spare buffers, given back at capacities from 64
bytes up, then a reservation of 1,000 bytes and its release, timed 20,000 times after 2,000 warm-up
rounds. A node keeps a spare for each peer and client it may serve (mantle's
`Fixed::new(.., peers + clients)`), and on the range leader above `Fixed::reserve` was 6.7% of
`hyper-rt-shard-0`, 94% of it under `Core::read_exchange` and mantle's `Node::read_exchange`.

Same machine, one run each: before at load average 12.6, after at 5.2. Before, perf-10k at
`518e78b8`: a reservation searched every spare for the smallest that held it, twice when none did.
After: the spares are kept in TLSF's classes (Masmano et al., ECRTS 2004), sixty-fourths of a power
of two, a power of two's classes one word of an occupancy map; a reservation takes the last given
back of its size's class if that holds it, else one of the first later class that holds any, else
the largest class's, grown.

| Spare buffers | Before: allocations | Before: time | After: allocations | After: time |
|---|---|---|---|---|
| 1 | 0.00 | 3.3 ns | 0.00 | 7.7 ns |
| 16 | 0.00 | 6.0 ns | 0.00 | 7.7 ns |
| 64 | 0.00 | 17.9 ns | 0.00 | 7.9 ns |
| 256 | 0.00 | 118.0 ns | 0.00 | 7.7 ns |
| 1024 | 0.00 | 694.7 ns | 0.00 | 8.4 ns |
| 4096 | 0.00 | 2953.4 ns | 0.00 | 8.4 ns |
| 16384 | 0.00 | 12061.4 ns | 0.00 | 8.4 ns |

At 4,096 spares a reservation cost 2.95 µs, about what the leader's share implies (some 35,000
reservations a second in 6.7% of a busy core is 1.8 µs each); now 8.4 ns at any count, 4 ns more
than before beside one. The choice differs from the search's only where a size's class, a
sixty-fourth of its power of two, holds buffers of several capacities: the last given back is
tried, not the smallest of them.

## Exchange lookup by stream, again (2026-11-19)

`benches/lookup.rs`, hyper-raft's, unchanged: one exchange of 600 bytes each way beside `n` open
and idle, the check that the change costs the common path nothing. Same machine, run while the
crate's test suite was running (load average 6.9), so the rows are noisy; allocations are the same
before and after, and the time does not grow with `n` in either.

| Open beside it | Before: allocations | Before: time per exchange | After: allocations | After: time per exchange |
|---|---|---|---|---|
| 1 | 10.82 | 21.5 µs | 10.82 | 10.0 µs |
| 16 | 10.81 | 9.5 µs | 10.81 | 5.6 µs |
| 256 | 10.57 | 9.9 µs | 10.57 | 8.7 µs |
| 1023 | 10.00 | 29.6 µs | 10.00 | 9.0 µs |

## transmit: an endpoint holding thousands of connections

`cargo bench --manifest-path vendor/hyper-transport/Cargo.toml --bench transmit --offline`
(`benches/transmit.rs`; the vendored crate has no lock file of its own, so the build resolves
offline from the local registry). One server endpoint holds a connection from each of 2,048 client
endpoints over an in-memory wire that delivers each datagram the instant it is sent. Each turn 512
of the clients, a different run of them each turn, send a request with a 16-byte head and no body,
and the server answers each with a 16-byte head. Timed: the server's transmit side alone, as a
driver's turn runs it (`poll_transmit` until it has nothing, `poll_timeout`, `handle_timeout` when
a timer is due), over 400 turns after 40 untimed, each turn timed alone. The clock moves 200 µs a
turn.

Why: a macOS `sample` of mantle's three-node cell serving native GETs through 2,048 client
connections (perf-10k `518e78b8`, 2026-11-19, `mantle bench doors --sizes 1 --objects 0 --kinds
PUT,GET --load open:12000 --clients 2048 --ops 150000`) put a follower's first shard's transmit
bookkeeping at about a third of its busy time: `Connection::poll_transmit` 14.2%,
`SentPackets::range` 3.9%, `Retransmits::is_empty` 1.8%, the connection deadline heap's
`sift_up` 5.4% and `sift_down` 4.1%, `Endpoint::place` 5.6%.

Machine: Apple M5 Max, 18 cores, 128 GiB, macOS 26.4.1, rustc 1.98.1, release profile. The host
was shared with other builds and benchmarks (load average 6 to 11), so each build was run six
times interleaved with the other, and the table gives the quickest run's median turn and the
median of the six runs' medians.

| Build | Median turn, quickest run | Median of the runs' medians | Tenth of turns under, quickest run | Datagrams a turn | Median turn a datagram, quickest run |
|---|---|---|---|---|---|
| perf-10k `518e78b8` | 434.2 µs | 469 µs | 360.8 µs | 816.2 | 532 ns |
| deferred deadline heap, `holds_flights` from the first packet held, empty sets not walked, keyless handshake spaces passed first | 389.3 µs | 419 µs | 322.8 µs | 816.2 | 477 ns |

The runs' medians, sorted: before 434.2, 453.3, 456.3, 482.0, 563.3, 726.9 µs; after 389.3,
400.5, 415.8, 422.5, 426.2, 514.7 µs. About a tenth less a turn, at the same datagrams.

Where the transmit side's time went, from a macOS `sample` of a 12,000-turn run of each build,
self time within the server's timed calls:

| Frame | Before | After |
|---|---|---|
| `deadlines::sift_down` + `sift_up` | 13.1% | 2.2% |
| `Connection::fill_step` | 16.0% | 7.8% |
| `PacketSpace::holds_flights` (`SentPackets::range`) | 1.2% | under 0.3% |
| `Endpoint::place` | 1.9% | 2.2% |
| `Retransmits::is_empty` | 1.7% | 2.7% |

What is left is mostly each datagram's own: building its packet and sealing it (AES-GCM).
