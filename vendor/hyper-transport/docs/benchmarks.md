# hyper-transport benchmarks

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
