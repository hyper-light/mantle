# 91. Wire profiles and the upgrade fence: a cell of old and new binaries

Status: research note, 2026-11-15. For node.md §1.7 ("Versions") and `crates/protocol/src/wire.rs`:
how a node and a client from different releases agree on the formats they speak, what a node of a
newer release may ask of an older one, and when a cell may write a format its older members cannot
read. Sources were read on 2026-11-15; quotations are verbatim. kiln's cell is the case in point:
its machines are upgraded at different times, so for a while a cell holds binaries of two
releases, and every operation must keep working through it.

## Sources

[1] RFC 9000, QUIC: A UDP-Based Multiplexed and Secure Transport, §6 "Version Negotiation",
https://www.rfc-editor.org/rfc/rfc9000.html#section-6.
[2] RFC 9368, Compatible Version Negotiation for QUIC, https://www.rfc-editor.org/rfc/rfc9368.html.
[3] RFC 7301, TLS Application-Layer Protocol Negotiation Extension, §3.1, §3.2,
https://www.rfc-editor.org/rfc/rfc7301.html.
[4] RFC 9001, Using TLS to Secure QUIC, §8.1 "Protocol Negotiation",
https://www.rfc-editor.org/rfc/rfc9001.html#section-8.1.
[5] RFC 8446, The Transport Layer Security (TLS) Protocol Version 1.3, §2 and the extension table
of §4.2, https://www.rfc-editor.org/rfc/rfc8446.html.
[6] etcd v3.5 documentation, "Upgrade etcd from v3.4 to v3.5",
https://etcd.io/docs/v3.5/upgrades/upgrade_3_5/.
[7] D. Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford, 2014, §3.10
"Leadership transfer extension", as research/06 §A1.3 quotes it.
[8] R. Taft et al., "CockroachDB: The Resilient Geo-Distributed SQL Database", SIGMOD 2020, §7.4,
as research/06 §A4.3 quotes it.
[9] focal (github.com/hyper-light/focal) at f1e761f9: `crates/focal-wire/src/message.rs`
(`Hello`, `WireLimits::negotiate_ordered`), `crates/focal-node/src/upgrade.rs` (the upgrade
fence) and `crates/focal-node/tests/cli_upgrade.rs`; and its decoder fences, research/07 §2.5.

## 1. Two kinds of format

What a cell's binaries exchange is of two kinds, and a rolling upgrade treats them differently.

- **Point to point.** A message one side writes and the other reads once: the native protocol's
  requests and replies between a client and a node (`mantle_protocol::object`), and the reads a
  node asks of a range's leader (mantle-node's `read`). Only the connection's two sides need to
  know its format, so each connection can agree on its own.
- **Kept.** What a range's members all apply and keep: the entries a Raft log carries, the
  records a range's engine holds. An entry one member writes, every member applies, now or when it
  catches up, and every later binary reads again at restart. CockroachDB learned that what a member
  does with an entry is part of its format too: "replicating requests and evaluating them on each
  replica let mixed versions diverge", so they "now propose the effect of an evaluated request,
  rather than the request itself" [8]. A new kept format, or a new way to apply an old one, may be
  written only once every member can read it.

## 2. Agreeing on a profile, connection by connection

QUIC's own versions are negotiated so that "a client and server ... select a mutually supported
version" [2]: the client lists what it can speak, "the server MUST select one of these versions
that it (1) supports" [2, §2.3], and its preference may be its own, "servers MAY choose to use
their own preference instead" [2, §3]. RFC 9000 §6 leaves the procedure itself open: "How to
perform version negotiation is left as future work" [1]. For the application protocol a QUIC
connection carries, the mechanism is fixed: "Unless another mechanism is used for agreeing on an
application protocol, endpoints MUST use ALPN for this purpose" [4], whose client sends its
protocols "in descending order of preference", and whose server "SHOULD select the most highly
preferred protocol that it supports and that is also advertised by the client", or answers
"no_application_protocol" where none overlaps [3].

So mantle names each wire profile by an ALPN protocol, and each side offers every profile it speaks,
newest first. rustls, which mantle's TLS is (vendor/hyper-tls), takes the first of the server's own
list that the client offers (`server/hs.rs`): a pair speaks the newest profile both know, a new pair
the new one, and a pair with an old member the old one. The offer travels in the ClientHello and the
choice in the EncryptedExtensions ("CH, EE" for ALPN in RFC 8446's table), both under the Finished
message, "A MAC (Message Authentication Code) over the entire handshake" [5], so no one between the
two can talk them down to an older profile: the downgrade RFC 9368 closes for QUIC's versions by
authenticating its version information [2, §4, §9].

Every mantle before profiles offered the transport's own protocol, `hyper-transport/1`, and nothing
else. That name is profile one, so a binary that speaks profiles meets one that does not on the
older's formats, with no flag day; profile two is `mantle/2`. focal agrees on its profile the same
way but after the handshake, in a `Hello` of the versions each side speaks, its server taking the
newest of its profiles the client lists, each profile implying every earlier one
(`negotiate_ordered`, [9]). ALPN does it inside the handshake, which mantle's transport already
authenticates, and which a binary from before profiles already speaks.

## 3. What a newer side may ask of an older one

- **A client and a node.** A request is written in its connection's profile. One that profile
  cannot carry, an operation a later profile brought or a field of one (a put's metadata, a version
  named), is refused before anything is sent, never written without the part the older profile has
  no field for, which would commit another object than the one asked. The client asks the next node
  instead, which in a cell mid-upgrade may be upgraded already, and says the operation needs the
  later profile if none is. A reply states only what its profile carries: profile one's has no
  field for a version's metadata, and no request of profile one asks for it.
- **A node and a range's leader.** A read a later profile brought is asked only of a leader whose
  connection speaks that profile. A leader on an older one cannot read it, so the operation is
  answered S3's `503 ServiceUnavailable`, "Service is unable to handle request." (research/05
  §11.2), which stock clients ask again, until that leader is upgraded or another leads. A rolling
  upgrade moves leadership off a node before it stops (node.md §1.7's orderly stop), the transfer
  the dissertation describes: the leader "stops accepting new client requests", "fully replicates
  its log to the target" and sends `TimeoutNow` [7].

## 4. The upgrade fence

etcd states the rule for a Raft cell of mixed versions: it "supports mixed versions of etcd members,
and operates with the protocol of the lowest common version", and "The cluster is only considered
upgraded once all of its members are upgraded"; "after running all v3.5 processes, new features in
v3.5 are available to the cluster" [6]. Until then the cell may go back: "it is possible from this
mixed cluster state to return to using a v3.4 etcd binary on all members"; after, "downgrade from
this completed state is not possible" [6]. focal holds the same rule as a fence: "every node reports
its binary's capability level; the founder raises the fence only once every node reports the level,
never lowers it, and a binary announcing less than the committed fence refuses to serve" [9], and
its decoder fences gate each new record format the same way, so "an older binary refuses the entire
physical WAL" once one is written (research/07 §2.5).

mantle's fence is a fact the cell's root range holds: the profile every member has been seen to
speak. A node learns what each member speaks from its connections' handshakes, which are the
members' own statements, authenticated. The root range's leader raises the fence to a profile once
every member the volume table holds speaks it on a connection to the leader, and never lowers it;
the entry that raises it is itself the first entry of the new profile, written only once every
member can read it. A kept format, or a new way to apply one, a profile brought is written only
once the fence has reached that profile. Once it has, a node refuses a member's connection that
speaks an older profile, since that member could not read what the cell now writes, and a binary
whose newest profile is below the fence refuses to start. An operator who wants the cell able to go
back holds the fence by having one node announce the older profile (`MANTLE_WIRE`), as focal's
`FOCAL_CAPABILITY_LEVEL` "lowers the announced level for a staged rollout or a rehearsal" [9].
Clients keep no kept format, so a node serves a client of an older profile on that profile's
formats whatever the fence: a release keeps its predecessor's profile.

## 5. What profile two is

The formats research/89 brought, point to point alone: the native protocol's metadata, a version
named, its copies, batch deletes and listing of versions, their statuses and listings; and the
reads of a named version, of a page of versions and of a write's mark. No kept format changed with
them: a version's record already held its headers, and a part's its time (research/89 §1.1, §4.2).
So profile two needs no fence; the fence is built with it for the first profile that keeps
something new, an object's tags (research/89 §6).
</content>
</invoke>
