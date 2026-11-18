# TLS session resumption without early data on the S3 listener

Sources for the S3 listener's resumption (node.md §4.1): what the design decided, what the
vendored TLS stack offers, and what fixes the store's lifetime, its tickets and its bound. RFC text
is quoted from the RFCs; the vendored stack from its source at the tree this note was written on.

| Tag | Source | Kind |
|---|---|---|
| RFC8446 | RFC 8446, The Transport Layer Security (TLS) Protocol Version 1.3 | primary |
| RFC5246 | RFC 5246, The Transport Layer Security (TLS) Protocol Version 1.2 | primary |
| RFC5077 | RFC 5077, Transport Layer Security (TLS) Session Resumption without Server-Side State | primary |
| RFC8470 | RFC 8470, Using Early Data in HTTP (through research/30 §4.9) | primary |
| RFC9001 | RFC 9001, Using TLS to Secure QUIC (through research/25 §5) | primary |
| HYPER-TLS | vendor/hyper-tls (rustls 0.23.45, conformed; its VENDORED.md) | primary (the code mantle runs) |

## 1. What the design decided

- node.md §3.2 and research/30 §4.9 (D15): 0-RTT is off toward clients; "Clients resume TLS
  sessions without early data, which skips the certificate chain ... with no replay exposure,
  since no application data rides the resumption." RFC 8470 §4: clients "MUST NOT send unsafe
  methods (or methods whose safety is not known) in early data"; RFC 9001 §9.2: "Disabling 0-RTT
  entirely is the most effective defense against replay attack."
- Left open: stateless tickets or a store kept by the server, the lifetime, and how many tickets
  a handshake gives. §2 to §6 decide them.
- What resumption saves: "When the server authenticates via a PSK, it does not send a Certificate
  or a CertificateVerify message" [RFC8446 §2.2].

## 2. The lifetime

- "An upper limit of 24 hours is suggested for session ID lifetimes, since an attacker who
  obtains a master_secret may be able to impersonate the compromised party until the
  corresponding session ID is retired." [RFC5246 Appendix F.1.4]
- `ticket_lifetime`: "Servers MUST NOT use any value greater than 604800 seconds (7 days). ...
  Clients MUST NOT cache tickets for longer than 7 days, regardless of the ticket_lifetime, and
  MAY delete tickets earlier based on local policy. A server MAY treat a ticket as valid for a
  shorter period of time than what is stated in the ticket_lifetime." [RFC8446 §4.6.1]
- "in principle it is possible to continue issuing new tickets which indefinitely extend the
  lifetime of the keying material originally derived from an initial non-PSK handshake ... It is
  RECOMMENDED that implementations place limits on the total lifetime of such keying material"
  [RFC8446 §4.6.1].
- HYPER-TLS states a lifetime of 24 hours on every TLS 1.3 ticket a store keeps
  (`server/tls13.rs`, `emit_ticket`: `let stateful_lifetime = 24 * 60 * 60;`).

**For mantle (DERIVED from the above).** A session is resumed for 24 hours from the full handshake
that authenticated the endpoint: RFC 5246's upper limit, the lifetime the ticket states, and
within RFC 8446's 7 days. A resumed TLS 1.3 handshake's tickets keep the time of the session it
resumed, so no chain of tickets outlives the full handshake by more than the lifetime.

## 3. What hyper-tls offers and what a session is

- A server configuration holds `session_storage` (`StoresServerSessions`: `put`, `get`, `take`,
  `can_cache`) and `ticketer` (`ProducesTickets`); every mutating store method takes `&mut self`
  (HYPER-TLS VENDORED.md §2.3). The default ticketer produces none (`NeverProducesTickets`).
- With no ticketer, a TLS 1.3 ticket is a random 32-octet id the server keeps the session under
  (`emit_ticket`: `rand::random_vec(secure_random, 32)`, then `session_storage.put`), and a ticket
  offered is looked up with `take` (`attempt_tls13_ticket_decryption`): used once.
- TLS 1.2: a full handshake draws a 32-octet session ID (`SessionId::random`) when the store can
  cache, and keeps the session with `put` once the client's Finished is verified; a client's
  session ID is looked up with `get`, so it is resumed as often as the store keeps it; a resumed
  handshake keeps nothing new (`server/tls12.rs`). `SessionID<0..32>` [RFC5246 §7.4.1.2]; the
  server answers "with the same value as was supplied by the client" when it resumes
  [RFC5246 §7.4.1.3].
- A session's encoding (`msgs/persist.rs`, `ServerSessionValue::encode`): the server name (a flag,
  an 8-bit length, the name), version, suite, the secret (an 8-bit length, at most 48 octets: a
  TLS 1.2 master secret or a TLS 1.3 PSK of SHA-384), the extended master secret flag, the client's
  chain (a flag; none is asked for), ALPN (a flag, an 8-bit length, the protocol), application data
  (a 16-bit length; none set), the creation time (8) and the age offset (4): at most 336 octets
  for the listener's sessions.
- A resumption is refused for another suite or server name (`hs::can_resume`, RFC 6066's rule
  quoted there).

## 4. Tickets a handshake gives

- "Clients SHOULD NOT reuse a ticket for multiple connections. ... Servers that issue tickets
  SHOULD offer at least as many tickets as the number of connections that a client might use ...
  Servers SHOULD issue new tickets with every connection. This ensures that clients are always
  able to use a new ticket when creating a new connection." [RFC8446 Appendix C.4]
- "the server can maintain a database of all outstanding valid tickets, deleting each ticket from
  the database as it is used. If an unknown ticket is provided, the server would then fall back to
  a full handshake." [RFC8446 §8.1]

**For mantle (DERIVED).** A resumed connection spends a ticket, so a handshake that gave one would
leave a client exactly the tickets it had; two is the least count with which its tickets grow,
one more for each connection it resumes, so a client that has had `n` connections holds a ticket
for each of its next `n`. A full handshake of each of a stock client's ten parallel connections
leaves it twenty.

## 5. Early data

- A NewSessionTicket's one extension is `early_data`, "indicating that the ticket may be used to
  send 0-RTT data" [RFC8446 §4.6.1]. HYPER-TLS adds it only while `max_early_data_size` is above
  zero (`emit_ticket`); the listener sets zero, so no ticket allows early data, and a client that
  would send it has none to send.
- A server that receives an `early_data` extension may "Ignore the extension and return a regular
  1-RTT response. The server then skips past early data by attempting to deprotect received
  records using the handshake traffic key, discarding records which fail deprotection (up to the
  configured max_early_data_size)." [RFC8446 §4.2.10] HYPER-TLS rejects and skips at most one
  record's 16,384 octets when its own size is zero (`server/tls13.rs`, `max_early_data_size`).

## 6. Stateless tickets or a store

- RFC 5077's ticket carries the session's state, the master secret among it, encrypted under a
  key of the server's, and "This message is sent by the server during the TLS handshake before
  the ChangeCipherSpec message" [RFC5077 §3.3]; the key "should be changed regularly"
  [RFC5077 §5.5]. A TLS 1.2 ticket so goes in the clear under the ticket key, and whoever later
  holds that key reads the master secret of every session it sealed.
- A store kept by the server needs no such key: nothing to rotate, to distribute or to lose, and
  no secret leaves the node. Its cost is memory, which the listener reserves whole and bounds by
  the connections it serves (node.md §4.1, `listener::SESSION_BYTES`), and that a session is
  resumed only at the node that made it: a client reconnects to the endpoint it dialled.

**Decision.** A store on the node: TLS 1.3 tickets as keys into it, used once [RFC8446 §8.1], and
TLS 1.2 session IDs; no ticket key and no RFC 5077 ticket.
