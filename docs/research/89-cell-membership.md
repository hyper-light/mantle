# 89. A cell whose members come and go: grants, revocation, absence and spare space

Status: research note, 2026-11-14. For docs/design/membership.md (branch `fleet-cell`): how a node
gets an identity from the cell's authority and loses it, how the cell tells a member away for a
while from one gone, and how much of a machine's disk a joining node takes. Sources [1] to [7] were
read on the web on 2026-11-14 and are quoted where read; the dissertation and the 2015 bug are
research/06 §A1.7's, and Ford et al. research/15's.

## The questions

1. A node joining a running cell needs an identity every member admits. A node removed must lose
   it, and the cell's certificates do not expire (node.md §1.5). What state does revocation take,
   and is it bounded?
2. A member silent now may be asleep, stopped, partitioned or dead. What tells the cell, from its
   own records, that it is gone for good, with no timeout chosen?
3. A machine's disk is shared with the machine's other work. What may a node take of it?

## Sources

[1] RFC 4303, IP Encapsulating Security Payload, §3.4.3 "Sequence Number Verification",
https://www.rfc-editor.org/rfc/rfc4303.txt: "The "right" edge of the window represents the
highest, validated Sequence Number value received on this SA." "Packets that contain sequence
numbers lower than the "left" edge of the window are rejected." "Packets falling within the window
are checked against a list of received packets within the window." "A minimum window size of 32
packets MUST be supported when 32-bit sequence numbers are employed;" "a window size of 64 is
preferred and SHOULD be employed as the default."

[2] RFC 5280, Internet X.509 PKI Certificate and CRL Profile,
https://www.rfc-editor.org/rfc/rfc5280.txt. §3.3 "Revocation": "An entry MUST NOT be removed from
the CRL until it appears on one regularly scheduled CRL issued beyond the revoked certificate's
validity period." §4.1.2.4: "Implementations of this specification MUST be prepared to receive the
following standard attribute types in issuer and subject (Section 4.1.2.6) names", among them
"serial number". Appendix A.1: "id-at-serialNumber AttributeType ::= { id-at 5 }",
"X520SerialNumber ::= PrintableString (SIZE (1..ub-serial-number))", "ub-serial-number INTEGER ::=
64".

[3] S. S. Wilks, "Determination of Sample Sizes for Setting Tolerance Limits", Annals of
Mathematical Statistics 12(1):91–96, 1941: the share of a continuous population below the largest
of `n` independent observations has the distribution of the largest of `n` uniform variables
whatever the population, so it is at least `p` with probability `1 − pⁿ`; the law hyper-rt's
calibration stops its probes by (research/64, "Wilks' 59 samples, a 95 %/95 % bound"). DERIVED:
its mean is `n / (n + 1)`, so a further observation exceeds the largest of the `n` with
probability `1 / (n + 1)`; by symmetry alone, the same holds for any `n + 1` exchangeable
observations without ties, each equally likely to be the largest.

[4] D. Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford, 2014,
§4.1–§4.3, and "bug in single-server membership changes", raft-dev, July 2015, as research/06
§A1.7 quotes them: single-server changes, catching new servers up as non-voting members, removing
the leader, joint consensus, and the fix "A leader may not append a new configuration entry until
it has committed an entry from its current term."

[5] The Open Group Base Specifications Issue 8, `<sys/statvfs.h>`,
https://pubs.opengroup.org/onlinepubs/9799919799/basedefs/sys_statvfs.h.html: `f_frsize`
"Fundamental file system block size."; `f_bfree` "Total number of free blocks."; `f_bavail`
"Number of free blocks available to non-privileged process."

[6] mke2fs(8), https://man7.org/linux/man-pages/man8/mke2fs.8.html, `-m reserved-blocks-percentage`:
"Specify the percentage of the file system blocks reserved for the super-user." "This avoids
fragmentation, and allows root-owned daemons, such as syslogd(8), to continue to function
correctly" "after non-privileged processes are prevented from writing to the file system." "The
default percentage is 5%."

[7] Microsoft, GetDiskFreeSpaceExW,
https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdiskfreespaceexw,
`lpFreeBytesAvailableToCaller`: "A pointer to a variable that receives the total number of free
bytes on a disk that are available to the user who is associated with the calling thread." "If
per-user quotas are being used, this value may be less than the total number of free bytes on a
disk."

## 1. Revocation is bounded only by expiry, or by spending grants once

A revoked certificate's entry stays on a revocation list until the certificate has expired [2].
mantle's node certificates take RFC 5280's "no well-defined expiration" until rotation is built
(node.md §1.5), so a list of removed nodes' certificates could never drop an entry: a cell whose
laptops come and go for years would carry every one, against CLAUDE.md §2.

IPsec meets the same problem for packets, a replay of any past sequence number, with constant
state: the receiver keeps the highest number it has validated and which numbers within a window
below it it has seen, rejects anything left of the window, and checks the rest against the window's
list [1]. The same structure bounds revocation if every node identity is issued under a number of
its authority's, in order, and a member's join spends its number: a removed member's number is
spent, and a node can join only under a number the window has not seen. The state is the highest
number spent and a bit for each number in the window.

IPsec's window sizes, 32 at least and 64 preferred [1], are its own tuning for reordered packets,
and are not taken here. A grant's window must hold every grant that can still lawfully join out of
order. The cell holds at most `root.max_nodes` members (node.md §6.6), so a granter that has issued
a grant for each slot the cell could fill has at most that many outstanding: the window is
`root.max_nodes` wide, the cell's own statement (membership.md §2).

The grant's number travels in the certificate's subject as X.520's `serialNumber`, an attribute
every RFC 5280 implementation must take in a subject [2], a printable string of at most 64
characters [2]: a `u64` in decimal is at most 20.

## 2. Gone, judged from the cell's own returns

Silence alone cannot tell a sleeping laptop from a dead one; what the cell can know is how long its
members have been silent before and come back. Ford et al. found most unavailability in Google's
fleet transient ("The vast majority of such unavailability events are transient and do not result
in permanent data loss", research/15 §8.4), and GFS waited 15 minutes, a delay set by that
fleet's distribution, before treating a node's chunks as lost (research/15 §1.4).

The rule membership.md §5 takes is that distribution's own support: a member absent past the
longest absence any member has returned from is judged gone. If the cell has seen `n` returns, an
absence that will end in a return outlasts all `n` with probability `1 / (n + 1)` at most [3]
(exchangeable absences, no ties), so the rule's false judgments are bounded by a figure the cell
states from its own history, and fall as it grows. It chooses no quantile and no timeout; a cell
with no return seen judges nothing, and an operator or a supervisor states what it knows.

Not taken: a quantile below the maximum (Ford's 15 minutes is a fleet's choice of where transient
ends, research/15 §1.4), which would need a chosen coverage; a Bayesian posterior of permanence,
which would need a prior on how often members vanish for good and a ratio of costs, neither of
which the cell measures.

## 3. The space a process may write

Each operating system states what a process may still write on a file system: POSIX's
`f_bavail × f_frsize` [5], which on ext4 leaves out the blocks mke2fs reserves so the system's own
daemons keep working once others cannot write [6], and Windows' bytes available to the caller,
less any quota [7]. `mantle_disk::probe` reads both already (`FileSystem::available_bytes`). A
volume sized to that statement, less what the node itself writes on the same file system and less
what the machine's operator states its other work keeps, takes no share mantle chose
(membership.md §3.1).

## Open

- How a machine's other work grows over time, measured, so a volume could follow it rather than
  be sized once.
- An absence's start and end are stamped by the root range's leaders' clocks; the clocks' offset
  (node.md §2.4) enters each recorded length.
