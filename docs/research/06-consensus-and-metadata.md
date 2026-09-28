# 06 — Consensus and the metadata layer: literature and Rust ecosystem ground truth

*Research date: 2026-09-28. Scope: mantle's metadata layer is a range-partitioned, consensus-replicated, strongly consistent ordered KV store (multi-Raft). This document is research input, not a decision record.*

## How to read this document

- **Evidence labels.** **[PR]** = peer-reviewed paper (or a doctoral dissertation). **[PS]** = primary source that is not peer-reviewed: the author's own tech report, errata, mailing-list post, official project README/docs/changelog, crate source code, or crates.io/GitHub API metadata. **[VB]** = vendor or engineering blog (not peer-reviewed), cited only where nothing better exists and flagged as such. **UNVERIFIED** = could not be confirmed in a primary source; do not rely on it without checking.
- **Method.** Every paper cited in Part A was downloaded and converted with `pdftotext`, and section numbers, page numbers, figures and quotes were checked against the extracted text. DOIs and page ranges were checked against Crossref where a DOI exists. Part B facts come from the crates.io API, the GitHub API, raw repository files, docs.rs, and grep over the published crate tarballs (`static.crates.io`), as observed on 2026-09-28. Short verbatim quotes are in quotation marks; everything else is paraphrase.
- **Section references.** "Diss §6.4 (p. 72)" = Ongaro's dissertation, section and printed page. "ATC §5.4.2" = the USENIX ATC 2014 Raft paper. "Ext §8" = the extended Raft tech report.
- **A correction to the brief.** In the dissertation, batching and pipelining are **§10.2.2**. §10.2.1 is "Writing to the leader's disk in parallel". Both are covered in A1.9 and A1.2.

## Summary

1. **Raft rules that are easy to get wrong** (A1). Persist `term`, `vote` and log entries before responding. Persist `applied_index` atomically with a persistent state machine. Never commit an entry from a previous term by counting replicas. Use PreVote, leader stickiness and step-down-on-lost-quorum together. Default to ReadIndex reads; treat leases as an optimization that depends on a clock-drift bound. Apply the 2015 membership-change fix: no configuration entry until an entry from the current term has committed.
2. **Production multi-Raft systems converge on the same set of techniques** (A4). One group per range. Split is a Raft command that bumps a range epoch. Merge needs co-location. Heartbeats are coalesced per node and idle groups quiesce. Leases are separate from leadership but co-located with it, with lease-sequence checks at apply time. CockroachDB recommends **joint consensus** for replica moves.
3. **Deterministic simulation is the best-evidenced testing method for this class of system** (A2, A5). It requires every source of nondeterminism to be injectable, which argues for a sans-IO consensus core and a storage layer whose I/O the simulator owns. Linearizability checking with P-compositional WGL (A6) supplies the oracle.
4. **Storage faults are outside Raft's model** (A3). Do not crash, truncate or wipe on corruption. Detect it, disentangle crash from corruption, and repair through the protocol (PAR/CTRL). On fsync failure, never retry (A10).
5. **Recommendations (Part C):**
   - **(a) Consensus.** Write a mantle-owned, sans-IO Raft core with etcd/raft semantics and no panics. Use raft-rs as a differential-test oracle and as the fallback to fork. The case against depending on either library:
     - raft-rs panics by design, has not been released since 2023-03-07, and depends on protobuf 2, which is affected by RUSTSEC-2024-0437.
     - openraft is pre-1.0, had core safety bugs fixed in July 2026, and spawns tasks and heartbeats per follower in every group.
   - **(b) State-machine engine.** RocksDB 11.8.1 via `rocksdb` 0.25.0, behind a narrow engine trait. Make the Raft log the only WAL, and gate Raft-log GC on the engine's *flushed* applied index. Keep redb on the watch-list; fjall is not ready yet.
   - **(c) Raft log.** One shared, append-only, multi-group WAL per disk, built in-house on tikv/raft-engine's design but not its crate. It uses group commit via flush pipelining, PAR-style persist records, and fencing on fsync failure.
   - **(d) Testing.** A TLA+ spec with spec-guided conformance tests, and an in-house deterministic simulator with BUGGIFY-style fault injection. Check histories with per-key P-compositional WGL plus Elle, add real-disk crash tests, and run Jepsen-style real-cluster tests.

# Part A — Literature

## A1. Raft — paper, extended paper, dissertation, errata

**Citations (all fetched and text-extracted; section/page refs below were checked against the extracted text).**

- [PR] D. Ongaro, J. Ousterhout. "In Search of an Understandable Consensus Algorithm." *Proc. 2014 USENIX Annual Technical Conference (ATC '14)*, Philadelphia, PA, June 19–20 2014, pp. 305–319. ISBN 978-1-931971-10-2. https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro (PDF: https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf). Cited below as **ATC §x**.
- [PS] D. Ongaro, J. Ousterhout. "In Search of an Understandable Consensus Algorithm (Extended Version)." Tech report, published May 20 2014. https://raft.github.io/raft.pdf. The ATC paper's §7 ("Clients and log compaction") is a stub that defers to this version (ATC p. 315: "This section has been omitted due to space limitations, but the material is available in the extended version"). Cited as **Ext §x**.
- [PR — doctoral dissertation] D. Ongaro. *Consensus: Bridging Theory and Practice.* Ph.D. dissertation, Stanford University, August 2014. http://purl.stanford.edu/qr033xr6097 (PDF fetched: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf; LaTeX + errata: https://github.com/ongardie/dissertation). Cited as **Diss §x (p. N)**.
- [PS] Dissertation errata ("Updates and Errata" section of https://github.com/ongardie/dissertation/blob/master/README.md, maintained by Ongaro).
- [PS] D. Ongaro, "bug in single-server membership changes," raft-dev mailing list (Google Groups), thread https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E (message linked from the errata: https://groups.google.com/d/msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J). Google Groups renders the timestamp as "July 9, 2015, 11:58:53 PM" (viewer-local time zone; i.e., 9–10 July 2015).

### A1.1 Safety properties (exact)

ATC Figure 3 / Diss Figure 3.2 (p. 14): "Raft guarantees that each of these properties is true at all times."

| Property | Statement (verbatim) | Where argued |
|---|---|---|
| Election Safety | "at most one leader can be elected in a given term." | ATC §5.2 / Diss §3.4 |
| Leader Append-Only | "a leader never overwrites or deletes entries in its log; it only appends new entries." | ATC §5.3 / Diss §3.5 |
| Log Matching | "if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index." | ATC §5.3 / Diss §3.5 |
| Leader Completeness | "if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms." | ATC §5.4 / Diss §3.6 |
| State Machine Safety | "if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index." | ATC §5.4.3 / Diss §3.6.3 |

Supporting rules that implementers most often get wrong:

- **Commitment rule (ATC §5.4.2, Diss §3.6.2 p. 24):** "Raft never commits log entries from previous terms by counting replicas. Only log entries from the leader's current term are committed by counting replicas; once an entry from the current term has been committed in this way, then all prior entries are committed indirectly because of the Log Matching Property." Figure 2's leader rule encodes it: advance commitIndex to N only if a majority of matchIndex ≥ N **and** `log[N].term == currentTerm`.
- **Election restriction (ATC §5.4.1):** a voter denies its vote if its own log is more up-to-date than the candidate's (compare last term, then length).
- **Timing is for liveness only (ATC §5.6, Diss §3.9 p. 27–28):** "safety must not depend on timing"; availability requires `broadcastTime ≪ electionTimeout ≪ MTBF`; the dissertation estimates broadcast time 0.5–20 ms "depending on storage technology" (because RPC receivers persist before replying) and therefore election timeouts of 10–500 ms.
- **Failure model of the proof (Diss §8.1 p. 112):** asynchronous; "Servers fail by stopping and may later restart from stable storage on disk"; network "may reorder, drop, and duplicate messages". Storage corruption is *outside* the model (see A3, PAR).
- **What is formally covered (Diss ch. 8 p. 111; §8.1 p. 112–113):** the ~450-line TLA+ spec and the ~3,500-word proof of State Machine Safety cover only the basic algorithm (ch. 3), **not** membership changes, log compaction, or client interaction. A later machine-checked Coq proof exists for the basic algorithm (errata; peer-reviewed: J. R. Wilcox et al., "Verdi: A Framework for Implementing and Formally Verifying Distributed Systems," PLDI 2015, pp. 357–368, doi:10.1145/2737924.2737958; D. Woos et al., "Planning for Change in a Formal Verification of the Raft Consensus Protocol," CPP 2016, pp. 154–165, doi:10.1145/2854065.2854081 — both DOIs verified via Crossref).
- **Atomicity granularity (Diss §8.1 p. 113):** the spec lets a follower truncate "just the last entry" per atomic step; "The specification shows that implementations may safely truncate the entries back to front, one or more at a time" — relevant when a log spans multiple files and cannot be truncated atomically.

### A1.2 Persistent state and what must be durable *before* responding

- ATC Figure 2 (p. 308), "State" box: **"Persistent state on all servers: (Updated on stable storage before responding to RPCs)"** — `currentTerm` ("latest term server has seen (initialized to 0 on first boot, increases monotonically)"), `votedFor` ("candidateId that received vote in current term (or null if none)"), `log[]` ("each entry contains command for state machine, and term when entry was received by leader (first index is 1)"). Volatile on all servers: `commitIndex`, `lastApplied`. Volatile on leaders: `nextIndex[]`, `matchIndex[]`.
- Diss §3.8 "Persisted state and server restarts" (p. 27): term and vote are persisted "to prevent the server from voting twice in the same term or replacing log entries from a newer leader with those from a deposed leader"; "Each server also persists new log entries before they are counted towards the entries' commitment; this prevents committed entries from being lost or 'uncommitted' when servers restart." `commitIndex` "can safely be reinitialized to zero on a restart."
- **Persistent state machines:** Diss §3.8: "A persistent state machine, however, has already applied most entries after a restart; to avoid reapplying them, its last applied index must also be persistent." Errata to Figure 3.1: "lastApplied … should be as volatile as the state machine … If the state machine is persistent, lastApplied should be just as persistent."
- **Losing persistent state = losing identity:** Diss §3.8: "If a server loses any of its persistent state, it cannot safely rejoin the cluster with its prior identity. Such a server can usually be added back into the cluster with a new identity by invoking a cluster membership change."
- **Leader's own write can be off the critical path:** Diss §10.2.1 "Writing to the leader's disk in parallel" (p. 141–142): the leader may write its entry concurrently with replication; "the leader uses its own match index to indicate the latest entry to have been durably written to its disk"; "The leader may even commit an entry before it has been written to its own disk, if a majority of followers have written it to their disks; this is still safe." (Followers must still persist before acknowledging.)
- **Snapshot metadata** that must be persisted with a snapshot (Diss §5.1 p. 51; Ext §7): last included index, last included term, and "the latest configuration as of that index"; state machines "must also serialize the information they keep for providing linearizability to clients" (client sessions).
- Diss §11.7.3 "Avoiding persistent storage writes" (p. 168): diskless designs à la VR Revisited are possible, but "we think the risk of availability or data loss usually outweighs the benefits."

### A1.3 Election hardening: PreVote, leader stickiness, CheckQuorum, leadership transfer

- **PreVote — Diss §9.6 "Preventing disruptions when a server rejoins the cluster" (p. 136–137).** Problem: a partitioned server keeps incrementing its term; on reconnection "its larger term number will propagate to the rest of the cluster … This will force the cluster leader to step down." Fix: "In the Pre-Vote algorithm, a candidate only increments its term if it first learns from a majority of the cluster that they would be willing to grant the candidate their votes (if the candidate's log is sufficiently up-to-date, and the voters have not received heartbeats from a valid leader for at least a baseline election timeout)." Inspired by ZooKeeper. "We recommend the Pre-Vote extension in deployments that would benefit from additional robustness"; AvailSim tests showed it "does not appear to significantly harm election performance."
- **PreVote alone is insufficient for removed/disruptive servers — Diss §4.2.3 "Disruptive servers" (p. 40–42), Figure 4.7:** a server being removed can have a log "as up-to-date as a majority of either cluster", so "no solution based on comparing logs alone (such as the Pre-Vote check) will be sufficient".
- **Leader stickiness — Diss §4.2.3 p. 42; also ATC §6 p. 315** (Jensen et al., below, describe etcd's `CheckQuorum` in exactly these terms: a node with a stable leader ignores election requests): "if a server receives a RequestVote request within the minimum election timeout of hearing from a current leader, it does not update its term or grant its vote. It can either drop the request, reply with a vote denial, or delay the request." Consequence: "while a leader is able to get heartbeats to its cluster, it will not be deposed by larger term numbers."
- **Interaction with leadership transfer (Diss §4.2.3 p. 42):** transfer targets must be able to bypass stickiness; "Those RequestVote requests can include a special flag to indicate this behavior ('I have permission to disrupt the leader—it told me to!')."
- **Leader step-down on loss of quorum (the leader side of CheckQuorum) — Diss §6.2 p. 69:** "a leader in Raft steps down if an election timeout elapses without a successful round of heartbeats to a majority of its cluster; this allows clients to retry their requests with another server." LogCabin runs a dedicated timer thread for it (Diss §10.1.1 p. 140).
- **Leadership transfer — Diss §3.10 (p. 28–29):** (1) prior leader stops accepting new client requests; (2) fully replicates its log to the target; (3) sends `TimeoutNow`, which makes the target start an election immediately. Abort "after about an election timeout" if not complete. Safety argument: TimeoutNow "is equivalent to the target server's clock jumping forwards quickly, which is safe." Note the dissertation says "we have not currently implemented or evaluated this leadership transfer approach."
- **Empirical follow-up [PR]:** C. Jensen, H. Howard, R. Mortier. "Examining Raft's behaviour during partial network failures." *Proc. 1st Workshop on High Availability and Observability of Cloud Systems (HAOC '21)*, pp. 11–17, 2021, doi:10.1145/3447851.3458739 (PDF: https://www.repository.cam.ac.uk/bitstreams/4826a463-6b65-427d-b8b2-56fa22fb3841/download). Reproduces Cloudflare's Nov 2020 etcd livelock under a *partial* partition with their `reckon` emulator; finds etcd's CheckQuorum (enabled on followers, "currently disabled for leaders" in the etcd version studied) changed behaviour relative to textbook Raft; reports PreVote (available since etcd v3.4 but off by default at the time) eliminated leader elections during the partial partition in their test: "there are no leader elections throughout the partition nor an election afterwards." Also notes requests routed to the partitioned node are still delayed — PreVote fixes churn, not client routing.
- **Election timing refinements [PR]:** H. Howard, M. Schwarzkopf, A. Madhavapeddy, J. Crowcroft. "Raft Refloated: Do We Have Consensus?" *ACM SIGOPS Operating Systems Review* 49(1):12–21, 2015, doi:10.1145/2723872.2723876. Using a discrete-event simulator of ocaml-raft: a shorter *candidate* timeout (min = µ+2σ of broadcast time) cut the 95th-percentile time-to-leader in a highly contested setting from 1330 ms to 281 ms (§4.3); binary exponential backoff for rejected candidates also helped. Safety checked on "thousands of simulation traces" with no violations, but they observed "permanent livelock" from the interaction of the previous-term commitment rule with their client-request cache, and recommend the leader append a no-op when a client request is blocked by that rule (§4.4).

### A1.4 Linearizable reads without the log: ReadIndex (Diss §6.4, p. 72–73)

Verbatim algorithm (leader side):
1. "If the leader has not yet marked an entry from its current term committed, it waits until it has done so." (Raft has each leader commit a blank no-op at the start of its term; "As soon as this no-op entry is committed, the leader's commit index will be at least as large as any other servers' during its term.")
2. "The leader saves its current commit index in a local variable readIndex."
3. "It issues a new round of heartbeats and waits for their acknowledgments from a majority of the cluster."
4. "The leader waits for its state machine to advance at least as far as the readIndex."
5. "Finally, the leader issues the query against its state machine and replies to the client with the results."

- Amortization: "it can use a single round of heartbeats for any number of read-only queries that it has accumulated."
- Follower reads: a follower asks the leader for a current readIndex (leader executes steps 1–3), then performs steps 4–5 locally "for any number of accumulated read-only queries."
- Why it matters: "Problems due to stale reads have already been discovered in two third-party Raft implementations" (§6.4, citing Jepsen). Serving reads from a possibly-deposed leader "would only provide serializability".
- The extended paper (Ext §8) gives the same two precautions (no-op commit + heartbeat exchange) and notes the lease alternative "would rely on timing for safety (it assumes bounded clock skew)."

### A1.5 Leader leases and their clock assumptions (Diss §6.4.1, p. 73–75; Figure 6.3)

- Mechanism: "Once the leader's heartbeats were acknowledged by a majority of the cluster, the leader would assume that no other server will become leader for about an election timeout, and it could extend its lease accordingly." Figure 6.3 caption: the leader extends its lease to **start + election timeout / clock drift bound**, where *start* is when the heartbeat round was sent ("since the followers shouldn't time out before then").
- Clock assumption, verbatim: "The lease approach assumes a bound on clock drift across servers (over a given time period, no server's clock increases more than this bound times any other)." Hazards named: "scheduling and garbage collection pauses, virtual machine migrations, or clock rate adjustments for time synchronization." Failure consequence: "If the assumptions are violated, the system could return arbitrarily stale information."
- Recommendation in the source: "LogCabin does not currently implement this alternative, and we do not recommend using it unless necessary to meet performance requirements."
- Leadership transfer interaction: "a leader would need to expire its lease before transferring leadership."
- Cheap safety net under asynchrony: return the applied index with every reply; clients send the highest index they have seen; servers refuse to serve a client whose index exceeds their lastApplied — gives per-client monotonic reads ("sequential consistency") even if clocks misbehave (p. 74–75).
- Errata: Figure 6.3 caption typo only ("extends" → "extend").
- Compare Gray & Cheriton (A7) and Paxos Made Live §5.2 (A2): the lease holder must use a shorter lease than the grantors assume.

### A1.6 Log compaction and InstallSnapshot (Diss ch. 5, p. 48–64; Ext §7)

- Principle (Diss ch. 5 p. 50): each server compacts the committed prefix independently; Raft retains last-included index/term (to anchor the AppendEntries consistency check) and the latest configuration from the discarded prefix.
- **InstallSnapshot RPC** (Ext Figure 13; Diss Figure 5.3 p. 53): args `term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data[], done`; result `term`. Receiver: (1) reply immediately if term < currentTerm; (2) create new snapshot file if offset is 0; (3) write data at offset; (4) reply and wait for more chunks if not done; (5) save snapshot file, discard any existing or partial snapshot with a smaller index; (6) if an existing log entry has the same index and term as the snapshot's last included entry, retain log entries following it and reply; (7) discard the entire log; (8) reset state machine using snapshot contents (and load the snapshot's cluster configuration). Chunks are sent in order, and "this gives the follower a sign of life with each chunk, so it can reset its election timer."
- **Disk-based state machines (Diss §5.2 p. 57–58) — the case for mantle:** "Applying each entry from the Raft log mutates the on-disk state and effectively arrives at a new snapshot. Thus, once an entry is applied, it can be discarded from the Raft log" (once buffered writes reach disk). Snapshots are needed only to ship state to slow/new followers and still require copy-on-write to hold a consistent image during transfer.
- **LSM state machines (Diss §5.3.3 p. 60–62):** "Applying LSM trees to Raft appears to be fairly straightforward"; transferring state means sending the immutable runs, "fortunately, runs are immutable, so there is no concern of the runs being modified during the transfer." Do not clean the Raft log in place (holes complicate replication); let the state machine own its compaction.
- **When to snapshot (Diss §5.1.2 p. 54–55):** snapshot when log size exceeds previous snapshot size × expansion factor; factor 4 ⇒ ~20% of disk bandwidth to snapshotting and ~6× state size on disk.
- **Implementation concerns (Diss §5.1.3 p. 55–57):** stream + compress + checksum snapshots; "LogCabin writes each snapshot to a temporary file first, then renames the file when writing is complete and has been flushed to disk"; compaction breaks the assumption that entry *i* implies entries 1..*i−1* exist — "This would have been easier with help from a more powerful type system, if the compiler could enforce that every access to the log also handled the case that the index was out of bounds"; during development "We recommend taking snapshots after applying every log entry … since that can help catch bugs quickly."

### A1.7 Membership changes: single-server vs joint consensus, and the 2015 bug

- **Single-server changes (Diss §4.1 p. 33–36):** adding/removing one server at a time guarantees any old majority overlaps any new majority (Figure 4.3), so the leader can switch directly. The new configuration "takes effect on each server as soon as it is added to that server's log" (not on commit); a new change may start only after the previous `Cnew` entry commits; a server "must be prepared to fall back to the previous configuration in its log" if an uncommitted config entry is removed. Servers accept AppendEntries and grant votes to callers not in their configuration (§4.1 p. 36).
- **Catch-up / learners (Diss §4.2.1 p. 37–39):** new servers join first as non-voting members; the leader replicates in rounds and adds the server when "the last round lasts less than an election timeout" after a fixed number of rounds (e.g., 10), else aborts. Followers can return their log length in AppendEntries responses so the leader caps nextIndex quickly.
- **Removing the leader (Diss §4.2.2 p. 39–40):** prefer leadership transfer; otherwise the leader steps down only after `Cnew` commits.
- **Joint consensus (Diss §4.3 p. 43–45; ATC §6):** C_old,new requires "separate majorities from both the old and new configurations" for elections and commitment; then C_new. The dissertation now recommends single-server changes ("Now that we know about the simpler single-server approach, we recommend that one instead"), while noting joint consensus handles arbitrary changes and "Implementing this required finding and changing about six comparisons in our Raft implementation."
- **System integration (Diss §4.4 p. 45–46):** don't auto-remove failed servers below the intended replication; add before remove when replacing (a 3→4→3 path tolerates one failure throughout, 3→2→3 does not); bootstrap a new cluster by initializing one server with a config entry listing only itself.
- **The single-server membership bug (raft-dev, July 2015; listed in errata as an "important bug").** Scope, per the post: affects implementations using single-server changes, "does not affect Raft implementations that use joint consensus." Mechanism: "two concurrent, competing changes across term boundaries" can "have quorums that don't overlap with each other, causing a safety violation (split brain)." Counter-example 1 (paraphrase with the post's labels): C = {S1,S2,S3,S4}; leader S1 appends D = C+{S5}, replicates only to S5, goes offline; S2 is elected in term 2 by {S2,S3,S4}, appends E = C−{S4} = {S1,S2,S3}, replicates it to S3 and commits it ({S2,S3} is a majority of E); S1 then wins term 3 with votes from {S1,S4,S5} (a majority of D) and overwrites the committed E. **Fix (verbatim): "A leader may not append a new configuration entry until it has committed an entry from its current term."** In practice this means rejecting/delaying membership changes until the leader's start-of-term no-op commits. The post states no formal proof of membership-change correctness yet existed. The WebFetch tool would not return the full verbatim post; the quotes above are exact short excerpts, the scenario is paraphrased.
- Related anecdote (Diss acknowledgments p. vii): Ezra Hoch found a *liveness* bug in membership changes and David Mazières found a safety bug in an early version of the commitment rule — i.e., reconfiguration and commitment are where Raft designs historically broke.

### A1.8 Client interaction: sessions and exactly-once (Diss §6.3, p. 69–72; Ext §8)

- Without sessions Raft is at-least-once: a leader can commit, crash before replying, and the retried command is applied twice (Figure 6.2 lock example).
- Mechanism: each client gets a unique ID (via a `RegisterClient` RPC); each command carries a serial number; the **replicated state machine** keeps a session per client with "the latest serial number processed for the client, along with the associated response"; duplicates are answered from the session without re-execution. With concurrent requests per client the session holds a set of (seq, response) pairs and the client piggybacks the lowest unacknowledged sequence number so the state machine can discard older responses.
- "Commands take effect instantaneously and exactly once according to their first appearance in the Raft log."
- Expiry must be deterministic: "session expiry must be deterministic, just as normal state machine operations must be." Options: bounded count with LRU, or time agreed through the log — "In LogCabin, the leader augments each command that it appends to the Raft log with its current time"; keep-alives are also logged.
- A command for an unknown/expired session is rejected with an error rather than creating a fresh session (prevents duplicate execution after expiry).
- Leader discovery (Diss §6.1–6.2): external directory (e.g., DNS) must be *inclusive*; non-leaders reject with a leader hint (recommended) or proxy.

### A1.9 Performance: parallel leader write, batching, pipelining (Diss §10.2, p. 141–144)

Correction to the task brief: batching and pipelining are **§10.2.2**; §10.2.1 is "Writing to the leader's disk in parallel" (summarized in A1.2).
- Batching: AppendEntries carries many entries; LogCabin sends "as many entries as are available between the follower's next index and the end of the log, up to one megabyte"; the cap exists so heartbeats stay frequent ("if one RPC got to be too large, the follower might suspect the leader of failure"); the follower writes the whole batch "to its disk at once."
- Pipelining: "The AppendEntries consistency check guarantees that pipelining is safe; in fact, the leader can safely send entries in any order." The leader advances nextIndex optimistically; on timeout, reset nextIndex; on consistency failure, back off further. Works best with in-order delivery (single TCP connection per follower).
- Scheduling: LogCabin originally used double-buffering (one outstanding RPC per follower; next RPC carries everything accumulated), which self-adjusts batch size to load. Errata: "The first implementation of pipelining wasn't quite right and never made it into LogCabin's master branch" (LogCabin issue 97).
- Reference numbers (Diss §10.3, 2014 hardware, 3 servers, 1 KB writes, SSD with write cache off): ~19,500 writes/s with 100 client threads; ~1.0 ms latency for 2–5 server clusters.

### A1.10 Engineering and testing guidance from the dissertation (Diss §8.3, p. 115–116)

- Structure: cites Howard's ocaml-raft design (all state transitions in one module with precondition assertions and no system code; a separate module decides when to invoke them) and its ability to "simulate an entire cluster in a single process, which allows it to assert Raft's invariants across virtual servers during execution."
- End-to-end: Jepsen + Knossos (linearizability checking) found read-path bugs in two Raft implementations.
- Make rare events common: very low election timeouts and high heartbeat intervals; very frequent snapshots; random restarts; continuous membership changes; resource contention; random message drops (varying per link), random delays, link disable/restore, random partitions; long runs on many machines; tmpfs to speed up.

### Implications for mantle (A1)

1. **Durability contract per Raft group:** `HardState{term, vote}` and appended entries must be durable before any RequestVote/AppendEntries response that depends on them. Leader-local appends may be written concurrently with replication (§10.2.1) as long as the leader's own `matchIndex` tracks *durable* index. Because mantle's state machine is persistent, `applied_index` must be persisted atomically with the state-machine mutation it covers (errata to Fig. 3.1).
2. **Node with lost/corrupted persistent state rejoins with a new replica ID** (Diss §3.8), unless mantle implements PAR-style recovery (A3). The placement layer must be able to issue "replace replica X with fresh replica X′" as a normal membership change.
3. **Turn on all three election guards** — PreVote, follower stickiness (ignore RequestVote within min election timeout of a heartbeat), and leader step-down on lost quorum — plus leadership transfer with a "disrupt allowed" flag. Add partial-partition scenarios to simulation (Jensen et al.).
4. **Reads:** default to ReadIndex (safe under asynchrony; batch many reads per heartbeat round; follower reads via leader-issued readIndex). Treat leader/lease reads as an explicitly configured optimization with a documented max-clock-drift bound, lease measured from heartbeat *send* time and divided by the drift bound, and lease expiry before any leadership transfer. Consider shipping the per-client applied-index monotonicity guard regardless.
5. **Membership:**
   - Implement learners and catch-up rounds.
   - Enforce "at most one uncommitted configuration change", **and** the 2015 rule "no configuration entry until an entry from the current term is committed". Apply it on every path. It costs little: openraft added exactly this barrier to its single-step path in 2026 (B2).
   - Prefer add-before-remove.
   - For atomic replica *swaps* ({A,B,C}→{A,B,D}) that never pass through an even-sized configuration, use joint consensus (ATC §6 / Diss §4.3). CockroachDB recommends it for all production Raft systems (A4.3). The 2015 post says joint consensus is not affected by that bug. It still has to be model-checked, because it is outside the published proof.
   - Decide explicitly whether a configuration takes effect *on append* (dissertation) or *on apply* (etcd/raft-rs; B1). Encode that choice in the TLA+ model.
6. **Snapshots for a disk-based (LSM/B-tree) state machine:** truncate the Raft log once applied state is durable; build snapshots from engine checkpoints/immutable files; chunk + checksum + write-to-temp-then-rename; include last index/term, the configuration, and the client-session table.
7. **Exactly-once for metadata mutations:** client IDs + sequence numbers stored *in the replicated state* (per range), expiry driven by leader-stamped log time, reject unknown sessions. Multi-range operations need an equivalent transaction-ID-based idempotency story (outside Raft's scope).
8. **Performance:** batch across *groups* as well as within a group (see A9 and Part C.c); cap AppendEntries batch size so heartbeats are not starved; pipeline per follower over one ordered connection.
9. **Testability:** the dissertation, Paxos Made Live and Raft Refloated all converge on "pure state-transition core + whole-cluster single-process simulation + invariant assertions + adversarial knobs" — this is exactly a sans-IO core.

## A2. Paxos Made Live (Chandra, Griesemer, Redstone)

**Citation [PR]:** T. D. Chandra, R. Griesemer, J. Redstone. "Paxos Made Live: An Engineering Perspective." *Proc. 26th ACM Symposium on Principles of Distributed Computing (PODC '07)*, pp. 398–407, 2007. doi:10.1145/1281100.1281103 (DOI, pages verified via Crossref). Text read from the authors' version dated June 26, 2007: https://static.googleusercontent.com/media/research.google.com/en//archive/paxos_made_live.pdf (section numbers below are from that version).

Context: replaced Chubby's replication layer (3DB) with a Multi-Paxos fault-tolerant log + database.

- **§5.1 Handling disk corruption.** Corruption shows up as changed file contents or inaccessible files. Detection: "we store the checksum of the contents of each file in the file"; an emptied disk is indistinguishable from a new replica, so each replica leaves "a marker in GFS after start-up" and, if it later starts with an empty disk, discovers the marker and declares corruption. Recovery: the replica "participates in Paxos as a non-voting member … It remains in this state until it observes one complete instance of Paxos that was started after the replica started rebuilding its state," so it "could not have reneged on an earlier promise." Footnote: this does not detect files rolled back to an old state. **Caveat:** PAR (A3, §2) reports this "MarkNonVoting" approach "can sometimes violate safety as noted by prior work" (van Renesse, Schiper, Schneider, IEEE TDSC 12(4), 2015) and hurts availability.
- **§5.2 Master leases.** Reads through Paxos are expensive; with a lease "it is guaranteed that other replicas cannot successfully submit values to Paxos," so the master serves reads locally. "all replicas implicitly grant a lease to the master of the previous Paxos instance and refuse to process Paxos messages from any other replica while the lease is held. **The master maintains a shorter timeout for the lease than the replicas – this protects the system against clock drift.**" Leases are refreshed by a periodic dummy "heartbeat" value; "masters successfully maintain leases for several days at a time." Master churn under intermittent connectivity is damped by periodically "boosting" the sequence number with a full Paxos round. Replica (follower) leases were examined but not implemented.
- **§5.3 Epoch numbers.** A request must abort if mastership was lost and/or re-acquired while it was being handled; the epoch "is stored as an entry in the database, and all database operations are made conditional on the value of the epoch number."
- **§5.4 Group membership:** the literature did not spell out membership with Multi-Paxos + disk corruption; "The details – though relatively minor – are subtle and beyond the scope of this paper."
- **§5.5 Snapshots.** Application-driven; a *snapshot handle* (Paxos instance number + group membership) ties snapshot to log; three-phase snapshot (get handle → take snapshot concurrently → hand back handle; log truncated only then), so a failed or corrupt snapshot never truncates the log; lagging replicas fetch a snapshot then the remaining log, possibly retrying with a newer snapshot.
- **§5.6 MultiOp.** Atomic `guard` (list of tests) + `t_op` + `f_op` lists submitted as a single Paxos value; used to implement Chubby's operations and later to make every operation conditional on the epoch.
- **§6.1** Core algorithm written as two explicit state machines in a small spec language compiled to C++ (with generated transition logging/coverage); a fundamental membership-algorithm change took "about one hour to make … and three days to modify our tests."
- **§6.2 Runtime consistency checking.** Liberal asserts; the master periodically submits a checksum request through the log, all replicas checksum their database and compare. Three inconsistency incidents: one operator error, one unexplained (possibly hardware memory corruption), one suspected errant memory access — after which they "maintain a second database of checksums and double-check every database access."
- **§6.3 Testing.** Two randomized long-running fault tests, each in *safety mode* (consistency only) then *liveness mode* (must make progress after faults stop). The log test simulates "a random number of replicas" through "network outages, message delays, timeouts, process crashes and recoveries, file corruptions, schedule interleavings"; runs are repeatable from an RNG seed by running "in a single thread to remove unwanted non-determinism". They deliberately left known protocol bugs in to measure test strength; later ran on "a farm of several hundred Google machines"; some bugs "took weeks of simulated execution time (at extremely high failure rates) to find." A second test uses failure-injection hooks (crash, disconnect, force "pretend no longer master") and "found five subtle bugs in Chubby related to master failover in its first two weeks." Unsolved: fault tolerance masks bugs/misconfiguration (a misspelled replica name silently reduced tolerance from 2 to 1 failure).
- **§6.4 Concurrency.** Repeatability came from keeping the log free of its own threads; as the database and log became multi-threaded "we were unable to adhere to these goals."
- **§7 Unexpected failures (>100 machine-years).** Rollback with an old snapshot lost 15 hours of data; a failed upgrade left months-old snapshot files that cost ~30 minutes of data; semantic mismatch with Chubby required epoch numbers; on Linux 2.4, fsync of a small log file could stall for seconds behind buffered snapshot writes — workaround: "write all large files in small chunks, with a flush to disk after each small chunk" to protect "the more critical log writes"; memory corruption detected by checksums led them to "crash a replica when it detects this problem."
- **§9 Summary.** "There are significant gaps between the description of the Paxos algorithm and the needs of a real-world system … the final system will be based on an unproven protocol"; the community "has not paid enough attention to testing."

### Implications for mantle (A2)

1. Checksum every persistent structure; detect a *wiped* disk via an external marker (mantle's placement/meta service can record "replica X has initialized storage"); on detection do **not** use MarkNonVoting in place of a proven protocol — rejoin with a fresh replica identity (Raft Diss §3.8) or implement PAR (A3).
2. Leases: holder's lease strictly shorter than grantors' timeout; all ops carry a lease/epoch and are conditional on it (the MultiOp/epoch pattern maps directly onto mantle's range-local conditional batches).
3. Build a periodic, log-ordered replica consistency checker (checksum at a log index) from day one.
4. Isolate Raft-log fsync latency from bulk writes (snapshots, compactions, data ingest) — chunked writes with intermediate flushes, rate limiting, or separate devices; measure fsync tail latency under compaction.
5. Testing: deterministic seed-replayable single-threaded simulation, safety-then-liveness phases, mutation-style "planted bug" checks of test strength, and scale-out runs. Keep concurrency at the edges or the repeatability is lost (their explicit regret).
6. Operations: version and verify snapshots used for rollback; make upgrade scripts idempotent; alert on "silently degraded" fault tolerance (replica permanently catching up, missing member).

## A3. Protocol-Aware Recovery (PAR) — corrupted Raft log entries only (brief)

**Citation [PR]:** R. Alagappan, A. Ganesan, E. Lee, A. Albarghouthi, V. Chidambaram, A. C. Arpaci-Dusseau, R. H. Arpaci-Dusseau. "Protocol-Aware Recovery for Consensus-Based Storage." *16th USENIX Conference on File and Storage Technologies (FAST '18)*, Oakland, CA, Feb 12–15 2018, pp. 15–32. ISBN 978-1-931971-42-3. https://www.usenix.org/conference/fast18/presentation/alagappan. (Another researcher covers PAR in depth.)

What a Raft node should do with a corrupted log entry (CTRL = corruption-tolerant replication):
- **Don'ts (§2, Table 1, Figure 2):** *Crash* preserves safety but "even a single storage fault results in unavailability," and restarts hit the same persistent fault (LogCabin, ZooKeeper, etcd were observed to crash on some log faults). *Truncate* (discard the bad entry and everything after) "can cause a safety violation (data loss)": a node that truncated can form a majority with lagging nodes, elect itself and overwrite committed entries (found in ZooKeeper and LogCabin). *DeleteRebuild* has the same flaw. *MarkNonVoting* (Paxos Made Live) "can sometimes violate safety". *Reconfigure* often cannot commit the needed config entry.
- **Detect (§3.3.2):** per-entry checksums; EIO from reads; fixed-size preallocated log/metainfo files detect size anomalies. File-system *metadata* faults → crash the node (safety over availability).
- **Disentangle crash vs corruption (§3.3.3):** write a small, checksummed *persist record* per entry; append protocol `write(e_i); write(p_i); fsync()` (no extra fsync). On checksum mismatch of e_i: p_i absent ⇒ torn write from a crash ⇒ safe to discard locally; p_i present and a later e_{i+1}/p_{i+1} present ⇒ corruption; last entry with p_i present ⇒ undecidable ⇒ mark corrupted and let the distributed protocol decide. Rationale: "if an entry is corrupted, the entry cannot be simply discarded since it could be globally committed."
- **Identify (§3.3.4):** store identifiers ⟨term(epoch), index, offset, checksum⟩ physically separate from entries (at the log head; 32 bytes each) so a misdirected write is unlikely to hit both.
- **Metainfo (§3.3.1):** currentTerm/votedFor "cannot be recovered from other nodes" — keep two local checksummed copies.
- **Distributed log recovery (§3.4):** a node with faulty entries *may* still be elected (the naive "no faulty leader" restriction causes unavailability). Followers report faulty ⟨term,index⟩; the leader supplies the entry, or, if it doesn't have that ⟨term,index⟩, the entry is uncommitted and the follower truncates it. A faulty **leader** queries followers: any `have` ⇒ repair; `dontHave` from a majority ⇒ uncommitted ⇒ discard it and all later entries; `haveFaulty` ⇒ wait. The leader must not apply/accept new commands until its faulty entries are repaired or discarded (no out-of-order apply). Partial repairs by a leader that dies are harmless.
- **Snapshots (§3.5):** checksummed chunks; leader-initiated identical snapshots so chunks can be fetched from peers; faulty snapshots can never be discarded (all committed).
- **Cost (§5):** 8–10% throughput overhead on HDD at 32 clients (extra seek for separated identifiers); "4% in the worst case" on SSD, write-only workload.

### Implications for mantle (A3)
- mantle's no-panic requirement lines up with PAR: storage faults should surface as typed errors to the Raft layer, which then runs a protocol-aware repair path rather than panicking, truncating, or wiping.
- The Raft log format needs per-record checksums **and** separately stored, checksummed identifiers/persist records; `HardState` needs duplicate checksummed copies.
- Recovery must be expressed per ⟨group, term, index⟩ even if the physical log is shared across groups (design inference, not from the paper: a corrupt shared-WAL batch must be mapped to each affected group's index range, and each group runs the have/dontHave/haveFaulty protocol).

## A4. Range-partitioned metadata systems: Bigtable, Spanner, CockroachDB, TiDB

All four PDFs were fetched, text-extracted and grepped; page ranges confirmed via PDF footers or Crossref.

### A4.1 Bigtable

**Citation [PR]:** F. Chang, J. Dean, S. Ghemawat, W. C. Hsieh, D. A. Wallach, M. Burrows, T. Chandra, A. Fikes, R. E. Gruber. "Bigtable: A Distributed Storage System for Structured Data." *Proc. 7th USENIX Symposium on Operating Systems Design and Implementation (OSDI '06)*, 2006, pp. 205–218 (no DOI for the OSDI version). Journal version: *ACM TOCS* 26(2), June 2008, pp. 1–26, doi:10.1145/1365815.1365816. Fetched: https://www.usenix.org/legacy/event/osdi06/tech/chang/chang.pdf

**Tablet location hierarchy (§5.1, Fig. 4).**
- Three levels, "analogous to that of a B+ tree": a Chubby file holds the root tablet's location; the root tablet holds all METADATA tablet locations; each METADATA tablet holds a set of user-tablet locations.
- "The root tablet is just the first tablet in the METADATA table, but is treated specially—it is never split—to ensure that the tablet location hierarchy has no more than three levels."
- Capacity: METADATA row key = encoding of (table id, end row). "Each METADATA row stores approximately 1KB of data in memory. With a modest limit of 128 MB METADATA tablets, our three-level location scheme is sufficient to address 2^34 tablets (or 2^61 bytes in 128 MB tablets)." (Superscripts flattened by extraction; the arithmetic confirms: 128 MB / 1 KB = 2^17 per level ⇒ 2^34 tablets; × 2^27 B = 2^61 B.)
- Client cost: an empty cache "requires three network round-trips, including one read from Chubby"; a stale cache "could take up to six round-trips, because stale cache entries are only discovered upon misses." Prefetch: "it reads the metadata for more than one tablet whenever it reads the METADATA table." Location data sits in an in-memory locality group (§6).

**Tablet assignment (§5.2; Chubby §4).** One server per tablet; the master tracks live servers and assignments. Each tablet server holds an exclusive Chubby lock on a uniquely named file in a "servers directory"; losing the lock stops serving, and a deleted file makes it "kill itself." The master polls lock status; if a server lost its lock or is unreachable, the master grabs the server's file lock and deletes the file, which "ensures that the tablet server can never serve again." "the master kills itself if its Chubby session expires." Startup: master lock → scan servers dir → ask servers what they serve → scan METADATA (adding the root tablet first if unassigned). Chubby dependency cost (§4): "If Chubby becomes unavailable for an extended period of time, Bigtable becomes unavailable" — average 0.0047% of server-hours affected across 14 clusters (worst 0.0326%).

**Splits and merges (§5, §5.2, §5.3, §6).**
- Tables start as one tablet and split automatically at "approximately 100-200 MB in size by default"; a server typically holds 10–1000 tablets.
- The master initiates create/delete/merge; **tablet servers initiate splits**: "The tablet server commits the split by recording information for the new tablet in the METADATA table. When the split has committed, it notifies the master." If the notification is lost, "the master detects the new tablet when it asks a tablet server to load the tablet that has now split … the tablet entry it finds in the METADATA table will specify only a portion of the tablet that the master asked it to load." (§5.2)
- "Incoming read and write operations can continue while tablets are split and merged." (§5.3)
- Splits are cheap due to immutability: "we let the child tablets share the SSTables of the parent tablet" (§6 "Exploiting immutability").

**Redo points and the shared per-server commit log (§5.3; §6 "Commit-log implementation", "Speeding up tablet recovery").**
- METADATA holds each tablet's SSTable list and "a set of a redo points, which are pointers into any commit logs that may contain data for the tablet."
- "Group commit is used to improve the throughput of lots of small mutations" (§5.3).
- Why one log per server: per-tablet logs would mean "a very large number of files would be written concurrently in GFS" (many seeks) and would reduce "the effectiveness of the group commit optimization, since groups would tend to be smaller." Hence: "we append mutations to a single commit log per tablet server, co-mingling mutations for different tablets in the same physical log file."
- Cost: on server death, each new owner would read the whole log ("the log file would be read 100 times"); fix: sort entries by ⟨table, row name, log sequence number⟩ in parallel over 64 MB segments, coordinated by the master.
- Latency hiding: "two log writing threads, each writing to its own log file; only one of these two threads is actively in use at a time"; "Log entries contain sequence numbers to allow the recovery process to elide duplicated entries resulting from this log switching process."
- Planned moves avoid log replay: minor compaction, stop serving, second (fast) minor compaction, then load elsewhere "without requiring any recovery of log entries."
- GC: mark-and-sweep over SSTables with METADATA as the root set.

**Lessons (§9).** Real failures included "memory and network corruption, large clock skew, hung machines, extended and asymmetric network partitions, bugs in other systems that we are using (Chubby for example), overflow of GFS quotas, and planned and unplanned hardware maintenance"; responses included "we added checksumming to our RPC mechanism" and "we stopped assuming a given Chubby operation could return only one of a fixed set of errors." "The most important lesson we learned is the value of simple designs": an early master-issued lease protocol for tablet-server membership "reduced availability significantly in the presence of network problems, and was also sensitive to master recovery time," and was replaced with one "that depends solely on widely-used Chubby features."

**Implications for mantle.**
- Range directory: bounded-depth B+-tree-like hierarchy (bootstrap pointer → unsplittable root/meta range → meta ranges → data ranges) ⇒ ≤3 hops cold, ≤6 with stale caches; clients cache and prefetch descriptors; servers must reject misrouted requests so stale entries surface on a miss.
- Split notification can be lost: the meta-range descriptor is authoritative; reconcile on load/first contact (TiDB's region epoch, A4.4, is the Raft-native way to reject stale descriptors).
- Shared per-node WAL: Bigtable is the peer-reviewed argument *for* co-mingling many groups' records in one log (fewer seeks/fsyncs, larger group-commit batches) and documents the cost (demultiplexing on recovery). Under Raft, other nodes do not replay a failed node's log (followers have their own copies), so the sort-and-redistribute step disappears; what remains is per-group truncation/GC across shared files and sequence numbers so duplicated/rewritten records can be elided.
- Persist a per-range "redo point" (applied index / snapshot index) with the state machine so recovery replays only the log suffix. For planned moves, flush/snapshot before handoff.
- Design for non-fail-stop faults: checksum RPCs and on-disk records; never assume dependencies return a closed set of errors (also a Rust `#[non_exhaustive]` error-type lesson).

### A4.2 Spanner

**Citation [PR]:** J. C. Corbett, J. Dean, M. Epstein, A. Fikes, C. Frost, J. J. Furman, S. Ghemawat, A. Gubarev, C. Heiser, P. Hochschild, W. Hsieh, S. Kanthak, E. Kogan, H. Li, A. Lloyd, S. Melnik, D. Mwaura, D. Nagle, S. Quinlan, R. Rao, L. Rolig, Y. Saito, M. Szymaniak, C. Taylor, R. Wang, D. Woodford. "Spanner: Google's Globally-Distributed Database." *Proc. 10th USENIX OSDI '12*, 2012, pp. 251–264. Journal version: *ACM TOCS* 31(3), Aug 2013, doi:10.1145/2491245. Fetched: https://www.usenix.org/system/files/conference/osdi12/osdi12-final-16.pdf

- **Organization (§2, §2.1):** universe → zones (one zonemaster, "between one hundred and several thousand spanservers"); location proxies; a singleton placement driver moving data across zones "on the timescale of minutes." Each spanserver holds "between 100 and 1000 instances of a data structure called a tablet" ((key, timestamp) → string, B-tree-like files + WAL on Colossus).
- **One consensus group per tablet:** "each spanserver implements a single Paxos state machine on top of each tablet. (An early Spanner incarnation supported multiple Paxos state machines per tablet … The complexity of that design led us to abandon it.)"
- **Paxos engineering:** "long-lived leaders with time-based leader leases, whose length defaults to 10 seconds"; "Our implementation of Paxos is pipelined … but writes are applied by Paxos in order"; "The current Spanner implementation logs every Paxos write twice: once in the tablet's log, and once in the Paxos log. This choice was made out of expediency, and we are likely to remedy this eventually." Leaders own the 2PL lock table ("having a long-lived Paxos leader is critical to efficiently managing the lock table") and the participant-leader transaction manager (TM state stored in the group); multi-group transactions use 2PC with a coordinator group.
- **Directories (§2.2, §7):** "a set of contiguous keys that share a common prefix," "the unit of data placement"; "One could expect that a 50MB directory can be moved in a few seconds." A tablet may hold several directories (not necessarily contiguous). **Movedir**: "not implemented as a single transaction, so as to avoid blocking ongoing reads and writes on a bulky data move"; it "registers the fact that it is starting to move data and moves the data in the background. When it has moved all but a nominal amount of the data, it uses a transaction to atomically move that nominal amount and update the metadata for the two Paxos groups." Movedir was also used to add/remove replicas "because Spanner does not yet support in-Paxos configuration changes." Oversized directories are sharded into fragments. §7 lists "automatic load-based resharding" as in progress.
- **Leader leases (§4.1.1–4.1.2, Appendix A):** "A potential leader sends requests for timed lease votes; upon receiving a quorum of lease votes the leader knows it has a lease. A replica extends its lease vote implicitly on a successful write." Disjointness invariant: "for each Paxos group, each Paxos leader's lease interval is disjoint from every other leader's." Enforcement: leader records v = TT.now().earliest before requesting; a replica's vote ends at TT.now().latest + 10 and it grants no new vote until TT.after(t_end); "To enforce this rule across different incarnations of r, Spanner logs a lease vote at the granting replica before granting the lease; this log write can be piggybacked upon existing Paxos-protocol log writes." Abdication waits until TT.after(s_max).
- **TrueTime (§3, §5.3):** interval clock; ε sawtooth ~1–7 ms (ε̄ ≈ 4 ms), 30 s poll, assumed drift 200 µs/s; "the most serious problem would be if a local clock's drift were greater than 200us/sec"; "bad CPUs are 6 times more likely than bad clocks."
- **Lease cost (§5.2):** after killing a zone of leaders, recovery waited for 10 s leases to expire; "Shorter lease times would reduce the effect of server deaths on availability, but would require greater amounts of lease-renewal network traffic."
- **Reads (§4.1.3–4.2.4):** a replica serves reads at t ≤ t_safe = min(t_safe^Paxos, t_safe^TM); idle groups advance via MinNextTS ("by default … every 8 seconds").

**Implications for mantle.**
- One Raft group per range; Spanner abandoned multiple groups per tablet for complexity.
- Leases are safe only with bounded clock uncertainty **and** lease promises that survive restarts: persist lease votes/promises, or make a restarted replica wait out the maximum lease length before voting. Lease length trades failover time against renewal traffic — amortize renewals across thousands of groups (see CockroachDB node liveness).
- Moves: background copy + short atomic cutover updating both groups' metadata; unlike Spanner (2012) do membership changes in-Raft.
- Avoid double logging: the Raft log should be the only WAL for replicated state; the state machine persists its applied index.
- Snapshot/"as-of" metadata reads need a closed-timestamp (t_safe/MinNextTS) mechanism.

### A4.3 CockroachDB

**Citation [PR]:** R. Taft, I. Sharif, A. Matei, N. VanBenschoten, J. Lewis, T. Grieger, K. Niemi, A. Woods, A. Birzin, R. Poss, P. Bardea, A. Ranade, B. Darnell, B. Gruneir, J. Jaffray, L. Zhang, P. Mattis. "CockroachDB: The Resilient Geo-Distributed SQL Database." *Proc. SIGMOD '20*, pp. 1493–1509, 2020. doi:10.1145/3318464.3386134. Fetched: https://www.cockroachlabs.com/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf

- **Ranges (§2.1.3–2.1.5, §8):** "contiguous ordered chunks of size ~64 MiB" — "small enough to allow Ranges to quickly move between nodes but large enough to store a contiguous set of data likely to be accessed together." "Ordering between Ranges is maintained in a two-level indexing structure inside a set of system Ranges, which are cached aggressively for fast key lookups." "Ranges start empty, grow, split when they get too large, and merge when they get too small. Ranges also split based on load to reduce hotspots and imbalances in CPU usage." Default 3 replicas; RocksDB per node at the time.
- **Unit of replication (§2.2.1):** "a command, which represents a sequence of low-level edits to be made to the storage engine."
- **Leaseholder vs Raft leader (§2.2.1):** distinct roles, leaseholder "usually the Raft group leader"; it "is the only replica allowed to serve authoritative up-to-date reads or propose writes to the Raft group leader"; "Because all writes go through the leaseholder, reads can bypass networking round trips required by Raft without sacrificing consistency."
- **Two lease kinds:** "Leases for user Ranges are tied to the liveness of the node the leaseholder is on; to signal liveness, nodes heartbeat a special record in a system Range every 4.5 seconds. System Ranges in turn use expiration based leases which must be renewed every 9 seconds." Leases are acquired by committing a Raft entry that includes "a copy of the lease believed to be valid at the time of request" (compare-and-swap).
- **Catch-up (§2.2.2–2.2.3):** snapshot vs log catch-up chosen "based on the number of writes that occurred while the replica was unavailable"; placement by attributes/constraints across failure domains; liveness/metrics via gossip.
- **Clocks (§4.1–4.3):** HLC; max offset "defaults to a conservative value of 500 ms"; lease disjointness "is enforced on cooperative lease handoff with causality transfer through the HLC and is enforced on non-cooperative lease acquisition through a delay equal to the maximum clock offset between lease intervals"; restarted nodes wait out max offset. "Raft does not have a clock dependency"; safeguards: leases carry start/end timestamps; "Each write to a Range's Raft log includes the sequence number of the Range lease that it was proposed under. Upon successful replication, the sequence number is checked against the currently active lease. If they do not match, the write is rejected." Result: serializability under any skew but stale reads possible beyond the bound; "If any node exceeds the configured maximum offset by more than 80% compared to a majority of other nodes, it self-terminates."
- **Follower reads (§3.5):** closed timestamps ("the timestamp below which no further writes will be accepted") exchanged with Raft log indexes, generated per node, typically trailing ~2 s. Parallel Commits "formally verified … using TLA+."
- **§7.1 "Raft Made Live":** (1) with "hundreds of thousands of consensus groups (one per Range)", "we coalesce the heartbeat messages into one per node to save on the per-RPC overhead, and (2) we pause Raft groups which have seen no recent write activity." (2) Single-member changes hurt availability during rebalancing (e.g., 3 regions: the intermediate 2- or 4-replica state loses availability on a region failure); they implemented joint consensus, "not significantly more complex than the default protocol, so we recommend that all production-grade Raft-based systems use Joint Consensus instead."
- **§7.4 version upgrades:** replicating requests and evaluating them on each replica let mixed versions diverge; "we moved the evaluation stage first, and now propose the effect of an evaluated request, rather than the request itself."
- Not in the paper (UNVERIFIED if relied on): the term "epoch-based lease", meta1/meta2 names, Jepsen/roachtest, how leaseholder/leader co-location is maintained, current default range size. Primary (non-peer-reviewed) CockroachDB design doc corroborates coalescing: "a single Node may have millions of consensus groups … Areas of optimization are chiefly coalesced heartbeats (so that the number of nodes dictates the number of heartbeats as opposed to the much larger number of ranges) and batch processing of requests" (https://github.com/cockroachdb/cockroach/blob/master/docs/design.md).

**Implications for mantle.**
- Size- and load-based splits (hot directories), merges for cold/small ranges, two-level meta index with aggressive client caching.
- Separate *lease* (who may serve/propose) from *Raft leadership* (who orders the log) but co-locate them; scale leases via one node-liveness heartbeat per node, with expiration leases for meta/liveness ranges.
- Adopt both skew safeguards: lease changes through the log with CAS on the previous lease; every proposal carries a lease sequence checked at apply; monitor offset and self-fence.
- Replicate *evaluated effects* (deterministic write batches), not high-level operations — protects mixed-version clusters.
- Day-one multi-Raft features: coalesced per-node-pair heartbeats, quiescence of idle groups, **joint consensus** for replica moves.

### A4.4 TiDB / TiKV

**Citation [PR]:** D. Huang, Q. Liu, Q. Cui, Z. Fang, X. Ma, F. Xu, L. Shen, L. Tang, Y. Zhou, M. Huang, W. Wei, C. Liu, J. Zhang, J. Li, X. Wu, L. Song, R. Sun, S. Yu, L. Zhao, N. Cameron, L. Pei, X. Tang. "TiDB: A Raft-based HTAP Database." *PVLDB* 13(12): 3072–3084, 2020. doi:10.14778/3415478.3415535. Fetched: https://www.vldb.org/pvldb/vol13/p3072-huang.pdf

- **Multi-Raft Regions (§3, §4):** one Raft group per Region ("we call it multi-Raft storage"); data and metadata persisted to RocksDB; "Each Region has a configurable max size, which is 96 MB by default."
- **PD (§3, §4.1.3, §6.2):** "responsible for managing Regions, including supplying each key's Region and physical location, and automatically moving Regions to balance workloads. PD is also our timestamp oracle"; "PD has no persistent state, and on startup a PD member gathers all necessary data from other members and TiKV nodes" (as stated in 2020). TSO = physical ms + 18-bit logical; ~1 M ts/s in practice, batched; measured 602,594 ts/s per server.
- **Raft pipeline optimizations (§4.1.1):** leader "sends logs to followers and appends logs locally in parallel" — "If appending logs fails on the leader but a quorum of the followers successfully append the logs, the logs can still be committed"; batching; optimistic pipelining with index rollback on error; asynchronous apply "by a different thread."
- **Reads (§4.1.2):** ReadIndex; lease read ("This approach works well if the CPU clock on each node does not differ very much"); follower read via leader read index.
- **Many Regions (§4.1.3):** PD scheduling with constraints (≥3 replicas on distinct TiKV instances) from heartbeats; "if a Raft group does not have any workloads, the heartbeat is unnecessary. Depending on how busy the Regions' the workloads are, we can adjust the frequency of sending heartbeats."
- **Split (§4.1.4):** PD sends split to the leader; the leader replicates a log entry containing only the split command; on commit every replica applies it, "updating the original Region's range and epoch metadata, and creating new Regions to cover the remaining range. Note that the command is applied atomically and synced to disk"; "The Region that covers the rightmost range reuses the Raft group of the original Region. Other Regions use new Raft groups"; "After the split, if the network is partitioned, the group of nodes with the most recent epoch wins"; "The overhead of region split is low as only metadata change is needed."
- **Merge (§4.1.4):** PD first co-locates replicas of adjacent Regions, then each server merges locally in two phases (stop one Region, merge into the other) because "it cannot use the log replication process between two Raft groups to agree on merging them."
- **Learners (§4.2):** TiFlash learners "do not participate in the Raft protocols to commit logs or elect leaders"; snapshot then log; learner reads use read-index from the leader.
- Not in paper: "hibernate region" name, current PD persistence, current default Region size, merge details beyond the sketch.

**Implications for mantle.**
- TiKV is the closest architectural analogue (ordered KV → Raft ranges on a shared per-node engine + replicated placement driver).
- Split = a Raft command in the parent's log, applied atomically and durably, bumping range+epoch; rightmost child keeps the parent group; every request carries the descriptor epoch to reject stale routes/replicas.
- Merge needs co-location + local stop-and-merge (cannot be one Raft command across groups); needs deeper implementation-level study and model checking.
- The Raft core API must separate *persist*, *send*, *apply* outputs — that enables parallel local append, batching, optimistic pipelining, async apply.
- Idle-group heartbeat suppression and learners are table stakes.

### A4.5 Cross-reference: Tectonic's metadata store (hash vs range partitioning)

**Citation [PR]:** S. Pan, T. Stavrinos, Y. Zhang, A. Sikaria, P. Zakharov, A. Sharma, S. Shankar P, M. Shuey, R. Wareing, M. Gangapuram, G. Cao, C. Preseau, P. Singh, K. Patiejunas, J. R. Tipton, E. Katz-Bassett, W. Lloyd. "Facebook's Tectonic Filesystem: Efficiency from Exascale." *19th USENIX FAST '21*, pp. 217–231. https://www.usenix.org/conference/fast21/presentation/pan (text already extracted by another researcher in this session; only the metadata-store passages were checked here).

- Tectonic "delegates filesystem metadata storage to ZippyDB [6], a linearizable, fault-tolerant, sharded key-value store"; shards are "the unit of replication", run RocksDB per node, are "replicated with Paxos", and "The key-value store does not provide cross-shard transactions, limiting certain filesystem metadata operations" (§3.2).
- Name/File/Block layers "are hash-partitioned by directory, file, and block IDs, respectively"; list-valued keys are "expanded" into one key per item (e.g., (d1, foo), (d1, bar)) and listed "by doing a prefix scan over keys."
- Explicit rejection of range partitioning for this workload: "Range partitioning tends to place related data on the same shard, e.g., subtrees of the directory hierarchy, making the metadata layer prone to hotspots if not carefully sharded. We found that hash partitioning effectively load-balances metadata operations." Cost of that choice (§6): no recursive list API and no `du`; per-directory usage is aggregated periodically and "can be stale."
- **Implication for mantle (design inference):** a range-partitioned store does not force range-partitioned *placement of the namespace*. Encoding keys as `hash(dir_id) ‖ dir_id ‖ name` keeps each directory listing a contiguous prefix scan while spreading directories across ranges as Tectonic does; load-based splitting (CockroachDB) then handles single hot directories. Decide this in the key-schema research, since it interacts with split points and with whether multi-range transactions are needed for rename.

## A5. FoundationDB — deterministic simulation testing

**Citation [PR]:** J. Zhou, M. Xu, A. Shraer, B. Namasivayam, A. Miller, E. Tschannen, S. Atherton, A. J. Beamon, R. Sears, J. Leach, D. Rosenthal, X. Dong, W. Wilson, B. Collins, D. Scherer, A. Grieser, Y. Liu, A. Moore, B. Muppana, X. Su, V. Yadav. "FoundationDB: A Distributed Unbundled Transactional Key Value Store." *Proc. SIGMOD '21*, pp. 2653–2666, 2021. doi:10.1145/3448016.3457559. Fetched: https://www.foundationdb.org/files/fdb-paper.pdf

**Methodology (§1, §2.1, §4, §6.2, §7).**
- Built first: "before building the database itself, we built a deterministic database simulation framework that can simulate a network of interacting processes and a variety of disk, process, network, and request-level failures and recoveries, all within a single physical process."
- Determinism: "All database code is deterministic; accordingly multithreaded concurrency is avoided (instead, one database node is deployed per core)." Network, disk, time and PRNG are abstracted; code is written in Flow (async/await-style C++ actor extension); many servers run in one discrete-event simulation; "The production implementation is a simple shim to the relevant system calls."
- Workloads (in Flow, composable): fault-injection instructions, mock applications, configuration changes, internal calls. **Oracles:** workload assertions of invariants only atomicity/isolation can maintain; pervasive local assertions; recoverability ("return the modelled hardware to a recoverable state and check that the cluster eventually recovers").
- **Fault injection:** "machine, rack, and data-center level fail-stop failures and reboots, a variety of network faults, partitions, and latency problems, disk behavior (e.g. the corruption of unsynchronized writes when machines reboot), and randomizes event times"; rates tuned so runs don't collapse into a small state space.
- **Buggify:** inject "unusual (but not contract-breaking) behavior such as unnecessarily returning an error from an operation that usually succeeds, injecting a delay in an operation that is usually fast, choosing an unusual value for a tuning parameter"; randomizing tuning parameters "ensures that specific performance tuning values do not accidentally become necessary for correctness."
- **Swarm testing:** per-run randomization of cluster size/config, workloads, fault parameters, tuning parameters and which buggify points are enabled. **Coverage macros** (`TEST( buffer.is_full() );`) count how often rare conditions are reached.
- **Speed:** the simulated clock fast-forwards when idle; runs are "embarrassingly parallel"; testing is "burst" before releases.
- **Limitations (§4):** "Simulation is not able to reliably detect performance issues, such as an imperfect load balancing algorithm. It is also unable to test third-party libraries or dependencies, or even first-party code not implemented in Flow. As a consequence, we have largely avoided taking dependencies on external systems"; "several bugs have resulted from the true operating system contract being weaker than it was believed to be."
- **Reported effectiveness (§6.2):** shortens time from bug introduction to detection; exact reproduction (logging does not perturb event order); production bugs are handled by first improving the simulator until it reproduces them; "CloudKit [59] has deployed FDB for more than 0.5M disk years without a single data corruption event"; continuous replica-consistency checks have never found an inconsistent replica; ZooKeeper was removed "after real-world fault injection found two independent bugs in Zookeeper (circa 2010)" and its Flow-Paxos replacement has had "No production bugs … reported since"; simulation made ground-up rewrites of major subsystems feasible.
- **Related work (§7):** Jepsen-style fault injection "lack[s] deterministic reproducibility"; model checking "can only verify the correctness of a model rather than of the actual implementation."
- Not in the paper (UNVERIFIED): "swizzle-clogging" (known only from non-peer-reviewed talks/docs), quantified simulation scale (CPU-hours), DataDistributor split criteria, Ratekeeper algorithm.

**Unbundled architecture (relevant to metadata design; §2.2–2.6, §5.3, §6.3–6.4).**
- Control plane: Coordinators form a disk Paxos group storing critical metadata and electing a singleton ClusterController, which recruits Sequencer, DataDistributor ("monitoring failures and balancing data among StorageServers") and Ratekeeper ("overload protection").
- Data plane: stateless Sequencer/Proxies/Resolvers; LogServers ("replicated, sharded, distributed persistent queues"); StorageServers on a modified SQLite B-tree.
- "FDB has no external dependency on other services"; system metadata under the 0xFF prefix. Reconfiguration: the Sequencer exits on any failure/config change; "transaction processing is divided into epochs." Commit: versions at "one million versions per second", OCC resolvers, ack after all designated LogServers are durable; StorageServers pull asynchronously. Recovery needs only the end of the redo log (PEV/RV computation); production median 3.08 s, p90 5.28 s, "not bounded by the data or transaction log size" (§5.3).
- Replication: Paxos for metadata; logs synchronous to f+1; storage asynchronous to f+1 teams — f+1 instead of 2f+1 by reconfiguring on failure, "best suited for deployments in a local or metro area."
- Adaptive batching (§2.6): "the batching degree is adjusted dynamically, shrinking when the system is lightly loaded to improve commit latency, and increasing when the system is busy."
- Limits: keys ≤10 KB, values ≤100 KB, transactions ≤10 MB, 5 s MVCC window. Upgrades restart all processes at once, so only on-disk compatibility is required (§6.3).

**Implications for mantle.**
- Put every source of nondeterminism (network, disk, clock, randomness, scheduling) behind interfaces a single-threaded, seeded discrete-event simulator can replace; production = thin shim. This is the strongest peer-reviewed argument for a **sans-IO Raft core and a storage layer whose file I/O the simulator owns**.
- Code outside the simulator is not tested by it — including third-party libraries and a C/C++ storage engine whose I/O bypasses the simulated disk (bears on the engine choice in Part C.b).
- Copy the fault model (node/rack/zone crash-reboot, loss/corruption of un-fsynced writes, partitions/latency, randomized timing), buggify hooks, swarm randomization, coverage counters, and "heal, then must recover" liveness oracles.
- Performance and OS-contract assumptions (fsync semantics) need separate real-hardware validation.
- Architectural contrast for the decision record: FDB keeps consensus only for control-plane state and recovers the whole write path in seconds; per-range Raft costs 2f+1 replicas but localizes failures to a range.
- "Fail fast, recover through one well-tested path" is compatible with a no-panic rule if the failing unit is a replica/group incarnation handed to a supervisor, not the process.

## A6. Linearizability: definition, complexity, and a practical checker

### A6.1 Herlihy & Wing — the definition

**Citation [PR]:** M. P. Herlihy, J. M. Wing. "Linearizability: A Correctness Condition for Concurrent Objects." *ACM TOPLAS* 12(3):463–492, July 1990. doi:10.1145/78969.78972. Fetched: https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf

- **Model (§2.1, pp. 467–468):** a history is "a finite sequence of operation invocation and response events"; a response matches an invocation when object and process names agree; an invocation with no matching response is *pending*; "complete(H) is the maximal subsequence of H consisting only of invocations and matching responses"; H|P and H|x are process/object subhistories; H and H′ are *equivalent* if H|P = H′|P for all P; H is *well-formed* if every H|P is sequential (assumed throughout); a sequential specification is "a prefix-closed set of single-object sequential histories."
- **Real-time order (§2.2, p. 468):** "e₀ <_H e₁ if res(e₀) precedes inv(e₁) in H"; unordered operations are concurrent.
- **Definition (§2.2, p. 469):** "A history H is linearizable if it can be extended (by appending zero or more response events) to some history H′ such that: L1: complete(H′) is equivalent to some legal sequential history S, and L2: <_H ⊆ <_S." Extension "captures the notion that some pending invocations may have taken effect even though their responses have not yet been returned"; dropping the rest "captures the notion that the remaining pending invocations have not yet had an effect." (This is the formal basis for treating timed-out operations in a checker.)
- **Locality (§3.1, Theorem 1, p. 470):** "H is linearizable if and only if, for each object x, H|x is linearizable" ⇒ "linearizable objects can be implemented, verified, and executed independently."
- **Nonblocking (§3.2, Theorem 2, pp. 471–472):** "a pending invocation of a totally-defined operation is never required to wait for another pending invocation to complete."
- **§3.3:** sequential consistency "does not require the original history's precedence ordering to be preserved"; serializability is neither local nor nonblocking.

**Implications for mantle.** Per-key linearizability composes across all ranges (Theorem 1) with no cross-range coordination. Locality is about *keys*, not Raft groups: when key ownership moves (split/merge/lease transfer), an operation acknowledged by the old owner must be visible to any operation later invoked at the new owner — i.e., fence the old owner before the new one serves. Multi-key operations (rename, link) need strict serializability (A6.7). A timed-out client operation may still take effect (H→H′), so retries must be idempotent (Raft client sessions, A1.8).

### A6.2 Wing & Gong — the original checking algorithm

**Citation [PR]:** J. M. Wing, C. Gong. "Testing and Verifying Concurrent Objects." *J. Parallel and Distributed Computing* 17(1–2):164–182, Jan 1993. doi:10.1006/jpdc.1993.1015. Fetched https://www.cs.cmu.edu/~wing/publications/WingGong93.pdf (scan; read from page images).

- §3.1 (p. 167): simulator runs N processes, stores the concurrent history, then calls `analyze`.
- §4 (p. 170): "we try every possible sequential order of H's concurrent operations while preserving its real-time order relation <_H … If every possible ordering of H fails … the history H is nonlinearizable." The specification is an *executable* sequential object (SeqObj).
- §4.1 (pp. 170–172): history as a doubly linked list with invocation→response pointers; pick an operation "whose invocation event is not preceded by any response event", run it on SeqObj; on success push it and *lift* its events; otherwise try another or pop, undo and *unlift*. §4.2 gives the correctness argument.
- §4.3 (p. 173): "There exists simple data types and histories for which testing linearizability is NP-complete … analyzing a long history (say, 1 million operations) is impractical; however, analyzing many short (100 operations) histories is tractable"; "we also have a better chance of finding a nonlinearizable history by testing many short histories rather than testing one long one." (1993 timings: 100 queue ops ≈ 1 min.)

### A6.3 Lowe — memoization ("WGL"), just-in-time linearization, competition parallel

**Citation [PR]:** G. Lowe. "Testing for linearizability." *Concurrency and Computation: Practice and Experience* 29(4):e3928, 2017. doi:10.1002/cpe.3928. Read from the author's preprint (section numbers from preprint): http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf

- §2 Lemma 2: deadlock-free object + all *complete* histories linearizable ⇒ all histories linearizable — an assumption that does **not** hold for crash-prone distributed systems (see A6.8 for indeterminate ops).
- §3: tree search; debugging extension "prints the maximum linearizable prefix, and the following event, necessarily a return event, and the alternative values that could have been returned at this point."
- §3.1 (the memoization Horn & Kroening name WGL): the tree search "fails to recognise when it encounters a configuration equivalent to one it has seen earlier, i.e. where the same operations have been linearized and the sequential object is in the same state"; fix = hash set of configurations "the state of the sequential object and the set of operations linearized so far"; requires an immutable specification object.
- §4 just-in-time linearization (a partial-order reduction): linearize "as late as possible"; for the graph variant a register has at most (N+1)·2^p·(p+1) configurations — "linear in the length of the history; it is exponential in the number of threads"; a map with K keys (N+1)·2^p·K^(p+1).
- §6: *competition parallel* — run two algorithms, "the first to complete interrupted the other." Tree searches are fast but erratic (Wing–Gong tree on a map: 43/50 runs within 30 ms, 5 hit the 10¹⁰-configuration cap after ~25 min each); graph search wins on non-linearizable histories.
- §7.1: shared synchronized logs hid memory-consistency bugs; use per-thread logs merged afterwards, with the condition "if updates made by an operation of thread t1 can be observed by an operation of thread t2, then the timestamp for t1's call event must be strictly less than the timestamp for t2's return event."
- §1/§8: "Normally, bugs (if they exist) are discovered within 20 seconds, often less than a second"; small key spaces, bad hash functions and noisy machines expose bugs faster.
- §9: wall-clock timestamping "will not work in a distributed setting, because of clock drift"; per-key checking of maps/sets gives an order-of-magnitude speed-up.

### A6.4 Horn & Kroening — P-compositionality

**Citation [PR]:** A. Horn, D. Kroening. "Faster Linearizability Checking via P-Compositionality." *FORTE 2015*, LNCS 9039, Springer, pp. 50–65. doi:10.1007/978-3-319-19195-9_4. Read from arXiv:1504.00204v1.

- §1: checking is NP-complete (citing Gibbons & Korach); worst case O(N!); "We call Lowe's extension of Wing and Gong's algorithm the WGL algorithm"; precise, "reports no false alarms."
- §2 Def. 5: considers only complete histories.
- §3 Def. 6: "A specification φ is called P-compositional whenever any history H is linearizable with respect to φ if and only if, for every history H′ ∈ P(H), H′ is linearizable with respect to φ." Herlihy–Wing locality is an instance (Ex. 6); sets/maps partition by key (Ex. 7), arrays by index (Ex. 8); a `size` operation "cannot be generally partitioned this way."
- §4 Algorithm 1 (WGL): persistent spec; doubly linked entry list with `match` pointers; stack `calls` of (entry, state); bitset `linearized`; `cache` of ⟨linearized, state⟩. At a call entry: apply model; if legal **and** ⟨linearized′, s′⟩ not in cache → push, set bit, LIFT call+return in O(1), restart from head. At a return entry: pop, clear bit, UNLIFT; empty stack at a return ⇒ not linearizable. Algorithm 3 partitions; Theorem 1: partitioned check is correct; sub-histories "could … run in parallel."
- §5.1: C++11; constant-time incremental bitset hash because "the bitwise XOR operator over fixed-size bit vectors forms an abelian group"; optional LRU cache eviction.
- §5.2 (4 threads × 70K ops, |H| = 560K entries): Intel TBB set — WGL 101 s / 9,792 MiB vs partitioned 6 s / 672 MiB; LRU eviction made 14% (LSL) and 9% (OPTIMIST) of runs hit the 1-hour timeout; plain Wing–Gong "times out on the majority of benchmarks."

### A6.5 Complexity

P. B. Gibbons, E. Korach. "Testing Shared Memories." *SIAM J. Comput.* 26(4):1208–1244, 1997, doi:10.1137/S0097539794279614 — **primary text not accessed (paywall)**; NP-completeness in general and an O(n log n) algorithm for a register with a fixed number of processors are as *reported by* Lowe 2017 §1, Horn & Kroening §1 and Kingsbury & Alvaro §1. Wing & Gong §4.3 independently asserts NP-completeness for some simple types.

### A6.6 Tool references (primary-source code, not peer-reviewed)

- **Porcupine** (Go, github.com/anishathalye/porcupine; tag v1.3.1, last commit 2026-09-21): README says it "implements the algorithm described in *Faster linearizability checking via P-compositionality*, an optimization of the algorithm described in *Testing for Linearizability*"; claims (author's own) "generally 1,000x–10,000x faster" than Knossos. `model.go`: [Call, Return] is a **closed** interval (monotonic clocks can tie); optional `Partition`; pure `Step(state, input, output) → (bool, state)`; optional `Hash`/`Equal`; timeouts return `Unknown`. `checker.go`: cache key = bitset hash XOR `Model.Hash(state)`; entries sorted by time with calls before returns on ties; lift/unlift; partitions checked in parallel. README warns weakly ordered CPUs may need barriers "to avoid spurious linearizability violations." Users listed include etcd's robustness tests. Crashed ops in its Jepsen-etcd parser get a synthetic return at the end with `unknown: true`, accepting any result.
- **Knossos** (Clojure/Jepsen; tag 0.3.16): completions `:ok`, `:fail`, `:info` ("not sure what happened, e.g. the operation timed out"); "If a process times out after invoking an operation, it is said to be *crashed* and cannot perform another operation ever again"; `knossos.competition` races graph and WGL tree searches; README caveat: "I am not certain the algorithm is correct yet; you should treat its results as plausible but verify by hand." Failed ops are stripped; incomplete invocations get synthetic `:info` completions at the end.
- **etcd robustness tests** (`tests/robustness/model`): non-deterministic model — "An unknown/error response doesn't inform whether the request was persisted or not, so the model considers both cases … Failed requests fork the possible states"; "all new writes need to be done with new stream id."
- **Rust:** `stateright` 0.31.0 `LinearizabilityTester` records, per invocation, the last completed op of every other thread (real-time order without a global clock) but its `serialize` is a recursive search **with no configuration cache** (fine for model-checked small histories, not ~10⁵ ops). `porcupine-rs` 0.3.0, `lincheck` 0.2.1, `spectroscope` 0.1.0 exist on crates.io — not evaluated (see B6).

### A6.7 Elle — transactional histories

**Citation [PR]:** K. Kingsbury, P. Alvaro. "Elle: Inferring Isolation Anomalies from Experimental Observations." *PVLDB* 14(3):268–280, 2020. doi:10.14778/3430915.3430918. Fetched https://www.vldb.org/pvldb/vol14/p268-alvaro.pdf

- §1: "Since strict serializability is equivalent to linearizability (where operations are transactions, and the linearizable object is a map), these checkers can be applied … its use is limited by the NP-complete nature of linearizability checking."
- Infers an Adya-style dependency graph from *traceable/recoverable* objects (e.g., list-append with unique values) (§2.1, §3).
- §6 Fig. 4: "With 40+ concurrent processes, even histories of 5000 transactions were (generally) uncheckable" by Knossos; §8: Elle "is linear in the length of a history and effectively constant with respect to concurrency."

### A6.8 Practical checker for mantle's Rust test harness (synthesis)

1. **Record client-observed histories:** unique op id, logical client id, input, outcome ∈ {`Ok(output)`, `Fail` (definitely not applied), `Indeterminate`}, invoke/return times. In deterministic simulation use the simulator's virtual clock (exact; sidesteps Lowe §9's drift problem — inference). In real-cluster tests, run clients in one process, take `Instant` just before send / just after receive, treat intervals as closed (Porcupine). Prefer per-client logs merged afterwards (Lowe §7.1).
2. **Well-formedness:** at most one outstanding op per logical client (Herlihy–Wing §2.1); after an indeterminate result, retire the client id and continue under a fresh one (Knossos "crashed"; etcd "new stream id").
3. **Normalize outcomes:** drop `Fail`; `Indeterminate` with side effects ⇒ return time = +∞ and any output accepted (the H→H′ extension, as in Knossos/Porcupine); or use a non-deterministic model that forks states (etcd). Indeterminate reads can be dropped. Write unique values so reads identify the write they observed.
4. **Partition by key** (Horn & Kroening Def. 6/Ex. 7; Herlihy–Wing Thm 1). Range scans, multi-key transactions and counts go to a coarser partition or to an Elle-style checker.
5. **Model:** immutable per-partition state with `step(&state, &input, &output) -> Option<State>` plus `Eq + Hash`; for a KV partitioned by key the state is `Option<Value>`.
6. **Search:** WGL (Horn & Kroening Alg. 1): time-sorted doubly linked entry list (calls before returns on ties), stack of (entry, prior state), linearized bitset, `HashSet<(bitset, state)>` cache with incremental XOR bitset hashing combined with the state hash. Linearizable when the list empties; not linearizable when a return entry is reached with an empty stack.
7. **Budget:** NP-complete in general, exponential in per-key concurrency (Lowe §4) ⇒ cap concurrent clients per key; prefer many short histories (Wing & Gong §4.3); per-partition timeout reporting `Unknown` distinctly; check partitions in parallel; optionally race tree vs graph search (Lowe §6); don't add LRU eviction without measurement (Horn & Kroening §5.2).
8. **Counterexamples:** for each failing partition print the longest partial linearization, the first op that could not be linearized, the model state there, and the outputs that would have been legal (Lowe §3; Knossos; Porcupine's visualizer).

## A7. Leases: Gray & Cheriton

**Citation [PR]:** C. G. Gray, D. R. Cheriton. "Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache Consistency." *Proc. 12th ACM SOSP*, 1989, pp. 202–210. doi:10.1145/74850.74870 (also *ACM SIGOPS OSR* 23(5)). Read from Stanford TR STAN-CS-90-1298 (http://i.stanford.edu/pub/cstr/reports/cs/tr/90/1298/CS-TR-90-1298.pdf; OCR/page images) — section numbers are the paper's; proceedings page numbers not mapped.

- **Definition (§2):** "A lease is a contract that gives its holder specified rights over property for a limited period of time … the server must obtain the approval of the leaseholder before the datum may be written." With an unreachable holder the server "must delay writes … until that lease expires." After a server crash it "must honor the leases it granted before it crashed … most easily done if it remembers the maximum term for which it had granted a lease, and it delays writes to all files for that period."
- **Term selection (§3.1):** effective term at the holder t_C = max(0, t_S − (m_prop + 2m_proc) − ε), where ε is the "allowance for uncertainty in clocks"; "a zero lease term is better than a very short lease term because non-zero t_S and zero t_C means that writes are penalized but reads do not benefit."
- **Clock assumptions (§5):** consistency holds "provided that the hosts and network do not suffer certain Byzantine failures including clock failure." Unsafe directions: "a server clock that advances too quickly can cause errors because it may allow a write before the term of a lease held by a previous client has expired at that client. Similarly, if a client clock fails by advancing too slowly, it may continue using a lease which the server regards as having expired." "The opposite errors—a slow server clock or fast client clock—do not result in inconsistencies, but do generate extra traffic." Detection: "by either a synchronization protocol or by including explicit timestamps in lease-related messages." Minimum: "the correct functioning of leases requires only that clocks have a known bounded drift, in which case the lease term can be communicated as its duration t."
- **Conclusions (§7):** "A key assumption is that clocks are reasonably accurate, at least in terms of drift if not mutual synchronization"; "Leases provide strict consistency in spite of non-Byzantine failures, including partitions"; for V-system file caching "a lease term of 10 seconds results in a server load that is within 5 percent of that achievable with infinite term."

**Implications for mantle.** Express leader/leaseholder leases as **durations on local monotonic clocks** (needs only bounded drift), never as absolute timestamps unless clocks are synchronized within ε. The holder discounts by measured delays and ε; the grantor/successor waits the full term. Unsafe directions are *fast grantor clock* and *slow holder clock* — include sender timestamps in lease/heartbeat messages to detect drift and self-fence (stop lease reads) on detection. A stalled holder process behaves like a slow holder clock ⇒ check lease validity immediately before serving each lease read (inference). After restart, wait out the maximum lease term possibly granted/held before allowing conflicting access. If ε + RTT approaches the term, disable lease reads (fall back to ReadIndex).

## A8. Failure detection

### A8.1 φ accrual failure detector

**Citation [PR]:** N. Hayashibara, X. Défago, R. Yared, T. Katayama. "The φ Accrual Failure Detector." *Proc. 23rd IEEE SRDS*, 2004, pp. 66–78. doi:10.1109/RELDIS.2004.1353004. Read from JAIST tech report IS-RR-2004-010 (https://dspace.jaist.ac.jp/dspace/bitstream/10119/4784/1/IS-RR-2004-010.pdf).

- §III: outputs a continuous suspicion level instead of trust/suspect (asymptotic completeness, eventual monotony, upper bound, reset); §III-C: transformable to ◇P, and accrual detectors "cannot be implemented deterministically in all possible asynchronous systems."
- §IV-A: φ(t_now) = −log₁₀(P_later(t_now − T_last)); "assuming that we decide to suspect p when φ ≥ Φ = 1, then the likeliness that we will make a mistake … is about 10%. The likeliness is about 1% with Φ = 2, 0.1% with Φ = 3."
- §IV-B: sliding window of heartbeat inter-arrival times (mean, variance), assumed **normally distributed**; P_later(t) = 1 − F(t).
- §V: one-week Japan↔Switzerland WAN experiment (100 ms heartbeats, ~0.4% loss in bursts, RTT ≈ 283 ms, window 1,000): mistake rate drops sharply for Φ 0.5→2 and again at Φ ∈ [8, 12]; detection time rises sharply above Φ ≈ 10–11; larger windows help with diminishing returns. LAN not evaluated.

### A8.2 SWIM

**Citation [PR]:** A. Das, I. Gupta, A. Motivala. "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol." *Proc. DSN 2002*, pp. 303–312. doi:10.1109/DSN.2002.1028914. Fetched https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf

- §3.1: each protocol period T′ ping a random member; on timeout send ping-req to k random members; no ack by period end ⇒ failed. "The protocol does not require clocks to be synchronized"; T′ ≥ 3× RTT estimate; expected first-detection time ≤ T′·1/(1 − e^(−q_f)); constant per-member message load independent of group size.
- §4.1: infection-style dissemination piggybacked on ping/ping-req/ack.
- §4.2: suspicion (Suspect → Confirm after timeout; refuted by Alive with higher incarnation) "reduces (but does not eliminate)" false positives.
- §4.3: round-robin probing over a shuffled list gives time-bounded completeness ("no more than two times the group size" protocol periods).

**Implications for mantle (A8).** Raft elections use Raft's own timeouts; failure detectors should drive only liveness/placement (stop routing at low Φ; start re-replication at high Φ plus grace). Safety never depends on detector accuracy. Φ thresholds in the paper are WAN-calibrated and the normality assumption is a tuning risk — measure in mantle's environment. For thousands of storage nodes, SWIM-style gossip gives O(1) per-member load, but its membership is weakly consistent — use it as a hint; authoritative membership/placement lives in the strongly consistent metadata store.

## A9. Group commit and batching in consensus logs

- **[PR] DeWitt, Katz, Olken, Shapiro, Stonebraker, Wood.** "Implementation Techniques for Main Memory Database Systems." *SIGMOD 1984*, pp. 1–8, doi:10.1145/602259.602261. §5.2 is the origin of group commit: with one log device, ~100 commits/s if each transaction needs its own log I/O; commit records on the same log page "are committed as a group … A single log I/O is incurred to commit all transactions within the group" ⇒ ~1,000/s.
- **[PR] Santos, Schiper.** "Tuning Paxos for High-Throughput with Batching and Pipelining," *ICDCN 2012*, LNCS 7129, pp. 153–167, doi:10.1007/978-3-642-25959-3_11; extended: "Optimizing Paxos with batching and pipelining," *Theoretical Computer Science* 496:170–183, 2013, doi:10.1016/j.tcs.2012.10.002. Without these, throughput ≤ 1/(2L); batching/pipelining "usually provide performance gains of one to two orders of magnitude" (§1); "when stable storage is used, batching dramatically decreases its overhead, because a single stable storage access is enough to log the state of all requests in a batch" (§3); "batching by itself provides the largest gains both in high and low latency networks"; too many parallel instances cause "a performance collapse" (§6); tuning: largest batch meeting the latency target, then pick the pipeline window.
- **[PR] Johnson, Pandis, Stoica, Athanassoulis, Ailamaki.** "Aether: A Scalable Approach to Logging." *PVLDB* 3(1–2):681–692, 2010, doi:10.14778/1920841.1920928. Group commit "does not eliminate unwanted context switches"; asynchronous commit is fast "at the expense of durability"; §4.1 *flush pipelining*: a daemon flushes on "every X transactions, L bytes logged, or T time elapsed, whichever comes first" and wakes waiters — asynchronous-commit throughput "without sacrificing any safety."
- **[PR] Whittaker, Ailijiang, Charapko, Demirbas, Giridharan, Hellerstein, Howard, Stoica, Szekeres.** "Scaling Replicated State Machines with Compartmentalization." *PVLDB* 14(11):2203–2215, 2021, doi:10.14778/3476249.3476273. "a Raft leader acts as a batcher, a sequencer, a broadcaster, and a state machine replica" (§1); separate batchers/unbatchers offload batch formation and replies (§4).
- **Raft dissertation §10.2.1–10.2.2** (A1.9) and **TiDB §4.1.1** (A4.4) describe leader-parallel append, batching and pipelining in Raft specifically; **Paxos Made Live §8** attributes its multi-worker throughput gains to batching.
- Citations verified (Crossref) but not read: Helland et al., "Group commit timers and high volume transaction systems" (HPTS 1987; Springer LNCS 1989, pp. 301–329, doi:10.1007/3-540-51085-0_52); Friedman & Hadad, "Adaptive Batching for Replicated Servers" (SRDS 2006, pp. 311–320, doi:10.1109/SRDS.2006.8).

**Implications for mantle.** Batch at two levels — many client commands per Raft entry, and one WAL write + one fsync for *all* Raft groups on a node (DeWitt's commit group across groups; the core argument for a shared multi-group WAL, Part C.c). Batch first, then pipeline with a capped window. Implement acknowledgement as flush pipelining (one WAL writer with an X-ops / L-bytes / T-time policy that wakes proposers). Never acknowledge before fsync (asynchronous commit breaks Raft's persistence rule). Coalesce messages per peer across groups (CockroachDB §7.1.1; Whittaker et al.'s batching logic — the cross-group application is an inference).

## A10. Additional evidence used in Part C

### A10.1 fsync failure semantics

**Citation [PR]:** A. Rebello, Y. Patel, R. Alagappan, A. C. Arpaci-Dusseau, R. H. Arpaci-Dusseau. "Can Applications Recover from fsync Failures?" *USENIX ATC '20*, pp. 753–767. https://www.usenix.org/conference/atc20/presentation/rebello (PDF fetched: https://www.usenix.org/system/files/atc20-rebello.pdf).

- The abstract covers ext4, XFS and Btrfs. Common to all three: "pages are always marked clean, certain block writes always lead to unavailability". Across PostgreSQL, LMDB, LevelDB, SQLite and Redis, "none are sufficient: fsync failures can cause catastrophic outcomes such as data loss and corruption."
- §3.3.4: "All the file systems mark the page clean even after fsync fails". ext4 and XFS keep the latest write in memory, while Btrfs reverts to the on-disk state. None of the three retries data or journal block writes.
- §7: "Application developers can only assume that the underlying file system experienced a fault and that data may have either been persisted partially, completely, or not at all"; "The widely perceived crash-restart fix in the face of fsync failures does not always work; applications recover incorrectly due to on-disk and in-memory mismatches." The authors recommend "sector- or block-level fault-injection tests", using CuttleFS.
- **Implication:** mantle must never retry a failed fsync or trust page-cache contents afterwards. Fence the affected WAL or engine and recover from checksummed on-disk state or from peers (C.c.5). This contradicts raft-engine's code comment that sync errors are "retriable" (B5). fjall's `Error::Poisoned` handling cites this paper (B4.2).

### A10.2 How bugs are found in Raft implementations (peer-reviewed evidence for C.a/C.d)

- **[PR] Mocket.** D. Wang, W. Dou, Y. Gao, C. Wu, J. Wei, T. Huang. "Model Checking Guided Testing for Distributed Systems." *EuroSys '23*, pp. 127–143, doi:10.1145/3552326.3587442 (PDF: https://gaoyu-cn.github.io/paper/2023-eurosys-mocket.pdf). It uses the TLC state space to drive tests of real implementations. On Xraft, Raft-java and ZooKeeper, "we find 7 bugs, among which 3 bugs are confirmed as previously unknown bugs, and 4 are known bugs. Besides, we find that 2 inconsistencies between the specification and the implementation are caused by specification bugs in the official Raft specification."
- **[PR] SandTable.** R. Tang, X. Sun, Y. Huang, Y. Wei, L. Ouyang, X. Ma. "SandTable: Scalable Distributed System Model Checking with Specification-Level State Exploration." *EuroSys '24*, pp. 736–753, doi:10.1145/3627703.3650077. It was applied to 8 systems implementing Raft or Zab and found 23 bugs, 18 of them new. The repository README (https://github.com/tangruize/SandTable) says 17 were confirmed and 13 fixed. Its demo reproduces two leaders in the same term in Xraft.
- **[PR] Model-guided fuzzing.** E. B. Gulcan, B. K. Ozkan, R. Majumdar, S. Nagendra. "Model-Guided Fuzzing of Distributed Systems." *PACMPL* 9(OOPSLA2):274–301, 2025, doi:10.1145/3763060 (arXiv:2410.02307). It uses TLA+ model coverage to guide fuzzing. On etcd-raft and RedisRaft, "we discovered 13 previously unknown bugs in their implementations, four of which could only be detected by model-guided fuzzing."
- **[VB]** Antithesis, "Finding bugs in Raft implementations," July 27, 2026 (https://antithesis.com/blog/2026/finding-bugs-in-raft-implementations/): "we've found bugs in *every* Raft implementation we've tested, including HashiCorp Raft, Aeron Cluster, OpenRaft, and MicroRaft." It gives no OpenRaft details and no matching openraft issue exists (B2), so this is UNVERIFIED.
- **Takeaway:** even the most-used Raft library, etcd-raft, still had unknown bugs that spec-guided testing found in 2025. Mature lineage lowers the risk but does not remove it. Testing against an executable specification (C.d.1–2) is the evidence-backed complement to simulation.

### A10.3 Primary (non-peer-reviewed) design references

- **[PS] etcd-io/raft README** (https://github.com/etcd-io/raft):
  - "the library only implements the Raft algorithm; both network and disk IO are left to the user"; "the library models Raft as a state machine."
  - The Ready contract: write Entries first, then HardState and Snapshot. "It is important that no messages be sent until the latest HardState has been persisted to disk, and all Entries written by any previous Ready batch (Messages may be sent while entries from the same batch are being persisted)." The leader-parallel write is attributed to "section 10.2.1 in Raft thesis".
  - Features listed include ReadIndex and lease reads ("this approach relies on the clock of the all the machines in raft group"), optimistic pipelining, flow control, batching of messages and entries, "Automatic stepping down when the leader loses quorum", and "Protection against unbounded log growth when quorum is lost".
- **[PS] CockroachDB `docs/design.md`**: "a single Node may have millions of consensus groups (one for each Range). Areas of optimization are chiefly coalesced heartbeats (so that the number of nodes dictates the number of heartbeats as opposed to the much larger number of ranges) and batch processing of requests."
- **[PS] ongardie/raft.tla**: TLA+ spec "slightly updated compared to the dissertation version" (repository last pushed 2025-02-18). To run TLC, see PR #4.
- **[PS] TiKV configuration docs**: `raftstore.hibernate-regions` idles long-idle Regions to cut heartbeat overhead. It was enabled by default in TiKV 5.0.2 (tikv/tikv#10266).

# Part B — Rust ecosystem ground truth (observed 2026-09-28)

Sources: crates.io API, GitHub API (read-only), raw repository files, docs.rs, and published crate tarballs from `static.crates.io` (plus branch tarballs where noted), all fetched on 2026-09-28. Panic-site counts come from a throwaway scanner that skips `#[cfg(test)]` modules, test directories and `//` comments, so treat them as **approximate**. File:line references are to the named version.

## B1. `raft` (tikv/raft-rs)

| Item | Value | Source |
|---|---|---|
| Latest crates.io | **0.7.0 — 2023-03-07** (prior: 0.6.0 2021-06-16; 0.6.0-alpha 2019-07-24; 0.5.0 2019-02-20) | crates.io API (re-checked) |
| `raft-proto` | 0.7.0 — 2023-03-07 | crates.io API |
| Default branch | `master`, last commit **2026-05-13** (ad13f3d9 "try to shrink unstable entries buffer…") | GitHub API |
| Unreleased work | master is **29 commits ahead of v0.7.0**; commits: 11 in 2024, 3 in 2025, 5 in 2026 (to 2026-05-13) | GitHub compare API |
| Open items | 50 issues, 30 PRs | GitHub search API |

- **TiKV uses git master, not crates.io.** tikv `Cargo.toml` declares `raft = { version = "0.7.0", default-features = false, features = ["protobuf-codec"] }` (lines 368–370) but overrides it in `[patch.crates-io]` with the comment `# TODO: remove this when new raft-rs is published.` (lines 205–208) → `git = "https://github.com/tikv/raft-rs", branch = "master"`; `Cargo.lock` pins `…raft-rs?branch=master#ad13f3d90780f53aea2488c6a4b76c0d334bf136`. TiKV also patches `protobuf` to `pingcap/rust-protobuf` branch `v2.8`.
- **No release in sight:** issue #576 "Release version" (2026-01-22) got no maintainer reply and was closed by its author 2026-02-13. A community republish `jopemachine-raft` 0.7.14 (2024-10-04) exists (used by raftify).
- **Protobuf:** in 0.7.0 both `raft` and `raft-proto` always depend on `protobuf = "2"`; default features `protobuf-codec` + `default-logger`; `prost-codec` uses prost 0.11. PR #579 (merged 2026-03-02, unreleased) makes protobuf optional under `prost-codec` (a maintainer called it breaking for prost users). **Issue #567 (open): protobuf 2.x is affected by RUSTSEC-2024-0437 / CVE-2025-53605 (stack overflow parsing untrusted input), patched only in ≥3.7.2; a maintainer (2025-10-14): "we don't have a plan to migrate to v3.x".** Protos are generated at build time via `protobuf-build`, which bundles `protoc` only for Linux x86/x86_64/aarch64/ppc64le, macOS x86_64 and Windows, and otherwise panics with "No suitable `protoc` (>= 3.1.0) found in PATH" (protobuf-build 0.14.1 `src/protobuf_impl.rs:27–35`) — inference: Apple Silicon hosts need a system `protoc`. `slog` is a required dependency.
- **Panics — by design.** `fatal!` is a `panic!` wrapper (`lib.rs:491–503`). Non-test `src/` counts (0.7.0 / master): `fatal!` 36/35, `panic!` 13/13, `.unwrap()` 62/61, `.expect(` 2/2, release `assert*!` 20/21, `debug_assert*!` 8/8. Invariant examples (0.7.0 → master line): `"to_commit {} is out of range [last_index {}]"` raft_log.rs:294→307; `"entry {} conflict with committed entry {}"` raft_log.rs:262→275; `"hs.commit {} is out of range"` raft.rs:2788→2835; `"need non-empty snapshot"` raft.rs:702→705.
- **Storage errors crash the process.** The `Storage` trait returns `Result`, and its doc says "If any Storage method returns an error, the raft instance will become inoperable and refuse to participate in elections", but the core panics on: `store.first_index().unwrap()` / `store.last_index().unwrap()` (0.7.0 raft_log.rs:80–81, 150, 162; master 92–93, 163, 175); `last_term` on any error; `term()` on errors other than Compacted/Unavailable (master:148); `slice()` on `StorageError::Unavailable`/`Other` (master:661–667, "entries[{}:{}] is unavailable from storage"); snapshot errors other than TemporarilyUnavailable (master raft.rs:701); errors while scanning unapplied entries (master raft.rs:1606). ⇒ an I/O error surfaced as `StorageError::Other` takes the process down.
- **Malformed peer input panics:** `m.entries[0]` (raft.rs:2125 in 0.7.0 / 2170 master) and `req.entries[0]` (read_only.rs:88/84) index without bounds checks; issue #595 (opened 2026-09-28, open): an empty `MsgReadIndex` panics RawNode with "index out of bounds".
- **Configuration-change gating vs Ongaro's 2015 bug (A1.7):** on election `become_leader` sets `pending_conf_index = last_index` (0.7.0 raft.rs:1232; master 1263) then appends an empty entry for the new term (1236/1267); a conf-change proposal is replaced by a no-op while `has_pending_conf()` (`pending_conf_index > applied`) (2742–2743 / 2789–2790; logged as "possible unapplied conf change"); `maybe_commit` only commits an entry whose term equals the current term (raft_log.rs:498–499 / 525–526). Membership takes effect **when applied** (`RawNode::apply_conf_change`, raw_node.rs:397/406); simple changes may alter only one voter, else "more than one voter changed without entering joint config" (changer.rs:152/153). *Inference (not confirmed with maintainers):* the gate is "everything in the leader's log at election time has been applied", which is stronger than but not literally Ongaro's rule; because configs activate on apply rather than append, Ongaro's append-time scenario does not map directly onto raft-rs.
- **Features/defaults:** PreVote, CheckQuorum, joint consensus, learners, async ready/async log fetch; reads via ReadIndex (`ReadOnlyOption::Safe`, default) or `LeaseBased` ("It can be affected by clock drift"). **`check_quorum` and `pre_vote` both default to `false`** (config.rs:120–121).
- **Tests:** ~250 `#[test]` functions incl. ports of etcd's tests (`harness/tests/…/test_raft_paper.rs`) and data-driven conf-change tests; source headers mark the core as a port of etcd/raft.
- **Multi-Raft:** no threads or timers; the application calls `tick()`. TiKV implements region hibernation itself (tikv `components/raftstore/src/store/config.rs`: `hibernate_regions: true` line 617; base tick 1 s, heartbeat every 2 ticks, election after 10 ticks, lines 539–541; `raft_store_max_leader_lease` 9 s line 602). TiKV docs: hibernation reduces heartbeat overhead for idle Regions and was enabled by default in TiKV 5.0.2 (PR tikv/tikv#10266, https://github.com/tikv/tikv/pull/10266; https://docs.pingcap.com/tidb/stable/tikv-configuration-file/) [PS].
- Related issues: #571 (progress corruption when a node rejoins with the same ID — maintainer: reusing an ID is misuse; cf. Diss §3.8 "new identity"); #570 (learners catch up correctly only if the cluster starts from a non-zero initial snapshot).
- **Maintenance assessment:** low-cadence maintenance mode — no release for 3.5 years, the main downstream consumes git master, an open security advisory on its mandatory protobuf 2 dependency with no migration plan.

## B2. `openraft` (databendlabs/openraft)

| Item | Value | Source |
|---|---|---|
| Latest overall | **0.10.0-alpha.35 — 2026-09-24** (alpha line on crates.io: alpha.14 2026-02-28 … alpha.35, ~22 releases) | crates.io API (re-checked) |
| Latest stable | **0.9.25 — 2026-07-28** (0.9.24 2026-04-21, 0.9.23 04-20, 0.9.22 04-08, 0.9.21 2025-07-12; 0.9.0 2024-03-11) | crates.io API |
| Activity | last push 2026-09-28; 519 commits in 2026, 390 in 2025; 18 open issues, 4 open PRs | GitHub API |

- **API stability (README):** "**OpenRaft API is not stable yet**. Before `1.0.0`, an upgrade may contain incompatible changes"; 0.10 "is in **alpha** — the API may still change"; release-0.9 "**Won't** accept new features but only bug fixes." Commit prefixes `data-change:` / `change:` flag on-disk / breaking changes.
- **Notable correctness bugs (GitHub issues/PRs, release notes):**

| Issue | Problem | Status |
|---|---|---|
| #1802 (2026-06-25) | leader could commit with fewer than a quorum (`VecProgress` ordering invariant broken; regression test committed with 2 of 5 voters) | fixed; backported to **0.9.25** |
| #1808 (2026-06-27) | snapshot install kept a stale effective membership → disjoint quorums, lost committed entries | fixed in 0.9.25 |
| #1601 | replication `unwrap()` on empty read guarded only by `debug_assert!` | fixed in 0.9.25 |
| #2118 | `become_leader` marked unflushed log I/O as flushed | fixed 91298ba3 (2026-09-24, in alpha.35); 0.9.x exposure **UNVERIFIED** |
| #1960 | out-of-order I/O completion stalled the leader | fixed 2026-08-06 |
| #1872 | comparing two `Vote`s panicked | fixed |
| #2095 | retry after `ForwardToLeader` during truncate/purge could double-write | fixed |
| #2080, #2091 | liveness deadlocks | fixed |
| #1722 | vote up-to-date ordering in advanced LeaderId mode | closed "not planned" (maintainer: unreachable) |
| #2116 (2026-09-21) | RaftCore panics on snapshot-purge/apply race via `get_log_id(...).unwrap()` (core/raft_core.rs:2462, still in alpha.35); reporter says quorum can be lost permanently | **open** |
| #2117 (2026-09-21) | vote rejection returns before adopting the higher term | **open** (maintainer: intentional) |

  The 0.9.25 release notes say #1802 and #1808 "can both corrupt cluster state in ways that do not heal on their own." openraft began as a fork of async-raft; `derived-from-async-raft.md` lists ~23 fixes to inherited code (e.g., "deleting entries after prev-log-id causes committed entry to be lost", "client_read has using wrong quorum=majority-1").
- **Antithesis claim [VB]:** a GitHub search of openraft issues/PRs for "antithesis" returns 0 results — the blog's OpenRaft claim could not be linked to a tracked issue (UNVERIFIED).
- **Model checking:** #1598 "Add Stateright.rs model checking tests" closed 2026-09-24 as completed; its last comment (2026-07-15) argued it was infeasible because Stateright needs a deterministic, cloneable state machine while openraft's safety depends on its async runtime (I/O stages, crash windows, spawned tasks), recommending deterministic simulation + Engine tests instead. #1597 (Jepsen) closed as completed 2026-09-22.
- **Ongaro-fix barrier:** single-step `append_membership()` (added 2026-08-28, d4a8088c) calls `ensure_leader_log_committed` requiring `cluster_committed >= noop_log_id` (alpha.35 core/raft_core.rs:526, 556–590), with the comment "Without it, two configurations proposed in different terms from the same committed parent can both become committed, even when their quorums do not intersect." Joint `change_membership()` "deliberately does not take this barrier" (argues joint quorums intersect) but waits for the previous membership to commit (~447–458).
- **Errors and panics:** storage methods return `Result<_, io::Error>`; a storage error becomes `Fatal::StorageError` and the node stops; a panic in the core task becomes `Fatal::Panicked` (errors/fatal.rs:4–31; raft/raft_inner.rs ~275–282) — inference: with `panic = "abort"` the process aborts instead. Non-test counts (0.9.25 / alpha.35): `unreachable!` 14/14, `panic!` 2/1, `.unwrap()` 61/78, `.expect(` 5/10, release `assert*!` 3/~5, `debug_assert*!` 65/60. Some release asserts fire on corrupted data (e.g., following_handler/mod.rs:263–285, "the snapshot or the local store is corrupted"). FAQ: losing disk data makes behaviour "**undefined**"; wiping one node "will panic the leader" unless `allow_log_reversion` is set; a misconfigured network address panics.
- **Runtime model / per-group cost:** `AsyncRuntime` trait (tokio default; compio, monoio, `single-threaded` mode). Each Raft instance spawns RaftCore, a Tick task, a state-machine worker, one ReplicationCore **per follower**, and one HeartbeatWorker **per follower** with its own network client (core/heartbeat/handle.rs ~97–117). Defaults: heartbeat 50 ms, election timeout 150–300 ms (config/config.rs:62–64). The internal `Engine` is sans-IO-style but crate-private (`pub(crate)`, engine/engine_impl.rs:71).
- **Multi-Raft:** initial support 2025-11-30 (9b6b5293); `openraft-multi` crate on crates.io since 2026-02-28 (latest 0.10.0-alpha.35), README: "adapters for connection sharing across Raft groups"; `examples/multi-raft-kv` runs 3 groups each with "Separate log storage… leader election." **No heartbeat coalescing and no hibernation/quiescence found.**
- **Reads/leases/elections:** `ensure_linearizable()` returns a `ReadLogId` (alpha.33+), ReadIndex reusing the leader's initial no-op log id; `ReadPolicy::LeaseRead` "assumes minimal clock drift between nodes" (docs/protocol/read.md:26–28); leader stickiness ("WILL NOT handle a VoteRequest before the leader_lease expires"); CheckQuorum implemented as proposal admission (`now < last_quorum_acked + leader_lease`); PreVote added 2026-06-12, disabled by default. `append()` must invoke its callback only "when the entries are persisted on disk" (storage/v2/raft_log_storage.rs:109–127).
- **Production users named in README:** Databend (meta-service), Walrus, CnosDB, RobustMQ, RocketMQ-rust, Hiqlite, Octopii, Renegade, Helyim, Ahnlich, tsoracle, KalamDB, and others.
- **Testing:** turmoil-based deterministic fuzzer (added 2026-04-12) checking paper/TLA+ invariants after every tick, plus client-side checker and liveness phase (needs `--cfg tokio_unstable`); Jepsen suite (added 2026-07-17) running 8 fault scenarios on every push to main; README claims 92% unit-test coverage. Both harnesses are 2026 additions and the safety bugs above were reported after the fuzzer landed; how they were found is UNVERIFIED.

## B3. Other Rust Raft crates (brief)

| Crate | Latest / date | Status |
|---|---|---|
| async-raft | 0.6.1 / 2021-05-19 | not archived; last push 2023-02-12; openraft is its fork |
| raftify | 0.1.82 / 2024-10-15 | self-described "experimental"; built on `jopemachine-raft` |
| riteraft | 0.1.0 / 2023-07-26 | built on `raft ^0.7` |
| little_raft | 0.2.0 / 2022-01-08 | repo pushed 2025-03-31 |
| **noraft** (formerly `raftbare`, by sile) | **0.7.1 / 2026-08-15** (versions 0.4.0 2026-02-15 … 0.7.1; `raftbare` 0.2.2 2026-01-11) | "Minimal, feature-complete no_std Raft for Rust - no I/O, no dependencies"; `Node` emits `Action`s (SetElectionTimeout, SaveCurrentTerm, SaveVotedFor, BroadcastMessage, AppendLogEntries, SendMessage, InstallSnapshot); `handle_message` returns `Result`; ~4.4k downloads; single author; no production evidence found (README and crates.io checked) |
| rafter | 0.0.2-alpha.1 / 2026-08-19 | sans-IO; 271 downloads |
| raft-log (drmingdrmer) | 0.4.6 / 2026-09-01 | Raft log storage crate (by the openraft author) — relevant to C.c |

## B4. Embedded ordered-KV engines (state-machine storage candidates)

Observed 2026-09-28 from crates.io, GitHub, docs.rs and published tarballs. File:line references point into the named crate version. Key items were checked a second time against source: the RocksDB version and options, raft-engine `sync()` and its panic, redb `Durability` and `StorageBackend`, and the status of the issues cited.

### B4.0 Comparison

| Engine | Latest (crates.io) | Published | Pure Rust? | On-disk format stability | Durability / commit model | Crash / fault testing | Panic-free on I/O error / corruption? | Maturity signals |
|---|---|---|---|---|---|---|---|---|
| **rocksdb** (rust-rocksdb) | 0.25.0 (librocksdb-sys 0.19.0+11.8.1 → **RocksDB 11.8.1**, `version.h:14–16`) | 2026-08-16 | **No.** C++ built with `cc`; bindgen needs libclang; C++20 | Upstream checks forward/backward format compatibility across releases (`tools/check_format_compatible.sh`) | WAL + memtable + SST. Per-write `sync` defaults to false. Leader-thread write batching. `paranoid_checks=true`: "True also enters a read-only mode when a DB write fails; see DB::Resume()" | Upstream: `db_stress`/`db_crashtest.py` (blackbox/whitebox), fault-injection FS, fuzzers. The Rust wrapper has only unit tests in CI | **Yes on data paths**: DB calls return `Result`; iterator `Item = Result<…>`. Residual panics: FFI null returns, lock poisoning, NUL in CF names. `verify_checksums = true` by default | Very mature upstream (self-reported USERS.md: Facebook MyRocks/ZippyDB/LogDevice, TiKV, CockroachDB, …). Wrapper since 2014, 55.7 M downloads, several maintainers, 33 commits in 2026 |
| **fjall** 3 / **lsm-tree** 3 | 3.1.10 / 3.1.10 | 2026-08-30 | Yes | "Future breaking changes will result in a major version bump and a migration path." Format broke at 2.0 (2024-09-20) and 3.0 (2026-01-02, separate migrator) | One journal for all keyspaces. **Default writes reach OS buffers only.** `PersistMode::{Buffer,SyncData,SyncAll}`. Failed fsync ⇒ `Error::Poisoned` (docs cite Rebello et al.) | Recovery unit tests and lsm-tree decode fuzzers (not run in CI). **No fault-injection or power-loss harness.** FS abstraction still requested (lsm-tree #306, open) | **No.** Mid-journal corruption is silently truncated (fjall #311, open). Panics decoding persisted config/metadata | One maintainer (~97% of commits). **All releases 3.0.0–3.1.5 yanked.** 2026 durability bugs #304, #308 fixed. Open snapshot bug lsm-tree #321 |
| **redb** | 4.3.0 | 2026-09-15 | Yes | "The file format is stable, and a reasonable effort will be made to provide an upgrade path." Broke at 2.0 (2024-03-22) and 3.0 (2025-08-09, with upgrade path) | COW B+trees, **single writer**, MVCC readers. `Durability::{None, Immediate}` only (Eventual/Paranoid removed by 3.0). Default 1-phase commit: two commit slots, "god byte" flip, one fsync, XXH3-128 Merkle checksums. Optional 2PC and quick-repair | cargo-fuzz with injected I/O errors + reopen (60 s/CI run; the fuzzer's `sync_data` is a no-op, so no power-loss reordering). Crash-image tests. Multiprocess power-loss simulation | **Mostly.** Typed `StorageError::{Io, Corrupted, PreviousIo, …}`. Only documented panic: empty table name. Internal invariant panics remain (9 `panic!`, 59 `unreachable!`). Checksums appear to be verified only on repair/`check_integrity` paths (code reading; UNVERIFIED) | One maintainer. 12.0 M downloads, 383 commits in 2026. **2026 crash-recovery fixes**: 4.2.0 "crash during recovery … could silently roll back or corrupt durably committed transactions"; 4.3.0 a post-repair crash could silently roll back a commit |
| **raft-engine** (tikv) | 0.4.2 | 2024-04-26 | **No** (`lz4-sys`; rust-protobuf 2) | `format_version` 2 | Shared append-only log for all groups; per-group memtable index; leader-batched `fdatasync`; LZ4 for batches over 8 KB | fail-rs failpoints, stress tool | **No** — see B5 | TiKV default since v6.1.0, but TiKV builds from git master |
| surrealkv | 0.21.4 | 2026-08-26 | Yes | 0.x, no statement | LSM + WAL + vlog; **default `Eventual` (no fsync)** | power-loss simulation tests | No (panics on a bad filter block) | SurrealDB docs: "SurrealKV (beta)" |
| canopydb | 0.2.5 | 2025-11-22 | Yes | "new releases could be incompatible" | COW B+tree, optional WAL, optimistic multi-writer, group commit | 16 failpoints, shuttle, 3 fuzz targets | panics on invariants | README: "Do not trust it with production data." |
| sled | 0.34.7 / 1.0.0-alpha.124 | 2021-09-12 / 2024-10-11 | Yes | format "is going to change … before the 1.0.0 release" | fsync every 500 ms by default | — | 1.0-alpha documents panics on I/O in export/import | README: "sled is beta" |
| persy | 1.8.1 | 2026-06-30 | Yes | none | COW + in-file WAL | — | No (panics on corrupted root/index) | one maintainer |
| nebari | 0.5.5 | 2023-02-27 | Yes | "format is considered stable" | append-only B-tree | — | — | "alpha … bugs that could lead to data loss"; inactive since 2023-10-11 |
| sanakirja | 1.4.3 (2.0.0-beta.3 2026-07-06) | 2024-10-13 | Yes | crate says "stable format" | COW, writers exclusive | — | — | one maintainer |
| agatedb (tikv) | 0.1.0 | 2020-10-09 | Yes | — | Badger port | — | — | "experimental"; last commit 2024-04-16 |
| heed (LMDB) / libmdbx | 0.22.1 / 0.9.0 | 2026-04-07 / 2026-09-24 | **No** (C) | LMDB's | LMDB's | LMDB's | — | Meilisearch-maintained |
| slatedb | 0.16.0 | 2026-08-31 | Yes | adjacent-version format compatibility; no API stability | LSM on object storage | — | panics only on size limits (documented) | 0.x |

**Pluggable storage, which decides whether an engine can run inside mantle's deterministic simulator (A5):**
- **redb** has a public `StorageBackend` trait (`db.rs:74`: `len`, `read`, `set_len`, `sync_data`, `write`, `close`, byte-range locks), `Builder::create_with_backend`, and an `InMemoryBackend`. Its library code spawns no background threads; the thread spawns in the crate are in tests.
- **raft-engine** has a `FileSystem` trait (`env/mod.rs:21`) and `Engine::open_with_file_system`.
- **rust-rocksdb** exposes only the default `Env`, `Env::mem_env()` (in memory, no fault control), and an unsafe raw-pointer constructor (`env.rs`). Its own comment says "the C API behinds C++ API", and RocksDB runs background flush and compaction threads.
- **fjall / lsm-tree** have no filesystem abstraction (lsm-tree #306 is open) and spawn worker threads (`fjall worker_pool.rs`).

### B4.1 rocksdb (rust-rocksdb/rust-rocksdb): build requirements and constraints

- **Versions:** rocksdb 0.25.0 (2026-08-16; before it 0.24.0 2025-08-10, 0.23.0 2024-12-24; MSRV 1.88). librocksdb-sys 0.19.0+11.8.1. The README says its version is "`X.Y.Z+RX.RY.RZ` … `RX.RY.RZ` is the version of the bundled rocksdb". Ignore crates.io's `max_version` 6.20.3 for librocksdb-sys: it is a 2021 release under the old numbering.
- **Build:**
  - README: "Requirements: Clang and LLVM". `build.rs` always runs bindgen, so libclang is needed. The default `bindgen-runtime` feature loads libclang dynamically; `bindgen-static` is intended for musl/Alpine.
  - C++ is compiled by the `cc` crate: 343 RocksDB sources plus snappy, `-std=c++20` (override with `ROCKSDB_CXX_STD`). **CMake is not required.**
  - Linux links `stdc++`. The `io-uring` feature needs `liburing` via pkg-config and fails the build without it.
  - macOS links `c++`. jemalloc is ignored on darwin, musl, android and dragonfly.
  - Windows: MSVC or `x86_64-pc-windows-gnu`; `mt_static` selects the static runtime. CI installs LLVM with `choco` and deletes `C:\msys64` "to resolve link error with …libclang.dll".
  - FreeBSD always uses the system librocksdb.
  - To use a system library: `ROCKSDB_LIB_DIR`, `ROCKSDB_STATIC`, `ROCKSDB_INCLUDE_DIR`. Per-library `*_LIB_DIR` works the same way; `<LIB>_COMPILE=1` forces the bundled copy.
  - `lto` needs clang with an LLVM version that matches rustc.
  - Without SSE4.2 (x86_64) or CRC (aarch64), checksum computation is slow.
- **Cross-compilation:** bindgen has to parse the *target's* headers through libclang. Issues still open on 2026-09-28: #1016 (Linux→Windows), #550 (arm64 on amd64), #635/#174/#440 (musl), #596/#752 (libclang on Alpine), #664/#866/#928 (Windows builds). Build time and binary size are not documented (UNVERIFIED).
- **Options relevant to a Raft state machine** (bundled `options.h`, re-verified):
  - `WriteOptions::disableWAL`: "writes will not first go to the write ahead log, and the write may get lost after a crash".
  - `DBOptions::atomic_flush`: "useful when there are column families with writes NOT protected by WAL".
  - `ReadOptions::verify_checksums = true`.
  - `paranoid_checks`: read-only mode after a failed write.
  - The wrapper exposes `WriteBatch::delete_range(_cf)`, `DB::delete_range_cf`, `ingest_external_file(_cf)(_opts)`, `Checkpoint::create_checkpoint`, and `set_env`.
- **Durability defaults:** a write without `sync` "has similar crash semantics as the 'write()' system call". A leader thread batches concurrent writes (`db/write_thread.h`).
- **Residual wrapper panic sites:** `prop_name.rs:22`; NUL in a CF name at `db.rs:672`; RwLock unwraps at `db.rs:132, 2651, 2663`; asserts on null C constructors at `db_options.rs:517, 1054, 3897` and `backup.rs:236`; `unimplemented!` for unsupported WAL compression at `db_options.rs:1474`. There are no `# Panics` doc sections.
- **tikv/rust-rocksdb** (TiKV's fork) is active (last commit 2026-09-22) and builds TiKV's `8.10.tikv` RocksDB branch plus Titan with cmake. Its README is stale, it is not on crates.io under its own name, and TiKV consumes it from git.

### B4.2 fjall / lsm-tree: details

- **Versions and yanks:** fjall and lsm-tree 3.1.10 (2026-08-30; MSRV 1.90). Every 3.x release from 3.0.0 (2026-01-02) to 3.1.5 (2026-06-08) is yanked; the maintainer gives no reason (UNVERIFIED). The 2.x line ended at 2.11.2 (2025-07-21).
- **Durability:** README: "By default, any operation will flush to OS buffers, but **not** to disk. This matches RocksDB's default durability." `PersistMode` is per batch or transaction. There is one journal with "cross-keyspace atomic semantics". `OptimisticTxDatabase` and `SingleWriterTxDatabase` provide transactions. A database may not be opened by several processes.
- **Error model:** `Error::Poisoned` means "Future writes will not be accepted … At this point, it's best to let the application crash and try to recover." The docs cite Rebello et al., ATC '20.
- **Journal recovery:**
  - A decode error or EOF truncates to the last valid position.
  - A malformed batch framing is truncated.
  - A complete batch with a bad checksum returns `ChecksumMismatch`.
  - Issue **#311 (open, 2026-08-06)**, opened by someone adding Raft replication: "There is currently no way to detect a corrupted, mid-journal batch that fails to parse. This could lead to nasty cases of silent data loss." This conflicts with PAR (A3).
- **2026 bug fixes:**
  - #304 "`SyncAll` can falsely imply durability" (journal write errors were discarded), fixed in 3.1.7.
  - #308 `WriteBatch::commit()` returned Ok for a batch that did not survive a restart, fixed in 3.1.9.
  - #287 concurrent `clear()` and ingestion caused corruption, fixed in 3.1.10.
  - 3.0.1 "second memtable being dropped before being flushed".
  - 3.1.1 "Ingested data being invisible after recovery".
  - lsm-tree 3.1.3 "corrupted keys could pass the [blob] check".
- **Still open:** lsm-tree **#321** (MVCC GC "drops the version the oldest snapshot must read"); #317 (`max_write_buffer_size` does nothing); #306 (filesystem abstraction).
- **Panics on persisted data:** fjall `keyspace/config/filter.rs:78,85`, `keyspace/options.rs:331,486`; lsm-tree `table/meta.rs:59,70,95,98–102`. About 32 `expect("lock is poisoned")` per crate. lsm-tree has 7 `todo!`/`unimplemented!` in library code. The documented `# Panics` sections (9 in fjall, 16 in lsm-tree) cover only argument limits.
- **Maintenance:** one maintainer (marvin-j97) with about 1,933 and 2,570 commits; the next contributor has 32. 2026 commits: fjall 136, lsm-tree 206.

### B4.3 redb: details

- **Versions:** 4.3.0 (2026-09-15), 4.2.0 (2026-08-17), 4.1.0 (2026-04-19), 4.0.0 and 3.1.3 (2026-04-02), 1.0.0 (2023-06-16). MSRV 1.90.
- **Format history:**
  - 1.0.0 "The file format is now gauranteed to be backward compatible" (sic).
  - 2.0.0 is "not backwards compatible with 1.x".
  - 2.6.0 added the v3 format and `Database::upgrade()`.
  - 3.0.0 "Removes support for file format v2".
  - 4.0.0 removed the `Legacy` tuple wrapper.
- **Durability** (re-verified in `transactions.rs`): `#[non_exhaustive] pub enum Durability { None, Immediate }`. `None` commits are persisted only by a later `Immediate` commit. After a crash the database rolls back to the last durable commit.
- **Commit strategy** (`docs/design.md`):
  - XXH3_128 Merkle checksums; two commit slots; the default is one-phase (write, flip the god byte, single fsync).
  - `set_two_phase_commit(true)` does fsync, flip, fsync.
  - `set_quick_repair(true)` persists allocator state on every commit (and forces 2PC) for near-instant recovery; otherwise recovery walks every tree.
  - Assumptions: atomic single-byte writes, fsync durability, "powersafe overwrite".
- **Concurrency:** "Only a single write may be in progress at a time … this function will block". Multi-process readers since 3.0. Multi-process writers are experimental.
- **Range operations:** `Table::retain_in`, `extract_from_if` (O(n) over the range), `delete_table`, `rename_table`, `persistent_savepoint`/`ephemeral_savepoint`, and `Database::compact()`. Space is reclaimed only by an explicit `compact()`.
- **Errors:** `StorageError::{Corrupted, ValueTooLarge, Io, Unsupported, PreviousIo, DatabaseClosed, LockPoisoned}`. After an I/O error, later calls return `PreviousIo`. After a failed commit, writes are refused until reopen (4.2.0).
- **Remaining panic sites:** 9 `panic!` (e.g. `page_manager.rs:346–355`, `base.rs:308–345`), 59 `unreachable!`, about 582 `.unwrap()` (158 of them `lock().unwrap()`, about 120 numeric or slice conversions). `lib.rs:7` disables the lint that requires documenting panics.
- **Performance** (**vendor benchmark** from redb's README; Ryzen 9950X3D, Samsung 9100 PRO; ms, lower is better):

| Workload | redb | lmdb | rocksdb | fjall |
|---|---|---|---|---|
| individual writes | **920** | 1598 | 2432 | 3488 |
| batch writes | 1595 | 942 | 451 | **353** |
| random reads, 1 thread | 1138 | **637** | 2911 | 2177 |
| random reads, 32 threads | 410 | **125** | 1100 | 576 |
| removals | 23297 | 10435 | 6900 | **6004** |

  On-disk size before compaction: redb 4.00 GiB, rocksdb 893 MiB, fjall 1001 MiB. redb after compaction: 1.69 GiB.
- **2026 fixes to note:**
  - 4.3.0 "a crash shortly after a commit being able to silently roll that commit back during recovery, if `check_integrity()` had previously repaired the database"; iterators could skip data after returning `Err(Corrupted)`.
  - 4.2.0 a crash during recovery "could silently roll back or corrupt durably committed transactions" (not with 2PC); cyclic pages now return `Corrupted` instead of aborting.
  - 4.1.0 "a large number of bug fixes discovered by AI coding agents".
  - 4.0.0 / 3.1.3 data loss when a `get_mut()` guard was dropped after commit.
- **Maintenance:** sole owner cberner (1,615 commits; next contributor 47); 3 open issues; 4,808 stars. crates.io dependents (a proxy for use, not a production claim) include iroh-blobs, iroh-docs, xet-client and native_db.

### B4.4 Others (brief)

- **surrealkv:** README says it is "designed specifically for use within SurrealDB, with the goal of reducing dependency on external storage engine (RocksDB)"; SurrealDB docs call it beta. Its default durability does not fsync. It has power-loss regression tests (e.g. surrealdb#7426: an unsynced compaction output left the store unopenable).
- **canopydb:** good testing for its size, but the README says "Do not trust it with production data."
- **sled:** stable 0.34.7 is from 2021; README says "if reliability is your primary constraint, use SQLite. sled is beta".
- **persy** and **nebari:** panic on corruption, or are inactive with "alpha … data loss" warnings.
- **sanakirja:** a single-maintainer copy-on-write B-tree (the Pijul project).
- **agatedb:** only a placeholder crate.
- **heed** and **libmdbx:** C libraries.
- **slatedb:** object-store LSM (0.x).
- Not evaluated: tonbo, rusty-leveldb, jammdb.

### B4.5 Panic-site counts (library code only; heuristic text scan)

| Crate | panic! | unreachable! | todo!/unimpl! | .unwrap() | .expect( | assert*! |
|---|---|---|---|---|---|---|
| rocksdb 0.25.0 | 1 | 0 | 1 | 43 | 3 | 44 (includes some test fns) |
| fjall 3.1.10 | 4 | 4 | 0 | 3 | 91 | 10 |
| lsm-tree 3.1.10 | 7 | 11 | 7 | 1 | 137 | 59 |
| redb 4.3.0 | 9 | 59 | 0 | 582 | 54 | 195 |
| raft-engine 0.4.2 | 4 | 6 | 0 | 138 | 1 | 23 |
| surrealkv 0.21.4 | 15 | 0 | 0 | 81 | 11 | 25 |
| canopydb 0.2.5 | 11 | 22 | 2 | 116 | 8 | 41 |
| persy 1.8.1 | 38 | 12 | 0 | 71 | 143 | 8 |
| sled 1.0.0-α.124 | 3 | 4 | 2 | 116 | 10 | 106 |
| nebari 0.5.5 | 0 | 31 | 1 | 79 | 4 | 13 |

## B5. tikv/raft-engine (shared multi-group Raft log store)

- **Release vs usage:** crates.io **0.4.2 (2024-04-26)**; master's Cargo.toml still says 0.4.2. TiKV master uses `raft-engine = { git = "https://github.com/tikv/raft-engine.git", features = ["swap"] }` (Cargo.toml:371–373).
- **Default in TiKV:** the config template (`etc/config-template.toml:1159–1162`) says "configurations of `raftdb` are ignored" when enabled. TiDB v6.1.0 release notes [PS, vendor claim]: "Since v6.1.0, TiDB uses Raft Engine as the default storage engine for logs … can reduce TiKV I/O write traffic by up to 40% and CPU usage by 10% … reducing tail latency by 20% under certain loads."
- **Design (README):** "each Raft Group holds its own memtable, containing all the key value pairs and the file locations of all log entries … Different Raft Groups share the same log stream."
  - Writers form a queue. "The first in line automatically becomes the queue leader, responsible for writing the entire group", and "the batch leader will call `fdatasync()`".
  - GC is "collaborative": the caller runs `purge_expired_files()` (TiKV does so every 10 s), and the engine reports which groups block cleanup and rewrites lagging groups' entries.
  - Defaults: 128 MB files, 10 GB purge threshold, LZ4 for batches over 8 KB, format v2, log recycling.
  - A pluggable `FileSystem` trait exists.
- **Recovery modes** (`config.rs:14–23`; behaviour in `pipe_builder.rs:405–490`). I/O errors always propagate.
  - `AbsoluteConsistency` errors on any corruption.
  - `TolerateTailCorruption` (the **default**) truncates corruption *only in the last file* and treats any corruption in that file as a torn tail. The code comment reads "TODO: More reliable tail detection." Contrast PAR §3.3.3, which separates crash from corruption.
  - `TolerateAnyCorruption` truncates whichever file is corrupt.
  - Each log batch carries a crc32.
- **Panics (re-verified):** the write-group leader calls `self.pipe_log.sync(LogQueue::Append).expect("pipe::sync()")`. The comment says: "As per trait protocol, sync error should be retriable. But we panic anyway to save the trouble of propagating it to other group members" (0.4.2 `engine.rs:175–177`; master 188–190). Rebello et al. (ATC '20) show that retrying fsync is *not* safe on Linux file systems. There are further panics at `log_file.rs:114` (a seek-back after a failed write) and `memtable.rs:600,610` (a gap in the index, documented).
- **Durability bug in the published crate (re-verified):**
  - In 0.4.2, `Engine::sync()` writes an empty `LogBatch` (`engine.rs:231–235`), and `write()` returns `Ok(0)` immediately for an empty batch (`engine.rs:140–143`). `sync()` therefore never fsyncs.
  - PR #400 "engine: have sync() actually fsync the active log (#395)" was merged 2026-05-07 and is **not released** on crates.io.
  - Non-empty writes with `sync=true` did fsync.
- **Coupling:** log entries must be rust-protobuf 2 `Message`s (`log_batch.rs:39–43`), which carries the protobuf-2 advisory in B1. It also depends on `lz4-sys` (C), `nix` and `fs2`.
- **Tests:** `tests/failpoints/{test_engine,test_io_error}.rs` using fail-rs.
- **Maintenance:** owners tabokie and Connor1996; 9 commits in 2026 (latest 2026-09-10); 639 stars.
- **Assessment for mantle:** it is the best available *design reference* for C.c, from a production system with a documented default-on rollout. As a *dependency* it conflicts with the no-panic rule (it panics on fsync failure), with PAR (its default tail-corruption heuristic), and with supply-chain hygiene (the crates.io release is stale with a no-op `sync()`, and it requires protobuf 2). Other options: `raft-log` 0.4.6 (2026-09-01, by the openraft author; not evaluated, UNVERIFIED quality).

## B6. Rust simulation / concurrency / linearizability testing tools

| Tool | Latest / date | What it gives mantle | Key limitation (verified) |
|---|---|---|---|
| **madsim** | 0.2.34 / 2025-10-11 (madsim-tokio 0.2.30 / 2024-10-04; repo push 2026-02-16) | deterministic simulator used by RisingWave; `check_determinism`; "buggify" (25% default) | requires `RUSTFLAGS="--cfg madsim"` + `[patch.crates-io]` for quanta, getrandom, tokio-retry, tokio-postgres, tokio-stream; overrides libc (getrandom, getentropy, CCRandomGenerateBytes, gettimeofday, clock_gettime, sched_getaffinity, sysconf, pthread_attr_init, gethostname) via `#[no_mangle]`; **does not simulate crash durability** — `power_fail` is a `// TODO` no-op and `sync_all` is a no-op (src/sim/fs.rs:50–53, 219–222) |
| **turmoil** (tokio-rs) | 0.7.2 / 2026-04-24 | many simulated hosts on one thread over a simulated network | `unstable-fs` models pending vs durable writes (crash drops unsynced data; `sync_probability` knob) (src/fs/mod.rs:19–26; sim.rs:156–163); seeding tokio's RNG needs `--cfg tokio_unstable`; no libc overrides (must use shim types); main branch splitting into turmoil-net (0.1.0 on crates.io 2026-05-04), turmoil-fs, turmoil-io-uring (latter two **not** on crates.io); torn-write injection claimed only in main-branch README (UNVERIFIED for 0.7.2) |
| **shuttle** (awslabs) | 0.9.4 / 2026-09-22 | randomized schedulers incl. PCT for concurrency bugs | README: "Shuttle is not sound (a passing Shuttle test does not prove the code is correct), but it scales to much larger test cases than Loom." |
| **loom** (tokio-rs) | 0.7.2 / 2024-04-23 (repo push 2026-02-20; 129 open issues) | exhaustive interleavings under the C11 memory model, bounded by `LOOM_MAX_PREEMPTIONS` | for small concurrent primitives only |
| **stateright** | 0.31.0 / 2025-07-27 | embedded model checker + actor runtime + `LinearizabilityTester` | tester is recursive backtracking **without memoization** (src/semantics/linearizability.rs:176ff) — small histories only |
| porcupine-rs | 0.3.0 / 2026-05-10 | WGL with cache + per-key partitioning | 5 stars, 234 downloads — immature, unevaluated |
| todc-utils | 0.1.1 / 2023-09-28 | `WGLChecker` with memoization | unevaluated |
| lincheck | 0.2.1 / 2023-08-03 | loom + proptest for in-memory data structures | not for distributed histories |
| spectroscope | 0.1.0 / 2026-02-02 | port of Jepsen's set-full checker | unevaluated |

**Takeaway:** no off-the-shelf Rust tool provides FoundationDB-grade *crash-durability* simulation out of the box (madsim's fs power-fail is unimplemented; turmoil's durable-vs-pending fs model is behind an unstable feature). The linearizability checker is small enough (A6.8) that mantle should own it (porting Porcupine's design) rather than depend on an immature crate.

# Part C — Recommendations for mantle

Each recommendation lists the evidence it rests on. Where a point is a design inference rather than a finding in a source, it says so.

## C.a Consensus: build a mantle-owned sans-IO Raft core with etcd/raft semantics; do not take raft-rs or openraft as runtime dependencies

### Decision matrix (evidence from Parts A and B)

| Criterion | raft-rs (0.7.0 / master) | openraft (0.9.25 / 0.10.0-alpha.35) | mantle-owned sans-IO core |
|---|---|---|---|
| **Correctness evidence** | Strongest lineage. A port of etcd/raft. The etcd README says: "As of 2016, it is **the most widely used** Raft library in production … It powers … CockroachDB, TiDB …" [PS]. Ports of etcd's tests (~250 `#[test]`). TiKV runs it in production, but from **git master**. Caveat: etcd-raft still yielded previously unknown bugs under model-guided fuzzing (Gulcan et al., OOPSLA 2025: 13 bugs across etcd-raft and RedisRaft). | Improving but recent. A turmoil DST and a Jepsen suite were added in 2026. Serious 2026 safety bugs: #1802 committed with fewer than a quorum; #1808 kept a stale membership after snapshot install and lost committed entries. Both were fixed only in 0.9.25 (2026-07-28), and the release notes say they can "corrupt cluster state in ways that do not heal on their own". Panic #2116 is still open. A vendor blog claims further bugs [VB, unverified]. | None at first; it has to be earned (see the correctness programme below). It can inherit etcd/raft semantics and test vectors, and use raft-rs as a differential oracle. |
| **No-panic requirement** | **Fails by design.** `fatal!` is `panic!` (36 sites), plus 13 `panic!` and about 62 `.unwrap()`. Storage errors (`first_index().unwrap()`, `StorageError::Other`) panic the process. A malformed peer message (empty `MsgReadIndex`, #595) panics. | **Partial.** A storage error becomes `Fatal::StorageError` and the node stops. The core still has 14 `unreachable!`, 61–78 `.unwrap()`, and release asserts on corrupted data. The FAQ says a wiped node "will panic the leader" unless `allow_log_reversion` is set. | **Meets it by construction.** Typed errors throughout; an invariant violation fences one group, not the process. |
| **Multi-Raft at 10⁴–10⁵ groups per node** | Good substrate: no threads and no timers, the application drives `tick()` and batches. The library has no heartbeat coalescing; TiKV adds hibernation in its own raftstore. | Weak today. Each group spawns RaftCore, a tick task, an SM worker, and one replication task plus one heartbeat worker **per follower**; default heartbeat is 50 ms. No coalescing or quiescence was found. "Multi-raft" means connection sharing only. | Built in: node-level tick, heartbeats coalesced per node pair (CockroachDB §7.1.1), quiescence of idle groups (CockroachDB §7.1.1, TiDB §4.1.3), batched `Ready` across groups into one WAL fsync (A9). |
| **Supply chain and maintenance** | No release since 2023-03-07. Master is 29 commits ahead and TiKV patches crates.io to git. Mandatory `protobuf = "2"` is hit by **RUSTSEC-2024-0437 / CVE-2025-53605** with "no plan to migrate to v3.x". Build-time `protoc`; `slog`. | Very active, but pre-1.0 with no API stability ("OpenRaft API is not stable yet"). 0.10 is alpha. Tokio-based by default. | Minimal dependencies; we own the maintenance. |
| **Fit for deterministic simulation** | Excellent (sans-IO; follows etcd's "models Raft as a state machine" design). | Poor. Safety depends on the async runtime. The maintainers judged Stateright model checking infeasible (#1598), and their DST needs turmoil plus `tokio_unstable`. | Excellent, by design (A5). |
| **Storage-fault recovery (PAR, A3)** | Not supported. Storage errors panic. | Not supported. Behaviour is "undefined" on data loss. | Can implement CTRL-style repair (A3). |

### Recommendation

1. **Own the code, not the algorithm.** Implement the etcd/raft (raft-rs) state machine semantics in a new mantle crate. Do not invent a variant. TiKV did the same thing when it ported etcd/raft rather than designing afresh. Paxos Made Live §9 notes that real systems need "several relatively small protocol extensions" and that "the final system will be based on an unproven protocol".
   - Membership: joint consensus plus learners (CockroachDB §7.1.2 recommends joint consensus for all production Raft).
   - Elections: PreVote, CheckQuorum (leader step-down) and follower stickiness, **all on by default**. raft-rs defaults `pre_vote` and `check_quorum` to `false`. The evidence is Diss §9.6, §4.2.3 and §6.2, and Jensen et al. (HAOC '21).
   - Leadership transfer with a "may disrupt" flag.
   - Reads: ReadIndex by default. Lease reads are opt-in only, with an explicit drift bound, a lease-sequence check at apply time, and self-fencing (A1.5, A4.3, A7).
   - Replication: flow control (maximum in-flight messages and maximum message size), pipelining, and the leader writing to its own disk in parallel with replication.
   - Commit: only entries from the current term count toward commitment. No configuration entry may be appended before an entry from the current term has committed (the 2015 fix), in addition to a "one pending configuration change" gate.
2. **API shape.** The pattern is common to etcd/raft, raft-rs, noraft, and TiDB §4.1.1's separation of persist, send and apply. The core exposes:
   - `step(msg) -> Result<(), StepError>`;
   - `tick(now)`, where time is injected;
   - `propose(..) -> Result<ProposalId, ProposeError>`;
   - `ready()`, returning a batch with three parts: entries plus HardState to persist, messages tagged "send after persist", and committed entries to apply;
   - `advance(persisted, applied)`.

   The core does no I/O and reads no wall clock. The election-timeout RNG is injected, and there are no threads or async. Enforce this at crate level:
   - `#![forbid(unsafe_code)]`
   - Clippy deny-lints: `unwrap_used`, `expect_used`, `panic`, `unreachable`, `indexing_slicing`, `arithmetic_side_effects`
   - every peer message validated before use (compare raft-rs #595).
3. **Invariant violation means fencing one replica, not the process.** An `Err(Invariant…)` stops that group's replica and reports it to placement. Other ranges on the node keep serving. A panic in a multi-Raft process would take down every range on the node; that blast-radius argument is an inference. It matches FDB's pattern of "fail one unit, recover through a single well-tested path" (A5) and PAR's argument against Crash (A3).
4. **Multi-Raft machinery lives outside the core**, in the node runtime:
   - a scheduler that drains `Ready` from many groups into one WAL batch and one fsync, then releases their messages (C.c);
   - coalesced heartbeats per node pair;
   - quiescence of idle groups, tied to node liveness so that a quiesced follower does not start elections (CockroachDB §2.2.1 and §7.1.1; TiDB §4.1.3);
   - range-level leases on node-liveness epochs (CockroachDB).
5. **Correctness programme (required before production).** Each item is backed by the cited sources.
   1. Port etcd's and raft-rs's unit and data-driven tests. raft-rs is Apache-2.0; the owner must confirm licence compatibility.
   2. **Differential testing against raft-rs** inside the simulator: feed identical input sequences and compare commit sequences and outgoing-message effects. Treat a panic in the raft-rs oracle as a finding to triage. This is a design inference.
   3. Write a TLA+ spec of mantle's exact variant. Start from `ongardie/raft.tla` and add PreVote, CheckQuorum, joint consensus, learners, leases and snapshots. Model-check it with TLC. Mocket (EuroSys '23) found 2 specification-implementation inconsistencies caused by bugs in the official Raft TLA+ spec, so the spec itself needs review.
   4. Spec-guided testing of the implementation, in the manner of Mocket, SandTable (EuroSys '24: 23 bugs in 8 Raft/Zab systems) or model-guided fuzzing (OOPSLA 2025: 13 bugs).
   5. Deterministic simulation and linearizability checking (C.d).
6. **Effort reference.** raft-rs 0.7.0 `src/` is about 10.7k lines including comments and inline tests; its harness tests are about 10.2k lines. LogCabin's Raft was "roughly 2,000 lines of C++" (Diss §10.1) without production features. Plan for the 10k-line class.
7. **Fallback if schedule forces reuse: fork and harden raft-rs.** Vendor it, convert about 130 panic, fatal and unwrap sites to errors, replace protobuf 2 with prost or a hand-written codec, bounds-check peer messages, and keep the etcd test suite. **Do not adopt openraft for the metadata layer.** Its per-group task and heartbeat model, pre-1.0 API, 2026 history of core safety bugs, and dependence on the async runtime for safety all conflict with mantle's requirements.
8. **noraft** (0.7.1, 2026-08-15; sans-IO, `no_std`, dependency-free, `Result`-returning `handle_message`) is worth reading as a minimal API reference. It is too young and has too little adoption to depend on.

## C.b State-machine storage engine: RocksDB (rust-rocksdb 0.25 / RocksDB 11.8.1) behind a narrow mantle-owned engine trait; keep redb as the pure-Rust candidate to re-evaluate; not fjall yet

**What the metadata state machine needs** (from A1.6, A4.3, A4.4):
- ordered keys and prefix scans, for directory listings (Tectonic's "expanded" keys, A4.5);
- atomic batches that include `applied_index` (A1.2);
- consistent point-in-time images for snapshots and splits (Diss §5.2);
- cheap deletion of a key range after a range moves away;
- fast bulk loading for replica catch-up;
- read-path checksums, so PAR-style detection can work (A3);
- errors, not panics, on I/O failure and corruption;
- ideally, participation in deterministic simulation (A5).

| Need | rocksdb | redb | fjall |
|---|---|---|---|
| Correctness track record | Long, large-scale production history upstream (self-reported USERS.md: Facebook, TiKV, CockroachDB, …). Upstream crash/stress testing with a fault-injection FS (B4.1) | 2026 crash-recovery fixes that could "silently roll back … durably committed transactions" (4.2.0, 4.3.0). Fuzzer does not model power-loss reordering | 2026 durability bugs (#304, #308). Open mid-journal silent truncation (#311) and snapshot GC bug (#321). All 3.0.0–3.1.5 releases yanked |
| No-panic on I/O / corruption | Best of the three: `Result` everywhere; read-only mode after a failed write; residual panics are avoidable (FFI nulls, NUL in CF names, lock poisoning) | Typed errors, but many internal `unreachable!`/`unwrap` sites remain | Panics on corrupted persisted config/metadata |
| Read-path checksums | `verify_checksums = true` by default | Apparently only on repair/`check_integrity` paths (code reading; UNVERIFIED) | Since 3.0 (xxh3) |
| Range delete / bulk load / checkpoint | `delete_range(_cf)`, `ingest_external_file`, `Checkpoint` (B4.1). TiKV and CockroachDB were built on these | O(n) `retain_in`/`extract_from_if`; no ingestion; space reclaimed only by explicit `compact()` | — |
| Throughput / space (vendor benchmark, redb README) | Batch writes 451 ms, removals 6.9 s, 893 MiB | Batch writes 1,595 ms, removals 23.3 s, 4.00 GiB before compaction (best individual-commit latency) | Batch writes 353 ms, removals 6.0 s |
| Deterministic simulation | **No.** Only `mem_env` (no fault control); background threads | **Yes.** Public `StorageBackend` trait, no background threads | No (no FS abstraction, worker threads) |
| Build / portability | C++20 + libclang; open cross-compile issues | Pure Rust | Pure Rust |
| Maintainers | Many (upstream) | 1 | 1 |

**Recommendation.**
1. **Production engine: RocksDB** through `rocksdb` 0.25.0.
   - One engine instance per node (or per disk), shared by all ranges, with **un-prefixed keys** so that a split is a metadata-only Raft command (TiDB §4.1.4).
   - `delete_range` to drop data from a range that moved away.
   - SST-based snapshot transfer for learners and moved replicas: build or ingest (`SstFileWriter`, `ingest_external_file`). This follows Diss §5.3.3 on immutable runs.
   - It is the only candidate that meets the correctness-evidence bar today. Its FFI panic sites can be avoided by construction.
   - The cost is a C++ toolchain in CI and for release builds. Pin Linux x86_64 and aarch64 as tier-1 targets and treat other targets as best-effort, given the open cross-compile issues.
2. **Make the Raft log the WAL** (Spanner's double-logging was "expedient", A4.2). Write state-machine batches with `disableWAL = true` and enable `atomic_flush` if more than one column family is used (RocksDB options quoted in B4.1). Persist `applied_index` inside each batch.

   Safety depends on one rule, from Diss §5.2: **Raft-log prefix GC is gated on the engine's *flushed* applied index, not the in-memory one** ("once they are written to disk, the corresponding entries can be discarded from the Raft log"). After a crash the engine comes back at an older `applied_index`, and the node replays the Raft log from there. That in turn requires the apply path to be deterministic and idempotent at a given index.

   This is a design inference assembled from the cited sources; TiKV's exact configuration was not checked (UNVERIFIED). If the team prefers to keep RocksDB's WAL, the cost is one extra fsync stream per node.

   After a RocksDB write or background error (the read-only mode set by `paranoid_checks`), fence the replicas on that engine and recover from durable state or from peers. Do not call `DB::Resume()` automatically. This is an inference from Rebello et al.'s evidence that blind retries after fsync failure are unsafe.
3. **Hide the engine behind a narrow `StateMachineEngine` trait:**
   - `apply_batch(ops, applied_index)`
   - `get`
   - `scan(prefix | range)`
   - `snapshot() -> consistent iterator`
   - `delete_range`
   - `export_range(files)` / `ingest(files)`
   - `flushed_applied_index()`

   In the deterministic simulator, run a mantle **model engine** that implements the trait (an in-memory ordered map with explicit "flushed vs unflushed" state, dropped on simulated crash). RocksDB itself is covered by its upstream crash testing plus mantle's **real-disk** fault-injection and crash tests (C.d.5).

   This split follows FDB's warning that code outside the simulator is not tested by it (A5).
4. **Watch-list, with re-evaluation criteria:**
   - **redb**: pure Rust, simulation-friendly. Revisit after ~2 release cycles with no recovery or rollback fixes, once read-path checksum verification is confirmed and the remaining internal panics are reduced. It would then be the natural choice if simulation coverage of the real engine becomes the priority.
   - **fjall**: revisit when #311, #321 and #306 are resolved and the 3.x line is stable.
   - **Not candidates for the metadata store:** sled, canopydb, surrealkv, persy, nebari, agatedb (maturity warnings in their own READMEs).

## C.c Raft log storage: one shared, append-only, multi-group WAL per node (or per disk), not per-group logs and not inside the state-machine LSM

**Why shared.**
- Bigtable §6 is the peer-reviewed precedent: "a single commit log per tablet server, co-mingling mutations for different tablets." Per-tablet logs caused concurrent-file seeks and "groups would tend to be smaller", which weakened group commit.
- DeWitt §5.2 and Santos & Schiper show that one stable-storage write per batch is where the throughput is.
- The Raft rule that entries be persisted before acknowledging (A1.2) turns every per-group fsync into latency and IOPS. With 10⁴–10⁵ groups per node, per-group files cannot give group commit.

**Why not inside the state-machine LSM.**
- Spanner calls its "logs every Paxos write twice" design "expedient" (A4.2).
- Log entries are short-lived, and an LSM rewrites them again during compaction. TiDB's v6.1.0 release notes credit moving Raft logs out of RocksDB into raft-engine with "up to 40%" less write I/O [PS, vendor claim] (B5).
- The Raft log has a simpler access pattern (append, prefix-truncate, suffix-overwrite, sequential read) than a general KV.

**Design requirements.**
1. **Records:** `(group_id, kind ∈ {entries, hard_state, truncate_suffix, compact_prefix, snapshot_meta}, term/index range, payload)`, each with a per-record checksum. Entries and `HardState` for the same `Ready` go in one record batch, which satisfies etcd's ordering rule: entries, then HardState, before messages are released.
2. **Group commit through flush pipelining** (Aether §4.1). A single writer accepts `Ready`s from all groups and flushes on X records, L bytes or T µs, whichever comes first. It issues one `fdatasync` per batch, then wakes each group so it can send its messages. A leader may send AppendEntries before its own fsync completes (Diss §10.2.1; TiDB §4.1.1). Followers acknowledge only after the fsync.
3. **Per-group in-memory index** mapping index to (segment, offset). A conflicting suffix is handled by appending a logical `truncate_suffix` record. Diss §8.1 shows back-to-front truncation is safe, and the physical log stays append-only.
4. **Garbage collection.** A segment can be deleted once every group's `first_index` has moved past all of its records. Lagging groups pin old segments, so either:
   - rewrite their live entries forward (the raft-engine approach, B5), or
   - cap per-group log size and force a snapshot for followers that are too slow (Diss ch. 5).

   Prefix compaction happens when the state machine has made applied state durable (Diss §5.2).
5. **Corruption and fsync failures.**
   - Store PAR-style identifiers or persist records separately from entries, and keep two copies of `HardState` (A3).
   - Corruption surfaces as typed per-group errors, followed by PAR's have/dontHave/haveFaulty repair.
   - **Never retry a failed fsync.** ext4, XFS and Btrfs mark pages clean after an fsync failure (Rebello et al., ATC '20). Fence the WAL, then either re-open and re-validate from on-disk contents with checksums, or rebuild the affected replicas from peers.
6. **I/O isolation.** Keep WAL fsync latency apart from bulk I/O: put the WAL on a separate device or partition if available, and rate-limit snapshot and compaction writes. Paxos Made Live §7 saw fsync stalls of several seconds on the log behind snapshot writes.
7. **Recovery** scans segments, validates checksums, rebuilds the per-group indexes and HardStates, then hands each group its durable state. Measure recovery time as a function of WAL size, and bound it with segment GC and rewrite.

**Build or reuse?** Build it. See C.c.1 below and B5: raft-engine is the design reference, not a dependency.
- **HardState placement:** in the WAL, in the same record batch as the entries of the same `Ready`, plus a periodically checkpointed duplicate copy (PAR metainfo, A3).
- **Per-group log size caps** turn chronic laggards into snapshot recipients (Diss §5.1.2 expansion-factor heuristic) rather than GC blockers.
- **One WAL per disk** when a node has several disks, so a single device failure fences only the replicas stored on it.

This is a design inference. It bounds the blast radius the same way Bigtable's two log files hide GFS latency spikes (A4.1).

### C.c.1 Build vs reuse: don't depend on tikv/raft-engine

Use tikv/raft-engine (B5) as the **design reference**: shared log stream, per-group memtable index, leader-batched `fdatasync`, collaborative purge plus rewrite, pluggable `FileSystem`. **Do not depend on the crate.** The reasons, all verified in B5:
- The crates.io release (0.4.2) is from 2024 and its `sync()` is a no-op; the fix is unreleased.
- It panics on fsync failure, on the comment's assumption that fsync errors are retriable, which Rebello et al. refute.
- Its default recovery mode treats *any* corruption in the last file as a torn tail, contrary to PAR.
- It requires rust-protobuf 2 messages, which carry RUSTSEC-2024-0437.

Build mantle's WAL to the requirements in C.c. It is a bounded component: an append-only segmented log plus index and GC. Test it in the simulator with the same disk-fault model as everything else.

## C.d Testing strategy: layered, simulation-first

1. **Specification layer.**
   - TLA+ for the Raft variant (C.a.5) and for mantle's own protocols: split, merge, lease transfer, range moves, and any multi-range transaction commit. CockroachDB verified Parallel Commits in TLA+ (A4.3).
   - Model-check with TLC at small bounds.
   - Review the spec itself: Mocket found bugs in the official Raft TLA+ spec.
2. **Implementation–spec conformance.** Replay model-checker traces through the sans-IO core (Mocket, SandTable), or use TLA+-coverage-guided fuzzing (Gulcan et al.). Stateright can model-check the Rust core directly because the core is deterministic and cloneable, which the openraft maintainers found infeasible for their async design (#1598).
3. **Deterministic whole-cluster simulation** (FDB §4; Paxos Made Live §6.3; Diss §8.3; Raft Refloated).
   - **Execution:** one thread, seeded PRNG, virtual time that fast-forwards when idle. The *real* Raft core, WAL, apply logic and range manager run on simulated network, disk and clock implementations. The model engine stands in for RocksDB (C.b.3).
   - **Network faults:** drop, delay, duplicate and reorder messages; symmetric **and partial** partitions (Jensen et al.); link flapping.
   - **Disk faults:** latency spikes; `EIO`; fsync failure (Rebello et al.); loss of un-fsynced writes on crash (FDB); torn writes; bit rot and misdirected writes (PAR fault model).
   - **Clock faults:** drift within and beyond the configured bound, and jumps, to exercise lease safety (A1.5, A7).
   - **Process and cluster events:** crash and restart; continuous membership changes, splits, merges and snapshots. Configure timeouts to maximise rare events: very short election timeouts, snapshot on every apply during development (Diss §5.1.3, §8.3).
   - **BUGGIFY-style hooks and swarm randomization** of configuration, workload, fault rates and tuning parameters. Include coverage counters for rare states (FDB §4).
   - **Invariant oracles checked after every step:**
     - Election Safety, Log Matching, Leader Completeness, State Machine Safety (Figure 3.2);
     - lease disjointness;
     - range-descriptor sanity: no overlapping or missing key ranges, monotone epochs;
     - applied index ≤ durable commit;
     - the replica consistency checksum at a log index (Paxos Made Live §6.2).
   - **Two phases per run:** a safety phase with faults, then a liveness phase ("heal, then must recover") (Paxos Made Live §6.3; FDB).
   - **Test the tests:** re-introduce known bugs as mutation tests, for example the 2015 membership bug, commit-by-counting-old-term, and ReadIndex without a no-op. The simulator must catch each one (Paxos Made Live §6.3).
   - **Tooling.** Build mantle's own simulator runtime rather than depend on madsim or turmoil. madsim's filesystem `power_fail` and `sync_all` are no-ops. turmoil's durable-vs-pending filesystem model sits behind an unstable feature, and parts of it are unpublished (B6). A sans-IO core makes an in-house simulator cheap. madsim and turmoil remain options for integration-level tests of async glue code.
4. **Linearizability and isolation checking** on every simulated and real-cluster history.
   - Per-key P-compositional WGL, as in A6.8: an in-house port of the Porcupine design, with indeterminate operations taken as +∞ returns, a per-partition timeout, and counterexample output.
   - Elle-style list-append checks for multi-key metadata transactions (A6.7).
5. **Storage-engine crash consistency.** Block-level fault injection, following CuttleFS (Rebello et al.) and the PAR fault model. Run it against the chosen engine (C.b) and the WAL (C.c).
   - Real-disk kill and power-cut tests of the RocksDB integration. They check the `disableWAL` path and that Raft-log GC is gated on the flushed applied index (C.b.2); that path is outside the simulator.
   - The ALICE/CrashMonkey family is covered by another research track.
6. **Real-cluster tests.** Jepsen-style runs on real OS, filesystem and network cover what simulation cannot: performance, OS-contract assumptions, third-party code (FDB §4). Include partial partitions and clock skew.
7. **Scale.** Run many short histories rather than one long one (Wing & Gong §4.3; Lowe). Run seeds nightly on a farm and archive failing seeds (Paxos Made Live: some bugs "took weeks of simulated execution time").

# Appendix Z — Consolidated UNVERIFIED items and caveats

Items below were **not** confirmed in a primary source (or are the author's/vendor's own claims) and must not be relied on without re-checking:

**Part A**
- raft-dev 2015 post: only short verbatim excerpts could be retrieved (WebFetch refused full reproduction); the counterexample is paraphrased; the timestamp's time zone is viewer-local.
- Spanner: resharding behaviour after 2012; TOCS article number.
- CockroachDB (not in the SIGMOD paper): the term "epoch-based lease", meta1/meta2 names, Jepsen/roachtest/nemesis testing, how leaseholder–leader co-location is maintained, current default range size (~64 MiB in the paper).
- TiDB (not in the PVLDB paper): the name "hibernate region" (the TiKV configuration docs confirm `raftstore.hibernate-regions` [PS]), current PD persistence model, current default Region size (96 MB in the paper), merge details beyond the paper's sketch.
- FoundationDB (not in the SIGMOD paper): "swizzle-clogging", quantified simulation scale (CPU-hours/runs), DataDistributor split/merge criteria, Ratekeeper algorithm.
- Gibbons & Korach 1997: primary text not accessed; NP-completeness/O(n log n) register results are as reported by Lowe, Horn & Kroening, and Kingsbury & Alvaro.
- Lowe 2017: section numbers/quotes from the author's preprint; published e3928 pagination not checked. Horn & Kroening: read from arXiv v1 (minor differences from LNCS possible). φ accrual: read from the JAIST tech report (equivalence to SRDS camera-ready not checked). Gray & Cheriton: read from the Stanford TR reprint (proceedings pages not mapped). Helland et al. 1989 and Friedman & Hadad 2006: citations verified via Crossref only; content not read.
- Porcupine's "1,000x–10,000x faster" is the author's own claim; Knossos's README itself says to treat results "as plausible but verify by hand."
- Antithesis blog (July 2026) claim of OpenRaft bugs: no corresponding public openraft issue found.

**Part B**
- rust-rocksdb build time and binary size: not documented anywhere found. RocksDB's USERS.md is self-reported and may be stale. Whether the C++ library has release-build `abort()` paths that bypass Rust's error model was not audited.
- redb: whether checksums are verified on ordinary (non-repair) reads. The "no" comes from reading the code, not from documentation.
- fjall/lsm-tree: whether a crash or power-loss harness exists outside the repository (none found); why 3.0.0–3.1.5 were yanked (no stated reason); the 1.x→2.x format break is sourced only from the 2024-09-20 announcement.
- Production users of redb, fjall and surrealkv: no primary source; crates.io dependents are only a proxy. Whether surrealkv's internal `vfs::File` can be plugged in externally was not checked.
- TiKV's current configuration of the KV-engine WAL when raft-engine is enabled (relevant to C.b.2): not checked. `raft-log` 0.4.6: not evaluated.
- openraft: how #1802/#1808 were discovered; whether #2118 affects 0.9.x.
- raft-rs: maintainers' release plans (no public statement); whether the `prost-codec` build path needs a system `protoc`; the Apple-Silicon `protoc` requirement is an inference from protobuf-build's bundled-binary list.
- turmoil 0.7.2: torn-write injection is claimed only in the main-branch README.
- Panic-site counts are from a heuristic scanner (skips `#[cfg(test)]`, test dirs, `//` comments) — approximate.

**Design inferences (clearly not source claims)** are marked "inference" in the text: e.g., cross-group WAL batching as an application of Bigtable/DeWitt; differential testing against raft-rs; process-wide panic blast radius in multi-Raft; hash-prefixed namespace keys (A4.5); virtual-time recording for linearizability histories; treating a stalled lease holder like a slow holder clock.
