# A reply told past its connection's end

**Question.** Why did the roll test (tests/serve.rs,
`a_cell_made_by_init_rolls_one_node_at_a_time_and_reads_every_object_back`) fail on CI's
linux-aarch64-1cpu (run 40161728764, 203c50f8) and macos-aarch64 (run 40223736182, 3a338887), each
time with a node the roll was not stopping ending on "mantle: the node's history:
Refused(Malformed)"? A node's process ends when its statement of its history (node.md §2.4 item 7)
is answered other than `Kept` (`crates/node/src/process.rs`, `Living::take`). Both failures were
rare: the local gates and CI's other targets passed the same commits.

**Where the answer can come from.** A node's own command is answered `Refused(Malformed)` where no
entry carries it alone, where a reply's answers do not decode or do not match its commands, where
the leader refused the entry as malformed (its head or body not decoding, or past the range's entry
bound), and where the reply's head does not decode (`crates/node/src/ask.rs`,
`crates/node/src/node.rs`).

**Reproduced.** A scratch branch (`diag/history-malformed`, not landed) printed a tagged line at
each of those places and had each node state its history again at every turn once answered, so a
statement was out at every stop. The roll test ran six copies at once beside twelve busy loops on
this Mac (an Apple M5 Max of 18 cores and 128 GiB, other sessions' builds running, 2026-10-25). The
first round of six passed; in the second, one copy failed as CI did. Its node `12949001530363522278`
and the leader it asked, `1321587705727983183`, which the roll was stopping:

| µs past the statement | what the node's lines said |
|---|---|
| 0 | the node asks its statement (session 5308416, serial 15) of the leader, on exchange `68719476737` |
| 2,233 | the node takes another node's command as the root range's leader: the leader's stop has handed the range to it |
| 62,131 | the reply on `68719476737`: no head (`raw=None`), read as an empty one, `Err(Head(0))` |
| 66,336 | the statement refused `Malformed`; the node ends |

Without the statements at every turn, two rounds of six passed, no command refused.

**Cause.** hyper-transport tells its owner of a reply by an event that holds the exchange's slot
"removed or not" (its endpoint's documentation): when the connection is lost after the reply's head
arrived and before the owner takes the event, the exchange is removed with its head
(`finish_exchange`), and its refusal, `Closed`, is queued behind the reply. hyper-rt-transport's
driver handles every datagram that arrived before it hands out an event. A leader whose stop
answers a command and then closes its connections (node.md §1.7), the reply and the close reaching
the asker in one turn of its driver, has its reply told with no head; the node read the missing
head as an empty one (`endpoint.head(exchange).unwrap_or_default()`), which no reply head decodes
as, and refused the command `Malformed`. A busy or slow runner makes the one turn likelier; a
statement out at every stop made it likely here.

**What changes.** A request or reply whose head is gone is its exchange's end: the node takes
nothing of it (`Node::on_event`), its client neither (`Client`'s command, object and chunk
exchanges), and the refusal queued behind it asks the command again with its serials, which the
range's session answers without applying twice. `crates/node/tests/transport_ended.rs` holds the
transport's side on an in-memory network, both nodes' endpoints driven by the test: a reply taken
before its connection is lost holds its head; one told after has none, and its refusal follows.

**Verified.** The scratch branch with this change, still stating at every turn, beside the same
twelve busy loops, ten rounds of six (60 copies): no command was refused `Malformed`, and eight
requests and replies were told after their exchange ended and taken as ended, every copy that took
one passing. Two copies of one round failed otherwise: a client's object request ended with its
connection, `Lost(Closed)`, a loss this change does not make, since it changes only how an exchange
already ended is told. This change without the scratch branch's lines, the build of the commit
after it (6613ce71), beside the same busy loops: ten rounds of six, all 60 passed.

| run | build | copies | passed | refused `Malformed` | told after their end | failed |
|---|---|---|---|---|---|---|
| reproduction | scratch, before this change | 12 | 11 | 1 | — | 1 (`Malformed`) |
| this change, stating | scratch, with this change | 60 | 58 | 0 | 8 | 2 (`Lost(Closed)`) |
| this change | 6613ce71 | 60 | 60 | 0 | — | 0 |
