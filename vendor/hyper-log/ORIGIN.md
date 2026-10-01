# hyper-log: origin

- **Source.** mantle's `crates/log` (mantle-log) at mantle `147f035`
  (`147f0355513e802fd4b8b3fa3717d8cc1138ded5`), brought in with its history by `git subtree`.
  - The imported tree hash equals mantle's `147f035:crates/log`
    (`96dc4f2375283a2db82644c41fb58edb49393a87`).
  - Its design is mantle's `docs/design/raft-log.md`, which this crate keeps citing; the on-disk
    format is mantle's, unchanged (format 3, magics `MNLS`, `MNLF`, `MNLP`), so hyper-log reads the
    files mantle-log wrote and writes the files it reads.

## L-1: moved, behaviour unchanged (commit `5bd0699`)

- Package `hyper-log`, on `hyper-block` (`../hyper-block/ORIGIN.md`).
- mantle-codec's `Reader` and `Writer` are `src/codec.rs`; CRC-32C is the `crc32c` crate (RFC 3720
  §B.4's vectors and a pieces-equal-the-whole property test added), where mantle-crc used
  `crc-fast`.
- **Equivalence** (`tests/equivalence.rs`): 24 seeds, each four cycles of 40 rounds of groups
  appending, conflicting, compacting, voting, proposing, leaving and sending invalid updates, each
  round's batch fixed by holding the device in a flush, with power cuts, random-sector crashes and
  bit flips at rest between cycles, so opens restore, mark and fence. The same harness, renamed, run
  against mantle `147f035` wrote 48 files: transcripts of every answer, view and recovery, and the
  device image with each segment's random nonce replaced by its incarnation and the checksums over
  it recomputed (a checksum that failed keeps failing). This crate writes the same 48 files byte for
  byte, at L-1 and at every commit since. `EXPECTED` holds mantle's hashes of them.

## L-2: one owner, tickets, fetches by ticket (commit `4e58930`)

mantle note 32 §3.9 and §5.2 L-2.

- **The owner** (`src/owner/`). One thread holds everything the log knows: groups, segments, the
  queue's room and its waiters, the writer's batch and fair queue. mantle shared it behind
  `Arc<Shared>` with a `RwLock<State>`, a `Mutex` around the room and atomics. Callers reach the
  owner through a bounded inbox; nothing else is shared.
- **Tickets** (`src/ticket.rs`). Every call carries a ticket and is answered through a reply port of
  the caller's own, which only the caller receives on, so an answer unparks only its caller; a
  `Waker` given with a submission is woken once, after its answer. mantle answered through an mpsc
  channel per submission and woke a waker beside it. Ports are reused: the answer carries the port's
  sending end back, and a thread keeps the ports it has finished with.
- **The device thread** (`src/device.rs`). The owner never waits on the device: it hands each I/O
  (a frame and its persist record then one flush, a confirmation, a sweep's read of the tail,
  entries read back, a caller's look at the file) to the log's device thread, which owns the file,
  and goes on answering callers; each completion comes back to the owner as a message. mantle's
  writer thread wrote and flushed while readers took the lock. A log runs these two threads,
  whatever its groups or callers. The device runs each job inside an unwind boundary, since the
  file is the caller's code.
- **The writer's rules are mantle's** (`src/writer.rs`), run by the owner as steps between messages
  (`src/owner/write.rs`); the order of everything the log decides is mantle's, which the equivalence
  shows.
- **Fetches by ticket.** `Log::fetch` and `Log::fetch_waking` copy entries into the caller's
  reservation (`Fetched`), which comes back with the answer and keeps its capacity. `Log::entries`
  is that, copied out into `Entry`s.
- **Owned bytes.** `Entry` and `Proposal` hold `Vec<u8>`, which the log takes and keeps while the
  entry is recent; mantle held `Arc<[u8]>`.
- **Files back.** `Log::close` gives the file back once everything taken is answered;
  `Log::try_open` and `Log::try_create` give it back with a refusal (`Refused`). `Log::with_file`
  runs a closure on the device thread, how a simulation reaches the device the log owns.

### Tests

The suite's assertions are unchanged; its plumbing is not:
- a shared `Arc<SimFile>` is now the log's while it runs, reached by `Log::with_file`, and taken
  back by `Log::close` or a refusal;
- the devices that held the writer (`Stepped`, `Gated`, `Gate`, each a `Mutex` and a `Condvar`) are
  one `Held` device the test commands over a channel (`tests/common`), which also arms a fault while
  the device is held;
- threads share the log by reference in a scope;
- the room's three tests drive the owner-held room directly and assert who is told, where they
  counted thread wakes;
- `a_waker_is_woken_once_for_each_answer` counts each waker's wakes (`hyper_measure::wake`) where it
  read a channel's disconnection;
- `a_completion_wakes_only_its_submitter` is new: 48 submitters with counting wakers over 12 rounds
  each, every wake its own answer's, each waker woken exactly as often as answered.
- `a_groups_queued_updates_become_durable_in_order` hung once on Linux, in mantle's test as
  written: each round holds the writer by a plug update and waits for its flush, but a log full of
  what the groups keep refuses the plug `Full` with no frame written, so no flush ever came. The
  round now waits for the plug's flush or its answer, whichever comes first (its waker tells the
  holder), takes a `Full` or `Backlog` plug as the refusal it is, and runs that round unheld; the
  holder forgets each round's events before the next. A direct case (a 4-block, 8-segment log
  filled by one group's 8 KiB entries) shows the plug refused `Full` unheld. mantle's test at
  `147f035` carries the same wait.

## Allocations (commit `4b48295`)

Appends and fetches into a reservation allocate nothing once warm: `docs/benchmarks.md`,
"hyper-log". What changed, none of it in the bytes written:
- format: entries records encoded and sized from the update's entries with no list made of them; a
  frame header laid into an array; a persist record encoded into a kept buffer;
- placements are offsets, an entry's place following from its record's;
- the batch order sorts in place, with unstable sorts over keys made unique by position, which order
  exactly as mantle's two stable sorts;
- the owner keeps from frame to frame the payload, the persist record, the aligned buffers, the
  ordering scratch, the batch's deque, one update list for each frame it holds at once, and a fetch's
  lists.

## The wall (commit `af2679b`)

- Every public item documented, every const with its derivation.
- Functions over the complexity threshold split along the lines they had, the moved bodies
  unchanged (the commit lists each).
- Test opt-outs as the rules allow: `cognitive_complexity` and `unwrap_in_result` at test crate
  roots. No test opts out of `disallowed_types`: proptest's `prop_oneof!` boxes its arms in `Arc`,
  so the step strategy draws the kind as a number by the same weights. The seven cases
  `log.proptest-regressions` recorded are run as the inputs they shrank to
  (`recorded_regressions_hold`), since the file's seeds replay other inputs under the new strategy.

## Not done

- The writes through hyper-block's device issuer (mantle `docs/design/node.md` §1.2): the log has its
  own device thread until then.
- Entries read from the file wait behind a frame's write and flush on the one device thread.
- mantle's replica shell on this crate (D-1), and mantle and focal consuming it (F-1).
