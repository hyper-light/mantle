# Protocol-aware recovery and the last frame (AGL+18)

What a log may do when it answers a frame at the frame's own flush, with no second write
confirming that flush: the source the design rests on (durability.md §7; raft-log.md §6).
research/06 §A3 summarized the paper first; this note quotes the passages the design uses, from
the paper itself.

**Citation.** R. Alagappan, A. Ganesan, E. Lee, A. Albarghouthi, V. Chidambaram, A. C.
Arpaci-Dusseau, R. H. Arpaci-Dusseau. "Protocol-Aware Recovery for Consensus-Based Storage."
*16th USENIX Conference on File and Storage Technologies (FAST '18)*, Oakland, CA, February 2018,
pp. 15–32 ("AGL+18"). Open access:
<https://www.usenix.org/system/files/conference/fast18/fast18-alagappan.pdf>, fetched
2026-11-17, 480,460 bytes, SHA-256
`b18cc6fc2d6605e54a153697b608ac3d60bc363bacd6a8395860430286af5dc5`. Page numbers below are the
proceedings' printed pages (the PDF's page 2 is printed page 15). "CLSTORE" is the paper's local
storage layer and "CTRL" its whole protocol.

## 1. The guarantee (§3.2, p. 19)

> "CTRL guarantees that if there exists at least one correct copy of a committed data item, it
> will be recovered or the system will wait for that item to be fixed; committed data will never
> be lost. In unlikely cases where all copies of a committed item are faulty, the system will
> correctly remain unavailable. CTRL also guarantees that the system will make a decision about
> an uncommitted faulty item as early as possible, ensuring high availability."

## 2. A crash and a corruption, told apart (§3.3.3, p. 20)

The two causes of a mismatch:

> "A checksum mismatch for a log entry could occur due to two different situations. First, the
> system could have crashed in the middle of an update; in this case, the entry would be
> partially written and hence cause a mismatch. Second, the entry could be safely persisted but
> corrupted at a later point. Most log-based systems conflate these two cases: they treat a
> mismatch as a crash [30]. On a mismatch, they discard the corrupted entry and all subsequent
> entries, losing the data."

Why the difference matters:

> "if the mismatch were really due to a crash, it is safe to discard the partially written
> entry. It is safe because the node would not have acknowledged to any external entity that it
> has written the entry. However, if an entry is corrupted, the entry cannot be simply discarded
> since it could be globally committed."

With the persist record ordered after its entry by a flush of its own, the cases are told apart
("write(ei), fsync(), write(pi), fsync()"); the record is atomic:

> "pi is checksummed and is very small; it can be atomically written and thus cannot be
> 'corrupted' due to a crash."

With one flush, the protocol CTRL uses, they are not:

> "such ordering requires an (additional) expensive fsync in the critical path, affecting
> log-update performance. For this reason, CLSTORE does not order ei before pi; thus, the
> append protocol is t1:write(ei), t2:write(pi), t3:fsync(). Given this update sequence, assume
> a checksum mismatch occurs for ei. If pi is not present, CLSTORE can conclude that it is a
> crash (before t2) and discard ei. Contrarily, if pi is present, there are two possibilities:
> either ei could be affected by a corruption after t3 or a crash could have occurred between t2
> and t3 in which pi hit the disk while ei was only partially written. [...] CLSTORE can still
> conclude that it is a corruption if ei+1 or pi+1 is present. However, if ei is the last entry,
> then we cannot determine whether it was a crash or a corruption."

The footnote to that sentence: "The proof of this claim is available [1]." And what CTRL does
with it:

> "The inability to disentangle the last entry when its persist record is present is not
> specific to CLSTORE, but rather a fundamental limitation in log-based systems. [...] Thus, when
> the last entry cannot be disentangled, CLSTORE safely marks the entry as corrupted and leaves it
> to the distributed recovery to fix or discard the entry based on the global commitment."

## 3. Identifiers apart from their items (§3.3.4, pp. 20–21)

> "storing the identifier near the item is less useful; a misdirected write can corrupt both the
> item and its identifier [9,10]. Hence, identifiers are physically separated from the items they
> identify. [...] The identifier of a log entry also conveniently serves as its persist record."

> "It is highly unlikely an item and its identifier will both be faulty since they are
> physically separated [...]. In such unlikely and unfortunate cases, CLSTORE crashes the node to
> preserve safety."

## 4. The group decides (§3.4, pp. 21–22)

A follower's faulty entry the leader does not hold (§3.4, p. 21):

> "If the leader does not have an entry that the follower has, then the entry must be an
> uncommitted entry because the leader is guaranteed to have all committed data; thus, the leader
> instructs S5 to truncate the faulty entry and also replicates the correct entry."

A faulty node may lead, but serves nothing until its log is fixed (§3.4.1, p. 21):

> "any node that has a more up-to-date log can now be elected the leader even if it has faulty
> entries. [...] faulty commands cannot be skipped and later applied when they are fixed; such
> out-of-order application would violate safety. Hence, it is required for the leader to fix its
> faulty entries before it can accept new commands."

Commitment, decided by the group (§3.4.2, p. 22):

> "If a majority of the followers respond that they do not have the entry (negative
> acknowledgment), then the leader concludes that the entry is uncommitted. In this case, the
> leader safely discards that and all subsequent entries; it is safe to discard the subsequent
> entries because entries are committed in order. Conversely, if the entry were committed, at
> least one node in this majority would have that entry and inform the leader of it"

> "The leader, in this case, has to wait for either S4 or S5 to respond; discarding the entry
> without waiting for S4 or S5 could violate safety. [...] In the unfortunate and unlikely case
> where all copies of an entry are faulty, the system will remain unavailable."

The answers and what each decides (§3.4.3, p. 22): "have – a follower could respond that it has
the entry ⟨epoch:e, index:i⟩ and is not faulty in its log"; "dontHave – [...] it does not have
the entry"; "haveFaulty – [...] it has ⟨epoch:e, index:i⟩ but is faulty in its log too". "Case 1:
if it gets a have response from at least one follower, it fixes the entry in its log. Case 2: if
it gets a dontHave response from a majority of followers, it confirms that the entry is
uncommitted, discards that entry and all subsequent entries. Case 3: if it gets a haveFaulty
response from a follower, it waits for either Case 1 or Case 2 to happen." "Case 1 and Case 2 can
happen in any order; both orderings are safe."

The count is exact (§5.1.1, p. 26): "the leader concludes that a faulty entry is uncommitted
only after gathering ⌊N/2⌋ + 1 dontHave responses; if this number is reduced, then the checker
finds a safety violation."

## 5. What mantle takes, and what the paper leaves open for it

- **The rule's ground.** One flush a frame is CTRL's own append protocol (t1, t2, t3 above). Its
  price is the last frame: with its record present and its body failing, the device alone cannot
  say whether it was answered (§2), and the group must decide (§4). raft-log.md §6 kept a second,
  confirming write so that no frame was ever in that state; answering at the frame's own flush
  puts the last frame back in it.
- **A frame holds many groups.** The paper's log is one replicated state machine's; hyper-log's
  frame holds updates of every range on the device (raft-log.md §1). The question CTRL asks per
  entry is asked per group the last frame's record names, through each group's own entries
  (research/06 §A3 drew the same inference).
- **A record need not be atomic here.** The paper's persist record "can be atomically written".
  hyper-log's names every group its frame holds, and a record of five groups spans two 512-byte
  sectors (raft-log.md §6). A record torn by a crash is then not told from one damaged at rest,
  and a frame whose record no longer reads cannot say which groups to mark. The paper does not
  cover this; the design closes it by writing a record in pieces no larger than the device's
  atomic write unit, each checked on its own (durability.md §7.2).
- **"Unlikely" becomes reachable by a crash alone.** In the paper, every copy of an entry faulty
  is the unlikely case of independent corruptions. With one flush an append, a power cut that
  tears the same entry's last frame after its record landed, on every member holding it, leaves
  every copy undetermined, with no corruption at all; a group of one voter is in that state after
  any such tear. CTRL then waits (§4), as it must. The design states where mantle accepts that and
  where the confirmation stays (durability.md §7.5).
