# 96. Ordering a PUT's writes: which may go at once

Status: notes, 2026-11-17. Question: a PUT writes its Block rows, then its File row, then its Name
row, each a range command of two flushes after the last one answered (research/71, step 9: 7
flushes on a one-byte PUT's critical path). Which of them may go at once without a committed name
ever referring to anything that is not, and what then must the sweeps collect? Used by
docs/design/gateway.md §2.2.

## 0. Sources

| Key | Source | Kind | Where read |
|---|---|---|---|
| GMSP00 | G. R. Ganger, M. K. McKusick, C. A. N. Soules, Y. N. Patt. "Soft Updates: A Solution to the Metadata Update Problem in File Systems." *ACM Transactions on Computer Systems* 18(2), May 2000, pp. 127–153 | peer-reviewed | https://users.ece.cmu.edu/~ganger/papers/softupdates.pdf (SHA-256 prefix ada4597221099d2e), text by `pdftotext` |
| TSM+20 | R. Taft, I. Sharif, A. Matei, N. VanBenschoten, J. Lewis, T. Grieger, et al. "CockroachDB: The Resilient Geo-Distributed SQL Database." *SIGMOD '20*, pp. 1493–1509. doi:10.1145/3318464.3386134 (authors in note 06 §A4.3) | peer-reviewed | https://www.cockroachlabs.com/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf (SHA-256 prefix 142dbe2ecc31e948), text by `pdftotext` |

## 1. Soft updates: only the pointer waits for what it points to [GMSP00]

**The rules** (§2, p. 130). Creating a file "allocates an inode, initializes it, and constructs a
directory entry that points to it"; "the initialized inode must reach stable storage before the new
directory entry", an *update dependency*. "The ordering constraints map onto three simple rules:
(1) Never point to a structure before it has been initialized (e.g., an inode must be initialized
before a directory entry references it). (2) Never reuse a resource before nullifying all previous
pointers to it (e.g., an inode's pointer to a data block must be nullified before that disk block
may be reallocated for a new inode). (3) Never reset the last pointer to a live resource before a
new pointer has been set (e.g., when renaming a file, do not remove the old name for an inode until
after the new name has been written)."

**What a crash leaves** (§3.3). With the dependencies kept, "a file system can be safely mounted
and used immediately after a system failure. However, it may contain several minor
inconsistencies: Unused blocks may not appear in the free space maps. Unreferenced inodes may not
appear in the free inode maps. Inode link counts may exceed the actual number of associated
directory entries". These are reclaimed later: "a background version of fsck can be used to
reclaim these resources while the file system is actively being used".

**Reading for mantle (inference).** Rule 1 orders a pointer after what it points to, and nothing
else: two structures neither of which points to the other carry no dependency, and soft updates
writes them in any order. Its subject is what a reader can reach after a crash. A PUT's Name row
points to its File row, and its File row to its Block rows; readers reach a file only through a
name (metadata.md §2), so the File row's pointer to a block not yet recorded is reached by no
reader, only by the sweeps between layers. Rule 1 for readers is kept by the Name alone waiting for
both. What a crash leaves, a block or file nothing names, is §3.3's leaked inode and block, which
mantle's sweeps reclaim while the cell serves, as background fsck does. Rule 2 is the sweep's
side: a block is released, its chunks reclaimed, only once no file will name it, which is the bound
the File row's write carries (metadata.md §2, "Blocks never named").

## 2. Parallel Commits: committing beside the writes, with readers resolving [TSM+20]

**The protocol** (§3.1.1, "Parallel Commits"). Committing only "once all of its writes are known to
have replicated" requires "at least two sequential rounds of consensus. Instead, the Parallel
Commits protocol employs a staging transaction status which makes the true status of the
transaction conditional on whether all of its writes have been replicated." The coordinator
replicates the staging status "in parallel with the verification of the outstanding writes";
"Assuming both succeed, the coordinator can immediately acknowledge the transaction as
committed". Its safety was "formally verified ... using TLA+".

**What readers must do** (§3.2). Provisional values are "write intents", each pointing to "a
transaction record ... that stores the current disposition of the transaction: pending, staging,
committed or aborted". "Upon encountering an intent, a reader follows the indirection and reads the
intent's transaction record"; a pending one makes the reader block; "If the transaction is in the
staging state ... the reader attempts to abort the transaction by preventing one of its writes from
being replicated. If all writes are already replicated, the transaction is in fact committed".

**Reading for mantle (inference).** Committing a PUT's name beside its file and blocks is this
protocol: the name would be an intent whose commit is conditional on the file and blocks, and every
reader of the name, GET, HEAD, listings, a precondition judged at a later write, the replacement of a
null version that releases the file it held (metadata.md §2), and the answer an operation's row
records (metadata.md §2, "Operations"), would have to resolve it or wait, with a way to abort a
write still in flight. None of that exists in the Name range, whose commit is the PUT's
linearization point; it is a change of the Name layer's semantics, not of the PUT's order.
