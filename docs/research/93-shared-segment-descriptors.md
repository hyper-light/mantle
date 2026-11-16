# 93. One descriptor a segment: sharing a store's open files among its handles

**Question.** A range's store, kept as segments (hyper-block's `SegmentedFile`, research/48), is
read and written by the shard, by each of its maintenance workers and by each of the device
issuer's workers. Each of those handles opened every segment it reached for itself, so a range's
descriptors grew as segments times handles, and a 100M fill ran out of them at macOS's default
limit and in a Linux container's (docs/measurements/2026-11-06-targets.md, 2026-11-12-targets.md).
Can every handle transfer through one open file a segment, on every platform, and what does it
cost?

## What the platforms promise of one open file shared by threads

- **POSIX.** `pread()` "shall be equivalent to read(), except that it shall read from a given
  position in the file without changing the file pointer", and `pwrite()` likewise; both are
  among the functions that "shall be atomic with respect to each other in the effects specified in
  POSIX.1-2017 when they operate on regular files" (IEEE Std 1003.1-2017, pread, pwrite, and
  §2.9.7 "Thread Interactions with Regular File Operations").
- **Linux.** pread(2): "The pread() and pwrite() system calls are especially useful in
  multithreaded applications. They allow multiple threads to perform I/O on the same file
  descriptor without being affected by changes to the file offset by other threads." A duplicate
  made by dup(2) refers to the same open file description, so the issuer's duplicates already
  shared one with the handle they were made from.
- **macOS.** read(2): "Pread() performs the same function as read(), but reads from the specified
  position in the file without modifying the file pointer", and pwrite() likewise (write(2)).
  `F_NOCACHE` is set on the open file, not the descriptor (XNU `bsd/kern/kern_descrip.c`, fcntl's
  `F_NOCACHE` setting `FNOCACHE` in the fileglob's flags), so every handle of a segment keeps the
  caching its owner chose, as a dup(2) of it did.
- **Windows.** A handle opened without `FILE_FLAG_OVERLAPPED` takes its threads in turn: "If this
  flag is not specified, then I/O operations are serialized, even if the calls to the read and
  write functions specify an OVERLAPPED structure"; with it "the file can be used for simultaneous
  read and write operations" (CreateFileW). On such a handle "the system does not maintain the file
  pointer ... therefore the file position must be passed to the read and write functions in the
  related offset data members of the OVERLAPPED structure", and "if an application must perform
  multiple asynchronous I/O operations on a single file, it should wait on the event handle in the
  specific OVERLAPPED structure for each I/O operation, rather than on the common file handle"
  ("Synchronous and Asynchronous I/O"); each call needs "a valid and unique OVERLAPPED structure",
  and ReadFile "resets the event to a nonsignaled state when it begins the I/O operation"
  (ReadFile). std's positioned reads and writes wait on the handle itself
  (`library/std/src/sys/pal/windows/handle.rs`), so hyper-block issues ReadFile and WriteFile with
  an OVERLAPPED block a call and its thread's own manual-reset event (hyper-block `overlapped.rs`).
- **Durability.** fsync(2), of fdatasync as of fsync, "transfers ("flushes") all modified in-core
  data of (i.e., modified buffer cache pages for) the file referred to by the file descriptor fd",
  whichever descriptor wrote it; `F_FULLFSYNC` "does the same thing as fsync(2) then asks the
  drive to flush all buffered data to the permanent storage device" (macOS fcntl(2)), and
  FlushFileBuffers "flushes the buffers of a specified file and causes all buffered data to be
  written to a file" (Microsoft). A flush is the file's, not the handle's.

## Who closes a shared open file

Handles outlive one another: the issuer runs a store's accepted writes after the store's own handle
has gone, and maintenance workers retire after their shard (mantle's runtime retirement). hyper-raft
allows no shared ownership (its `clippy.toml` denies `Arc`; `CLAUDE.md` §1); it allows the shard
registry's protocol, an entry published once, read under counted pins for the span of a call, and
retired by its owner, the last pin freeing it (hyper-rt `retire::Pinned`, loom-modelled). The open
files are such an entry: the file's owner (`SegmentedFile::create`, `open`) holds them; every other
handle (`BlockFile::try_clone`) pins them for each transfer; the owner's drop retires them, and the
last transfer running closes them. The entry sits in a process-static slot that carries its owner's
generation, so a handle used after its owner closed the file is refused, typed, and never reaches
another file's descriptors in a slot reused since. mantle's ranges bench and tests hold the owner
apart, past the runtime and the issuer, so no handle outlives it in their teardown.

## Flushes

Each handle kept marks of the segments it wrote and flushed those alone. The device's issuer writes
a batch on its workers' handles and flushes it on one worker's (`issuer.rs`, `Op::Flush` handed to
the first idle worker), so a segment the batch wrote only through other workers' handles was not
flushed with it: the batch was answered durable on a flush that had covered none of those writes.
The writes and flushes are now counted on the segment, each write as it returns and each flush's
cover as it returns, so a flush through any handle covers every handle's completed writes, and one
that finds nothing past the last flush relies only on a flush that has returned.

## What sharing one open file costs

The earlier patch (2026-11-06-targets-hyper-block-fd.patch) shared one open file among a handle's
duplicates and cost fills about 12% at 10M on adacachy against an A/A of 3%, its cause untraced. A
microbenchmark of positioned 4 KiB reads of a 1 GiB file held in the page cache, from T threads
through one descriptor, through T duplicates of it and through T separate opens, three rounds
interleaved (diagnostic only, not committed; adacachy, Linux 6.x, btrfs, in a container):

| threads | one descriptor | duplicates | separate opens | one descriptor and a pin a read |
|---|---|---|---|---|
| 1 | 2.28 M/s | 2.32 M/s | 2.29 M/s | 2.18 M/s |
| 4 | 5.40 M/s | 5.74 M/s | 6.11 M/s | 5.43 M/s |
| 8 | 6.51 M/s | 6.26 M/s | 7.17 M/s | 5.88 M/s |
| 16 | 8.30 M/s | 8.07 M/s | 8.69 M/s | 7.90 M/s |
| 32 | 8.71 M/s | 8.97 M/s | 9.40 M/s | 8.37 M/s |

Medians of the three rounds. One descriptor and its duplicates read alike: they are one open file
description, whose reference count each system call takes and gives back, and whose read-ahead
state each buffered read writes (Linux `fdget`, `filemap_read`'s `ra->prev_pos`), from every thread.
Separate opens read 5-13% more at 4 to 32 threads; a shared counter pinned around each read costs
another 4-10% there. On macOS (Apple M4 Max, APFS, the same program) the three read alike at every
thread count: its reads stop scaling past four threads whatever the descriptors. These are reads
and nothing else; what a fill pays is measured where the system does its work, in
docs/measurements/2026-11-14-shared-segment-fds.md.
