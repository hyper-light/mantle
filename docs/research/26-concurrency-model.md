# 26 — Concurrency: device I/O in flight, many logical clients, and how threads are bounded

**Status:** research input for `docs/design/node.md` §1.2–§1.3, `docs/design/measurement.md`
and the benchmarks in `crates/mantle`. This is not a decision record; the decisions it
supports belong in the design.
**Compiled:** 2026-09-30.
**Scope:** why one OS thread per unit of concurrency failed, down to the kernel code that
panicked; how to keep many device I/Os in flight on Linux, Windows and macOS; how load
generators model many clients on few threads, and what closed and open loops measure; the
runtime models (thread-per-core, SEDA, threads with compiler or runtime help, kernel-bypass
runtimes) and how each bounds threads; how to wake one waiter among many; the cost each
mechanism charges per logical client at the scale of millions of agents; and which of it a
laptop needs and what each larger step adds. Earlier notes already cover the OS storage APIs
and io_uring's availability (02 §2.13, §4.9, §6), tokio's blocking pool (25 §1), and cores
among pools, Shenango and Caladan (25 §16).

---

## 0. How to read this note

**Citation tags.** `[KEY §section]` for papers, `[KEY path:line]` for source files at the
revision named in the Sources table, `[KEY "heading"]` for web pages. Earlier notes are cited
as "note 25 §x".

**Quotes.** Quotes in "double quotes" are verbatim from the fetched text. A line break in the
source is replaced by one space; ligatures that PDF extraction dropped are restored; words
hyphenated across a line break are joined; "..." marks an elision.

**Evidence labels.**
- **primary**: kernel or library source, man pages, vendor documentation, read directly.
- *(no label)*: a peer-reviewed paper, checked against its text.
- **NON-PEER-REVIEWED**: a tool's README, an engineering note or a blog.
- **MEASURED**: a value read on the development machine on 2026-09-30 with `sysctl`, `ioreg`
  or `sw_vers` (read-only queries; no mantle code was built or run for this note). The
  machine: Mac17,6, 18 cores (`hw.perflevel0.physicalcpu` 6, `hw.perflevel1.physicalcpu` 12),
  macOS 26.4.1 (25E253), kernel `xnu-12377.101.15~1/RELEASE_ARM64_T6050`.
- **DERIVED**: arithmetic on stated facts. **INFERENCE**: reasoning in this note that the
  sources do not state. **UNVERIFIED**: not confirmed against a primary text.

**Method.** Sources were fetched on 2026-09-30 as raw text: GitHub raw files at the tags or
commits named below, man-page sources from the man-pages and liburing repositories, Microsoft
documentation from the MicrosoftDocs repositories, and papers as PDFs converted with
`pdftotext`. Line numbers are those of the fetched revision.

---

## Sources

| Key | Source | Label |
|---|---|---|
| PSYNCH | Apple libpthread `kern/kern_synch.c` at tag `libpthread-539.100.4` (the kernel side of pthread mutexes and condition variables, built as the `com.apple.kec.pthread` kernel extension) | primary (source) |
| LIBPTHREAD | Apple libpthread `src/pthread_cond.c` at `libpthread-539.100.4` | primary (source) |
| XNU | Apple xnu at tag `xnu-12377.101.15` (the running kernel's version) for `osfmk/kern/locks.c` and `osfmk/arm64/machine_routines.c`; at `xnu-12377.121.6` (newest tag) for `bsd/pthread/pthread_shims.c`, `bsd/pthread/pthread_workqueue.c`, `bsd/pthread/workqueue_internal.h`, `bsd/kern/kern_aio.c`, `bsd/conf/param.c`, `osfmk/kern/waitq.c`, `osfmk/kern/sched_prim.c`, `osfmk/kern/thread.c`, `config/MASTER`. The two `locks.c` revisions are identical at the lines cited. | primary (source) |
| MACSDK | macOS SDK headers `os/os_sync_wait_on_address.h`, `sys/aio.h` (Command Line Tools SDK on the development machine); `man 2 aio_read` | primary |
| APPLE-THR | Apple, *Threading Programming Guide*, "Thread Management", §"Thread Costs", Table 2-1. https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/Multithreading/CreatingThreads/CreatingThreads.html | primary (vendor documentation, archived guide) |
| DISPATCH | Apple libdispatch `src/io.c`, `src/io_internal.h`, main branch (newest tag `libdispatch-1542.100.32`) | primary (source) |
| RUST | Rust standard library at tag `1.94.1` (the toolchain on the development machine): `library/std/src/sys/sync/condvar/mod.rs`, `.../condvar/pthread.rs`, `.../thread_parking/mod.rs`, `.../thread_parking/darwin.rs`, `library/std/src/sys/pal/unix/sync/condvar.rs`, `library/std/src/sync/barrier.rs`, `library/std/src/thread/mod.rs` | primary (source) |
| FIO | fio `HOWTO.rst`, `engines/posixaio.c`, `engines/windowsaio.c`, master at e27aa8b47c371491044ce9d7b3ee2f8488ba91b4 | primary (tool documentation and source), NON-PEER-REVIEWED |
| URING | J. Axboe, "Efficient IO with io_uring", 2019, https://kernel.dk/io_uring.pdf, read from the Internet Archive (kernel.dk served a certificate for another host) | NON-PEER-REVIEWED (author's design note) |
| LIBURING | liburing man pages `io_uring_setup.2`, `io_uring_register.2`, `io_uring_enter.2`, master at 78dce99b660fd1caaf70ef6573a88bae9bbf6476 | primary |
| LINUX | Linux master at 551c722f40809618230001baccf219193e22fc5a: `io_uring/io_uring.h`, `io_uring/io-wq.c`, `io_uring/tctx.c`, `Documentation/admin-guide/sysctl/kernel.rst`, `Documentation/arch/x86/kernel-stacks.rst`, `Documentation/ABI/stable/sysfs-block` | primary (source and kernel documentation) |
| MANPAGES | Linux man-pages: `FUTEX_WAKE(2const)`, `FUTEX_CMP_REQUEUE(2const)`, `pthread_create(3)`, from git.kernel.org man-pages.git | primary |
| MS-IOCP | Microsoft, "I/O Completion Ports", MicrosoftDocs/win32 `desktop-src/FileIO/i-o-completion-ports.md` | primary |
| MS-SYNC | Microsoft, "Synchronous and Asynchronous I/O", `desktop-src/FileIO/synchronous-and-asynchronous-i-o.md` | primary |
| MS-KB156932 | Microsoft, "Asynchronous disk I/O appears synchronous" (KB 156932), https://learn.microsoft.com/en-us/troubleshoot/windows/win32/asynchronous-disk-io-synchronous, archive commit 468430500b8772305fc0af80eac06220b3103578 | primary (archived support article) |
| MS-THREAD | Microsoft, `CreateThread`, MicrosoftDocs/sdk-api `nf-processthreadsapi-createthread.md`; `WaitOnAddress`, `nf-synchapi-waitonaddress.md` | primary |
| OPENCLOSED | B. Schroeder, A. Wierman, M. Harchol-Balter. "Open Versus Closed: A Cautionary Tale." NSDI '06, pp. 239–252. https://www.usenix.org/legacy/event/nsdi06/tech/full_papers/schroeder/schroeder.pdf | peer-reviewed |
| WRK2 | G. Tene, wrk2 `README.md`, master; W. Glozer, wrk `README.md`, master | NON-PEER-REVIEWED (tool READMEs) |
| YCSB | B. F. Cooper, A. Silberstein, E. Tam, R. Ramakrishnan, R. Sears. "Benchmarking Cloud Serving Systems with YCSB." SoCC '10 | peer-reviewed |
| DBBENCH | RocksDB `tools/db_bench_tool.cc`, main at 11516577ab2ab5a319b630258ab569e2c9f812e4 | primary (source), NON-PEER-REVIEWED |
| MEMTIER | memtier_benchmark `memtier_benchmark.cpp`, `README.md`, master at abdbb3564a79439c74d18411130b3d0835d2ecd2 | primary (source), NON-PEER-REVIEWED |
| SEDA | M. Welsh, D. Culler, E. Brewer. "SEDA: An Architecture for Well-Conditioned, Scalable Internet Services." SOSP '01. https://www.sosp.org/2001/papers/welsh.pdf | peer-reviewed |
| EVENTS | R. von Behren, J. Condit, E. Brewer. "Why Events Are A Bad Idea (for high-concurrency servers)." HotOS IX, 2003. https://www.usenix.org/legacy/events/hotos03/tech/full_papers/vonbehren/vonbehren_html/ | peer-reviewed (workshop) |
| CAPRICCIO | R. von Behren, J. Condit, F. Zhou, G. C. Necula, E. Brewer. "Capriccio: Scalable Threads for Internet Services." SOSP '03. https://people.eecs.berkeley.edu/~brewer/papers/capriccio-sosp-2003.pdf | peer-reviewed |
| LOZI | J.-P. Lozi, B. Lepers, J. Funston, F. Gaud, V. Quéma, A. Fedorova. "The Linux Scheduler: a Decade of Wasted Cores." EuroSys '16 | peer-reviewed |
| SHENANGO, CALADAN | as in note 25's Sources table | peer-reviewed |
| SEASTAR | seastar.io, "Shared-nothing Design"; Seastar `doc/tutorial.md`, master | primary (project documentation), NON-PEER-REVIEWED |
| TOKIO-TUT | tokio website, `content/tokio/tutorial/spawning.md`, master | primary (project documentation) |

---

## 1. The failure

### 1.1 Where mantle's threads come from today

- **Measurement.** `crates/disk/src/measure.rs` realises a job's depth "as that many threads
  issuing blocking positional I/O" (module doc) and starts them through
  `crates/disk/src/workers.rs` `spawn_all`, which holds every worker at a `Latch` (a `Mutex`
  and one `Condvar`) until all have started and then calls `notify_all` (`workers.rs:21–27`).
  A calibration ladder doubles the depth up to `depth_for(queue_depth)`, the queue the
  operating system reports (`calibrate.rs:63–69`): 253 on the development machine's NVMe
  controller (`IOCommandPoolSize`, MEASURED with `ioreg`; `probe/macos.rs:304–308`). A depth
  the OS will not start threads for ends the ladder (`calibrate.rs:325`, `DiskError::Threads`).
- **The log benchmark.** `crates/mantle/src/bench_log.rs` drives the log with closed-loop
  replicas, one thread each (`bench_log.rs:289–290`); without `--replicas` the ladder doubles
  "while appends a second still grow" and ends when "The machine will not start that many
  replicas' threads" (`bench_log.rs:156–160`). Each replica calls `Log::write_waiting`, which
  waits for queue room on the log's shared `room: Condvar` (`crates/log/src/lib.rs:296`,
  `send` at 470–506). Every answered submission calls `Shared::release`, which ends with
  `self.room.notify_all()` (`lib.rs:312–324`); the writer's `fence` does the same
  (`writer.rs:400–405`).
- **The chunk benchmark.** `crates/mantle/src/bench.rs` starts one closed-loop worker thread
  per request in flight (`bench.rs:726–727, 829–830, 921–922, 990–991`).
- **The chunk store.** Each volume starts a writer, a cleaner and a scrubber (audit §14.1: 300
  threads at 100 volumes), and `write_together` starts a scoped thread for every region of a
  batch after the first (`crates/chunk/src/writer.rs:1730–1735`).

The read gate already does what §5 recommends: each waiting read parks on its own thread
handle, and a turn given back unparks exactly the reads it admits
(`crates/chunk/src/read.rs:105–153`).

### 1.2 On macOS a Rust `Condvar` is a psynch condition variable

Rust's standard library chooses its condition variable per platform. Linux, Windows (not
win7), FreeBSD, OpenBSD and others get the futex implementation; any other Unix, macOS
included, gets `pthread` [RUST `sys/sync/condvar/mod.rs:1–22`]. `Condvar::notify_all` there is
`libc::pthread_cond_broadcast` [RUST `sys/pal/unix/sync/condvar.rs:36`], and Apple's
`pthread_cond_broadcast` enters the kernel through `__psynch_cvbroad` [LIBPTHREAD
`pthread_cond.c:449`]. `std::sync::Barrier` is built on the same `Condvar` and wakes with
`notify_all` [RUST `sync/barrier.rs:31, 133`]. Thread parking is different: on Apple targets
`std::thread::park` uses one libdispatch semaphore per thread, because "Darwin actually has
futex syscalls (`__ulock_wait`/`__ulock_wake`), but they cannot be used in `std` because they
are non-public" [RUST `thread_parking/darwin.rs:1–11`].

### 1.3 What the kernel does on a broadcast

Each psynch wait queue is protected by a spinlock: `lck_spin_t kw_lock; /* spinlock
protecting this structure */` [PSYNCH `kern_synch.c:168`], taken by `ksyn_wqlock` with
`lck_spin_lock` [PSYNCH 521–524]. The unlock carries a telling comment: "remove timeout
override when rdar://96649414 is addressed", followed by
`pthread_kern->abandon_preemption_disable_measurement()` [PSYNCH 527–533]. INFERENCE: Apple
knows that this lock can be held, with preemption disabled, longer than the scheduler's
preemption-disable measurement tolerates, and suppresses that measurement rather than
shortening the hold.

`__psynch_cvbroad` accepts a broadcast unless the waiter count it is told exceeds the task's
thread maximum ("cvbroad: difference greater than maximum possible thread count", [PSYNCH
1134–1147]). It takes `kw_lock` [1083], calls `ksyn_handle_cvbroad` [1101], and releases the
lock only after it returns [1112]. `ksyn_handle_cvbroad` walks the whole waiter list with
`TAILQ_FOREACH_SAFE` [2726–2737] and, for every waiter in range, "Wake only non-canceled
threads waiting on this CV", calling `ksyn_signal` on each [2734–2737]. `ksyn_signal` calls
the kernel shim `psynch_wait_wakeup` [2034–2064], which for a condition variable (only plain
mutexes use turnstiles: `_kwq_use_turnstile` returns `_kwq_type(kwq) == KSYN_WQTYPE_MTX`,
[PSYNCH 234–240]) calls `thread_wakeup_thread((event_t)kwq, th)` [XNU
`bsd/pthread/pthread_shims.c:307`]. That locks the global wait queue the event hashes to
(`&global_waitqs[os_hash_uint64(event) & (g_num_waitqs - 1)]`, [XNU `osfmk/kern/waitq.c:394`]),
locks the thread, and makes it runnable (`waitq_wakeup64_thread_and_unlock`, waitq.c
1682–1716). Every waiter on one condition variable waits on the same event, so they share one
global wait-queue bucket (DERIVED from the hash of a single event).

So one broadcast to N waiters is N wait-queue lock/unlock pairs, N thread locks and N
scheduler wake-ups, all inside one spinlock hold with preemption disabled: O(N) work that no
other CPU can enter. Waiting is not cheap either: a waiter enters with `ksyn_wait` [PSYNCH
1951], which inserts into the same queue under the same lock with `ksyn_queue_insert(...,
SEQFIT)` [1284]; an insertion that lands neither at the head nor the tail scans the list
[2405], also O(N).

### 1.4 The panic

The panic string "Spinlock[...] ... timeout" is produced by `hw_spin_timeout_panic`, whose
`panic("Spinlock[%p] " HW_SPIN_TIMEOUT_FMT ...` call spans lines 787–798 of `locks.c` in the
running kernel's source [XNU `osfmk/kern/locks.c:787–798`]; the report's `@locks.c:798` is the
end of that call. It is the timeout policy of every `hw_lock_t`, which `lck_spin_t` is built
on: `.hwsp_name = "hw_lock_t", .hwsp_timeout_atomic = &lock_panic_timeout, .hwsp_op_timeout =
hw_spin_timeout_panic` [locks.c:800–805]. On arm64 the related `LockTimeOut` defaults to "6e6
/* 0.25s */" timebase ticks [XNU `osfmk/arm64/machine_routines.c:114`]; where
`lock_panic_timeout` is set from it was not traced (§10).

**The mechanism (INFERENCE from §1.1–§1.4).** In the log ladder, thousands of replica threads
wait on `room`. The writer answers a frame's submissions one by one, and each answer's
`release` broadcasts to every waiter. Each broadcast holds `kw_lock` for O(N) wake-ups; each
woken thread takes the user mutex, finds no room (the queue admits `queue_submissions` at a
time and `GROUP_SUBMISSIONS = 2` per group, `lib.rs:280`), and re-enters `ksyn_wait`, which
needs `kw_lock` again. With K answers a frame and N waiters, a frame costs K·N wake-ups, all
serialised on one spinlock, while N re-waits spin for it. At N in the thousands the spinners
wait past the lock timeout and the kernel panics with the pthread kext in the backtrace, which
is what the five reports show. Nothing here is specific to an error in mantle's logic; the
design made an O(N) kernel operation O(K·N) per frame and let N grow until the OS refused a
thread.

**The lesson.** The thread count the OS will *create* is not the limit that matters. The OS
creates 16,384 threads in one task on this machine (`kern.num_taskthreads`, MEASURED), and
the kernel's own check in `__psynch_cvbroad` admits a broadcast to that many. What fails is any
operation whose kernel cost grows with the number of waiters on one object. No vendor
documents a "safe" number of waiters on one condition variable; the design has to make that
number small by construction.

### 1.5 What the operating systems document about thread counts

- **macOS.** The task and system limits are `kern.num_taskthreads` 16,384 and
  `kern.num_threads` 81,920 (MEASURED); the source's configured base is `CONFIG_THREAD_MAX=2560`
  for `<medium,large,xlarge>` kernels [XNU `config/MASTER:658`], scaled at boot (DERIVED: the
  measured values exceed it). Apple's own thread pool for a process, the workqueue under GCD
  and Swift concurrency, caps itself far lower: `wq_max_threads = WORKQUEUE_MAXTHREADS` with
  `#define WORKQUEUE_MAXTHREADS 512` [XNU `pthread_workqueue.c:146`,
  `workqueue_internal.h:290`], and the constrained pool at
  "wq_max_constrained_threads = max(64, N_CPU * WORKQUEUE_CONSTRAINED_FACTOR)" with the factor
  5 [workqueue_internal.h:62–67]; MEASURED `kern.wq_max_threads` 512 and
  `kern.wq_max_constrained_threads` 90 (5 × 18 cores). Each thread costs "Approximately 1 KB"
  of kernel data structures, "much of which is allocated as wired memory and therefore cannot
  be paged to disk", and a stack of "512 KB (secondary threads)", of which "the actual pages
  associated with that memory are not created until they are needed" [APPLE-THR Table 2-1].
- **Linux.** `threads-max` is set at boot "such that even if the maximum number of threads is
  created, the thread structures occupy only a part (1/8th) of the available RAM pages"
  [LINUX `Documentation/admin-guide/sysctl/kernel.rst:1591–1600`]. Each thread has a kernel
  stack of "THREAD_SIZE (4*PAGE_SIZE)", 16 KiB on x86-64 [LINUX
  `Documentation/arch/x86/kernel-stacks.rst`]. A new thread's default user stack is
  `RLIMIT_STACK`, typically 8 MB [MANPAGES `pthread_create(3)`, NOTES and EXAMPLES].
- **Windows.** "The number of threads a process can create is limited by the available virtual
  memory. By default, every thread has one megabyte of stack space. ... However, your
  application will have better performance if you create one thread per processor and build
  queues of requests for which the application maintains the context information"
  [MS-THREAD `CreateThread`, Remarks, line 155].
- **Rust.** A thread's default stack "is 2 MiB on all Tier-1 platforms"
  [RUST `thread/mod.rs:127–128`].

None of these is a statement that N threads are safe. Apple's 512 is the strongest primary
evidence of what a vendor's own runtime is willing to run in one process; Microsoft's advice
is the thread-per-processor design of §4.

---

## 2. Keeping many device I/Os in flight

### 2.1 What fio does

fio separates the two quantities mantle's measurement conflates. `iodepth` is the "Number of
I/O units to keep in flight against the file", and "increasing *iodepth* beyond 1 will not
affect synchronous ioengines" [FIO `HOWTO.rst:3541–3550`]. `numjobs` makes "clones of this job.
Each clone of job is spawned as an independent thread or process" [HOWTO.rst:677–683]. Depth
comes from an asynchronous engine on one thread: `io_uring` ("Fast Linux native asynchronous
I/O"), `libaio` ("Linux may only support queued behavior with non-buffered I/O (set
``direct=1``"), `posixaio` (`aio_read(3)`, `aio_write(3)`), and `windowsaio` ("Windows native
asynchronous I/O. Default on Windows") [HOWTO.rst:2145–2195]. `psync` is the default "on all
supported operating systems except for Windows" [2156–2158]. Batching is explicit:
`iodepth_batch_submit` "defines how many pieces of I/O to submit at once" and
`iodepth_batch_complete_min/max` how many to reap [3552–3591]. Even then fio warns: "Even async
engines may impose OS restrictions causing the desired depth not to be achieved. ... Keep an
eye on the I/O depth distribution in the fio output to verify that the achieved depth is as
expected" [3544–3550].

fio's `windowsaio` opens with `FILE_FLAG_OVERLAPPED`, associates each file with one completion
port and reaps with `GetQueuedCompletionStatusEx` from one completion thread
[FIO `engines/windowsaio.c:106, 131, 206, 281, 352, 497–513`]. Its `posixaio` submits with
`aio_read`/`aio_write` and waits with `aio_suspend` [`engines/posixaio.c:112, 134–136`].

**For mantle (INFERENCE).** A measured "depth" must be the number of I/Os the device actually
has in flight, which fio reports as a distribution. With a blocking engine, depth is the
number of threads blocked in the call; with an asynchronous engine it is the ring or port's
occupancy, and one thread suffices.

### 2.2 Linux: io_uring

io_uring is a submission ring and a completion ring shared with the kernel; one thread fills
submission entries and reaps completions, and "Completion events may arrive in any order"
[URING §4.2]. The entry is "aligned nicely in memory at 64 bytes" [URING §4.1]; the completion
entry holds a 64-bit `user_data`, a 32-bit result and 32-bit flags (DERIVED: 16 bytes). Depth
is the application's to bound: "it's possible for the application to drive a higher pending
request count than the SQ ring size would indicate. The application must take care not to do
so, or it could risk overflowing the CQ ring. By default, the CQ ring is twice the size of the
SQ ring" [URING §4.2]. The kernel caps a ring at `IORING_MAX_ENTRIES 32768` and the completion
ring at `2 * IORING_MAX_ENTRIES` [LINUX `io_uring/io_uring.h:171–172`]; `IORING_SETUP_CLAMP`
clamps a larger request instead of refusing it [LIBURING `io_uring_setup.2`].

Registration removes per-I/O costs: a registered file set avoids taking "a reference to said
file" on each submission, "a noticeable slowdown for high IOPS workloads", and registered
buffers avoid mapping "the application pages into the kernel" for every O_DIRECT I/O [URING
§8.1]. `IORING_SETUP_SQPOLL` creates "a kernel thread ... to perform submission queue polling"
so the application can "submit and reap I/Os without doing a single system call" [LIBURING
`io_uring_setup.2`], at the cost of a polling CPU [FIO HOWTO.rst:2581–2589].

io_uring itself is bounded the way §2.5 recommends. Work that cannot complete inline is handed
to an `io-wq` pool whose bounded workers are sized "Do QD, or 4 * CPUS, whatever is smallest":
`concurrency = min(ctx->sq_entries, 4 * num_online_cpus())` [LINUX `io_uring/tctx.c:40–43`];
unbounded workers are capped at `RLIMIT_NPROC` [`io_uring/io-wq.c:1279–1281`], and
`IORING_REGISTER_IOWQ_MAX_WORKERS` changes both per ring because "Sometimes this can be
excessive (or too little, for bounded)" [LIBURING `io_uring_register.2:519–526`]. The design
note records the measured gain: "io_uring is able to drive about 1.2M IOPS" without polling,
"twice the amount of IOPS" of Linux aio for the same workload, and 1.7M with polling [URING
§9.1]. Note 02 §2.13 covers detecting it: `ENOSYS` without `CONFIG_IO_URING`, `EPERM` when
`kernel.io_uring_disabled` forbids it, and the fallback to "a thread pool doing
`pread`/`pwrite`" (note 02 §6).

### 2.3 Windows: overlapped I/O and completion ports

A file opened with `FILE_FLAG_OVERLAPPED` and associated with a completion port queues one
completion packet per finished I/O "in first-in-first-out (FIFO) order", and "Threads that
block their execution on an I/O completion port are released in last-in-first-out (LIFO)
order" [MS-IOCP line 16, 21]. The port's concurrency value "limits the number of runnable
threads associated with the completion port", and "The best overall maximum value to pick for
the concurrency value is the number of CPUs on the computer" [lines 36, 43]; when a thread
blocks for another reason another may run, which is why the pool should hold "a minimum of
twice as many threads in the thread pool as there are processors" [line 45]. Depth is the
number of `OVERLAPPED` structures outstanding: "if you have three outstanding I/O operations,
you must use three `OVERLAPPED` structures" [MS-KB156932 §"Set up asynchronous I/O"].

The handle being overlapped does not make the I/O asynchronous. "the system reserves the right
to make an operation synchronous if it needs to" [MS-KB156932]; NTFS-compressed and encrypted
files are made synchronous; "any write operation to a file that extends its length will be
synchronous" unless the valid data length is advanced with `SetFileValidData`, which needs
`SeManageVolumePrivilege`; cached reads that miss go to "a limited pool of worker threads",
and "If you issue numerous I/O operations for data that is not in the cache, the cache manager
and memory manager become saturated and your requests are made synchronous". The remedy:
"The `FILE_FLAG_NO_BUFFERING` flag ... is the best way to guarantee that I/O requests are
asynchronous" [MS-KB156932 §"Asynchronous I/O still appears to be synchronous"]. MS-SYNC repeats
that an overlapped handle's calls "generally return immediately but can also behave
synchronously" [line 40]. Windows 11 adds IoRing (note 02 §4.9), which this note does not
re-examine.

**For mantle (INFERENCE).** On Windows the measurement issues `ReadFile`/`WriteFile` with
`FILE_FLAG_NO_BUFFERING` against a file already allocated and written to its full length (so
no write extends it), keeps `depth` `OVERLAPPED` slots outstanding, and reaps with
`GetQueuedCompletionStatusEx` on the same thread. A call that returns `TRUE` completed
synchronously and must be counted as such, so the achieved depth is measured, as fio advises,
not assumed.

### 2.4 macOS: what asynchronous file I/O exists

**POSIX AIO is a kernel thread pool with a per-process depth of 16.** XNU's implementation is
"support for the POSIX 1003.1B AIO/LIO facility" [XNU `bsd/kern/kern_aio.c:30`] in which kernel
worker threads perform each request with the ordinary synchronous file path: the worker switches
to the caller's address space and calls `do_aio_read`, `do_aio_write` or `do_aio_fsync`
[kern_aio.c:1800–1810 for the original workers, 2685–2719 for the workqueue], which call `dofileread`/`dofilewrite` [2256–2270, 2285–2290]. The new
workqueue implementation is the default (`TUNABLE(uint32_t, bootarg_aio_new_workq,
"aio_new_workq", 1)`, [kern_aio.c:118]) and creates kernel threads per process only while
`wa_nthreads < WORKQUEUE_AIO_MAXTHREADS`, with `#define WORKQUEUE_AIO_MAXTHREADS 16`
[kern_aio.c:225, 3384]. A process may have `aio_max_requests_per_process` requests in flight
[kern_aio.c:445], the system `aio_max_requests` [454]; MEASURED `kern.aioprocmax` 16,
`kern.aiomax` 90, `kern.aiothreads` 4. `lio_listio` takes at most `AIO_LISTIO_MAX` 16 entries
[MACSDK `sys/aio.h:126`]. A request past a limit fails: "[EAGAIN] Because of system resource
limitations, the request was not queued" [MACSDK `aio_read(2)`].

AIO also cannot carry mantle's durable write. `aio_fsync` performs `VNOP_FSYNC(vp,
sync_flag, ...)` with `MNT_WAIT` or `MNT_DWAIT` [kern_aio.c:2345–2381], the ordinary `fsync`
path, not `F_FULLFSYNC` (note 02 §6: macOS commits with `fcntl(fd, F_FULLFSYNC)`), and the
O_DSYNC variant is marked "(not supported yet)" [kern_aio.c:124].

**kqueue does not watch AIO from user space.** `EVFILT_AIO` exists, but `filt_aioattach`
refuses a registration without the kernel's flag: "Don't allow kevent registration from the
user-space", setting `EPERM` [kern_aio.c:2548–2560, the comment at 2552]. A completion is delivered as a kevent only
when the request itself asks for it with `SIGEV_KEVENT`, which the kernel registers on the
caller's behalf with `EV_KERNEL` [kern_aio.c:1557–1566, 1670–1683, 2001–2003].

**dispatch_io serialises a device.** libdispatch keeps one `dispatch_disk_t` per device, keyed
by `dev_t`, with a serial "pick queue" [DISPATCH `io.c:1754–1785`]. Its handler returns at once
`if (disk->io_active)` and otherwise sets `disk->io_active = true` and performs one operation
[io.c:2138–2180]; it looks ahead only to issue up to `DIO_MAX_PENDING_IO_REQS 6u // Pending
I/O read advises` (`F_RDADVISE` hints) [`io_internal.h:48`; io.c:2302–2304], and performs the
transfer itself with `read`/`pread`/`pwrite` [io.c:2471, 2481, 2554]. It is a sequential
streaming facility, not a way to put 253 commands on an NVMe queue.

**Conclusion for macOS.** A user process has no interface that keeps more than 16 file I/Os in
flight without one blocked thread per I/O, and none that carries `F_FULLFSYNC`. Capriccio states
the consequence for any blocking design: the kernel's queueing of disk requests in such a system
"is limited by the number of kernel threads used, which is often made deliberately small to
reduce kernel scheduling overhead" [CAPRICCIO §2.6 "I/O Performance", PDF p. 4]. To measure a depth of d on macOS,
mantle needs d threads blocked in `pread`/`pwrite`. That is acceptable for d up to the device's
queue (253 here), well under the 512 Apple's own pool allows (§1.5), provided nothing makes the
kernel's cost grow with d beyond one wake-up per completed I/O (§5).

### 2.5 The portable fallback: a bounded pool of reusable workers

Every system read here falls back to the same structure when there is no asynchronous disk
interface: SEDA's file layer, "Because the underlying operating system does not provide
nonblocking file I/O primitives, we are forced to make use of blocking I/O and a bounded thread
pool", with "only one thread may process events for a particular file at a time" [SEDA §4 "Asynchronous I/O Primitives",
"Asynchronous file I/O", PDF p. 8]; Capriccio "falls back on ... a pool of kernel threads for
disk I/O" [CAPRICCIO §2.2 "Implementation", PDF p. 3]; io_uring's own io-wq at `min(QD, 4 * CPUs)` (§2.2); Windows's
completion-port pool (§2.3); Apple's workqueue at 512 (§1.5); note 02 §6's fallback for Linux.

For mantle's purpose, depth realised with threads, the pool's size is not a CPU question: the
threads are blocked in the device, not running. The bound therefore comes from the device and
the measurement:

1. **The device's queue as the OS reports it**: `nr_requests` on Linux, which "controls how
   many requests may be allocated in the block layer" for "a single blk_mq_tags instance"
   [LINUX `sysfs-block:601–609`]; `IOCommandPoolSize` on macOS; the conservative
   `UNDESCRIBED_QUEUE_DEPTH` when the OS cannot say (`calibrate.rs:60`, CLAUDE.md §5).
2. **The measured knee.** The ladder stops doubling once throughput stops growing
   (`calibrate.rs:300–335`); the pool never needs more workers than the last step measured.
3. **A process-wide thread budget** that every pool draws from, so that devices × depth cannot
   add up past it. INFERENCE: on macOS the budget's ceiling is the OS's own pool ceiling read at
   run time (`kern.wq_max_threads`, 512 here), which is a vendor's statement of what one process
   may run; on Linux and Windows, where the native interface needs no threads per I/O, the pool
   exists only as the fallback (io_uring unavailable) and the same budget applies. Reaching the
   budget is a typed refusal naming the device and depth, as `DiskError::Threads` is today, but
   decided before any thread is started.

Workers are started once per calibration (or once per node for the reader pools of node.md
§1.3) and reused across points: a point hands each worker its job through the worker's own slot
and wakes it alone (§5), and the worker answers through its own completion slot. No latch is
needed, because a worker that has not yet received a job does no work; a pool that cannot start
all its workers starts none of the jobs and returns the error, as `spawn_all` intends. Thread
start-up cost (about 90 µs per thread in Apple's 2008 figure [APPLE-THR Table 2-1]) is paid
once, not per point, and stays outside every timed interval as audit P09 requires.

### 2.6 Recommendation for measurement

| Platform | Mechanism | Threads | Depth bound |
|---|---|---|---|
| Linux, io_uring usable | one ring per job, `O_DIRECT`, registered buffers and file, submit and reap on the job's thread; durable writes linked to `IORING_OP_FSYNC` with `IORING_FSYNC_DATASYNC` (note 02 §6) | 1 per job (plus io-wq's own, bounded by `min(QD, 4×CPUs)`) | min(device queue, knee, `IORING_MAX_ENTRIES`) |
| Windows | overlapped `ReadFile`/`WriteFile`, `FILE_FLAG_NO_BUFFERING`, file pre-written to full length, one completion port, `GetQueuedCompletionStatusEx` on the job's thread | 1 per job | min(device queue, knee); synchronous completions counted |
| macOS; Linux without io_uring | blocking `pread`/`pwrite` (and `F_FULLFSYNC` / `fdatasync`) on a reusable pool, per-worker job and completion slots | = depth | min(device queue, knee, process thread budget) |

The measurement reports achieved depth (in-flight count sampled at each completion) beside the
requested depth, as fio does. The test that proves the bound is in §8.

---

## 3. Many clients on few threads: load generation

### 3.1 Closed and open systems

"In a closed system, a new request is only triggered by the completion of a previous request"
and the number of users is "the multiprogramming level (MPL)"; in an open system "a request
completion does not trigger a new request: a new request is only triggered by a new user
arrival" [OPENCLOSED §2, p. 241]. The paper's principles that bear on mantle:

- "Principle (i): For a given load, mean response times are significantly lower in closed
  systems than in open systems." [§5.1, p. 246]
- "Principle (ii): As the MPL grows, closed systems become open, but convergence is slow for
  practical purposes." with "a significant difference between mean response times in closed
  and open systems even for an MPL of 1000" [§5.1, p. 247; §1, p. 240]
- "Principle (vii): A partly-open system behaves similarly to an open system when the expected
  number of requests per session is small (≤ 5 as a rule-of-thumb) and similarly to a closed
  system when the expected number of requests per session is large (≥ 10 as a rule-of-thumb)."
  [§6, p. 249]

**For mantle (INFERENCE).** A Raft replica is genuinely closed: each waits for its update to be
durable before the next `Ready` (replica.md §3), so `bench log`'s closed loop models a replica
correctly, and its MPL is the number of replicas. An S3 client population of agents is not:
millions of independent clients, each making a few requests per burst, is open or partly open
with few requests per session, where Principle (i) says a closed benchmark understates response
time by up to an order of magnitude. The chunk and gateway benchmarks need an open-loop mode.

### 3.2 Coordinated omission

wrk2's README states the problem: a generator that times "from the sending of the first byte
of the request to the time the complete response was received" while "each connection will
only begin to send a request after receiving a response" exhibits "a strong Coordinated
Omission effect, through which most of the high latency artifacts exhibited by the measured
server will be ignored". Its remedy: "constant throughput load generation with latency
measurement that takes the intended constant throughput into account", measuring "from the time
the transmission *should* have occurred"; the technique "requires a 'model' or 'plan' that can
provide the intended start time if each request" [WRK2 wrk2 `README.md:295–343`]
(NON-PEER-REVIEWED).

### 3.3 How the tools put many clients on few threads

- **wrk**: "-c, --connections: total number of HTTP connections to keep open with each thread
  handling N = connections/threads", over "scalable event notification systems such as epoll
  and kqueue" [WRK2 wrk README]. wrk2's example: "using 2 threads, keeping 100 HTTP connections
  open, and a constant throughput of 2000 requests per second".
- **memtier_benchmark**: "-c, --clients=NUMBER Number of clients per thread (default: 50)" and
  "-t, --threads=NUMBER Number of threads (default: 4)" [MEMTIER `memtier_benchmark.cpp:2664–2665`];
  `--rate-limiting` sets "The max number of requests to make per second from an individual
  connection" [2660].
- **db_bench**: `--threads` is "Number of concurrent threads to run" [DBBENCH
  `db_bench_tool.cc:371`]; `--num_multi_db` spreads them over several DBs [445]; and the
  coroutine benchmarks multiplex: `coro_jobs_per_thread` is the "number of concurrent coroutine
  jobs per executor thread. Each job does the same work as one benchmark thread; the executor
  has -threads threads. Values > 1 over-subscribe so a job whose IO is in flight yields its
  thread to another job" [2094–2099].
- **fio**: depth on one thread through an async engine (§2.1).
- **YCSB** is the exception: "the workload executor drives multiple client threads. Each thread
  executes a sequential series of operations", and "The threads throttle the rate at which they
  generate requests, so that we may directly control the offered load" [YCSB §5.1]. Its
  evaluation ran "up to 500 threads, depending on the desired offered throughput", and checked
  "that the client machine was not a bottleneck" [§6.1].

Every tool that scales to many clients keeps a logical client as data (a connection, a job, a
coroutine) and a thread per core or per few hundred clients. YCSB's thread-per-client works at
hundreds, and its authors still had to verify the generator was not the bottleneck.

### 3.4 Recommendation for mantle's benchmarks

1. **A logical client is a state machine, not a thread.** `bench log`'s replica is a record
   (group, next index, intended start time, the `Pending` it holds); `bench`'s worker likewise.
   A driver thread submits for every ready client and then takes completions. The log already
   offers `submit` → `Pending` with `poll`/`wait` (`lib.rs:161–172`); node.md §1.3 already
   plans to give `Pending` a `std::task::Waker`. With a waker that pushes the client's index onto
   the driver's ready queue (bounded by the client count) and unparks the driver, a completion
   costs one push and at most one unpark, never a scan of all clients.
2. **Driver threads = the cores the benchmark is granted**, at most, and fewer when one driver
   keeps the system saturated; the generator reports its own CPU time and, in open loop, its
   lateness (intended versus actual submit time), so a run where the generator was the
   bottleneck is visible, as YCSB checked by hand.
3. **Closed loop where the system is closed** (replicas; the log ladder), reporting MPL. **Open
   loop where it is not** (gateway, chunk puts and gets from clients), with arrivals from a
   stated process (fio's `rate_process=poisson` is the precedent [FIO HOWTO.rst:3721–3730]) and
   latency from the intended start, per §3.2.
4. **The ladder's end is stated, not discovered.** The replica ladder doubles while appends a
   second grow, up to the replicas a node will host (a derived bound, node.md §11 "the idle
   engine instance's cost"), never "until the OS refuses a thread". With clients as data the OS
   is never asked for a thread per client.

---

## 4. The runtime model

### 4.1 Thread-per-core, shared-nothing

Seastar "runs one application thread per core, and depends on explicit message passing, not
shared memory between threads. This design avoids slow, unscalable lock primitives and cache
bounces" [SEASTAR "Shared-nothing Design"]. Its tutorial gives the reason threads multiply
otherwise: "if we only have one thread per core, the event-handling functions must _never_
block, or the core will remain idle. But some existing programming languages and frameworks
leave the server author no choice but to use blocking functions, and therefore multiple
threads", with Cassandra's mmap'ed disk I/O as the example that forced "multiple threads per
CPU" [SEASTAR tutorial.md, introduction]. The model's precondition is an asynchronous disk
interface; Seastar's reactor backends are Linux's (linux-aio, io_uring, epoll: tutorial.md
§"reactor-backend", UNVERIFIED as a complete list).

### 4.2 SEDA

SEDA measured the cost of threads directly: "As the number of concurrent tasks increases,
throughput increases until the number of threads grows large, after which throughput degrades
substantially. Response time becomes unbounded as task queue lengths increase" [SEDA §2.1
"Thread-based concurrency", Figure 2 caption]. Bounding the pool avoids the collapse but moves the queue: "By limiting the number of
concurrent threads, the server can avoid throughput degradation ... However, this approach can
introduce a great deal of unfairness to clients: when all server threads are busy or blocked,
client requests queue up in the network" [§2.2]. SEDA's answer is stages joined by explicit,
bounded queues, each with a small thread pool sized by a controller [§3].

### 4.3 Threads without the cost: von Behren and Capriccio

"Why Events Are A Bad Idea" argues that "the weaknesses of threads are artifacts of specific
threading implementations and not inherent to the threading paradigm", and names the artifact:
"A major source of overhead is the presence of operations that are O(n) in the number of
threads" [EVENTS §3 "Performance"]. With those removed from a user-level scheduler, "Our optimized version of
Pth scales quite well up to 100,000 threads, easily matching the performance of the event-based
server" [EVENTS §3 "Performance"]. Capriccio builds that package: user-level threads over epoll and Linux AIO,
offering "scalability to 100,000 threads" [CAPRICCIO Abstract], with blocking disk I/O pushed
to "a pool of kernel threads" when AIO is absent [§2.2, PDF p. 3].

**For mantle (INFERENCE).** Both papers' 100,000 threads are *user-level*; the kernel sees a few.
The XNU broadcast in §1.3 is exactly the O(n) operation von Behren identifies, but in the kernel,
where no runtime can remove it. Rust's async tasks are the modern form of Capriccio's threads:
"Tasks in Tokio are very lightweight. Under the hood, they require only a single allocation and
64 bytes of memory. Applications should feel free to spawn thousands, if not millions of tasks"
[TOKIO-TUT, "Spawning"]; the future's own state adds to the 64 bytes (DERIVED).

### 4.4 Kernel-bypass runtimes

Shenango runs "lightweight user-level threads" (uthreads) on "each per-core kernel thread" and
balances them by work stealing; "Our design scales to thousands of uthreads, each capable of
performing arbitrary computation interspersed with synchronous I/O operations" [SHENANGO §3–§4].
It depends on an IOKernel on a dedicated core and kernel-bypass networking; Caladan adds a kernel
module, KSCHED [CALADAN §3]. Note 25 §16 covers their core reallocation. INFERENCE: neither is
portable to macOS or Windows or usable without privileges, so mantle can take their structure (a
bounded set of kernel threads, many cheap user-level units, queueing delay as the signal) but not
their mechanisms.

### 4.5 The OS scheduler is not a free resource

Lozi et al. found the Linux scheduler breaking its basic invariant: "Cores may stay idle for
seconds while ready threads are waiting in runqueues", with degradations "in the range 13-24%
for typical Linux workloads, and reach 138× in some corner cases" [LOZI Abstract, §1]. INFERENCE:
the fewer runnable threads a process presents beyond its cores, the less of its performance
rests on the OS's load balancing; thread-per-core designs keep that count at the core count.

### 4.6 How each design bounds threads

| Design | Kernel threads | What bounds them | Per logical client |
|---|---|---|---|
| Thread per client or per I/O | = clients or in-flight I/Os | nothing but the OS's refusal | a thread (§6) |
| Bounded blocking pool (SEDA stage, io-wq, Apple workqueue) | ≤ configured or derived bound | the pool's bound; excess waits in a bounded queue or is refused | a queue entry |
| Thread-per-core with async I/O (Seastar, tokio + io_uring/IOCP) | = cores, plus the OS's own bounded helpers | the core count | a task or state record |
| Thread-per-core with blocking device I/O on a side pool (node.md) | = cores per pool + Σ devices × measured depth | cores; each device's measured depth | a task or state record |

### 4.7 node.md checked against the evidence

node.md §1.2 decides: a tokio runtime with one worker per granted core; one metadata shard per
core; a coding pool of one per core; per device, reader threads up to the measured read depth;
one log writer per metadata device; per volume a writer, a cleaner and a scrubber. With C
granted cores, D data devices with measured read depths d_i, and L metadata devices, the
process's long-lived threads are 3C + Σ d_i + L + 3V for V volumes (DERIVED).

- **Holds:** the three per-core pools (Seastar's and Microsoft's thread-per-processor design,
  §4.1, §1.5), whose count does not grow with clients; ranges as records on shards, not threads
  (the shard's ready queue is §3.4's driver); one log writer per device; tokio tasks for
  connections and requests at 64 bytes plus state each (§4.3); Waker-based tickets crossing
  from device threads to tasks (node.md §1.3), which is §5's one-to-one wake.
- **Holds, with the bound made explicit:** readers per device at the measured depth are §2.5's
  pool. On macOS this is the only mechanism (§2.4); on Linux and Windows it is the portable path
  until §7's node step.
- **Does not hold:** 3V threads per node grow with volumes, not with devices or cores (audit
  §14.1), and `write_together`'s thread per region per batch is a thread start on the write
  path. One issuer per physical device (node.md §11 already names it) bounds both by devices.

---

## 5. Waking many waiters

### 5.1 Why a shared condition variable with `notify_all` is a hazard

The futex documentation names the pattern: when "all of the waiters that are woken need to
acquire another futex", "waking all of the threads in this manner would be pointless because all
except one of the threads would immediately block on lock A again"; this is the "thundering
herd" [MANPAGES `FUTEX_CMP_REQUEUE(2const)`, NOTES]. On Linux the cost is a herd of runnable
threads; on macOS §1.3 shows it is also O(N) work inside one kernel spinlock, which is what
panicked. `notify_all` on a condition variable shared by many waiters is therefore wrong in
mantle at any N that a client or peer can raise, and `Barrier` inherits the problem (§1.2).

### 5.2 One-to-one wake mechanisms

| Mechanism | Platform | What a wake costs | Source |
|---|---|---|---|
| per-waiter slot + `Thread::park`/`unpark` | all (std) | one wake of one thread: futex on Linux, `WaitOnAddress` family on Windows, one dispatch semaphore per thread on macOS | [RUST `thread_parking/mod.rs`, `darwin.rs:1–11`] |
| `FUTEX_WAKE` with `val` 1 | Linux | "wake up a single waiter" | [MANPAGES `FUTEX_WAKE(2const)`] |
| `WakeByAddressSingle` | Windows 8+ | "If **WakeByAddressSingle** is called, other waiting threads continue to wait" | [MS-THREAD `WaitOnAddress`, line 87] |
| `os_sync_wake_by_address_any` | macOS 14.4+ | "wakes up one waiter out of all those blocked in os_sync_wait_on_address" | [MACSDK `os_sync_wait_on_address.h:303–341`, availability line 39] |
| completion port | Windows | one packet releases one thread, LIFO, at most the port's concurrency running | [MS-IOCP lines 21, 25, 36] |
| io_uring completion ring | Linux | one CQE per finished I/O, reaped by the ring's owner | [URING §4.2] |
| `std::task::Waker` | all | the waker's own action: for a tokio task, scheduling that task; for a shard, pushing one range and unparking one thread (node.md §1.3) | [TOKIO-TUT]; node.md §1.3 |

INFERENCE: the per-waiter slot with `park`/`unpark`, as the read gate already does, is the
portable mechanism for threads; a `Waker` is the mechanism for anything that is not a thread
(tasks, ranges on shards, logical clients in a benchmark). Neither needs a kernel object per
waiter beyond a thread's own parker; neither does work for a waiter that is not being admitted.

### 5.3 The rule for mantle

1. **A wake reaches only the waiters it admits.** A queue with room hands that room to waiters
   in arrival order and wakes those it admitted, exactly as `read::Gate` does; the log's `room`
   becomes such a queue, its waiting list bounded (past it, `Busy`).
2. **An event that concerns every waiter (a fence, a shutdown) completes each waiter's slot**
   with the answer and wakes each once. Its cost is proportional to the waiters, which rule 1
   and the waiting list's bound make a bounded number; it happens once per fence, not once per
   answer.
3. **A `Condvar` is used only where its waiters are bounded by construction to a few known
   threads** (a writer and its owner), and with `notify_one` where one waiter can proceed.
   `notify_all`, `Barrier` and the `Latch` of `workers.rs` are not used where waiters are a
   pool, a client population or anything sized by measurement.

---

## 6. Cost per logical client at agent scale

The owner's requirement is millions of concurrent logical clients (agents) against one cell,
arriving in correlated bursts. Per-client costs of each mechanism:

| Mechanism | Memory per client | Kernel objects per client | Wake-ups per completion | Ceiling on one node | Source |
|---|---|---|---|---|---|
| OS thread per client | stack: 512 KiB reserved on macOS pthreads, 2 MiB in Rust, pages committed on touch; kernel: ~1 KiB wired (macOS), 16 KiB stack (Linux x86-64) | 1 thread | 1, or N under a shared broadcast | 16,384 per task on this Mac (MEASURED); Linux `threads-max` at ≤ 1/8 of RAM in thread structures | §1.5 |
| tokio task per client or request | 64 bytes + the future's state, one allocation | none | 1 (`Waker`) | memory budget / per-task state | [TOKIO-TUT] |
| state record on a shard or benchmark driver | the record (DERIVED: tens of bytes for a replica's group, index, start time and ticket) | none | 1 queue push, ≤ 1 unpark of the driver | memory budget / record | §3.4 |
| in-flight device I/O, io_uring | 64-byte SQE while submitting, 16-byte CQE | none | 0 (reaped in batches) | ≤ 32,768 entries a ring, ≤ device queue | §2.2 |
| in-flight device I/O, IOCP | one `OVERLAPPED` + buffer | none | ≤ 1 | ≤ device queue | §2.3 |
| in-flight device I/O, blocking pool | a pool slot; the I/O holds a thread | none (the thread is pooled) | 1 | ≤ pool bound (§2.5) | §2.5 |

**Clients one node supports** (DERIVED, as a formula; the per-client bytes must be measured).
With per-client state S bytes (task or record, plus the connection state of note 25 §4 when the
client holds a connection) and the node's memory budget M (node.md §1.4), the node holds at most
M / S clients; admission (node.md §2.5) refuses past it with `503 SlowDown`. Threads do not enter
the formula: they are 3C + Σ d_i + L whatever the client count. A per-connection S includes
socket buffers and QUIC or HTTP/2 flow-control windows, which note 25 §4 and §2 show are set by
configuration, so S is a configured product, not a guess. The benchmarks must report S as
measured bytes per idle and per active client (resident memory over client count at two client
counts, which separates the fixed cost from the slope); a claim of "a million agents per node"
names that run.

**Bursts.** A correlated burst of B arrivals costs B queue entries, not B threads; the bounded
queues of node.md §1.3 and §2.5 refuse the excess at a cost of one refusal each. The wake-up
cost of a burst is one wake per admitted request; the herd of §5.1 cannot occur because no
object has more waiters than its queue bound.

---

## 7. Stepped complexity: what each step of scale needs

The same binary runs at every step; each step adds a mechanism only when a limit it cannot
pass appears, and the limit is named with its evidence.

**Laptop** (one node, any of the three OSes, a few to 18 cores, one NVMe device, a few GB):
- Network and requests: tokio, C workers; clients and connections are tasks (§4.3, §6).
- Metadata: C shards, ranges as records, Waker-based tickets (node.md §1.3).
- Device I/O: on every OS the portable blocking pool per device at the measured read depth
  (§2.5), and one writer per device for writes; no io_uring or IOCP. Reason: one device's
  measured depth is at most its queue (253 here) and the pool is within Apple's 512 (§1.5,
  §2.4); the native interfaces would add a second code path that this step does not need.
  On macOS no other mechanism exists (§2.4).
- Wakes: per-waiter slots only (§5.3).
- Fixed cost (DERIVED): 3C + d + 1 + 3 threads for one data volume on one device sharing the
  metadata log, before per-device issuers; with the issuer of node.md §11, 3C + d + 2. On this
  18-core machine with d at the device's knee, at most 54 + 253 + 2 = 309 threads if the knee
  were at the full queue (DERIVED; the measured knee sets d).
- Benchmarks: driver threads ≤ C, logical clients as records (§3.4).

**Node** (a server: tens of cores, many devices):
- Limit that appears: Σ d_i over many devices. Each NVMe device's depth adds d_i blocked
  threads; at tens of devices the sum passes the process thread budget of §2.5. Step: on Linux,
  one io_uring per device issued from the device's issuer thread; on Windows, overlapped I/O on
  one completion port with concurrency C (§2.3); the pool remains the fallback where io_uring is
  disabled (note 02 §2.13). Evidence that the step pays: io_uring's measured 2× IOPS over aio and
  1.2M IOPS a core without polling [URING §9.1]; the trigger itself is the budget, not a guess.
- Limit that appears: volumes per device. Step: one issuer per physical device serving all its
  volumes (audit §14.1, node.md §11), replacing 3V threads.
- SQPOLL and registered buffers are adopted only if the node's measured submission cost (CPU
  per I/O) shows them worth a core; they are options, not defaults [FIO HOWTO.rst:2581–2589].

**Cell** (many nodes):
- Limit that appears: peers and connections. QUIC connections and streams are tasks on the
  same runtime; no threads are added. The bound is per-connection memory and the admission
  authorities (note 25 §4, node.md §2.5, §3.3).

**Region and fleet:**
- No change in a node's threads or wake mechanisms; client counts are an admission and memory
  question per node (§6). What changes is load generation: millions of open-loop clients need
  more than one generator machine, so the generator runs as several processes, each reporting
  its own lateness and CPU so that a generator bottleneck is never reported as the system's
  (§3.4, [YCSB §6.1]).

---

## 8. Tests that prove the bounds

1. **Threads a process creates.** An end-to-end test runs a measurement at the device's full
   reported depth and `bench log` with logical replicas at 100 times the driver count, samples
   the process's thread count throughout, and asserts it never exceeds the formula of §4.7 (or
   §2.5's pool bound for measurement). Thread count from the OS: Linux `/proc/self/status`
   `Threads:` (no `unsafe`); macOS `proc_pidinfo(PROC_PIDTASKINFO)` `pti_threadnum`; Windows a
   `CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD)` walk; the last two belong in the OS-interface
   files `scripts/check-contracts.py` lists (CLAUDE.md §7).
2. **Logical clients cost no threads.** The same benchmark at R and 10R logical clients shows
   the same peak thread count, and resident memory whose slope over R is the per-client S of §6.
3. **One wake per admission.** Instrumented builds (test only) count unparks and waker calls per
   queue; a test with W waiters and K completions asserts at most K + (waiters admitted) wakes,
   never K × W. A second asserts a fence wakes each waiter exactly once.
4. **Achieved depth.** The measurement's sampled in-flight count reaches the requested depth on
   each platform's mechanism (§2.6), or the run reports the shortfall (Windows synchronous
   completions, §2.3).
5. **Refusal before start.** A pool or ladder asked for more than the thread budget refuses with
   the typed error and has started no thread (counted as in test 1).
6. **Simulation.** The node simulator (node.md §9.1) runs the shard and benchmark drivers on
   one thread with logical clients in the millions, which is possible only if clients are
   records, and checks the queue bounds and wake counts under injected bursts.

---

## Recommendations

1. **Never one OS thread per logical client, replica, request or connection.** Logical units are
   records or tasks; kernel threads are 3C + Σ d_i + L (node.md §1.2) plus bounded helpers.
   [OPENCLOSED; SEDA §2.1; EVENTS §3; MS-THREAD; TOKIO-TUT; §1.3–§1.4]
2. **Replace every `notify_all` on a condition variable that a pool or population waits on**
   (the log's `room`, `workers.rs`'s `Latch`, any `Barrier`) with per-waiter slots woken one to
   one, in arrival order, admitting only what fits; a fence completes each slot once.
   [PSYNCH 1083–1112, 2726–2737; XNU locks.c:787–805; MANPAGES FUTEX_CMP_REQUEUE; §5.3]
3. **Measurement keeps depth in the kernel where the OS allows it**: io_uring on Linux, overlapped
   I/O on a completion port with `FILE_FLAG_NO_BUFFERING` on Windows, one thread per job; and a
   reusable pool of exactly `depth` blocking workers on macOS and wherever io_uring is
   unavailable. [FIO HOWTO.rst:3541–3550; URING §4.2, §8.1, §9.1; MS-IOCP; MS-KB156932;
   XNU kern_aio.c:225, 445, 2345–2382, 2548–2560; DISPATCH io.c:2138–2180; §2.6]
4. **Every pool's size is derived**: min(the device queue the OS reports, the measured knee, a
   process thread budget). The budget on macOS is read from `kern.wq_max_threads`, the ceiling
   Apple's own pool uses; reaching it is a typed refusal before any thread starts. Pools start
   once and are reused; start-up is outside every timed interval.
   [XNU pthread_workqueue.c:146, workqueue_internal.h:62–67, 290; LINUX tctx.c:40–43;
   sysfs-block:601–609; APPLE-THR Table 2-1; §2.5]
5. **Benchmarks multiplex logical clients on at most C driver threads** through `submit` and
   Waker-based completion; the replica ladder ends at a stated replica bound, not at the OS's
   refusal; generators report their own CPU and lateness. [WRK2; MEMTIER; DBBENCH 2094–2099;
   YCSB §5.1, §6.1; §3.4]
6. **Closed loop only where the system is closed** (replicas), open loop with intended-start
   latency everywhere clients are independent (S3 gateway, chunk store clients), with the arrival
   process stated. [OPENCLOSED Principles (i), (ii), (vii); WRK2; FIO HOWTO.rst:3721–3730]
7. **Keep node.md's per-core pools, shards, Waker tickets and per-device readers**, and replace
   per-volume writer, cleaner and scrubber threads and `write_together`'s per-batch threads with
   one issuer per physical device. [SEASTAR; MS-IOCP line 43; audit §14.1; §4.7]
8. **Adopt native asynchronous device I/O at the node step, not before**: when Σ d_i over a
   node's devices would pass the thread budget, Linux moves to io_uring per device and Windows
   to a completion port; the laptop runs the portable pool on every OS. [URING §9.1;
   LINUX io_uring.h:171–172; note 02 §2.13; §7]
9. **State and measure the per-client cost S** (bytes idle and active, per task and per
   connection) and derive the node's client capacity as M / S under admission; publish the run
   that measured it. [TOKIO-TUT; note 25 §4; node.md §1.4, §2.5; §6]
10. **Prove the bounds with tests that count**: the process's threads from the OS, wakes per
    admission, achieved depth, refusal before start, and a simulation with millions of logical
    clients. [§8]

## What remains unknown

- **The exact psynch hold time per wake-up** on this hardware, and therefore the waiter count at
  which a single broadcast alone (without the K-fold repetition) exceeds the lock timeout. The
  design removes the broadcast rather than relying on that number; reproducing the panic to
  measure it is not proposed.
- **Where `lock_panic_timeout` is initialised** on arm64 and whether it equals `LockTimeOut`
  (0.25 s); the definition was not located in the files read.
- **Whether `os_sync_wait_on_address` (macOS 14.4+) would let Rust's std or mantle drop the
  per-thread dispatch semaphore**; std does not use it in 1.94.1, and mantle's minimum macOS
  version would have to admit it.
- **The measured knee d on the development machine and on server NVMe**, which sets the
  laptop's real thread count (§7) and the node step's trigger; calibration measures it, this note
  does not.
- **Per-connection memory S for QUIC and HTTP connections under mantle's configuration**, which
  sets clients per node (§6).
- **IoRing on Windows 11** as an alternative to completion ports (note 02 §4.9), not evaluated.
- **The achieved-depth shortfall on Windows** for unbuffered I/O to a pre-written file on NTFS
  and ReFS; KB 156932 lists the causes of synchronous completion but does not quantify them on
  current Windows.
- **Whether macOS's per-process AIO limit (16) is raised by `kern.aioprocmax`** enough to use AIO
  for reads at depth; even so it cannot carry `F_FULLFSYNC` (§2.4), so it would serve reads only.
- **Seastar's complete list of reactor backends** (§4.1, UNVERIFIED).
