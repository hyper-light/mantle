# 44. Foreground isolation, page-cache pollution and space amplification

Research note, 2026-10-12. It collects evidence for three measured problems in mantle's single-node engine:

- **A.** Seeks ran at 0.46-0.51x RocksDB while maintenance rewrote about 550M entries beside them. The OS cache hit rate was 82%, against 91% for an arm with less maintenance left.
- **B.** The database file peaked at 146-154 GB, against 102 GB for RocksDB, for about 73 GB of live data.
- **C.** Under external CPU saturation, mantle's lead fell from 2.08x to 1.81x on reads and from 1.79x to 1.44x on seeks.

Tags: [paper] peer-reviewed paper (section, figure or table); [code] source at a pinned commit (path:line); [man] manual page; [docs] vendor documentation, vendor commit or PR text, or vendor blog; [inferred] my inference, not stated by any source. Nothing was built or benchmarked for this note.

## Sources

Papers
- [P1] O. Balmau, F. Dinu, W. Zwaenepoel, K. Gupta, R. Chandhiramoorthi, D. Didona. SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores. USENIX ATC 2019, 753-766. https://www.usenix.org/system/files/atc19-balmau.pdf
- [P2] A. Conway, A. Gupta, V. Chidambaram, M. Farach-Colton, R. Spillane, A. Tai, R. Johnson. SplinterDB: Closing the Bandwidth Gap for NVMe Key-Value Stores. USENIX ATC 2020. https://www.usenix.org/system/files/atc20-conway.pdf
- [P3] A. Conway, M. Farach-Colton, R. Johnson. SplinterDB and Maplets: Improving the Tradeoffs in Key-Value Store Compaction Policy. Proc. ACM Manag. Data 1(1), Art. 46, 2023. doi:10.1145/3588726. **Only the abstract was retrieved** (publisher-deposited abstract in the Crossref record); the ACM DL refused automated access to the full text.
- [P4] N. Dayan, S. Idreos. Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree Based Key-Value Stores via Adaptive Removal of Superfluous Merging. SIGMOD 2018. https://nivdayan.github.io/dostoevsky.pdf
- [P5] N. Dayan, T. Weiss, S. Dashevsky, M. Pan, E. Bortnikov, M. Twitto. Spooky: Granulating LSM-Tree Compactions Correctly. PVLDB 15(11):3071-3084, 2022. https://www.vldb.org/pvldb/vol15/p3071-dayan.pdf
- [P6] C. Luo, M. J. Carey. On Performance Stability in LSM-based Storage Systems. PVLDB 13(4):449-462, 2019. https://www.vldb.org/pvldb/vol13/p449-luo.pdf
- [P7] O. Balmau, D. Didona, R. Guerraoui, W. Zwaenepoel, H. Yuan, A. Arora, K. Gupta, P. Konka. TRIAD: Creating Synergies Between Memory, Disk and Log in Log Structured Key-Value Stores. USENIX ATC 2017. https://www.usenix.org/system/files/conference/atc17/atc17-balmau.pdf
- [P8] B. Lepers, O. Balmau, K. Gupta, W. Zwaenepoel. KVell: the Design and Implementation of a Fast Persistent Key-Value Store. SOSP 2019, 447-461. Author copy: github.com/BLepers/KVell @ a6cbc0cb, sosp19-final40.pdf
- [P9] L. Yang et al. Leaper: A Learned Prefetcher for Cache Invalidation in LSM-tree based Storage Engines. PVLDB 13(11):1976-1989, 2020. https://www.vldb.org/pvldb/vol13/p1976-yang.pdf
- [P10] L. Guo, D. Teng, R. Lee, F. Chen, S. Ma, X. Zhang. Re-enabling high-speed caching for LSM-trees. arXiv:1606.02015, 2016 (preprint of the ICDCS 2017 LSbM-tree paper).
- [P11] M. Y. Ahmad, B. Kemme. Compaction management in distributed key-value datastores. PVLDB 8(8):850-861, 2015. https://www.vldb.org/pvldb/vol8/p850-ahmad.pdf
- [P12] S. Dong, M. Callaghan, L. Galanis, D. Borthakur, T. Savor, M. Stumm. Optimizing Space Amplification in RocksDB. CIDR 2017. https://www.cidrdb.org/cidr2017/papers/p82-dong-cidr17.pdf
- [P13] S. Dong, A. Kryczka, Y. Jin, M. Stumm. Evolution of Development Priorities in Key-value Stores Serving Large-scale Applications: The RocksDB Experience. FAST 2021. https://www.usenix.org/system/files/fast21-dong.pdf
- [P14] J. Li, N. K. Sharma, D. R. K. Ports, S. D. Gribble. Tales of the Tail: Hardware, OS, and Application-level Sources of Tail Latency. SoCC 2014. https://drkp.net/papers/latency-socc14.pdf
- [P15] H. Qin, Q. Li, J. Speiser, P. Kraft, J. Ousterhout. Arachne: Core-Aware Thread Management. OSDI 2018. https://www.usenix.org/system/files/osdi18-qin.pdf
- [P16] T. Zhang et al. FPGA-Accelerated Compactions for LSM-based Key-Value Store. FAST 2020. https://www.usenix.org/system/files/fast20-zhang_teng.pdf
- Not retrieved: R. Sears, R. Ramakrishnan, bLSM (SIGMOD 2012). It is cited here only through P6's evaluation of it.

Source code (pinned)
- [C1] RocksDB v11.8.1: github.com/facebook/rocksdb @ abeebd9630f11bd08c28b7bd43c7bdfc62050654
- [C2] SplinterDB: github.com/vmware/splinterdb @ 7d607b118b1f606c51430759adc75b069423afae
- [C3] Seastar: github.com/scylladb/seastar @ cdc45209f1d615b8957eae54ece47d1d61abd53d
- [C4] ScyllaDB: github.com/scylladb/scylladb @ 31f345d460b84cd16ef62b27faf461ae12a50442
- [C5] Linux v7.2: github.com/torvalds/linux @ 8d3ae59288f1e7d58d76558a6ee96d533bc5019f
- [C6] XNU xnu-12377.121.6: github.com/apple-oss-distributions/xnu @ ac9718fb1af618d5ce8678d0dc6e8a58f252216f. This includes doc/scheduler/sched_clutch_edge.md and the man pages getiopolicy_np(3) and fcntl(2).
- [C7] libpthread-539.100.4: github.com/apple-oss-distributions/libpthread @ 1f4f5265b319111142f1bf3a27d4484ef5a98314

Manuals, vendor documentation and vendor commits
- [M1] Linux man-pages 6.19 (git.kernel.org/pub/scm/docs/man-pages/man-pages.git @ adb436b2e4471218021d86f188fb58dec3946eb8): sched(7), ioprio_set(2), posix_fadvise(2), readv(2) and sched_setattr(2).
- [D1] Apple, "Prioritize Work at the Task Level", Energy Efficiency Guide for Mac Apps. https://developer.apple.com/library/archive/documentation/Performance/Conceptual/power_efficiency_guidelines_osx/PrioritizeWorkAtTheTaskLevel.html
- [D2] Apple, "Tuning your code's performance for Apple silicon". https://developer.apple.com/documentation/apple-silicon/tuning-your-code-s-performance-for-apple-silicon
- [D3] Microsoft Win32 documentation. github.com/MicrosoftDocs/sdk-api @ 656e9656bfaef34dd29cd7ee9e1146aec17e924c: SetThreadPriority, SetPriorityClass, SetThreadInformation, MEMORY_PRIORITY_INFORMATION, FILE_IO_PRIORITY_HINT_INFO, PRIORITY_HINT, CreateFileW. github.com/MicrosoftDocs/win32 @ ca8274decead49403d70ceeed4af8d4ca74eaf11: quality-of-service, priority-boosts, scheduling-priorities, file-caching, file-buffering.
- [D4] RocksDB wiki @ 98273e83a8c7897bdbcd01ffbaef39571ba949ee: Universal-Compaction, Direct-IO, Rate-Limiter and Thread-Pool.
- [D5] A. Kryczka, "Auto-tuned Rate Limiter", RocksDB blog, 2017-12-18. https://rocksdb.org/blog/2017/12/18/17-auto-tuned-rate-limiter.html
- [D6] RocksDB PR #14743, "Add use_direct_io_for_compaction_reads option", merged as 7affaee1c49ebc80cb213ad86fe7d2a3ad447da2 (2026-06-09). The measurements are in the PR description.
- [D7] Linux commits 8026e49bff9b "mm/filemap: add read support for RWF_DONTCACHE" (J. Axboe, 2025-01); e1bf79628453 "mm: kick writeback flusher for IOCB_DONTCACHE with targeted dirty tracking" (J. Layton, 2026-06, in v7.2); 857b158dc5e8 "sched/eevdf: Use sched_attr::sched_runtime to set request/slice suggestion" (2024-08, Linux 6.12). Also J. Axboe, "[PATCHSET v7 0/11] Uncached buffered IO", LKML, 2024-12-13.

## 1. Foreground/background isolation in storage engines

### 1.1 RocksDB

**OS priorities.**
- `LowerThreadPoolCPUPriority(pool)` defaults to `kLow`, which is `setpriority(..., 19)`. `kIdle` selects `SCHED_IDLE` [code C1 env/env_posix.cc:389-398; port/port_posix.cc:281-306].
- `LowerThreadPoolIOPriority` puts pool threads in `IOPRIO_CLASS_IDLE`. The code comment notes that this has an effect only "in conjunction with an I/O scheduler that supports I/O priorities" [code C1 util/threadpool_imp.cc:285-306].
- Both are compiled only under `OS_LINUX` and are no-ops on macOS and Windows [code same lines; env/env_posix.cc:380-387].
- So RocksDB had no OS-level separation between compaction and client threads on the test Mac [inferred].

**Rate limiter.**
- It is a token bucket. By default it charges only writes (`kWritesOnly`): flush writes at IO_HIGH and compaction writes at IO_LOW. `IO_USER` requests are always served first. `fairness` (default 10) lets lower priorities jump the queue one time in ten [code C1 include/rocksdb/rate_limiter.h:141-170; util/rate_limiter.cc:341-372].
- `auto_tuned` re-evaluates every 100 refill periods. The limit falls by a factor of 1.05 when fewer than 50% of periods drained the bucket, and rises by 1.05 when more than 90% did. When none drained, it drops to max/20. It always stays within [max/20, max] [code util/rate_limiter.cc:213-220, 434-482].
- It follows *background demand*, not foreground load. The only published measurement covers the background write rate: about 125 MB/s for 10 MB/s of ingest under a 1000 MB/s cap. No foreground latency is reported [docs D5].

**Debt bounds.**
- Writes are delayed at 20 L0 files or 64 GiB of pending compaction bytes, and stopped at 36 files or 256 GiB [code C1 db/column_family.cc:1016-1045; include/rocksdb/advanced_options.h:547, 554, 709, 717].
- The delay is a credit at `delayed_write_rate`, refilled every 1 ms [code db/write_controller.cc:51-101].
- Parallelism is bounded too:
  - `max_background_jobs` defaults to 2 [code include/rocksdb/options.h:900].
  - Without "compaction pressure", one compaction runs at a time ("throttle background compactions until we deem necessary") [code db/db_impl/db_impl_compaction_flush.cc:3461-3482].
  - Pressure starts at min(2×trigger, a quarter of the way to slowdown) L0 files, or at min(max(bottommost/8, 2×level-base), soft/4) pending bytes [code db/column_family.cc:936-999, 1163-1192].
- `waitforcompaction` in the comparator run means RocksDB answered queries with zero debt [inferred from the brief].

**Read path.**
- `Get` and `Seek` read with `pread` on the calling thread [code C1 env/io_posix.cc:775-800].
- `io_uring` paths exist only with liburing on Linux, and are submitted from the calling thread's own event base. Without it they fall back to the synchronous `Read` [code env/io_posix.cc:718-760].
- A page-cache hit costs one syscall and a miss costs one blocking wait. There is no cross-thread handoff [inferred].

**Writeback smoothing.** `bytes_per_sync` issues `sync_file_range(SYNC_FILE_RANGE_WRITE)` every N bytes "to smooth out write I/Os over time", and is set to 1 MB when the rate limiter is on. Without `sync_file_range` (macOS) it does nothing unless `strict_bytes_per_sync` makes it a full `Sync` [code include/rocksdb/options.h:1307-1324; env/io_posix.cc:1923-1949; include/rocksdb/file_system.h:1392-1404].

### 1.2 SILK (P1)

- **Problem.** Tail latency comes from writes blocking on a full memtable, when L0→L1 falls behind or concurrent compactions starve flushes. "Simply limiting bandwidth for internal operations... effectively postpones compactions" and makes later concurrency likelier [paper P1 §4.7, Lessons 1-2].
- **Mechanisms.**
  - Opportunistic bandwidth: a monitor thread measures client I/O C every 10 ms and sets the internal limit to I = T − C − ε. It changes the limit only when the change exceeds 10 MB/s [paper §5.2.1].
  - Priorities: flushes, then L0→L1, then higher compactions. L0→L1 preempts a higher-level compaction if needed and is never paused [paper §5.1, §5.2.2].
  - Higher compactions pause when user load is high [paper §5.2.2].
  - The thread count is sized from device bandwidth, not core count: four threads for a 200 MB/s drive [paper §5.2.2].
- **Effect.** Up to two orders of magnitude lower p99 in write-dominated workloads. "Reads do not suffer significantly", since Bloom filters keep most reads to one L0 file, and read-dominated YCSB loses at most 5% throughput [paper abstract, §6.2-6.3]. Setup: 20-core Xeon, 1 GB RAM cgroup, 200 MB/s budget, 4 internal threads [paper §6.1].
- **Reads.** Client operations are queued to a fixed worker pool and executed synchronously in RocksDB [paper §2.2].

### 1.3 Merge scheduling and query latency (P6; bLSM via P6)

- **Greedy versus fair scheduling.** A "greedy" scheduler gives the whole I/O budget to the merge with the fewest remaining bytes. This minimizes the number of components, and it "always improves query performance". Point lookups and short scans gain most, and tiering gains more than leveling [paper P6 §5.1.5, §5.2 "Impact on Query Performance"].
- **Forced writes.** Forcing merge output to SSD every 16 MB, instead of in large forces at merge end, "significantly reduces the percentile latencies of small queries... by 5x to 10x" [paper P6 §3.1, §5.2]. P6 reports this from its extended version.
- **bLSM.** Its spring-and-gear scheduler "still has large variances in its processing rate" [paper P6 §7].
- **Recommendation.** Use greedy scheduling at runtime and process writes as fast as possible. Catch-up after a stall "negatively impacts query performance" [paper P6 §5.2, §7].

### 1.4 SplinterDB (P2, C2)

- **Task system.**
  - Two task groups, memtable and normal, each with its own thread count [code C2 include/splinterdb/splinterdb.h:80-92].
  - Foreground *inserters* run a queued task when the queue exceeds `queue_scale_percent`% (default 100) of the group's threads: "high tail latencies" in exchange for queues that "are always short" [code splinterdb.h:139-181; src/task.c:407-422; src/core.c:2878].
  - Lookup paths never run tasks [code C2; the callers are inserts, memtable rotation, checkpoints and notification waits].
  - The ATC '20 evaluation configured SplinterDB "without background compaction threads" [paper P2 §6.1].
- **Reads.**
  - A cache miss is a synchronous `pread` on the calling thread [code src/clockcache.c:2312; src/platform_linux/laio.c:331-365].
  - libaio is used for asynchronous prefetch [paper §5.1]. The data passes through SplinterDB's own concurrent clock cache [paper §5.2].
  - The default open flags are `O_RDWR|O_CREAT`, without `O_DIRECT` [code src/platform_linux/platform_io.h:38].

### 1.5 Seastar and ScyllaDB (C3, C4)

- **Scheduling groups.**
  - Groups carry CPU shares ("Use numbers in the 1-1000 range") [code C3 include/seastar/core/scheduling.hh:85-95].
  - `set_shares` exists so that "the shares can be started at a low value, increased when the background job's backlog increases" [code scheduling.hh:336-347].
  - I/O priority classes are keyed by scheduling group, with shares and bandwidth caps [code include/seastar/core/io_queue.hh:129-131, 214-216].
- **I/O scheduler.** It models the disk as `bw_r/bw_r_max + bw_w/bw_w_max + iops_r/iops_r_max + iops_w/iops_w_max ≤ 1`, enforced by a token bucket holding one `io_latency_goal` of tokens, and detects disk-side queueing from the ratio of dispatched to completed requests [code C3 doc/io-scheduler.md].
- **ScyllaDB backlog controller.**
  - A proportional controller holds the backlog's derivative at zero [code C4 backlog_controller.hh:20-38].
  - Compaction shares follow control points: backlog 0 gives 50 shares, 1.5 gives 100, and 30 gives 1000 [code backlog_controller.hh:127-133; backlog_controller_fwd.hh:13].
  - The STCS backlog per SSTable is Bᵢ = Eᵢ·log₄(T/Sᵢ) [code compaction/size_tiered_backlog_tracker.hh:15-44].
  - Statements get 1000 shares and maintenance groups 200 [code main.cc:1274-1288].
- **Reads.**
  - `open_file_dma` files are "unbuffered and uncached" [code C3 include/seastar/core/seastar.hh:200-217].
  - "Each shard handles its own I/O processing... directly on its assigned application core", and the reactor reaps completions itself.
  - The optional asymmetric backend offloads I/O to *dedicated* cores and is "unlikely to improve I/O-bound workloads" [code C3 doc/reactor_backend_asymmetric_uring.md].
  - The task quota defaults to 500 µs [code C3 doc/tutorial.md:335-339].

### 1.6 Other measured evidence

- **KVell (P8).** Client threads push requests onto worker queues (one handoff), and workers batch `io_submit` [paper P8 §5.4, Algorithm 1]. On Optane, 4 KiB random writes over a dataset 3× RAM reached 10K/60K IOPS through mmap and the page cache (1/8 threads), 88K with direct I/O, and 376K with AIO at QD 64 [paper P8 Table 3]. RocksDB fell from 63K to 1.5K req/s during stalls [paper P8 §3.2].
- **Compaction CPU (P16).**
  - With short KV pairs (8-byte keys), computation is up to 60% of compaction time [paper P16 §2.2, Fig. 3].
  - Under a 75% lookup / 25% write mix, throughput rose with compaction threads up to 32 and then fell as CPUs saturated [paper P16 §1, Fig. 1].
- **OS scheduling and tail latency (P14).** With background processes sharing a core, p99 was 10-1000× ideal. "niceness... is not powerful enough", while realtime priority "nearly eliminat[es]" the effect (Linux CFS, 2014) [paper P14 §4.1]. Requiring specific workers for requests negates the benefit of multicore [paper §7].
- **Thread wakeup cost (P15).** A kernel-thread condition-variable wakeup across cores costs a median of 4,962 ns, best case on an idle machine. Arachne's user-level equivalent costs 251 ns [paper P15 Table 2].

## 2. OS mechanisms

### 2.1 macOS

**QoS classes and CPU.**
- `pthread_create` threads default to `QOS_CLASS_DEFAULT` [code C7 include/sys/qos.h:81-86].
- QoS maps to base priorities [code C6 osfmk/kern/thread_policy.c:84-90; osfmk/kern/sched.h:159-167]:

  | QoS | Base priority |
  |---|---|
  | USER_INTERACTIVE | 46 |
  | USER_INITIATED | 37 |
  | DEFAULT | 31 |
  | UTILITY | 20 |
  | BACKGROUND / MAINTENANCE | 4 |

- QoS degradation "only occurs during system resource contention, proportionally to the QOS class" [code C7 include/sys/qos.h:46-48].
- Calling `pthread_setschedparam` or similar on a thread opts it out of QoS permanently [code C7 include/pthread/qos.h:128-133]. Apple advises using QoS instead of `setpriority` [docs D2].
- QoS also sets "CPU and I/O throughput, and timer latency" [docs D1].

**I/O tiers.** DEFAULT and above use TIER0, UTILITY uses TIER1, BACKGROUND uses TIER2 and MAINTENANCE uses TIER3 [code C6 thread_policy.c:96-102].

**Scheduling under oversubscription (Clutch and Edge).**
- A thread's bucket follows its base priority: above 37 is FG, above 31 IN, above 20 DF, above 4 UT, otherwise BG. So UI→FG, USER_INITIATED→IN, DEFAULT→DF, UTILITY→UT [code C6 osfmk/kern/sched_clutch.c:345-361].
- Root buckets are chosen by EDF. A bucket's deadline is now + WCEL: FG 0, IN 37.5 ms, DF 75 ms, UT 150 ms, BG 250 ms. This "ensures that the bucket would be scheduled at WCEL[bucket] even in a heavily loaded system" [code C6 doc/scheduler/sched_clutch_edge.md:28-41].
- A "warp" lets a higher bucket run ahead of deadline order during bursts: FG 8 ms, IN 4, DF 2, UT 1, BG 0 [code :44-53].
- A starved bucket gets a window of one thread quantum [code :57]. Quanta are FG 10 ms, IN 8, DF 6, UT 4 and BG 2 [code :101-110].
- Inside a bucket, thread groups compete by an interactivity score: the ratio of blocked time to CPU time, computed per *(thread group, QoS) bucket group* [code :66-91, :142-152].
- Threads within that group use Mach timeshare decay over the group's load only [code :93-100].
- On Apple silicon, the performance controller recommends a cluster per (thread group, QoS) [code :188-194]. Apple: "the system is more likely to run background tasks on lower performance cores" [docs D2].
- **Caveat for C.** A thread group belongs to the jetsam coalition [code C6 osfmk/kern/coalition.c:1068-1079, 1726-1736], and a new task inherits all its parent's coalitions [code osfmk/kern/task.c:1824-1845].
  - So mantle and a busy-loop generator started from the same shell share one thread group [inferred].
  - At equal QoS they then share one bucket group, and the interactivity score cannot separate them.

**I/O policy.**
- `setiopolicy_np` (thread or process scope): IMPORTANT is the default; UTILITY is "throttled to prevent a significant impact"; THROTTLE is "for long-running I/O intensive background work"; PASSIVE I/O does not trigger throttling of others. A throttleable request "within a small time window of a request of higher priority" puts its thread to sleep, and an IMPORTANT request "may bypass a previously issued throttleable I/O request in kernel or driver queues" [man C6 bsd/man/man3/getiopolicy_np.3:60-106].
- Throttle windows are 25/100/500 ms and SSD sleep periods 5/15/25 ms for tiers 1-3 [code C6 bsd/miscfs/specfs/spec_vnops.c:181-191].
- The accounting happens in the device strategy routine [code spec_vnops.c:2770], and the sleep is taken at syscall return [code bsd/dev/arm/systemcalls.c:216-220]. So page-cache hits are not throttled [inferred].
- A cluster-layer read by a TIER2 or TIER3 thread (or with F_NOCACHE) sets `take_reference = 0`. Its pages get no reference, and new pages commit as "speculative" rather than "inactive" [code C6 bsd/vfs/vfs_cluster.c:4743-4758, 5202-5213].

**Wakeup latency.**
- No Apple source gives figures. The WCEL and quanta above are the documented bounds.
- macOS has no handoff-free asynchronous file read. POSIX AIO is served by kernel worker threads, `CONFIG_AIO_THREAD_COUNT=4` with 16 requests per process in the "medium" configuration that macOS arm64 uses [code C6 bsd/kern/kern_aio.c:1759; config/MASTER:250-268; config/MASTER.arm64.MacOSX:24].

### 2.2 Linux

**CPU.**
- Each nice step is a weight factor of 1.25 [man M1 sched(7)]. Weight is 1024 at nice 0 and 15 at nice 19; `SCHED_IDLE` has weight 3 [code C5 kernel/sched/core.c:10606-10615; kernel/sched/sched.h:2511].
- `SCHED_BATCH` takes "a small scheduling penalty with respect to wakeup behavior" [man sched(7)].
- EEVDF wakeup rules [code C5 kernel/sched/fair.c:9821-9850]:
  - A waking non-idle task preempts an idle current task. The comparison is made at the common cgroup level.
  - BATCH and IDLE wakees never preempt.
  - A wakee with a shorter slice may override the current task's slice protection (`PREEMPT_SHORT`).
- Slices can be set per thread through `sched_attr.sched_runtime`, clamped to 0.1-100 ms [code fair.c:5951-5965]. The kernel commit advises using the task's "periodic runtime at a high confidence interval (95%+)" [docs D7, 857b158dc5e8]. The man page still documents `sched_runtime` as SCHED_DEADLINE-only [man sched_setattr(2)].
- The default base slice is 0.7 ms × (1 + log2(min(ncpu, 8))), which is 2.8 ms on 8 or more CPUs [code fair.c:73-79, 192-211].
- No latency-nice field exists in `sched_attr` [code include/uapi/linux/sched/types.h:98-112].
- cgroup v2:
  - `cpu.weight` ranges from 1 to 10000, and `cpu.idle` makes a group SCHED_IDLE.
  - `cpu` is a *threaded* controller, so threads of one process can sit in different groups. `io` is not [code C5 Documentation/admin-guide/cgroup-v2.rst:312-336, 410-416, 1163-1175, 1255-1265].
  - `io.latency` throttles peers with looser targets [code cgroup-v2.rst:2238-2251].
- Autogrouping can split CPU by session, so Linux runs of C must control for it [man sched(7)].

**I/O priority.**
- "Support for io priorities is io scheduler dependent and currently supported by bfq and mq-deadline" [code C5 Documentation/block/ioprio.rst:9-12].
- Multi-queue devices default to the "none" scheduler [code block/elevator.c:743-758]. NVMe therefore ignores `ioprio_set` unless the scheduler is changed [inferred].
- mq-deadline ages lower classes after 10 s [code block/mq-deadline.c:36, 49-51, 107-111].
- I/O priority applies to reads and synchronous writes only, "not... asynchronous writes" [man ioprio_set(2)]. The page still describes CFQ, which was removed in 5.0. cgroup writeback attribution is the documented route for buffered writes [code cgroup-v2.rst:2178-2193].

### 2.3 Windows

- **Priorities.** `THREAD_MODE_BACKGROUND_BEGIN` "lowers the resource scheduling priorities of the thread" (current thread only), and such a thread "will never be starved". CPU priority alone is insufficient: "even an idle CPU priority thread can easily interfere with system responsiveness when it uses the disk and memory" [docs D3 SetThreadPriority].
- **Per-handle I/O hints.** `SetFileInformationByHandle(FileIoPriorityHintInfo)` takes VeryLow, Low or Normal. "Whether these priorities are supported and honored by the underlying drivers depends on their implementation" [docs D3 FILE_IO_PRIORITY_HINT_INFO, PRIORITY_HINT].
- **Memory priority.** `ThreadMemoryPriority` lowers the priority of pages "added to the process working set by that thread", which are then trimmed first [docs D3 SetThreadInformation, MEMORY_PRIORITY_INFORMATION].
- **EcoQoS.** It "always selects most efficient CPU frequency and schedules to efficient cores" [docs D3 quality-of-service.md].
- **Scheduler.** Threads with base priority 0-15 get a dynamic boost "when the wait conditions for a blocked thread are satisfied", for example after disk I/O [docs D3 priority-boosts.md], and a newly ready higher-priority thread preempts at once [docs D3 scheduling-priorities.md].

## 3. Page-cache pollution by background I/O

### 3.1 Linux

- **`POSIX_FADV_DONTNEED`.** It starts writeback (`filemap_flush_range`) and invalidates only full, clean, unmapped pages. Dirty pages are not freed [code C5 mm/fadvise.c:113-171]. The man page says to call `fsync` or `fdatasync` first [man posix_fadvise(2)].
- **`POSIX_FADV_NOREUSE`.**
  - It was a no-op from 2.6.18 to 6.2. Since 6.3 it lets reclaim "ignore access to mapped page cache" [man posix_fadvise(2)].
  - In v7.2 `FMODE_NOREUSE` is read only by `vma_has_recency()` [code C5 mm/fadvise.c:108-112; include/linux/mm_inline.h:622-631]. It therefore does nothing for `pread` [inferred from code].
  - RANDOM, SEQUENTIAL and NOREUSE apply per open file description [man posix_fadvise(2)].
- **`RWF_DONTCACHE` (6.14) semantics.**
  - Pages the operation *instantiates* are pruned after the read, or after writeback completes. Ranges "already in cache... will not be pruned", and a write kicks off writeback (in v7.2 through a flusher kick, not inline). It is "a hint, or best effort", and returns EOPNOTSUPP where unsupported [man readv(2); code C5 mm/filemap.c:2615-2720, 2758-2768, 1609-1645; include/linux/fs.h:2660-2675, 3452-3459].
  - Support at v7.2: ext4 and XFS set `FOP_DONTCACHE` [code fs/ext4/file.c:993; fs/xfs/xfs_file.c:2105]. Btrfs and block devices do not.
- **`RWF_DONTCACHE` measurements.**
  - Reads, 32 drives, 64 KiB requests [docs D7, 8026e49bff9b]:

    | Mode | Throughput | Reclaim CPU |
    |---|---|---|
    | Buffered | ~158 GB/s, falling to ~95 GB/s once the cache filled | kswapd on ~28 cores |
    | `RWF_DONTCACHE` | steady ~155-158 GB/s | no reclaim; "half the CPU" |

  - Writes, XFS on NVMe, after the v7.2 flusher change [docs D7, e1bf79628453]:

    | Single-stream metric | Before | After |
    |---|---|---|
    | Throughput | 298 MB/s | 897 MB/s |
    | p99.9 | 8.9 s | 28 ms |

    The multi-writer aggregate matched buffered I/O. The peak dirty pages in a 32-file test fell from 1.8 GB to 213 MB.

### 3.2 RocksDB direct I/O for compaction

- **Options** [code C1 include/rocksdb/options.h:1153-1196, 1274-1281; HISTORY.md:51]:
  - `use_direct_io_for_flush_and_compaction` covers writes.
  - `use_direct_io_for_compaction_reads`, new in 11.x, covers compaction inputs only. It is "useful when sequential compaction reads would otherwise evict the hot user-read working set".
  - `compaction_readahead_size` defaults to 2 MB.
- **Platform mapping.** O_DIRECT on Linux, `F_NOCACHE` on macOS, and `FILE_FLAG_NO_BUFFERING` on Windows [code env/fs_posix.cc:290, 361, 444; port/win/env_win.cc:247, 280, 372, 471].
- **Measured** [docs D6]: Linux 7.0 in a 14-vCPU VM on Apple silicon, btrfs, a 1 GB cgroup, MGLRU disabled. The workload was 4 readers over a hot set plus one writer at 200 MB/s.

  | Hot set (share of cache) | Configuration | Result |
  |---|---|---|
  | ~40% | Direct I/O for compaction reads and writes | p99 −46%, p99.99 −73%, p50 −23.5%, throughput +52% |
  | ~3% | Same | modest gains |
  | ~3% | Direct compaction reads alone | p99 regressed by 25% (lost kernel readahead) |
  | either | Direct I/O for *all* user reads | p50 regressed by 14.5% |

  The author expects no change when the hot set exceeds the cache.
- **Caveat.** "After enable direct I/O, compaction writes will no longer be in the OS page cache, so first read will do real IO" [docs D4 Direct-IO, note 3].

### 3.3 Compaction-induced cache misses (peer-reviewed)

- **LevelDB on HDD (P10).** With 1,000 writes/s, the hit ratio "fluctuates between 1% and 92%, with an average hit ratio 47%", against about 96% read-only. Read throughput fell to about 10% [paper P10 §I-A, Fig. 2].
- **Large compactions (P11).** They compete for resources while running, then cause misses because "all references to the input files are obsoleted". The authors warm the cache incrementally [paper P11 §1].
- **X-Engine (P9).** Invalidations caused "up to 10× latency spikes and 90%" QPS drops. Learned prefetching removed about 70% of invalidations and 99% of spikes [paper P9 §1, Fig. 1, abstract].

### 3.4 macOS

- **Scope of the flags.** `F_NOCACHE` and `F_RDAHEAD` set flags on the *open file*, not the vnode. `F_GLOBAL_NOCACHE` is the vnode variant [code C6 bsd/kern/kern_descrip.c:3687-3713; bsd/vfs/vfs_vnops.c:1153-1167]. A maintenance-only descriptor therefore leaves shard reads unaffected.
- **Read path in the cluster layer.**
  - Read-ahead is disabled for NOCACHE [code bsd/vfs/vfs_cluster.c:4784].
  - The copy path dumps newly read pages after the copy [code :5202-5205].
  - The direct path first copies "pages already in the cache" without taking a reference, then reads the rest directly [code :5596-5607].
  - Note 02 already covers the aligned direct-write conditions [code :3006-3036].
- **Unverified.** APFS is closed source, so whether APFS routes through this layer is UNVERIFIED (note 02 §3.7 says the same). Apple publishes no measurement.
- **No writeback-only flush.** `fcntl(2)` documents no "start writeback only" command; its flush commands are `F_FULLFSYNC` and `F_BARRIERFSYNC` [man C6 bsd/man/man2/fcntl.2:166-245].

### 3.5 Windows

- **Bypassing the cache.** `FILE_FLAG_NO_BUFFERING` disables system caching, with sector-alignment rules [docs D3 file-caching.md, file-buffering.md].
- **Access hints.** `FILE_FLAG_SEQUENTIAL_SCAN` and `FILE_FLAG_RANDOM_ACCESS` are documented only as hints that "can... optimize file caching" [docs D3 CreateFileW, "Caching Behavior"].
- **Gaps.** No documented per-I/O "do not cache" flag or post-hoc eviction call was found [inferred from D3]. Memory priority (§2.3) is the documented lever for page lifetime.

## 4. Space amplification in tiered and Bε-tree designs

- **Definitions.**
  - amp = N/unique − 1 [paper P4 §3].
  - total = 1 + transient + durable, where transient is the space for "the original and merged data at the same time" [paper P5 §2, Eq. 1].
- **Leveling.**
  - Space is O(1/T) [paper P4 §3, Fig. 4]. RocksDB's dynamic level sizing bounds it below 1.111 for T = 10, against more than 2 for static levels [paper P12 §3.1].
  - Measured: 11.8-12.7% steady overhead with dynamic sizing and 12.2-25.6% without it [paper P13 Table 4]. Leveled RocksDB 5.9 had 9.5% average and 9.8% maximum overhead at write amplification 16.07 [paper P13 Table 3].
- **Tiering.**
  - Space is O(T) in the worst case [paper P4 §3].
  - Universal compaction's `max_size_amplification_percent` defaults to 200, which allows 3×. It treats every run except the oldest as overhead [code C1 include/rocksdb/universal_compaction.h:38-48, 137; docs D4 Universal-Compaction]. A full compaction "temporarily double[s]" space [docs D4].
  - Measured for tiered: 45.5% average and 94.4% maximum overhead at write amplification 4.8 [paper P13 Table 3].
- **Lazy leveling.**
  - Tiering at levels 1..L−1 and leveling at L keeps space at O(1/T), with update cost O((L+T)/B) against O(L·T/B) for leveling and O(L/B) for tiering [paper P4 §3, §4.1]. Space amplification "derive[s] mostly from the largest level" [paper P4 §1].
  - In the Fluid generalization, amp = Z − 1 + 1/T [paper P4 §4.2, Eq. 13].
  - Measured: lazy leveling and leveling grow space amplification "at the same rate" as the update share rises [paper P4 §5, Fig. 10F].
- **Compaction granularity (P5).**
  - Full merges reach 2 + 1/(T−1); partial merges 1 + 1/(T−1); 2L-Spooky 1 + 1/T + 1/(T−1) [paper P5 Eqs. 6, 7, 9].
  - Dynamic capacity adaptation bounds durable space amplification at 1/(T−1) [paper P5 Eq. 3].
  - Spooky gives more than 2× lower space amplification than Full Merge and more than 2× lower write amplification than Partial Merge [paper P5 abstract].
  - Deleting files triggers TRIM, which "may also cause performance issues", so RocksDB rate-limits deletions [paper P13 §4].
- **SplinterDB, analysis.** The STBε-tree uses O(N) space for N distinct keys, against Θ(FN) for a size-tiered LSM; for random updates to existing keys "the space blowup would be roughly a factor of 3" [paper P2 §8, §1].
- **SplinterDB, maplets** [paper P3 abstract]:
  - Maplets "estimate when a compaction could resolve a high density of updates", enabling "targeted compactions for space recovery".
  - The system "is able to dynamically trade update performance for space efficiency, resulting in space overheads on update-heavy workloads as low as 15-61%, whereas RocksDB had 80-117% and SplinterDB had up to 137%".
  - The workloads and configurations are unknown because the full text was not retrieved.
- **SplinterDB, current code (C2).**
  - A leaf splits by *estimated unique* key bytes, computed from maplet fingerprint counts (`num_unique`). If the unique data fits one leaf, it is not split [code C2 src/trunk.c:5205-5413].
  - Branches are compacted when the eventual branch count exceeds `target_fanout` [code src/trunk.c:5660-5700].
  - `reclaim_threshold` defaults to UINT64_MAX [code src/splinterdb.c:201-202].
- **Duplicate estimation without maplets.** TRIAD estimates key overlap across files as 1 − UniqueKeys/ΣKeys with HyperLogLog, and defers compaction below a threshold (0.2 in its example) [paper P7 §4.2, Fig. 5].

## What the evidence implies for A, B and C

Each problem's candidates are ranked by strength of evidence. "Measure" marks the ones that need a measurement on mantle to decide.

### A: seeks during maintenance

1. **Bound maintenance debt, and compare at equal debt.** Strongest evidence, though it is mechanism evidence rather than a measured seek effect.
   - Every engine examined bounds debt with a stated rule: RocksDB stall and speedup thresholds (§1.1), SplinterDB foreground assist (§1.4), the ScyllaDB backlog controller (§1.5). Merely limiting bandwidth defers the work into later contention [paper P1 §4.7].
   - mantle's 0.46-0.51x compares a run still owing about 65 GB of maintenance I/O with a RocksDB run owing none; the 82% versus 91% arms show the debt alone moves the hit rate [inferred].
   - *Measure:* debt bytes at fill end; seeks after the debt drains and with it outstanding; the fill throughput any debt bound costs, since part of the 2.0x fill lead may be debt carried into the query phase [inferred].
2. **Make maintenance I/O cache-neutral.**
   - Strong peer-reviewed evidence that compaction hurts the cache [paper P9, P10, P11]. One vendor measurement shows the fix: p99 −46% and throughput +52% when the hot set is about 40% of cache [docs D6].
   - Mechanisms:
     - Linux: `RWF_DONTCACHE` (ext4/XFS) or O_DIRECT. `RWF_DONTCACHE` keeps already-cached ranges and has measured kernel results [docs D7].
     - macOS: `F_NOCACHE` on a maintenance-only descriptor. Its semantics are in code (§3.4), but there is no measurement and the APFS path is unverified.
     - Windows: a `FILE_FLAG_NO_BUFFERING` handle.
   - The open question is what to do with the *output*. Outputs are live data that seeks will read, so dropping them trades pollution for invalidation [docs D4; paper P9].
   - With 73 GB live and 128 GB RAM, a plausible policy [inferred]: read inputs without caching them; write outputs uncached only where the inputs they replace were not resident (mantle already calls `mincore`); drop dead extents' pages at commit.
     - On Linux, DONTNEED drops dead pages only after they are clean [code C5 mm/fadvise.c].
   - *Measure* (macOS and Linux): live and dead resident pages over the seek phase.
3. **Lower maintenance's CPU and I/O class.** The mechanisms are documented. No source measures them in a storage engine.
   - macOS UTILITY:
     - Base priority 20 against 31.
     - A separate root bucket with WCEL 150 ms.
     - I/O tier 1, which sleeps maintenance for SSD periods of about 5 ms whenever IMPORTANT I/O occurred in the last 25 ms [code C6].
   - macOS BACKGROUND: adds reads that take no page reference (anti-pollution) and a likely E-core placement, but a 100 ms window and a 15 ms period [code C6; docs D2].
   - Linux:
     - `ioprio_set` does nothing on NVMe under the default scheduler (§2.2). This resolves the UNVERIFIED item in note 28.
     - nice and SCHED_IDLE work on CPU only.
   - Risk: under external saturation, low-class maintenance starves and debt grows. Item 1's controller must raise the class as the backlog grows, as ScyllaDB raises shares (§1.5).
   - *Measure:* seek rate, maintenance progress, and debt over time for each class.
4. **Fewer workers, smaller-first ordering.** Moderate evidence.
   - RocksDB runs one compaction unless under pressure (§1.1).
   - SILK sizes the pool from device bandwidth and preempts large jobs [paper P1 §5.2.2].
   - A greedy smallest-first scheduler minimizes component count and improves short scans most, and more so for tiering [paper P6 §5.2].
   - Compaction threads past CPU saturation reduce total throughput [paper P16 Fig. 1].
   - mantle allows up to 17 workers. *Measure:* seek throughput against worker count at fixed debt.
5. **Steady writeback instead of bursts at commit.** Forcing writes every 16 MB cut small-query percentiles 5-10× [paper P6 §5.2]. Linux has `sync_file_range` for this; macOS documents no equivalent (§3.4). Low priority for a read-path problem [inferred].

### B: space

1. **Targeted space-recovery compaction driven by estimated duplicate density.** This is the strongest and most direct evidence.
   - The same STBε-tree family went from up to 137% overhead to 15-61% [paper P3 abstract]. Current SplinterDB splits leaves by estimated unique keys [code C2].
   - mantle's 2.0-2.1× of logical size matches plain SplinterDB's reported range [inferred].
   - mantle has per-branch Bloom filters rather than maplets. It needs another estimator, such as HyperLogLog per branch with TRIAD's overlap ratio [paper P7 §4.2] [inferred adaptation].
   - The fill makes this urgent. 1B uniform inserts over 1B keys leave about 1−1/e = 63.2% distinct keys, which matches the 632M live keys. So about 37% of written entries are obsolete versions waiting for a merge [inferred].
   - *Measure:* write amplification and fill rate against space for several thresholds.
2. **Keep the largest level near one run per leaf range (lazy leveling).** Space amplification comes mostly from the largest level. Leveling only there keeps O(1/T) space at O((L+T)/B) update cost [paper P4 §3, §4.1, Fig. 10F]. In a Bε-tree the leaves are that level [inferred]. *Measure:* the leaf-level write amplification this adds.
3. **Bound transient space.** Full merges need up to 2×; partitioning bounds transient space near 1/T [paper P5 Eqs. 7, 9]. With up to 17 concurrent consolidations the peak holds all their inputs [inferred]. *Measure:* split the peak into live, obsolete, held-by-in-flight-consolidation, and free-but-unreused bytes; cap concurrent input bytes as a stated bound.
4. **Reclaim promptly, rate-limit deallocation.** RocksDB rate-limits file deletion to avoid TRIM spikes [paper P13 §4]. `F_PUNCHHOLE` frees blocks without changing file size [man C6 fcntl(2)]; its cache and TRIM effects on APFS are unmeasured.
5. **Normalize the comparison.** Check RocksDB's compression setting (db_bench may compress values) and each engine's per-entry overhead before comparing 102 GB with 150 GB [inferred]. B feeds A: 146-154 GB against 128 GB of RAM [inferred].

### C: CPU contention

1. **Remove the two cross-thread wakeups from the read path when a shard has nothing else to do.**
   - The comparators hand off nothing on the read path: RocksDB and SplinterDB `pread` on the caller, and Seastar submits and reaps on the shard [code C1, C2, C3]. KVell hands off once and batches [paper P8].
   - A kernel-thread wakeup costs about 5 µs median even when idle [paper P15 Table 2]. Under saturation each wakeup can wait behind other threads' slices [paper P14 §4.1]: the XNU DF quantum is 6 ms and the Linux base slice 2.8 ms [code C6 sched_clutch_edge.md:101-110; C5 fair.c:73-79].
   - macOS has no kernel async read that avoids handoffs (POSIX AIO uses 4 kernel threads; §2.1).
   - Fill, which does no device reads, held at 2.0×: consistent with this hypothesis, not proof of it [inferred].
   - Candidate [inferred]: on a miss, `pread` inline when the shard's queue holds no other runnable request (the single-client case). Hand off to the issuer only when there is other work to overlap.
   - *Measure:* timestamps at park, issuer dequeue, completion and resume, under the 36-loop load. Then A/B inline against handoff.
2. **Separate maintenance from foreground in the OS scheduler.** The mechanisms are verified in code. The effect is unmeasured.
   - macOS: maintenance at UTILITY or BACKGROUND moves it to another root bucket and bucket group. mantle's DEFAULT group then consists of blocking threads and scores as interactive [code C6 sched_clutch_edge.md:66-91] [inferred].
     - In the C test, mantle and the busy loops probably share a thread group (§2.1). Separating QoS classes still separates root buckets.
     - Raising shard and issuer threads to USER_INITIATED buys an earlier EDF deadline (37.5 ms against 75 ms) and a 4 ms warp. Apple scopes that class to short user-waited work [code C7 qos.h; docs D1].
   - Linux:
     - A short custom slice for shard and issuer threads enables `PREEMPT_SHORT`.
     - SCHED_IDLE for maintenance threads guarantees that foreground wakeups preempt them [code C5 fair.c:5951-5965, 9821-9850].
   - Windows: background mode for maintenance. Waking threads are boosted already [docs D3].
   - Same starvation risk as A3.
3. **Fewer maintenance threads when the CPU is saturated.** Compaction of short KV pairs is CPU-bound, and threads past saturation lower throughput [paper P16]. *Measure:* mantle's read and seek rate against worker count under load.

This note closes two UNVERIFIED items: which Linux I/O schedulers honor I/O priority, with NVMe defaulting to none (note 28), and the macOS `F_NOCACHE` read path at the cluster layer (note 02). Still open: APFS behaviour, macOS wakeup latency under load, and the full text of P3.
