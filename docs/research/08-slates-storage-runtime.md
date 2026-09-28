# 08 — slates: storage, runtime, hardware and transport (what mantle can learn and reuse)

- **Source:** slates at commit `fd4f0ef` (2026-09-28), `/Users/adalundhe/Projects/slates`.
- **Working tree:** one uncommitted edit, in `crates/transport/src/stream.rs`. It does not affect anything below.
- **Method:** read-only.
  - `machine`, `land`, `rt`, `db` and `mem` were each read in full by a separate reader.
  - `wire`, `wire-derive`, `transport`, `cluster`, the rules files, the lint configuration and CI were read directly.
  - Every claim a recommendation rests on was re-checked against the source before this note was written.
  - Nothing was built or run. The only cargo command was `cargo metadata --no-deps`.
- **Paths:** relative to the slates root unless absolute. `a.rs:10-20` means lines 10–20.
- **Inference:** marks a conclusion drawn from reading code paths that no test or run has confirmed.
- **Consensus:** slates' consensus core is covered in a separate note. Here it appears only at the configuration level.

---

## Summary

### The one fact that explains the rest

slates is "a hermetic, purely in-memory, copy-on-write virtual filesystem service" (`CLAUDE.md:3-5`). Its first locked rule is "R1 RAM only; disk is the source of truth; disk is written only inside a granted landing" (`CLAUDE.md:24`). As a result, **slates has no durable on-disk storage engine**:

- **Database:** it lives in a shared-memory segment held by a small supervisor process, the "anchor".
  - It survives a daemon crash, but not a host crash or power loss (`crates/anchor/src/lib.rs:1-8`).
  - `docs/wip/recovery.md:49` scopes it as "daemon-crash survival … not host reboot".
  - The db crate contains no `fsync`, `msync` or flush call anywhere (grep of `crates/db/src`, `crates/anchor/src`).
- **Host writes:** the only code that writes host files is the *landing* engine. It copies an in-RAM volume onto a user's directory under a human-issued grant (`crates/land/src/lib.rs:1-3`).
- **Cross-host durability:** comes from f+1 of 2f+1 holders keeping a copy in RAM (design D-18, `docs/wip/SLATES_DESIGN.md:698-701`).

Mantle's product is durable bytes on disk. So very little of slates' storage code transfers as-is. What transfers well is:

- the engineering discipline;
- the measurement harness;
- a handful of well-tested building blocks.

### Key facts by crate

| Crate | What it is | Headline for mantle |
|---|---|---|
| `machine` | Boot calibration: OS facts plus microbenchmarks, feeding "derived" tunables | **No storage-device probing of any kind.** Logical cores only; SMT and NUMA are not used for placement. Windows is partly stubbed. |
| `land` | The grant-gated "landing" engine: write a temp file, sync its data, link or exchange it into place, then fsync the directory | Unix only. No preallocation, direct I/O, alignment or on-disk checksums. The production macOS path never flushes the drive cache. Crash tests model process death only. |
| `rt` | Thread-per-core executor: generational task slab, `Copy` wake words, loom-checked park/kick, a 6×64 timing wheel | **io_uring is used only as a readiness notifier** (`PollAdd`/`Nop`). No file I/O at all. IPv4 only. No TCP on Windows. |
| `db` | RAM/shared-memory op-log ring, full snapshots and radix-tree (ART) indexes over a closed, slates-specific schema | CRC32C records. Recovery truncates at the first bad record. No disk and no fsync. |
| `mem` | Budget ledger, generational slabs, buddy allocator over one lazily-faulted region per shard, SPSC/MPSC rings | Pre-faulting and locking are built but not wired in. Pages are never returned to the OS. No notion of I/O alignment. |
| `wire` | Canonical fixed-width little-endian codec, schema hash per message kind, 32-byte frame header, hardware CRC32C | Nearest to reusable. Decode copies. Any schema change is refused, so there is no tolerance for mixed versions. |
| `transport` | slates' own QUIC-*shaped* dialect over `rustls::quic`, with Copa congestion control, RFC 8899 path MTU discovery and three strict-priority classes | Not wire-compatible with RFC 9000. No FastRaft. IPv4 only. |
| `cluster` | SWIM+Lifeguard membership, Raft for configuration only, fenced register commits at f+1 of 2f+1, verified content replication with p95 hedging | Useful patterns. Most of the pieces are still simulations or first milestones. |

### Reuse verdict

Details and reasons are in §10.

| Crate | Depend as-is (git, pinned rev) | Port selected parts | Ignore |
|---|---|---|---|
| `machine` | no | yes: stats/bench harness, `derived!`, cgroup/rlimit walk, macOS sysctl readers, `clock.rs` | storage topology is absent and must be written from scratch |
| `land` | no | yes: the ~500-line `os.rs` primitive map, the crash-at-every-write oracle method, discard-and-rewrite on a failed sync | the engine, which is bound to slates' VFS |
| `rt` | no | yes: wake-word + generational slab + registry, park/kick protocol, timing wheel, admission receipts, deterministic simulation pattern | its drivers as an I/O layer: no file I/O, no IPv6, no Windows TCP |
| `db` | no | yes: record framing, guard-then-apply replay, effect-plus-completion atomicity, fenced-register core and its oracle, copyset math | the engine itself (RAM-only, closed schema) |
| `mem` | no | yes: `budget.rs` ledger, handle/slab/segmented, rings, loom bounds | arena/buddy/region as an I/O buffer pool: rewrite as device-aware |
| `wire` | possible, but not advised | yes: fork it and add version-skew tolerance and borrowed decode | `observe.rs` (slates' span registry) |

---

## 1. `crates/machine` — hardware and platform probing

`slates-machine` describes itself as "Boot calibration: the machine profile every slates tunable is derived from" (`crates/machine/Cargo.toml:2,9`). It is a leaf crate of about 4.9k lines:

- `facts.rs`: queries to the OS.
- `probes.rs` and `wake.rs`: microbenchmarks.
- `bench.rs` and `stats.rs`: the measurement harness.
- `profile.rs`: assembly and derived constants.
- `derived.rs`, `clock.rs`, `segment.rs`, `error.rs`: support modules.

### 1.1 What it probes

The `Facts` struct (`crates/machine/src/facts.rs:163-179`) holds:

- **Identity:** CPU string, OS and build, architecture, logical core count, total memory, base page. Its BLAKE3 hash is the profile cache key and the benchmark-ratchet machine key (`facts.rs:131-160, 202-210`; `xtask/src/ratchet.rs:124-132`).
- **Pages:** base size, explicit huge-page sizes, whether transparent huge pages (THP) are available, allocation granularity (`facts.rs:47-57`).
- **Cache:** one cache-line size.
- **Per logical core:** OS id, class (Super / Performance / Efficiency / Unknown), performance level, NUMA node, L2 bytes (`facts.rs:19-44`).
- **Memory:** total, available, and the tightest cgroup or rlimit bound (`facts.rs:60-76`).
  - It also reports `address_bits`, but that is `usize::BITS`, a compile-time constant (`facts.rs:420, 727, 983`), not a measured width as its doc says (`facts.rs:66`).
- **Power source** (`facts.rs:120-128`) and **bytes locked by the process** (`facts.rs:228-233`).

**It does not probe:**
- physical cores versus SMT siblings, or packages/sockets;
- L1 or L3 sizes, CPU feature flags, or frequency;
- memory per NUMA node, or clock resolution;
- the network;
- **any storage device**.

### 1.2 How each fact is obtained, per OS

| OS | Mechanism (all unprivileged) |
|---|---|
| Linux (`facts.rs:567-848`) | **Text reads of pseudo-files, plus rustix.** <br>• Page size: `rustix::param::page_size`. <br>• Huge pages: `/sys/kernel/mm/hugepages/hugepages-*`; THP from `/sys/kernel/mm/transparent_hugepage/enabled` (`594-609`). <br>• Cache line: **only** `cpu0/cache/index0/coherency_line_size` (`611-619`). <br>• Core class: `cpuN/cpu_capacity` (≥1024 means Performance), else `/sys/devices/cpu_atom/cpus` and `cpu_core/cpus`, else everything is Performance (`650-673`). <br>• NUMA: a `nodeN` link under `cpuN` (`694-700`). L2: `cache/index*/size` where level is 2 (`702-712`). <br>• Memory: `sysinfo(2)` plus `MemAvailable` from `/proc/meminfo` (`714-730, 782-789`). <br>• Memory bound: cgroup v1/v2 walk up `memory.max` / `memory.limit_in_bytes` (`737-778`), plus `getrlimit` on AS and DATA (`104-111`). <br>• Power: `/sys/class/power_supply` (`791-815`). Locked bytes: `VmLck` (`817-824`). Identity: `/proc/cpuinfo` and `uname` (`826-847`). <br>• *Inference:* cores are enumerated as ids `0..available_parallelism()` (`622-641`), which is wrong for non-contiguous cpusets or offline CPUs. |
| macOS (`facts.rs:266-565`) | • `sysctlbyname`: `hw.pagesize`, `hw.cachelinesize`, `hw.nperflevels`, `hw.perflevel{N}.{logicalcpu,name,l2cachesize}`, `hw.memsize`, `machdep.cpu.brand_string`, `hw.model`, `kern.osversion`, `kern.osproductversion`. <br>• Available memory: `host_statistics64(HOST_VM_INFO64)` (`436-461`). <br>• Power: IOKit `IOPSCopyPowerSourcesInfo` (`463-520`). <br>• Locked bytes: `proc_pid_rusage` `wired_size` (`522-554`). <br>• NUMA is always 0, there are no huge pages, and core ids are synthesized in perf-level order (`338, 358-402`). |
| Windows (`facts.rs:850-1026`) | • `GetSystemInfo`, `GetLargePageMinimum`, `GlobalMemoryStatusEx`, `GetSystemPowerStatus`, `GetVersion`. <br>• Cache line, L2 and NUMA come from **`GetLogicalProcessorInformation`, not the `…Ex` variant** (`facts.rs:865, 898`), although the design says `Ex` (`SLATES_DESIGN.md:789`). <br>• **Stubbed:** available memory returns `None` (`852-856`); core classes are never queried (`952-953`); no job-object memory limit (`113-117, 988-991`); locked bytes returns `None` (`1009-1011`). <br>• *Inference:* above 64 logical processors the NUMA mask test (`945`) and pinning (`probes.rs:866-876`) break, because processor groups are not handled. |

Platform coverage:
- Only macOS, Linux and Windows `platform` modules exist (`facts.rs:266, 567, 850`), so FreeBSD, Android and iOS will not compile (*inference*).
- There are no `target_arch` cfgs.
- CI runs this crate's tests only on `ubuntu-latest` and `macos-latest` (`.github/workflows/ci.yml:17, 57`).

### 1.3 When a probe fails or lacks privileges

- **`Facts::query()` cannot fail** (`facts.rs:194-220`). Every refusal becomes a fallback plus a note, for example:
  - cache line 128 B (`235-237`);
  - total memory 0 (`409-412, 971-978`);
  - available memory falls back to total (`413-416`);
  - cores become `parallelism()` cores of class Unknown (`389-400`);
  - an unreadable cgroup file means "no bound" (`89-95`).
- **Probes degrade instead of failing:**
  - running out of budget sets a `quick` flag (`bench.rs:104-113`; `profile.rs:132, 200-230`);
  - pinning degrades Pinned → Hint → Refused (`probes.rs:201-208`);
  - a refused lock records 0 bytes.
- **One hard failure exists.** The wake probe returns `MeasurementTimeout { probe: "wake" }` when too few samples survive (`crates/machine/src/wake.rs:207-247`). That aborts `MachineProfile::measure` (`profile.rs:94`) and so the daemon's boot (`crates/cli/src/daemon.rs:42`).
- **Nothing requires root** (rule R10, `CLAUDE.md:33`). Lock capacity is read from the soft `RLIMIT_MEMLOCK`. If that is unlimited, it uses `vm.user_wire_limit` (macOS) or available memory, confirmed by locking one page (`probes.rs:734-780`). Windows uses `GetProcessWorkingSetSize` (`895-924`).

### 1.4 Runtime calibration and measurement

Yes: this is the crate's main job.

**Harness** (`bench.rs`, `stats.rs`):
- Timer overhead is the mean of 4,096 `Instant::now()` calls (`bench.rs:54-64`). That is the cost of reading the clock, not its resolution.
- `measure()` doubles the batch until one sample is at least 100× the timer overhead.
- It stops at 16 or more samples once the 95% bootstrap interval around the median is within 10% of the median, or when the 250 ms budget runs out (`bench.rs:19-27, 74-114`).
- `stats.rs` is integer-only: nearest-rank percentiles, a fixed-seed xorshift64*, and a 1,000-resample bootstrap (`stats.rs:14-319`).

**Probes, in boot order** (`profile.rs:87-135`):
1. facts;
2. timer overhead;
3. null syscall: `getppid`, or `SetEvent` on Windows;
4. page-fault cost: 256-page regions, map/unmap subtracted, with `MAP_POPULATE` and THP variants on Linux;
5. **wake** latency: park→unpark, after confirming the waiter really sleeps via `/proc/self/task/<tid>/stat` or macOS `thread_info` (`wake.rs:543-656`), over 5 rounds, converging on the mean;
6. core-to-core cache-line round-trip matrix: all pairs up to 32 cores, else core 0 against the rest, with the original affinity restored afterwards (`probes.rs:171-223`; bug `docs/bugs/2026-09-14-core-matrix-leaves-the-anchor-pinned-to-one-core.md`);
7. memcpy curve, up to max(8×L2, 1,024 pages) capped at available/16 (`probes.rs:295-325`; `profile.rs:245-257`);
8. BLAKE3 throughput;
9. LZ4 and zstd levels 1/3/9/19 on a synthetic corpus (`codecs` feature);
10. lock capacity.

**Cost:**
- 250 ms per probe by default (`profile.rs:41-49`).
- Wake extension rounds can reach about 3.35 s (`wake.rs:204-222`).
- The only recorded full profile took 472 ms on an Apple M5 Max (`docs/wip/BENCHMARKS.md:32`), before the 2026-09-22 wake redesign.
- The CLI `--quick` mode uses 5 ms per probe (`crates/cli/src/daemon.rs:17-43`).

**Derived constants** (`profile.rs:307-355`):
- `spin_before_park_ns` = `task_step_budget_ns` = wake mean;
- `timer_tick_ns` = max(wake mean, 100 × timer overhead) (`profile.rs:334-338`);
- `arena_region_bytes` = next power of two of (200 · syscall/fault pages × page size), or the huge-page size when huge faults are cheaper;
- `ring_entries` = next power of two of (wake p99 / syscall);
- `copy_versus_remap_bytes`.

**Provenance:** `Derived<T> { value, formula, anchors }` plus the `derived!` macro (`derived.rs:12-52`) record where each tunable came from. The `cargo xtask literals` gate consumes them (§9).

**Online refinement:**
- rt keeps a fixed-point moving-average `WakeEstimate` (`wake.rs:109-179`), fed only by parks that were kicked and really slept (`crates/rt/src/shard.rs:359-363, 1035-1069`).
- `refresh_cheap` (`profile.rs:158-164`) and the power-change APIs have no callers outside the crate.

**Unused profile cache:** `segment.rs` builds a RAM-only seqlock cache of the profile that nothing outside the crate uses. The profile actually travels in the anchor segment (`crates/cli/src/anchor.rs:98-113`).

### 1.5 How the rest of slates uses the results

- **`rt`**, via `RuntimeConfig::from_profile` (`crates/rt/src/runtime.rs:93-122`):
  - **Shard count** = cores of the fastest class, minus one kept for control and the OS, with a minimum of one (`runtime.rs:154-184`). Those are *logical* cores, whereas the design says physical (`SLATES_DESIGN.md:821`). On Windows every core is class Unknown.
  - **Pinning:** threads are pinned via `pin_current_thread`, and the result is ignored (`runtime.rs:315-317`).
  - **`ring_entries`** sizes three things: the per-shard MPSC inbox, the SPSC pair rings, and the **io_uring submission-queue depth** (`crates/rt/src/driver.rs:242-249`; `crates/rt/src/uring.rs:80-89`).
  - **Batch size** per loop step = latency budget ÷ measured per-item cost (`runtime.rs:124-141, 186-206`).
  - **Timing wheel** tick = `timer_tick_ns`.
- **`server`**, via `DaemonConfig::derive` (`crates/server/src/config.rs:315-620`):
  - **Effective capacity** = min(total RAM, cgroup/rlimit bound) (`config.rs:344-351`). The **per-shard memory reserve** = capacity / shards / 3 (`config.rs:353`; `crates/mem/src/budget.rs:388-394`).
  - **Idle window** = spin × 100 (`config.rs:569-582`).
  - **Large class** = `arena_region_bytes` (`config.rs:599`). Huge pages come from the fault comparison (`config.rs:601`).
  - **Archive slice size** comes from BLAKE3 throughput × quantum (`config.rs:415-420`). The **codec policy** comes from the codec and memcpy points (`config.rs:887-912`).
  - **Assumed, not measured:** 4e6 req/s, a 5 µs p99, a 50 µs latency budget (`config.rs:18-31, 73`).
  - **Network:** unmeasured. Re-replication bandwidth is set to 0 as "deferred network-empirical measurement" (`config.rs:608-610`).
- **Measured but never consumed:** NUMA, the core round-trip matrix, the pinning result, lock capacity (still named as an input at `config.rs:359`), and `copy_versus_remap` (printed only).

### 1.6 Storage-device classification: **none**

- Grep of `crates/machine/src` for all of these returns **zero hits**:
  - `rotational`, `nvme`, `queue_depth`, `write_cache`;
  - `BLKSSZGET`, `BLKPBSZGET`, `/sys/block`;
  - `IOCTL_STORAGE`, `DKIOC`;
  - `statfs`, `st_blksize`.
- Workspace-wide, the only filesystem-type probe is `fstatfs` in `crates/base/src/unix.rs:148-156, 280-324`, and it only picks timestamp granularity.
- No fsync-latency, disk-throughput or queue-depth measurement exists anywhere.
- This is deliberate: `CLAUDE.md:97` says "Nothing is probed by writing disk, at boot or in tests, except inside a granted landing (D-26)."

### 1.7 Dependencies, unsafe code, tests

- **Leaf crate** (no internal dependencies). External dependencies: memmap2, serde, serde_json and blake3; optional lz4_flex and zstd behind the default `codecs` feature; a `pure-hash` feature; libc and rustix on Unix; windows-sys on Windows (`crates/machine/Cargo.toml:11-32`).
- **Codec feature:** the workspace dependency turns default features off (`Cargo.toml:15`). *Inference:* builds that pull only `slates-cli` (`Dockerfile:27`) therefore skip the codec probes.
- **Unsafe budget 49** (`unsafe-budget.toml:138`). It covers macOS sysctl/mach/IOKit and Win32 FFI; Linux-specific code has no unsafe.
- **Tests:** 46 unit tests and no integration tests. Live probes have no seam for injecting fake sysfs contents (`facts.rs:576-592`).

### 1.8 What this means for mantle

- **Build new:**
  - **Storage probing, for each data directory:** the backing device, its rotational/SSD/NVMe class, logical and physical block size, write-cache state, FUA/flush support and queue depth.
  - **Calibration by writing:** fsync or flush latency, sequential and random throughput.
  - **SMT-, NUMA- and processor-group-aware CPU topology,** including cpuset-aware Linux ids and `GetLogicalProcessorInformationEx` on Windows.
- **Port:**
  - the bench/stats stopping rule;
  - `derived!` provenance;
  - the cgroup/rlimit walk (`facts.rs:89-117, 737-778`);
  - the macOS sysctl and perf-level readers;
  - `clock.rs`: `CLOCK_BOOTTIME`, `CLOCK_MONOTONIC`, `QueryInterruptTimePrecise` (`clock.rs:9-37`);
  - the rule "restore the thread's affinity after probing".
- **Avoid:** a single probe that can abort boot. Degrade and report instead.
- **Invert one rule:** mantle must probe by writing, inside its own data directories. slates forbids exactly that (`CLAUDE.md:97`).

---

## 2. `crates/land` — how slates writes to host disks

`land` is the only crate allowed to link write-capable file syscalls. The structural gate enforces this with `WRITE_ALLOWED = ["slates-land"]` (`xtask/src/main.rs:337-413`).

### 2.1 What it is

- **The engine:** it plans a manifest of changed entries in an in-RAM volume, then applies it to a real directory under a grant.
  - The state machine runs Planning → AwaitingGrant → Validating → Writing → Syncing → Advancing → Done / Partial / Refused / Aborted (`crates/land/src/engine.rs:1-4, 76-98`).
  - Entry point: `land<H: LandFs>(host, target, vol: &mut Volume, store: &mut Store, grants, leases, audit, request, observer)` (`engine.rs:1516-1526`). The engine is bound to slates-vfs types (`engine.rs:37-41`).
- **`manifest.rs`:** eight actions (Create, Replace, Delete, Rename, Mkdir, Rmdir, Symlink, Clear), ordered by class (`manifest.rs:40-81, 216-234`). The canonical "SLMF" encoding is hashed with BLAKE3, and a grant binds to that hash (`manifest.rs:166-214, 261`).
- **`verdict.rs`:** a pure function over (witnessed base, disk now, overlay) that returns Apply, Skip, AcceptIdentical or Conflict (`verdict.rs:69-99`).
- **`grant.rs`:** grants carry once/session scope, expiry and revocation. A per-target lease carries a fencing generation (`grant.rs:1-4`). Both are in-process maps that the server mirrors into db records (`crates/server/src/landing.rs:62-76`).
- **`ramp.rs`:** a concurrency-doubling policy that is only recorded, never applied. Every landing runs at depth 1 (`ramp.rs:1-6`). The server passes `cores: 1, max_depth: 1` (`crates/server/src/landing.rs:266-267`).

### 2.2 File creation and placement

| Step | Linux | macOS / other Unix |
|---|---|---|
| Temp file | `openat(dir, ".", O_WRONLY\|O_TMPFILE\|O_CLOEXEC, 0600)` (`os.rs:200-207`). Falls back to a named temp on EOPNOTSUPP, EISDIR or EINVAL (`os.rs:220-222`). | Hidden sibling via `openat(dir, name, O_WRONLY\|O_CREAT\|O_EXCL\|O_CLOEXEC, 0600)` (`os.rs:180-187`). The name is `.slates-{landing_id:016x}-{n}` in the entry's own directory (`engine.rs:50-52, 469-473`). |
| Name an `O_TMPFILE` | `linkat(CWD, "/proc/self/fd/N", dir, name, AT_SYMLINK_FOLLOW)` (`os.rs:233-244`), so it needs `/proc`. | n/a |
| Create, no clobber | `linkat` temp → final name. EEXIST becomes a TargetInUse conflict, then the temp is unlinked (`engine.rs:922-948`; `os.rs:451-469`). | Same |
| Replace | `renameat2(RENAME_EXCHANGE)` (`os.rs:254-258`), then re-verify the displaced file against the witness and swap back on a mismatch (`engine.rs:981-1061`) | `renameatx_np(RENAME_SWAP)` (`os.rs:260-286`) |
| No exchange support | Detected from EINVAL/ENOTSUP. Falls back to `fstat`-verify then plain `renameat`; the unprotected gap is measured and reported as `NoExchange` (`engine.rs:995-998, 1063-1092`). | Same |
| Metadata | `fchmod`, and `futimens` with `UTIME_OMIT` for atime (`os.rs:428-449`). No chown, no xattrs. | Same |
| Containment | Target resolved from `/` one component at a time with `O_NOFOLLOW`; `..` refused; must be owned by the euid (`os.rs:120-146`). No `openat2(RESOLVE_BENEATH)`. | Same |

**Windows:** no code exists. The `os` module is `#[cfg(unix)]` (`crates/land/src/lib.rs:27-28`), and the server answers `Unsupported { feature: "landing" }` (`crates/server/src/landing.rs:145-153`).

### 2.3 Preallocation, direct I/O, alignment: none

- **No preallocation:** no `fallocate`, `posix_fallocate`, `F_PREALLOCATE`, `SetEndOfFile` or `set_len`.
- **Holes are not preserved:**
  - The design says "Sparse ranges are preserved… large files are preallocated" (`SLATES_DESIGN.md:3649-3650`).
  - The code does neither. It reads the whole file into a zeroed `Vec` (`engine.rs:1497-1511`) and issues one `pwrite` of the whole buffer, looping only on short writes (`engine.rs:901-903`; `os.rs:410-422`).
- **Buffered I/O only:** no `O_DIRECT`, `F_NOCACHE`, `FILE_FLAG_NO_BUFFERING`, `O_DSYNC` or write-through. Buffers are plain `Vec<u8>` with no alignment.

### 2.4 Durability primitives per OS

| Barrier | Linux / other Unix | macOS |
|---|---|---|
| File data, before link or exchange | `fdatasync` (`os.rs:295-299`) | `fcntl(F_BARRIERFSYNC)` (`os.rs:301-315`). Its own comment says this is ordering, "not a media flush". |
| Each touched directory, once, at the end | `fsync` (`os.rs:501-503`; `engine.rs:1331-1353`) | `fsync` (same code) |
| Media flush, only if `request.media_durability` | `fsync(target dir)` again (`os.rs:317-321`) | `fcntl(F_FULLFSYNC)` (`os.rs:323-336`) |

- **In production `media_durability` is hard-coded `false`** (`crates/server/src/landing.rs:264`). So on macOS no drive-cache flush ever happens, and landed data is ordered but not power-loss durable.
- The engine records a `BarriersOnly` degradation whenever media durability was not requested, on every OS including Linux (`engine.rs:1366-1368`). That misreports Linux, where `fdatasync` already reaches the media.
- **Never used:** `sync_file_range`, `syncfs`, `FlushFileBuffers`, `NtFlushBuffersFileEx`. The Windows design (`FlushFileBuffers` plus `FileRenameInfoEx` with POSIX semantics, `SLATES_DESIGN.md:3643-3648, 3663`) is listed as owed in `docs/wip/GAPS.md:633-634`.

### 2.5 How writes are issued, and error handling

**How writes are issued:**
- All I/O is synchronous rustix syscalls on the calling shard thread. There is no io_uring, no `rt` dependency and no helper thread.
- The server runs a landing inside `dispatch_inner(&mut ShardState)` (`crates/server/src/verbs.rs:2206`). *Inference:* a landing blocks its shard for its whole duration.
- There is no data batching (one `fdatasync` per file). Directory fsyncs are deferred to the end, which is the only group-commit-like step.

**Error handling:**
- **Short writes** are retried. A zero-byte write becomes EIO (`os.rs:413-420`).
- **EINTR** is never retried (no `retry_on_intr`, no EINTR handling in `crates/land/src`). It aborts the landing.
- **ENOSPC/EDQUOT** marks that entry Failed and the landing continues (`engine.rs:786-792`).
- **Other errnos** abort the landing (`engine.rs:1690-1704`).
- **A failed file sync** closes and unlinks the temp and never retries the sync on that descriptor. A resume rewrites from the in-memory source (`engine.rs:907-910`). This is the correct answer to "fsyncgate" (a retried fsync can report success after data was lost).

**Holes found by reading** (*inference*; no test covers them):
1. A directory-sync failure due to ENOSPC only clears `dirs_synced` (`engine.rs:1343-1347`), and the terminal state ignores it (`engine.rs:1772-1787`).
2. A resume re-fsyncs directories after a failed fsync. This is exactly the retry that can falsely succeed on Linux (`engine.rs:652-654, 1691-1693`).
3. A directory that can't be opened is silently skipped at sync time (`engine.rs:1337-1339`).
4. With no `/proc`, the `linkat` fails ENOENT. That maps to `Skipped(ParentMissing)`, which counts as clean, so the file is never written and the landing still reports Done (`engine.rs:800-803, 1776-1781`).
5. Hidden temps from earlier landings can leak, because the sweep matches only the current landing id (`engine.rs:475-477`) and the server mints a new id per call (`crates/server/src/landing.rs:256`).

**No stored checksums.** BLAKE3 is used only for verdicts and identity. Nothing is stored on disk, and nothing is read back after writing.

### 2.6 Crash-consistency testing

- **`tests/oracle.rs`** (19 tests, all over the simulated host `SimHost`):
  - `t_1_15_crash_at_every_write_instruction_then_resume` (`crates/land/tests/oracle.rs:739-748`, driven by `677-737`) crashes at every write verb. It asserts four things: every path is old or new; a resume reaches a clean reference run; hidden temps are swept; a re-plan comes out empty. `CLAUDE.md:111` names this test as the pattern to copy.
  - **Limit: the crash model is process death, not power loss.** Every verb before the crash point persists, and every later one returns EIO (`crates/vfs/src/host/sim.rs:162-168, 214-228`).
  - The simulator's `sync_file`/`sync_dir` only record handles (`sim.rs:756-763, 945-957`). `take_synced_files`/`take_synced_dirs` (`sim.rs:199-208`) are **never called**. *Inference:* deleting every fsync would still pass every test.
- **`tests/os.rs`** (5 tests, real filesystem): runs only with `SLATES_TEST_RAMDIR`, which is tmpfs in Linux CI (`.github/workflows/ci.yml:66-68`). It includes a real `kill -9` of a child landing loop with a resume (`tests/os.rs:296-447`). On tmpfs, fsync is essentially free, so this also exercises process death only.
- **Absent:**
  - LazyFS, dm-flakey, dm-log-writes or CrashMonkey-style replay;
  - ENOSPC/EINTR injection;
  - proptest (a dev-dependency, but unused);
  - any ordering check. The hermeticity tracer checks only *where* writes go (`xtask/src/conformance/hermeticity.rs:1-12`).

### 2.7 Performance

All recorded numbers come from the simulator on an M5 Max, with no disk involved (`docs/wip/BENCHMARKS.md:239-255`):

| Row | Median |
|---|---|
| Plan, per entry | 594 ns |
| Land, per entry | 5,633 ns |
| Re-plan after landing | 1,583 ns |

- Ratchet ceilings: 805, 7,016 and 2,417 ns (`ratchets.toml:56-58`).
- The real-filesystem bench rows (10k entries into a 10^6-entry tree, versus `cp -r`) run only as informational CI output on tmpfs (`ci.yml:131-134`). They were never recorded because a macOS RAM disk was not authorized (`BENCHMARKS.md:247-249`).
- **No durable-write latency or throughput number exists anywhere in slates.**

### 2.8 Dependencies

- **Direct:** `slates-base`, `slates-vfs`, `slates-machine`, `slates-mem`, blake3; rustix and libc on Unix only (`crates/land/Cargo.toml:10-19`).
- **Unused in `src/`:** `slates_machine` has 0 references; `slates_mem` appears only in `tests/common/mod.rs` and `examples/land_bench.rs`.
- **Transitive** via vfs: `slates-archive`, `slates-wire`, `slates-wire-derive`.
- **Not used:** no `rt`, no `db`.
- **Only dependent:** `slates-server`.
- **Unsafe budget 3:** macOS `renameatx_np`, `F_BARRIERFSYNC` and `F_FULLFSYNC` (`unsafe-budget.toml:127-129, 169`).

### 2.9 What this means for mantle

- **Port the primitive map** in `crates/land/src/os.rs`:
  - `O_TMPFILE` + `linkat`, or `O_CREAT|O_EXCL` hidden temps;
  - exchange with errno-learned fallback;
  - the `fdatasync` / `F_BARRIERFSYNC` / `F_FULLFSYNC` distinction;
  - descriptor-relative `O_NOFOLLOW` containment;
  - discard-and-rewrite on a failed sync.
- **Port the test method** (a seam, a simulated host, crash at every write verb, resume, compare to a clean reference). But **upgrade the simulator** so that at a crash it drops unsynced data and directory entries and can reorder or tear writes. Add a real-disk lane (LazyFS or dm-log-writes on Linux).
- **Build new, because land has none of it:**
  - preallocation;
  - aligned direct I/O;
  - group commit;
  - on-disk checksums with read-back verification;
  - a Windows write path (`FlushFileBuffers`, `FILE_FLAG_WRITE_THROUGH`, `FileRenameInfoEx`);
  - an explicit macOS policy choosing `F_FULLFSYNC` for acknowledged writes.

---

## 3. `crates/rt` — the runtime

### 3.1 Architecture

- **Thread-per-core, no work stealing, no reference counting.**
  - A task lives on one shard for its whole life (`crates/rt/src/lib.rs:6-19`).
  - `Runtime::start` spawns one thread per shard (`runtime.rs:289-341`); `LocalRuntime` runs a shard on the caller's thread (`runtime.rs:423-505`).
- **Shard count and pinning:** see §1.5. Pinning per OS (`crates/machine/src/probes.rs`):
  - Linux: hard `sched_setaffinity` (`626-639`).
  - macOS: an affinity-tag hint that Apple silicon refuses (`657-692`; `BENCHMARKS.md:23`).
  - Windows: `SetThreadAffinityMask`, for cores below 64 only (`860-875`).
  - *Inference:* on x86 servers SMT siblings become separate shards and NUMA is ignored.
- **Tasks:**
  - A per-shard `Slab<TaskSlot>` bounded at `tasks_per_shard` (`shard.rs:329-338`).
  - The future is a boxed `Pin<Box<dyn Future>>`, so every spawn allocates (`task.rs:22`).
  - Structured: a finishing parent cancels and joins its children (`shard.rs:1498-1579`).
  - Local spawns may be `!Send`; cross-thread spawns must be `Send` (`futures.rs:58-70`).
- **Wakers:** the `RawWaker` data pointer *is* a packed `u64` task word, `shard:16 | slot:24 | generation:24` (`crates/mem/src/handle.rs:82-107`). `clone` and `drop` are no-ops (`waker.rs:11-48`). Routing (`registry.rs:586-607`):
  - same shard: push to the local queue;
  - another shard of the same runtime: that pair's SPSC ring plus a kick;
  - a foreign thread: the target's MPSC ring.
  - Stale generations are dropped (`shard.rs:1270-1280`).
- **Run queue:** a pre-sized `VecDeque<u32>` with one pending flag per slot, so duplicate wakes collapse. FIFO, at most `batch` tasks per step (`queue.rs:18-86`).
- **Loop:**
  - Each step (`shard.rs:899-966`): drain control messages, then the rings, expire timers, wake pollers, and poll up to `batch` tasks.
  - When idle it spins for a measured window, polling rings and the driver each turn (`shard.rs:816-863`), then parks (`shard.rs:972-1003`).
- **Cross-shard messages:**
  - Only wake words cross shards. They travel over shards² SPSC pair rings plus one Vyukov MPSC ring per shard (`runtime.rs:220-241`; `crates/mem/src/mpsc.rs:1-11`).
  - Spawn, cancel and shutdown travel over a bounded `std::sync::mpsc::sync_channel` (`control.rs:1-23`).
  - There is **no general data-message primitive**, and join/cancel work within one shard only (`futures.rs:72-85`).
- **Registry:** a static table of 1,024 slots, 128-byte aligned, with a generation that is odd when free. Foreign readers are counted, and retirement waits for them (`registry.rs:1-31, 231-354, 438-509`).
- **Parking protocol:**
  - Shard side: set "parked", SeqCst fence, re-check the inboxes, then wait in the driver.
  - Sender side: publish, fence, and kick only if the shard is parked (`parking.rs:135-188`).
  - It is loom-checked: 27 bounded and 116 exhaustive interleavings (`docs/wip/concurrency.md:46-47`). Loom found a real lost wake that the fence fixed (`concurrency.md:74-81`).
  - **Code/doc mismatch:** pair-ring sends call `kick()` unconditionally after every push (`shard.rs:552`), and `Kick::kick` always writes the eventfd, triggers kqueue or posts to IOCP (`driver.rs:137-158`). Only the foreign and control paths use `kick_if_parked`. This contradicts "a message to a spinning shard costs no syscall" (`registry.rs:84-87`; `parking.rs:3-5`).
- **Attribution:** when a poll overruns its quantum, the shard decides whether the task or the host held the thread (`attribution.rs:1-30`):
  - Linux: thread CPU clock plus `getrusage(RUSAGE_THREAD).ru_nvcsw`;
  - macOS: CPU clock only;
  - Windows: nothing.
- **Admission:**
  - A full arena refuses `TooManyTasks`; a full control channel refuses `ControlFull` (`task.rs:81-149`; `shard.rs:1282-1326`).
  - The limit comes from Little's law (`runtime.rs:208-218`).
  - There is no preemption: tasks slice their own work against `step_budget_ns()` (`futures.rs:42-44`).

### 3.2 The timing wheel (`crates/rt/src/timer.rs`)

- **Shape:** hierarchical, Varghese–Lauck style: 6 levels × 64 slots, 2^36 ticks of range (`timer.rs:1-20`). The tick is `timer_tick_ns`: 2,300 ns on the reference Mac (`BENCHMARKS.md:40`). *Inference:* that gives about 44 h of horizon.
- **Storage:** entries live in a slab reserved once, with doubly linked slot lists. Arming, renewing and expiring never allocate (`timer.rs:56-72`; `crates/rt/tests/timer_allocations.rs:86`).
- **Arm and cancel:**
  - First poll of `Sleep` arms the timer. Dropping it disarms it with a generation check (`futures.rs:152-206`; `timer.rs:106-122`). The check fixed `docs/bugs/2026-09-10-timer-cancel-orphan.md`.
  - If arming fails (foreign waker, full slab), `Sleep` returns immediately (`futures.rs:172-188`).
- **Advance** jumps to the next occupied slot, so its cost is per event, not per tick (`timer.rs:145-187`).
- **Cost hazard:** `next_deadline_ns` rescans **every armed entry** after a fire or after the earliest timer is cancelled (`timer.rs:126-143`). It is called every loop step (`shard.rs:953`), so that step is O(armed timers).

### 3.3 "Wake words": two different things

- **In rt,** a "wake word" is the packed `u64` task word a waker carries (§3.1). It is not an OS primitive. The cross-thread wake is a **kick**:
  - Linux: an 8-byte write to an eventfd, watched by epoll or by a multishot io_uring `PollAdd`;
  - macOS/FreeBSD: an `EVFILT_USER` trigger;
  - Windows: `PostQueuedCompletionStatus`;
  - simulation: a flag (`driver.rs:137-158`; `uring.rs:57-63, 168-177`; `kqueue.rs:61-80`; `iocp.rs:143-147`).
- **In design D-10 and `slates-ipc`,** a "wake word" is a 32-bit word in client-shared memory (`SLATES_DESIGN.md:641`; `crates/ipc/src/wake.rs:1-16`):
  - Linux: a shared futex;
  - macOS 14.4+: `os_sync_wait_on_address` with the SHARED flag;
  - Windows: a named auto-reset Event, because `WaitOnAddress` only works within one process.
- **rt does not use the ipc wake word.** It uses `machine::wake` only for the boot probe and the `WakeEstimate`.

### 3.4 I/O drivers

**The seam** is `trait Driver` (`driver.rs:163-207`):
- `wait(timeout, &mut Vec<Completion>)`, `submit_nop`, one-shot `register_readable`/`register_writable`, `has_pending`, `now_ns`.
- **Every driver runs in readiness mode.** `submit_nop` is the only submitted "operation". The file and socket operations promised at `driver.rs:5-7` never arrived.

| Driver | Where | Details |
|---|---|---|
| io_uring (`io-uring` crate 0.7) | Linux | **Ops:** one-shot `PollAdd` for POLLIN/POLLOUT (`uring.rs:161-166`); multishot `PollAdd` on the kick eventfd (`uring.rs:168-177`); `Nop` (`uring.rs:216, 350`); synchronous cancel. **No read, write, fsync, accept, recv or send ops.** <br>**Setup:** `SINGLE_ISSUER\|DEFER_TASKRUN`. On EINVAL it retries a plain ring; any other `io_uring_setup` error (EPERM from seccomp, ENOSYS) selects epoll. The sync-cancel probe must also succeed (`uring.rs:65-107`), so in effect it needs kernel 6.0+ (design Appendix C, `SLATES_DESIGN.md:5690`). <br>**Absent:** `IORING_REGISTER_PROBE`, kernel-version checks, SQPOLL, registered buffers or files. One `io_uring_enter` per SQE (`uring.rs:181-193`). Zero-timeout harvests use a raw `enter(GETEVENTS, 0)`, which fixed 0.3–1.0 ms sleeps (`uring.rs:244-264`; `docs/bugs/2026-09-26-io-uring-zero-timeout-harvest-sleeps.md`). |
| epoll | Linux fallback | Level-triggered eventfd kick; `EPOLLONESHOT` with ADD, then MOD on EEXIST; 64 events per wait (`epoll.rs:19, 41-54, 157-176`) |
| kqueue | macOS, FreeBSD | `EVFILT_USER` kick; one-shot read/write filters with udata = wake word (`kqueue.rs:27-29, 61-80, 155-194`) |
| IOCP + AFD | Windows | Port concurrency 1. Socket readiness through `IOCTL_AFD_POLL` on `\Device\Afd`, the wepoll/mio technique (`afd.rs:1-15, 123-306`; `iocp.rs:73-219`). Millisecond timeouts, rounded up (`iocp.rs:167-169`). |
| Simulation | all | Never blocks. Virtual clock. Seeded UDP fabric (§3.6). |

The driver is chosen per platform with `cfg` (`driver.rs:241-308`). CI forces io_uring on Linux with `SLATES_TEST_DRIVER` (`ci.yml:54-57`).

### 3.5 How disk and network I/O are issued

- **Disk: rt does none.** Every disk access in slates is a synchronous rustix call on the calling thread:
  - `pread` in `crates/base/src/unix.rs:246`;
  - `pwrite`, `fdatasync` and friends in `crates/land/src/os.rs`.
- There is no blocking thread pool. The landing engine defers "arena pages through io_uring" to a later phase (`crates/land/src/engine.rs:28-29`).
- **TCP** (`tcp.rs`):
  - Behind `#[cfg(not(windows))]` (`lib.rs:45-48`), because it exists only for the NFS loopback mount server. IPv4 only.
  - Non-blocking accept, connect, read and `write_all` (`tcp.rs:43-179`).
  - No `TCP_NODELAY`, SO_REUSE*, buffer sizing, vectored I/O or sendfile.
- **UDP** (`udp.rs`, `netsys.rs`):
  - IPv4 only (`netsys.rs:19`). The workspace has 53 `SocketAddrV4` uses in `rt/src` + `transport/src` and no `SocketAddrV6`.
  - Don't-fragment is set: Linux `IP_MTU_DISCOVER=PROBE`, macOS `IP_DONTFRAG`, Windows `IP_DONTFRAGMENT` (`netsys.rs:80-112`).
  - One `recvfrom`/`sendto` per syscall: no GSO/GRO, `recvmmsg`/`sendmmsg`, ECN or `IP_PKTINFO`.
  - `sendto` EAGAIN surfaces as an error; there is no wait for writability (`udp.rs:129-140`).
  - *Inference:* `set_dont_fragment` has no FreeBSD arm, so a FreeBSD build likely fails.

### 3.6 Deterministic simulation (`crates/rt/src/sim.rs`)

- `SimRuntime` steps every shard in index order on one thread. When all are idle it jumps a shared virtual clock to the next timer or datagram (`sim.rs:1024-1071`).
- The seeded UDP fabric models (`sim.rs:12-18, 321-368, 487-563`):
  - delay and jitter, with or without reordering;
  - a drop-tail bottleneck;
  - Gilbert–Elliott loss;
  - path MTU and interface MTU (EMSGSIZE);
  - a 208 KiB receive buffer;
  - NAT expiry and rebinding.
- **Not virtualized:** only `futures::now_ns` sees virtual time, not `std::time::Instant`. TCP is not simulated.
- **Differential test:** the same task program yields an identical trace on the OS driver and on the simulator (`crates/rt/tests/differential.rs:111-113`).

### 3.7 Measured numbers

Unless noted, these come from an Apple M5 Max, macOS 26.4.1, Rust 1.98, release build, using the custom `slates_machine::bench::measure` harness (median with a 95% bootstrap interval). **No Linux x86_64, bare-metal or Windows wall-clock numbers are recorded for rt, mem or wire.**

**Runtime on kqueue** (`BENCHMARKS.md:76-106`):

| Row | Median | p99 |
|---|---|---|
| Idle loop step | 35 ns | 35 ns |
| Spawn, admission only | 24 ns | 43 ns |
| Spawn and run a trivial task | 179 ns | 187 ns |
| Local wake (yield once) | 281 ns | 302 ns |
| Zero-timeout `kevent` park | 15.1 µs | 16.3 µs |
| 100 µs sleep, lateness | p50 99.8 µs | 102.8 µs (max 107) |
| Cross-shard round trip, both parked | 6.25 µs | 8.2 µs |
| Cross-shard round trip, both spinning | 500 ns | 708 ns |
| Foreign spawn and reply | 8.7 µs | 12.6 µs |

- Ratchet ceilings are at `ratchets.toml:62-68`. Between-run drift on thread-placement rows is 27–52% (`BENCHMARKS.md:381-393`).
- *Inference:* since the spin began polling the driver every turn (`shard.rs:830-843`), a 15 µs `kevent` dominates a roughly 2 µs spin on macOS.

**Machine profile, same Mac** (`BENCHMARKS.md:8-43`):
- null syscall 164 ns;
- page fault 845–901 ns per 16 KiB page;
- park/unpark p50 2.0 µs, p99 4.6 µs;
- core-to-core round trip, median 153 ns;
- memcpy 128 GB/s (≤128 KiB) falling to 36 GB/s at 128 MiB;
- BLAKE3 2.54 GB/s (one thread);
- LZ4 1.74 / 7.6 GB/s;
- zstd-3 1.04 / 4.5 GB/s.

**NFS over io_uring** (`BENCHMARKS.md:445-488`): a Linux VM under Docker on the Mac, kernel 6.12.76, loaded. Best of 5, before → after the harvest fix, NFSv3:

| Phase | Before | After |
|---|---|---|
| create (256 files) | 1,544.6 ms | 212.2 ms |
| stat | 782.3 ms | 6.46 ms |
| read | 2,329.2 ms | 29.68 ms |
| seq-write (32 MiB) | 418.3 ms | 65.70 ms |

GETATTR took 27 µs on epoll against 999 µs on io_uring before the fix.

**Provisioning, end to end** (`BENCHMARKS.md:408-425`): single client p50 about 9 µs, p99 about 25 µs.

### 3.8 Dependencies, unsafe code, target matrix

- **Dependencies:**
  - Internal: `slates-machine` and `slates-mem` only (`crates/rt/Cargo.toml:11-31`).
  - External: `io-uring` on Linux; libc and rustix on Unix; windows-sys on Windows; loom under `cfg(loom)`.
- **Unsafe budget 61** (`unsafe-budget.toml:166`). Mostly Winsock/AFD/IOCP FFI, plus the `RawWaker` vtable, `kevent`, io_uring `push`, and the registry's slot protocol.
- *Inferences worth checking:*
  - `registry.rs:554` forms a `&mut Entry` while foreign readers may hold `&Entry`.
  - On 32-bit targets `waker.rs:32` turns any task word that doesn't fit in 32 bits into address 0 (`usize::try_from(word.word()).unwrap_or(0)`). i686-windows is in the release matrix.
- **Targets:**
  - Linux: io_uring or epoll. macOS/FreeBSD: kqueue. Windows: IOCP+AFD.
  - Windows lacks TCP, writable readiness (`readiness.rs:27-31`) and attribution.
  - CI runs full tests on `ubuntu-latest` (x86_64, io_uring) and `macos-latest` (arm64). Windows runs clippy plus `driver::` and `tests/udp` only (`ci.yml:216-288`).

### 3.9 What this means for mantle

rt would build as a pinned git dependency, since it pulls only `machine` and `mem`. But it is the wrong substrate for a storage server. Mantle needs things rt lacks:
- **Completion-based file I/O:**
  - Linux: io_uring read/write/fsync with registered buffers and opcode probing;
  - Windows: overlapped IOCP file I/O;
  - macOS: a bounded blocking pool, because kqueue cannot drive regular files. Slates' own design notes "no asynchronous file I/O (a pool)" (`SLATES_DESIGN.md:5695`).
- **Cross-platform TCP with IPv6.**
- **An HTTP stack.** slates has none on rt: its only HTTP server is the MCP edge, a 253-line blocking `std::net` HTTP/1.1 loop (`crates/mcp/src/http.rs:1-11, 51`).

Worth porting:
- the `Copy` wake word with a generation-checked slab and registry;
- the loom-checked park/kick protocol, with the unconditional pair-ring kick fixed;
- the per-event timing wheel, with the O(n) rescan fixed;
- admission receipts;
- long-poll attribution;
- the `SimRuntime` pattern, extended with a simulated disk.

If mantle adopts slates' "no tokio" rule (`CLAUDE.md:43`), it must budget for building the I/O layer, TCP/IPv6 and HTTP/S3 itself. slates' design rejected compio for lack of a maturity statement and monoio for lack of Windows (D-9, `SLATES_DESIGN.md:636-638`).

---

## 4. `crates/db` — the metadata database

### 4.1 What kind of engine

**An append log in shared memory, in-memory indexes, and periodic whole-partition snapshots.** It is not a B-tree and not an LSM tree.

- Recovery is "the newest valid snapshot of the partition plus the records after it" (`crates/db/src/lib.rs:1-16`).
- **Indexes** are an adaptive radix tree (Leis et al.) with node sizes 4/16/48/256, path compression and a `Vec` arena with `u32` handles (`art.rs:1-25, 338-346`). Iteration and `scan_prefix` materialize or filter the whole tree, so they are O(n) (`art.rs:530-555`).
- **Not a general key-value store:**
  - `Op` is a closed enum of 39 slates-specific mutations: volumes, snapshots, leases, attachments, exactly-once completions, grants, landings, audit, merge chains, NFSv4 state (`op.rs:17-277`).
  - A `Partition` is a fixed set of typed tables (`partition.rs:119-158`).
  - There is one partition per shard and at most `u16` partitions (`crates/anchor/src/layout.rs:30`).
- **Guard-then-apply:** `check` refuses an op before it is appended; `apply` is unconditional, so replay is deterministic (`partition.rs:491-653, 699-892`). `CLAUDE.md:113` names this as the pattern to copy.

### 4.2 Format: in shared memory, not on disk

- **The segment** is a `SparseObject`, created without any filesystem entry:
  - Linux: `memfd_create`;
  - macOS: `shm_open`;
  - Windows: a pagefile-backed `SEC_RESERVE` section (`crates/anchor/src/segment.rs:32-43`; `crates/mem/src/shared.rs:1-30`).
- **The header:** magic `SLAN`, layout version 3, a BLAKE3 hash of the machine identity, a seqlock generation word and a geometry block (`layout.rs:7-14`). It is checked on attach (`segment.rs:199-248`).
- **The regions** are page-aligned: supervision, profile, one log per partition, two snapshot slots per partition, consensus slots, audit, and landing slots (`layout.rs:179-215`).
- **The log ring:** head and tail are monotonic `u64` byte offsets, and records may wrap (`record.rs:5-10, 293-349`).
- **A record** is a 32-byte header (`record.rs:20-35`):
  - magic `SLRC` (0x43524C53);
  - `u32` body length and `u64` sequence;
  - CRC32C, then 4 bytes of padding, then a `u64` schema hash;
  - then the `slates-wire` canonical encoding of `LogEntry { ops: Vec<Op> }` (`record.rs:37-44`).
- **Snapshot slot:** a generation word (odd while being written), a length, then the whole partition as one `Wire` value (`layout.rs:75-81`; `partition.rs:79-116`).
- **Sizes are derived:** the log from recovery budget × measured replay throughput, the snapshot from measured state size × headroom (`layout.rs:123-139`).

### 4.3 Checksums

- **Log records:** CRC32C over sequence ‖ schema hash ‖ body, verified on every replay and every trim (`record.rs:193-251, 267-291`). Each check copies the body into a temporary `Vec` instead of using `crc32c_append`.
- **Snapshots: no checksum.** Validity is only an even seqlock generation plus a clean decode (`segment.rs:525-557`; `replay.rs:129-134`).
- **BLAKE3** is used only for register-record identity, not for stored data (`register.rs:649-653, 692-699`).

### 4.4 Write path and what "durable" means

- **The write path:**
  - A write copies bytes into the mapped segment, then publishes the tail with a Release store (`record.rs:152-156, 309-329`).
  - There is no land, no `std::fs`, no rt I/O, and **no fsync or msync**.
  - `mutate` runs check → append → apply. On `LogFull` it snapshots and retries once (`replay.rs:273-307`).
- **`begin`/`commit` is not group commit.** It batches one verb's ops into one `LogEntry` so that the effect and its exactly-once completion record land together (`replay.rs:309-372`; `CLAUDE.md:122`).
- **"Durable"** means the data survives a daemon crash while the anchor keeps the memory alive (`recovery.md:49`). The docs are explicit that "anchor RAM alone does not satisfy NFS's power-failure stable-storage contract" (`recovery.md:326-327`).

### 4.5 Recovery (`replay.rs`)

1. Pick the newest valid snapshot slot. Higher sequence wins; a tie goes to slot 0 (`replay.rs:122-140`).
2. Replay the records that follow it (`replay.rs:204-240`).
3. Measure replay throughput and set the snapshot cadence from it (`replay.rs:162-171`).

Each record is checked in turn: within the tail, magic, length bound, exact next sequence, schema hash, CRC, and decode (`record.rs:162-251`).

- **Corruption handling is truncation.**
  - The first failure of *any* kind marks the rest of the log torn, and the tail is reset to the last verified byte (`record.rs:177-180, 253-263`).
  - That is justified by the premise that a bad record "can only be the one being written when the crash came" (`lib.rs:12-13`). The premise holds for RAM under process crashes; it is wrong for disks.
  - A record that passes the CRC but fails `apply` makes recovery fail hard (`replay.rs:227-229`).
- *Inference (untested):* a new `Op` variant changes `LogEntry::SCHEMA_HASH`. A new binary attaching to an old segment would then treat every old record as a torn tail and silently drop it. The server only reports `torn_tail` (`crates/server/src/verbs.rs:1819`).

### 4.6 Compaction and growth

- **No compaction.** There is no LSM compaction, no incremental checkpoint and no segment files. The ring is trimmed after each full snapshot (`record.rs:265-282`).
- **Snapshots** re-serialize the whole partition on the writer thread (`partition.rs:1010-1073`). Cadence: bytes between snapshots = (1 s budget / 1000) × measured replay bytes per µs (`replay.rs:23-51`), or immediately on `LogFull`.
- **Growth bounds:**
  - Capped: volumes, snapshots and attachments (`partition.rs:504, 522, 547`).
  - Uncapped: grants, landings and consumers (`partition.rs:587-621`). The audit `Vec` is never trimmed. In practice these are bounded only by the snapshot slot size.

### 4.7 Registers, ledger, mirror, reconfiguration

- **`register.rs` (3,144 lines)** is a sans-io fenced-register core in the style of Vertical Paxos II:
  - **Objects:** an `ObjectId` is 128 bits whose high 8 bytes name the creator host (`register.rs:49-88`).
  - **Quorum:** 2f+1 candidates, commit at f+1 (`register.rs:99-129`).
  - **Fencing:** `Fence` refuses epochs below the highest seen with `StaleEpoch` (`register.rs:540-565`).
- **The `Acceptor`** (`register.rs:980-1121`):
  - It checks, in order: configuration generation, authorized owner, epoch.
  - It accepts **one value per (object, sequence) position**. A different value at the same position is `ConflictingPosition`; the same value is re-acknowledged idempotently. This is Paxos-style position acceptance, not compare-and-swap.
  - "Per-object authority is owed": today each acceptor holds one fence and one authorized owner (`register.rs:656-667`).
- **Commit and placement:**
  - `commit_over_holders` counts only distinct candidates whose acknowledgement binds to the exact record (`register.rs:1203-1230`).
  - Placement uses copysets plus rendezvous hashing across failure domains, with Cidon et al.'s loss math (`register.rs:246-538`).
- **Takeover ("promote")** is Prepare/Promise: raise the fence, then adopt the highest (sequence, epoch) (`register.rs:815-978, 1249-1320`).
- **Simulations:** `ledger.rs`, `mirror.rs` and `reconfig.rs` describe themselves as pure deterministic simulations (`ledger.rs:12-16`; `mirror.rs:1-19`; `reconfig.rs:1-25`). Only `adopt`, `LedgerAcceptor` and `LedgerPromise` are used in production, by `cluster`.

### 4.8 Concurrency

- **One writer per partition,** pinned to its shard. `Db` takes `&mut self` (`replay.rs:275, 334`). No locks.
- **Cross-process words** are Acquire/Release atomics (`record.rs:89-97`).
- **A register check-and-accept** must run in one shard turn with no await inside it (`register.rs:1092-1095`).
- **Not implemented:** the design's cross-partition (Calvin-style) transactions (`SLATES_DESIGN.md:2573-2577`).

### 4.9 Tests

- **`tests/model.rs`:**
  - A proptest (60 cases, up to 250 steps) of generated histories with crash steps. A crash is `drop(db)` and recovery from the same segment, so crashes happen only at operation boundaries (`tests/model.rs:730-778`).
  - Synthetic corruption: one flipped byte and hostile headers (`tests/model.rs:785-917`).
- **`tests/promote_oracle.rs`:** a proptest (8,192 cases, f ∈ {0,1,2}) against a serial reference. It checks Agreement, Continuity, StaleNeverCommits and a non-vacuity counter. Messages are direct calls with no loss or reordering (`tests/promote_oracle.rs:148-325`).
- **TLA+ specs** live in `docs/wip/models/`: `FencedRegister.tla` and `Reconfig.tla`.

### 4.10 Performance

From `BENCHMARKS.md:269-283` (M5 Max, 128 MiB `shm_open` segment):

| Row | Median |
|---|---|
| ART insert, per key (10^5 keys) | 53 ns |
| ART lookup, per key | 18 ns |
| One mutation (check, encode, append, apply) | 206 ns |
| Recover 10^4 volumes from 10^6 records (69.5 MB of log) | 96 ms |
| Replay, per record | 95 ns (about 720 B/µs) |

These are memory-speed numbers with no fsync. They say nothing about disk.

### 4.11 What this means for mantle

Don't depend on it:
- no disk persistence;
- a closed slates schema;
- O(n) snapshots and scans;
- keys routed by creator host;
- it couples to `anchor`, `mem`, `rt`, `machine` and `wire`.

It is not a Tectonic name/file/block metadata store, which needs a persistent, sharded, transactional key-value store.

Port these patterns:
- record framing: magic, length, sequence, CRC32C, schema hash, verify-before-decode, sequence continuity;
- guard-then-apply deterministic replay;
- effect plus completion record in one log entry;
- snapshot cadence derived from measured replay throughput against a recovery budget;
- the fenced-register `Acceptor` with its promote/commit oracles and copyset math, as a starting point for chunk-replica or ownership fencing.

A disk version must add:
- fsync'd files and group commit;
- checksummed, versioned snapshots;
- a way to tell a torn tail from corruption in the middle of the log;
- explicit schema migration;
- incremental checkpoints.

---

## 5. `crates/mem` — memory budgeting and allocation

### 5.1 Strategy and budgets

- **The crate doc** promises per-shard ownership, generational handles, storage that never moves, and regions "mapped, pre-faulted and locked at start" (`crates/mem/src/lib.rs:5-20`).
- **What the server does instead:** each shard maps **one lazily-faulted anonymous region** of `capacity / shards / 3` at boot (`crates/server/src/daemon.rs:1941-1947`; `crates/server/src/config.rs:327-353`).
  - It is **not** pre-faulted and **not** locked.
  - Pages are never returned: grep finds no `MADV_DONTNEED`, `MADV_FREE` or `MEM_DECOMMIT` anywhere in the workspace. RSS only rises to its high-water mark.
- **The budget** (`budget.rs`):
  - One `Ledger { reserve, committed, headroom, hold }` (`budget.rs:24-35`).
  - admittable = reserve − committed − headroom − hold, with saturating arithmetic (`budget.rs:47-53`).
  - `take` is whole-or-nothing and returns `BudgetExceeded { requested, available }` (`budget.rs:57-67`).
  - Three wrappers share it: `ShardBudget` (content bytes, with a retention sub-account), `VersionBudget` (inode-version slots) and `MetadataBudget` (`budget.rs:85-355`).
  - `set_hold` shrinks what can still be admitted under memory pressure without touching commitments (`budget.rs:151-153`). The server derives the hold from `memory_available_now` versus its boot baseline (`daemon.rs:2273-2292`).
- **Failure mode:** only a typed refusal. mem has no blocking, backpressure or eviction.

### 5.2 Structures

| Structure | Summary |
|---|---|
| `handle` | `Handle<T> { index: u32, generation: u32 }`, `Copy`. `Encoded` packs `shard:16 \| slot:24 \| generation:24` (`handle.rs:17-21, 82-108`). The doc says a 24-bit alias would be "counted, not silent", but no counter exists; generations wrap silently (`slab.rs:274, 296`). |
| `slab` | Segmented storage, an intrusive free list, a generation per slot. O(1) insert. `with_generation_base` lets a successor slab avoid old handles; `insert_at` rebuilds a slab from a durable image (`slab.rs:14-300`). |
| `segmented` | `Vec<Vec<T>>` of power-of-two segments with stable addresses. Allocates when a new segment is needed unless segments were reserved (`segmented.rs:11-32, 93-96`). |
| `buddy` | Binary buddy: one state byte per granule plus u32 free lists, about 9 heap bytes per granule, kept outside the region so cold pages stay untouched (`buddy.rs:6-165`). |
| `arena` | `ChunkArena`: regions plus buddies. `Extent { region: u16, offset, len }` carries no pointers. Only the largest power-of-two span of each region is usable. First fit across regions (`arena.rs:15-136`). |
| `region` | `Backing::Anon(MmapMut) \| Shared(SharedObject)`. `madvise(MADV_HUGEPAGE)` only if huge faults measured cheaper (`region.rs:206-227`). `numa_node` is recorded but never set. |
| `prefault`, `lock` | Batch = `slice_ns / fault_ns`. Locking goes metadata < rings < chunks, within lock capacity (`prefault.rs:32-72`; `lock.rs:12-66`). **Neither has callers outside mem.** |
| `ring`, `mpsc` | Lamport SPSC and Vyukov bounded MPSC over `AtomicU64`, 128-byte-aligned indices (`ring.rs:24-126`; `mpsc.rs:25-134`). Single-producer is *not* enforced by types: `split(&self)` can be called twice. |
| `shared` | RAM-backed shared memory with no filesystem entry: `memfd_create` on Linux, `shm_open` with O_EXCL and mode 0600 on macOS, a `Local\` section on Windows (`shared.rs:290-705`). Windows `SparseObject` commits per granule after a 167 GB layout was refused on a 16 GB runner (`shared.rs:23-30, 729-818`). |

### 5.3 OS primitives

- **Linux and macOS:** memmap2 `map_anon` (private anonymous). No `MAP_POPULATE`, `MAP_HUGETLB` or `mach_vm`. `mlock` goes through memmap2 (`region.rs:235-251`).
- **Windows:** raise the working set, then `VirtualLock` (`region.rs:263-289`). Large pages are never used.
  - *Inference:* memmap2's anonymous map on Windows is a committed pagefile section, so the whole arena is charged against commit at boot. That contradicts the "lazy mapping" rationale.
- **When mlock fails,** the region stays mapped and usable. The error reports `locked: 0`, which is hard-coded (`region.rs:243, 285`).
  - The only production lock is on a `require_locked` volume create. It locks the whole shard arena and refuses with `BudgetExceeded { available: 0 }` on failure (`crates/server/src/verbs.rs:2764-2779`).
  - *Inference:* that fails under Linux's default memlock limit.

### 5.4 Allocation policy and tests

- **No global allocator ships.**
- **`tests/no_alloc.rs`** installs a per-thread counting allocator and asserts zero allocations over slab and buddy churn, given pre-reserved segments (`tests/no_alloc.rs:13-117`). Production slabs mostly use `Slab::new` without reserving, so their first insert into each new segment does allocate. Only rt pre-reserves (`crates/rt/src/timer.rs:61`; `shard.rs:329-334`).
- **Loom** covers the SPSC ring, MPSC contention and lapping, and handle-vs-slot reuse, under shared bounds (preemption 2, branch cap 224, and a vacuous model fails) (`loom_bounds.rs:36-83`). Miri runs mem's lib tests in CI (`ci.yml:362-363`).

### 5.5 Use of machine facts

- **Page size and huge-page choice:** measured values are used.
- **Cache-line alignment:** hard-coded at 128 B (`ring.rs:27`). The comment says it is "checked at boot", but no such check exists.
- **NUMA:** recorded only.

### 5.6 Performance

M5 Max (`BENCHMARKS.md:53-75`):
- slab insert + remove: 12 ns;
- buddy alloc + free, one 16 KiB page: 62 ns;
- buddy, 64 pages (split and coalesce): 71 ns;
- SPSC round trip between two threads: 348 ns.

Callgrind benches exist, but there is no saved baseline and no enforced gate (`docs/wip/TBD_FIXES.md:323-342`).

### 5.7 Dependencies and unsafe code

- **Dependencies:** `slates-machine`, memmap2, rustix, windows-sys, loom (cfg).
- **Unsafe:** 19 blocks, exactly the budget. 16 of them are Windows-only (`unsafe-budget.toml:20-28, 139`).

### 5.8 What this means for mantle

- **Port:**
  - `budget.rs`: whole-or-nothing admission with a pressure hold. It fits disk bytes, open objects and metadata quotas.
  - `handle`, `slab`, `segmented`: add refusal on generation wrap, and reserve at boot.
  - `ring`/`mpsc`: make `split` consuming.
  - the `loom_bounds` pattern.
- **Rewrite** arena/buddy/region/prefault/lock as a **device-aware I/O buffer pool**:
  - alignment taken from the storage probe;
  - huge or large pages chosen by measurement;
  - an explicit memlock budget with honest reporting;
  - decommit on shrink;
  - NUMA placement.

---

## 6. `crates/transport` — brief

**What it is.** Two planes over UDP (`crates/transport/src/lib.rs:1-30`):

- **Control plane:** stateless datagrams sealed with AES-256-GCM. Keys come from an HKDF-Expand-Label schedule; nonces are per-direction counters.
- **Session plane:** "slates's owned RFC 9000/9002-shaped QUIC dialect" over `rustls::quic`.
  - Frames are fixed-layout little-endian, **not QUIC varints**: `Stream` at an absolute offset with `fin`, `Ack`, `MaxData`/`MaxStreamData`, `ResetStream`/`StopSending` (`session.rs:1-20`). It is therefore **not wire-compatible with RFC 9000 QUIC**.
  - `rustls::quic` runs the RFC 9001 handshake in CRYPTO frames and supplies packet-protection keys.
  - Authentication is **mutual TLS**. Configured peers pin the exact leaf certificate; operator CA roots admit enrollment candidates (`handshake.rs:1-12`).
  - The endpoint implements RFC 9001-shaped packet protection, including header protection, and tail-loss probes (RFC 9002 §6.2.1). An 8-byte connection id derived from the TLS exporter lets many peers share one socket (`endpoint.rs:1-27`).
  - rustls's config is the one `Arc` in slates, a documented exception to the no-`Arc` rule (`handshake.rs:9-12`).
- **Dependencies:** rustls 0.23 with the `ring` provider (`Cargo.toml:84`), aes-gcm, hkdf, sha2, rustix; `slates-rt` for the UDP edge only; `slates-machine`. Unsafe budget 0.
- **Lints:** the only crate that denies `indexing_slicing`, `string_slice`, `panic_in_result_fn` and `unwrap_in_result` (`lib.rs:36-47`).

**Copa congestion control.**
- Chosen by a bake-off on 2026-09-28. Five laws were run over 57 simulated scenarios × 3 seeds:
  - Copa's goodput-shortfall geomean was 1.068, against NewReno 15.8 and CUBIC 14.0.
  - BBRv3 stalled at 100 Mbit/s with 5% loss.
  - Copa with Meta's δ = 0.04 failed RTT fairness.
  - The losing laws were deleted, not kept as fallbacks (`congestion/mod.rs:1-16`).
- The chosen setting is **δ = 0.5** (`COPA_INV_DELTA = 2`, `congestion/mod.rs:59, 71`).
- Parameters (`congestion/copa.rs:1-40`): RTTmin over 10 s; RTTstanding over srtt/2; pacing at 2·cwnd/RTTstanding; a competitive mode that runs AIMD on 1/δ. Everything is integer arithmetic.

**Path MTU discovery (RFC 8899, applied to QUIC per RFC 9000 §14.3)** (`pmtud.rs:1-45`):
- Base size 1,200 B; binary search up to the peer's declared maximum, ending at a 29-byte granularity.
- `MAX_PROBES` = 3; `EMSGSIZE` bounds the search; a raise is retried after 600 s by rechecking the last failed size.
- A black hole is declared after 3 consecutive losses of above-floor packets.
- Probes are kept out of congestion control and RTT sampling.
- Measured on loopback: **1,625 → 7,840 Mbit/s** (9,209-byte packets), and neutral on floor-MTU paths (commit `fd4f0ef`; `BENCHMARKS.md:550-606`).

**Traffic priorities.**
- Three stream classes (`streams.rs:42-57`):
  - `Control`: membership, fencing, registers;
  - `Metadata`: lookups, small forwarded operations;
  - `Bulk`: content and archive transfers.
- A stream id packs kind (8 bits), class (2 bits), initiator and sequence (`streams.rs:5-10, 84-104`).
- The sender fills each packet from the most urgent class that has data, which is strict priority. A 13-scenario grid, all under Copa, compared three schedulers (`BENCHMARKS.md:535-545`):
  - strict priority: control p99 1.023× the best (geomean), selected;
  - round-robin: rejected, because the control tail was 4.8× worse;
  - deficit round-robin: rejected, because it starved control and metadata.
- Commit `4a3f6d7`: a control ping through a full 1 Mbit/s bulk queue now waits one RTT plus one queue drain, instead of the whole transfer (about 2 s).

**Is there a UDP layer specifically for consensus or FastRaft? No.**
- There is no "FastRaft" anywhere in code or docs (grep).
- Raft messages ride the session plane on `Priority::Control` (`crates/cluster/src/raft_wire.rs:430`), and so do SWIM probes (`crates/cluster/src/swim.rs:685`).
- The stateless control-datagram plane (`ControlDatagram`) is referenced only inside `crates/transport` (`lib.rs`, `enrollment.rs`, `schedule.rs`, `seal.rs`, `accept.rs`). No other crate consumes it.

**Relevance for mantle.**
- **Reusable pieces:** the congestion bake-off method, Copa, PMTUD, and strict-priority classes for node-to-node traffic (bulk chunk replication beside latency-critical metadata and consensus).
- **Not usable as-is:** it is IPv4-only, and its dialect is not standard QUIC. An S3 front end must speak HTTP over TCP, and possibly standard QUIC for HTTP/3.

---

## 7. `crates/cluster` — brief

- **Dependencies:** `slates-db` (acceptance rules), `wire`, `transport`, `rt`, `archive` (`crates/cluster/Cargo.toml`).
- **Dispatch:** the crate is the asynchronous dispatch that ships a register record to candidate holders (`lib.rs:1-24`):
  - it stops at f+1 distinct *binding* acknowledgements or at a deadline, with progress-based extension (`progress.rs`);
  - a deadline after partial acceptance is reported as **uncertain**, never as failed. The record identity is unchanged, so a retry is idempotent.
- **SWIM + Lifeguard** (`detector.rs:1-22`):
  - direct probes in randomized order, and indirect ping-req through k peers;
  - infection-style gossip piggybacked on probes, bounded at λ·ln(n+1) (`gossip.rs:1-4`);
  - Lifeguard's local-health multiplier;
  - a confirmation-count suspicion timeout, max − (max−min)·log(C+1)/log(K+1), computed in fixed point so it is bit-identical on every host (`fixed.rs:1-6`).
- **Vivaldi coordinates** (height plus adjustment) are carried on acks. Using them inside the detector is still owed (`coordinates.rs:1-22`; `swim.rs:8-10`).
- **Raft, configuration only:**
  - A regional council (a small elected voter set plus learners) folds a committed Raft log into the `RegionalConfiguration`: membership, neighbourhoods, per-host fencing epochs and fault tolerance (`config_group.rs:1-26`).
  - A root group does the same for regions, homes and promotions. Its cross-region wiring is owed (`root_group.rs:1-26`).
  - Features: PreVote, CheckQuorum, ReadIndex, joint consensus, snapshot/compaction (`raft.rs:1-26`).
  - The election timeout is 10 × max(voter RTT tail, heartbeat) (`timing.rs:1-22`).
  - Consensus is touched only on membership, takeover, neighbourhood and home changes, never per write (banned item 10, `CLAUDE.md:51`).
- **Fenced registers for data writes:** see §4.7. The fencing token is the **owner host's epoch**, bumped by a committed `TakeOver` (`config_group.rs:20-24`). Positions are per object (object, sequence), but fences are per acceptor; per-object authority is owed (`register.rs:656-667`).
- **Content replication** (`content.rs:1-38`; `docs/wip/content-replication.md`):
  - A sealed snapshot's archive is BLAKE3 content-addressed and verified before a holder acknowledges.
  - Three exchanges on Bulk-class streams: Offer→Missing (dedup), Put→Ack (the ack is bound to object, sequence and manifest), Fetch→Have.
  - Content goes to f+1 candidates first, then is **hedged** to the rest after the **measured p95 put latency** (Dean & Barroso). Measured: placed after 349/354 ms against a 3 s starvation hold (`content-replication.md:8-40`). The hedge lives in `crates/server/src/fleet.rs` (about lines 2758–3031, `hedge_delay_ns`, a bounded ring of latency readings).
  - An anti-entropy healer repairs a holder that lost placed content (`content-replication.md:96-125`).
  - A compress-or-not cost model uses the boot profile's codec points (`content-replication.md:162-185`).
- **Relevance for mantle.** The quorum-with-uncertain-outcome dispatch, the binding acknowledgements, p95 hedging and verified content addressing map directly onto Tectonic-style chunk writes. SWIM+Lifeguard is a sound membership base. The register core is a starting point for chunk-ownership fencing, but its per-object authority and message-loss testing are unfinished.

---

## 8. `crates/wire` and `crates/wire-derive` — serialization

### 8.1 Encoding rules (`crates/wire/src/codec.rs`)

- **The `Wire` trait:** `SCHEMA` (reflection text), `SCHEMA_HASH`, `encode(&self, &mut Vec<u8>)`, `decode(&mut &[u8])`. `from_bytes` refuses trailing bytes (`codec.rs:11-55`).
- **Integers:** fixed-width little-endian, `u16`…`u128` and `i8`…`i128`. There is no varint, and **`usize`/`isize` do not exist on the wire** (`codec.rs:81-99`).
- **`bool` and `Option`:** a tag of exactly 0 or 1; anything else is `BadTag` (`codec.rs:129-142, 203-222`).
- **`f64`:** canonical. The encoder canonicalizes NaN and −0, and the decoder refuses non-canonical bits (`codec.rs:144-172`).
- **Strings and vectors:** a `u32` length prefix. UTF-8 is validated. `take_len` checks that the bytes remain *before* any allocation, so a `u32::MAX` length allocates nothing (`codec.rs:71-79, 174-201`; test at `codec.rs:360-367`).
  - A length over `u32` is clamped by the encoder, so the receiver refuses it rather than the sender panicking (`codec.rs:252-256`).
  - `Vec<u8>` bulk-copies instead of dispatching per byte. This was measured: per-byte dispatch was 442 ms of a 487 ms recovery (`codec.rs:5-6, 108-127`).
- **Other types:** `[u8; N]` is raw; `Box<T>` is transparent (`codec.rs:227-250`).
- **Not implemented:** `f32`, maps, tuples, and borrowed `&[u8]`/`&str`.
- **Canonical and deterministic:** one value has one encoding, and a decoder refuses any non-canonical byte (`crates/wire/src/lib.rs:11-14`).

### 8.2 The derive (`crates/wire-derive/src/lib.rs`)

- **Structs:** fields in declaration order, with no tags and no lengths (`lib.rs:86-117`).
- **Enums:** a `u32` discriminant equal to the variant index, followed by the variant's fields. An unknown discriminant is `BadDiscriminant` (`lib.rs:127-191, 225-231`).
- **Compile-time refusals:**
  - generics: "no single schema" (`lib.rs:29-34`);
  - unions (`lib.rs:40-43`);
  - tuple structs: "tuple fields have no stable names in the schema" (`lib.rs:62-67`);
  - `usize`/`isize`, via a textual type check (`lib.rs:71-80`).
  - These are covered by trybuild UI tests at `crates/wire/tests/ui/*.rs`.
- **No field attributes at all**: no skip, default, rename or optional (`#[proc_macro_derive(Wire)]` declares no attributes, `lib.rs:19`).
- **Schema hash:** FNV-1a-64 over the reflection text (e.g. `struct Name{a:u32,b:Vec<Item>}`), mixed in order with every field type's hash (`crates/wire/src/schema.rs:10-45`). It is `const`, so the hash is a compile-time constant.

### 8.3 Framing and checksums

- **The 32-byte header** (`header.rs:1-34, 119-163`), little-endian, encoded byte by byte:
  - magic `SLTS` (0x53544C53);
  - major `u16` = 1, minor `u16` = 0;
  - flags `u32`, class `u16`, kind `u16`;
  - length `u32`, checksum `u32`;
  - request id `u64` (client:32 | sequence:32, for exactly-once requests per RIFL).
- **Header decode** refuses bad magic, then a different major, before looking at anything else (`header.rs:136-152`). Flags and minor are not validated.
- **Classes:** Control, Metadata, Bulk, Telemetry, each with its own cap and credit pool. **Bulk carries no CRC**; its bytes are identified by BLAKE3 at a higher layer (`header.rs:36-77`).
- **The `Framer`** (`frame.rs:122-214`):
  - Encode: kind lookup (a linear scan, `frame.rs:113-118`), schema-hash check, body = schema-hash word + message, cap check, CRC32C.
  - Decode: cap checked before allocating, CRC verified before decoding, schema word compared. **The body is then copied** (`rest.to_vec()`, `frame.rs:212`).
  - Caps are derived per class: max(bandwidth × class latency budget, MTU); bulk = max(memcpy knee, MTU) (`frame.rs:23-57`).
- **CRC32C** (`crc32c.rs`):
  - Hardware where available: AArch64 `__crc32cd` or x86_64 `_mm_crc32_u64`, 8 bytes per step, detected at run time on each call. Otherwise a slicing-by-8 table (`crc32c.rs:44-119`).
  - It is a **single dependent chain**, with no multi-lane interleaving. Measured 172 µs per MiB (6.1 GB/s, `BENCHMARKS.md:118-146`); ratchet ceiling 152,417 ns (`ratchets.toml`).
- **Credit:** absolute-offset credit per stream with one pool per class. The window is clamp(bandwidth × RTT, one frame cap, bandwidth × class budget) (`credit.rs:1-60`).
- **`request.rs`:** RIFL completion windows (`ClientWindow`, a `BTreeMap` with no cap of its own).
- **`observe.rs`** (1,184 lines) is slates' observability span registry, not serialization.

### 8.4 Versioning

- **The rule:** "append-only evolution within a major" (`lib.rs:1-5`).
- **The mechanism:**
  - Any change to a message's shape changes its `SCHEMA_HASH`, and a frame whose schema word differs is refused with `SchemaMismatch` (`frame.rs:201-208`).
  - The derived decoder cannot skip unknown appended fields, because `from_bytes` refuses trailing bytes.
  - So compatible evolution means **adding new kinds**, not changing existing ones. Mixed-version peers cannot exchange a changed kind.
- **Golden tests:** `crates/wire/tests/golden.rs` freezes the bytes and reflection prefixes of **test-local** sample types. It pins the mechanism, not slates' production messages.

### 8.5 Zero-copy

**No.** Frame decode copies the body, and message decode allocates owned `String`/`Vec` values. Measured: body decode 96 ns ("two string allocations, one vector") against 8 ns for encode into a reused buffer (`BENCHMARKS.md:118-146`).

### 8.6 Dependencies and reusability

- **Dependencies:** `wire` → `slates-machine` (only for `derived!`) + `slates-wire-derive`. `wire-derive` → syn, quote, proc-macro2.
- **Unsafe:** 4 sites, the CRC intrinsics.
- **Dependents:** anchor, client, bridge-virtiofs, db, ipc, cluster, sdk-python, vfs, server.
- **Reusability:**
  - **Inter-node RPC:** good as a *discipline*: canonical encoding, caps before allocation, checksum before decode, typed refusals. Mantle would need skew tolerance for rolling upgrades.
  - **On-disk metadata:** needs explicit per-record versions with decoders for old versions, because data outlives binaries.
  - **Bulk:** needs borrowed decode for payloads (chunks must not be copied).
  - **CRC:** needs a multi-lane CRC32C, because 6 GB/s per core is below NVMe line rate for checksumming chunks.

---

## 9. The engineering rules (for mantle to adopt)

**Sources:** `CLAUDE.md` and `AGENTS.md` (137 lines each), `Cargo.toml:107-148` (the lint wall), `clippy.toml`, `rust-toolchain.toml`, `xtask/src/*.rs`, `unsafe-budget.toml`, `ratchets.toml`, `.github/workflows/ci.yml`, `docs/bugs/`.

**`CLAUDE.md` and `AGENTS.md` are identical except in three places:**
- the title (line 1);
- banned item 6 (line 47);
- the "Errors, not panics" bullet (line 63).

`CLAUDE.md`, last changed 2026-09-27 in `4a3f6d7`, is strictly stricter than `AGENTS.md`, last changed 2026-09-05 in `a1ce993`. `AGENTS.md` still allows "`expect` with a message for a statically impossible failure". **Adopt the `CLAUDE.md` wording, and keep the two files identical by a check.**

### 9.1 Locked rules R1–R10 (`CLAUDE.md:20-33`)

| Rule | Content | Mantle |
|---|---|---|
| R1 | RAM only. Disk is written only in a granted landing. Enforced by clippy `disallowed-methods` (`clippy.toml:27-43`) and the structural test: only `slates-land` may link write-capable syscalls (`xtask/src/main.rs:337-413`). | **Invert the content, keep the mechanism:** only the storage-engine crate may link write syscalls. |
| R2 | No `Arc`/`Rc`, and `Mutex`/`RwLock`/`Condvar` are disallowed types. Sharing is by generational handle, move over a bounded channel, or epoch-published immutable root. Three D-8 exceptions. | Adopt. |
| R3 | No magic numbers: every tunable is measured and derived by a stated formula (`derived!` or a `/// Derived:` line). | Adopt. |
| R4 | Maximal correctness and performance. Evidence tiers: A paper, B standard/vendor doc, C deployed code, D blog (flagged), M measured here. Floors ratchet. | Adopt. |
| R5 | Tests exercise observable behaviour, never "a file exists", "a constant equals" or "an internal field". | Adopt. |
| R6 | Everything async on a thread-per-core runtime with completion drivers. | Adopt, with the caveat in §3.9. |
| R7 | Plain English. | Adopt. |
| R8 | Laptop ≡ fleet, one code path, an N=1 differential test, no mode switches. | Adopt. It matches "laptop to exabytes". |
| R9 | Sub-50 µs provisioning histogram. | slates-specific. Replace with mantle's own latency SLO gate. |
| R10 | Disk writes only on a human grant, and **no privilege ever required**. | Keep the no-privilege half: hardware detection must work unprivileged. |

### 9.2 Banned list: each item needs explicit per-item authorization (`CLAUDE.md:35-55`)

The procedure on meeting one is STOP, ASK, WAIT, and flag it for removal if found in the tree.

1. `Arc`, `Rc`, `Mutex`, `RwLock`, atomics on a per-item path, or any lock on a data path.
2. tokio, async-std, smol, or any external async runtime.
3. `std::fs`, `std::net`, `tempfile`, `/tmp`, `mkdir`, symlinks, or disk writes outside `land`.
4. Any capability or privilege requirement.
5. A hardcoded tuning number, timeout, size, threshold, percentage or retry count.
6. **Panics in non-test code, ever:**
   - `unwrap`, `expect` (a message doesn't make it acceptable), `panic!`, `todo!`, `unimplemented!`, `unreachable!`, `assert!`/`debug_assert!`;
   - indexing or slicing that can go out of bounds;
   - arithmetic that can overflow or divide by zero.
   - Use `.get()`, `checked_*`/`saturating_*`, `try_from` and `Option`/`Result` flow instead.
7. A fallback that preserves legacy behaviour, a compatibility shim, a "just in case" second path, or a mode switch. "Replace, do not layer."
8. Unbounded growth: every queue, cache, log, retry loop and task set has a derived bound and a typed refusal at it.
9. A lost or swallowed error, a fire-and-forget task, an orphaned future, or a thread with no owner that joins or cancels it.
10. Consensus, a lock service or a coordination call on a per-write path.
11. An inferred merge. (slates-specific.)
12. A global catalog or index for lookups: "ids route to owners". *For mantle:* a Tectonic metadata store is compatible only if it is hash- or range-partitioned and ids route to owning shards. State that explicitly.
13. Non-Rust tooling, a JDK, TLA+ tools or model checking in CI or on Ada's machine.
14. Subagents, background jobs, tool installs or system-state changes unless Ada asks.

### 9.3 Code shape (`CLAUDE.md:57-71`)

- **Module docs:** every module opens with `//!` naming its design section, its invariants and the evidence for its shape.
- **Constants:** every `const` has a `///` with its derivation or measurement and its anchors.
- **Unsafe is budgeted and only shrinks:**
  - `unsafe-budget.toml` sets per-crate ceilings; `cargo xtask unsafe [--tighten]` checks them.
  - A raise is an edit naming the new site and its reason. See the dated raise and lower history in the header and inline comments of `unsafe-budget.toml`.
  - Budgets: machine 49, mem 19, rt 61, wire 4, land 3, anchor 6, ipc 35, db 0, cluster 0, transport 0, bridge-winfsp 86.
  - Prefer rustix, memmap2, `Cell`/`RefCell` and `&'static`. `unsafe` is only for FFI without a safe wrapper, intrinsics behind runtime detection, the `RawWaker` vtable, and wrappers that are unsafe by signature.
  - No `unsafe impl Send`/`Sync`.
- **Every `unsafe` block has `// SAFETY:` directly above it,** enforced by `clippy::undocumented_unsafe_blocks` and `unsafe_op_in_unsafe_fn`. Miri runs in CI.
- **Errors:** a closed typed refusal taxonomy per subsystem; an uncategorized refusal is a bug. `String` errors only at boundaries, with a comment saying why.
- **Ownership:** objects live in arenas and are named by generational handles; a stale handle is a typed miss. Cross-shard work is a message on a bounded ring. Singletons are `&'static` via `OnceLock` or `Box::leak`.
- **Atomics:** hot atomics are cache-line padded (`#[repr(align(128))]`), sharded per thread where contended, `Relaxed` for statistics and Acquire/Release for flags.
- **Platform code:** paired `#[cfg]` functions with identical signatures. Pure decision logic is cfg-free and tested on every host. SIMD sits behind runtime detection with a scalar arm and bit-exact tests.
- **Bounded, cancellation-safe work:** loops over user-scaled data are chunked into cooperative slices under the shard's step budget, and dropping a future leaks nothing.
- **Integers:** `u64 → usize` goes through `try_from`, and i686 is a *tested* target. Parsers check length against the cap before allocating and verify the checksum before decoding.
- **Size and naming:** long honest names. **Cognitive complexity threshold 10** (`clippy.toml:6`), which "only ratchets down": a function above it is split.
- **Toolchain:** two-space indent (`rustfmt.toml`), edition 2024, **Rust pinned exactly to 1.98.0** (`rust-toolchain.toml:6`), `panic = "abort"` and LTO in release (`Cargo.toml:143-147`).

### 9.4 The lint wall as configured (`Cargo.toml:107-141`; `clippy.toml`)

- **rustc:** `unsafe_op_in_unsafe_fn`, `missing_docs`, `unused_must_use`, `unexpected_cfgs` (with `loom` and `shuttle` allowed) are deny; `unreachable_pub` is warn.
- **clippy:** `all`, `disallowed_types`, `disallowed_methods`, `undocumented_unsafe_blocks`, `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `unreachable`, `dbg_macro`, `cast_possible_truncation`, `cast_sign_loss`, `cast_possible_wrap`, `cognitive_complexity`, `mem_forget`, `missing_safety_doc`, `missing_panics_doc` are all deny. Tests may unwrap, expect, panic and dbg (`clippy.toml:8-12`).
- **`clippy.toml` disallows:**
  - types: `Arc`, `Rc`, `Mutex`, `RwLock`, `Condvar`;
  - methods: every `std::fs` write, create, remove, rename, link, copy, set_permissions and set_len; `std::env::temp_dir`; and `std::thread::sleep`, because time must go through the timing wheel (`clippy.toml:17-43`).

### 9.5 Structural gates (`xtask/src/main.rs`)

- **`cargo xtask structural`** (`main.rs:267-581`):
  - Forbidden dependencies anywhere in the resolved graph: tokio, tokio-util, async-std, smol, futures-executor, parking_lot, dashmap, rayon, crossbeam-epoch, arc-swap, async-lock.
  - Forbidden symbols: `Arc<`, `Rc<`, `Mutex<`, `RwLock<`, `Condvar`.
  - Host-path symbols allowed only in a named crate list, each entry with a reason.
  - Write syscalls allowed only in `land`: libc and rustix calls, `OFlags::CREATE`/`TRUNC`/`TMPFILE`, and Windows `MoveFileExW`/`FlushFileBuffers`/`SetFileInformationByHandle`.
  - `// structural: allow` exempts one line and must say why.
- **`cargo xtask literals`** (`main.rs:583-747`):
  - Numeric literals must sit inside `derived!(…)` or within 4 lines after a `Derived:`, `Measured:`, `Format:` or `Shape:` doc marker.
  - Allowed without a marker: 0, 1, 2, shift and bit-width contexts, and attributes.
- **`cargo xtask check`** runs structural, literals, unsafe budget and version together.

### 9.6 Tests (`CLAUDE.md:73-90`)

Every test is written as "do X, expect Y" and names the acceptance criterion or test id it serves (`/// AC-1.7`, `/// T-3.4`). The kinds:

- **Model and oracle tests:** a serial specification kept in the test module, compared on every generated history with proptest shrinking.
- **Non-vacuity counters:** every fast path exports a counter the test asserts moved.
- **Determinism gates:** build twice and compare byte for byte.
- **Golden vectors** for everything hashed on the wire or in a format.
- **Hostile-input tests** on every parser: `len = u32::MAX`, truncation, bit flips, foreign magic, overlapping ranges.
- **Differential tests** against tmpfs, an APFS RAM disk and an NTFS RAM VHD. Environment-gated tests **skip loudly**.
- **Conformance suites** with expected-failure lists that only shrink: pjdfstest, fsx, fsstress in CI; xfstests and LTP nightly.
- **Concurrency:** loom on rings and handle cores; Miri on mem, rt and wire; shuttle and TSan nightly.
- **Simulation and chaos:** the deterministic driver plus a nemesis over a seed budget, with register invariants as history checks and linearizability/Elle nightly.
- **Doc-truth tests:** tables in docs are asserted against source constants.
- **Real workloads:** git, cargo, npm, sqlite and others, byte-identical to the host.
- **Hygiene:** tests write only inside a RAM-backed temp directory named with the process id. Environment mutation is `unsafe` with a SAFETY note.
- **Bug fix = failing test first,** then the minimal change, then a sweep for sibling instances reported to Ada.

### 9.7 Benchmarks (`CLAUDE.md:92-97`; `BENCHMARKS.md:1-6`; `xtask/src/ratchet.rs:1-24`)

- **A benchmark is a recorded release-binary command** with its hardware, dataset commits, date, load discipline, and best-of-N with all N shown. "Numbers are honest points, not marketing; the commands are the contract."
- **Rejected experiments stay on record** with their numbers. A dead-even A/B does not land.
- **The ratchet** follows Kalibera & Jones's hierarchy:
  - A row's ceiling is the highest upper edge of the 95% bootstrap interval across N=3 runs.
  - A regression is declared only when every fresh run's lower edge sits above every recorded upper edge.
  - It is keyed by machine-identity hash. There are 72 rows, for one machine, the M5 Max (`ratchets.toml:37-114`).
  - Tightening needs an improvement larger than the row's drift. A raise is a documented edit (examples at `ratchets.toml:18-35`).
- `CLAUDE.md:97` also forbids probing by writing disk (see §1.8 for why mantle must invert this).

### 9.8 CI (`.github/workflows/ci.yml`, 574 lines)

| Job | What runs |
|---|---|
| `gates` (ubuntu, macOS) | fmt check, clippy `-D warnings`, `xtask structural`, `literals`, `version`, `cargo test --workspace` (io_uring forced on Linux), a 10^6-op model suite, tmpfs differential, landing with `kill -9`, CLI flow, FSKit, informational land bench, ratchet |
| `windows-lint` | clippy on a subset of crates, plus selected tests |
| `callgrind` | iai-callgrind for mem, rt, wire |
| `miri-and-loom` | loom on mem/rt/ipc; Miri on mem+wire (leak check on) and rt simulation tests |
| `conformance` | pjdfstest, fsx, fsstress, workloads, hermeticity tracer |
| nightly | shuttle, windows-nightly, KIND fleet lane |

### 9.9 Bug records (`docs/bugs/`)

- **Volume:** 206 records dated 2026-09-05 to 2026-09-28.
- **Format:**
  - a title stating the defect as a sentence;
  - Date and Contracts/Area (the design sections);
  - Severity;
  - "The rule", quoted from the design;
  - Symptom or Description, with the reproducing command and its timing;
  - Root cause, Fix, Evidence/Validation (with machine and load), Exact edits, and Siblings.
- **Examples:** `2026-09-13-durability-refusal.md`, `2026-09-26-io-uring-zero-timeout-harvest-sleeps.md`, `2026-09-28-copa-froze-an-overshot-window.md`.
- **The debugging protocol that produces them** (`CLAUDE.md:128`):
  1. Log to a file first.
  2. Consult the design.
  3. Confirm with Ada.
  4. Write the record.
  5. Fix in plan mode, touching nothing else.
  6. Report siblings.
  7. Never preserve the buggy behaviour.

### 9.10 Enforcement gaps mantle should close on day one

1. **The documented panic lints are not in the workspace lint wall.** `CLAUDE.md:47` lists `indexing_slicing`, `string_slice`, `panic_in_result_fn` and `unwrap_in_result` as enforced "in `Cargo.toml`", but `[workspace.lints.clippy]` lacks them. Only `crates/transport/src/lib.rs:36-47` denies them. No crate enables `arithmetic_side_effects`, so "no overflowing arithmetic" is policy, not lint.
2. **The unsafe budget is not a CI step.** CI runs `structural`, `literals` and `version` individually but never `xtask unsafe` or `xtask check` (`ci.yml:36-48`).
3. **Instruction counts are recorded, not gated.** No iai-callgrind regression thresholds are configured (grep of the benches and `ci.yml`), and there is no saved baseline (`TBD_FIXES.md:323-342`).
4. **The wall-clock ratchet only gates on the one recorded Mac.** CI runners skip it loudly (`ci.yml:135-138`). TSan is owed (`ci.yml:522-523`).
5. **The xtask checks are lexical.** They find test modules by brace counting and read code only after stripping strings and comments (`main.rs:209-265`). *Inference:* the unsafe counter mis-scopes a `#[cfg(test)] fn` in `crates/machine/src/segment.rs:141` and so counts 49 instead of 50. A `syn`-based checker would be exact.
6. **Two rules files have drifted apart:** `AGENTS.md` vs `CLAUDE.md` (above). Stale notes also appear in the budget file, e.g. the `SO_RCVBUF` site listed under `slates-land` (`unsafe-budget.toml:130-131`) actually lives in `crates/rt/src/netsys.rs:321`.

---

## 10. Reuse assessment

### 10.1 Internal dependency chains

From `cargo metadata --no-deps`, normal dependencies only:

```
slates-machine        (leaf)  ext: blake3, memmap2, serde, serde_json, [lz4_flex, zstd]opt, libc/rustix (unix), windows-sys
slates-mem         -> machine
slates-rt          -> machine, mem               (+ io-uring on Linux)
slates-wire-derive    (leaf)  ext: syn, quote, proc-macro2
slates-wire        -> machine, wire-derive
slates-archive        (leaf)  ext: blake3, lz4_flex, [zstd]
slates-vfs         -> archive, machine, mem, wire
slates-base        -> machine, vfs
slates-land        -> base, machine*, mem*, vfs  (*unused in src/; libc/rustix unix-only; no rt, no db)
slates-anchor      -> machine, mem, wire         (+ rt on unix)
slates-db          -> anchor, machine, mem, rt, wire      (no land)
slates-transport   -> machine, rt                (+ rustls, aes-gcm, hkdf, sha2)
slates-cluster     -> archive, db, rt, transport, wire
```

- `land` does **not** depend on `rt` or `db`. It pulls in the whole VFS through `vfs` and `base`.
- `db` does **not** go through `land`. It depends on `anchor`'s shared segment and uses `rt` only for `timer::Wheel`.

### 10.2 Verdict per crate

| Crate | Verdict | Why |
|---|---|---|
| `machine` | **Port selected parts** | • No storage probing, mantle's primary hardware need. <br>• Logical cores only: SMT, packages, NUMA and processor groups are not used. <br>• Windows is stubbed: no available memory, core classes or job limits, and it uses the non-`Ex` API. <br>• The wake probe can abort boot. <br>• The API is shaped around slates' spin/park/arena needs. <br>• **Port:** `stats.rs`/`bench.rs`, `derived.rs`, the cgroup/rlimit walk, the macOS sysctl readers, `clock.rs`. |
| `land` | **Ignore as a dependency; port `os.rs` and the test method** | • The engine's API takes slates' `Volume`/`Store`. <br>• Unix only. <br>• No preallocation, direct I/O, checksums or group commit. <br>• macOS production never uses `F_FULLFSYNC`. <br>• Tests model process death, not power loss. <br>• **Port:** the syscall primitive map and its errno-learned fallbacks, discard-and-rewrite on a failed sync, and crash-at-every-write with resume. |
| `rt` | **Port selected parts; don't depend** | • No file I/O: io_uring is readiness-only. <br>• No IPv6, no Windows TCP, no HTTP. <br>• SMT/NUMA-blind placement. <br>• A process-global 1,024-slot registry. <br>• **Port:** wake-word + slab + registry, park/kick (fix the unconditional kick), timing wheel (fix the O(n) rescan), admission receipts, attribution, `SimRuntime` + seeded fabric (add a simulated disk). |
| `db` | **Ignore as a dependency; port patterns** | • RAM/shared memory only, with no fsync. <br>• A closed 39-op slates schema. <br>• Full-state snapshots; O(n) scans. <br>• Truncates on any corruption; schema change looks like a torn tail. <br>• **Port:** record framing, guard-then-apply, effect-plus-completion atomicity, the fenced-register `Acceptor` with its oracles, and copyset math. |
| `mem` | **Port selected parts** | • The arena is built for RAM-as-store, not aligned I/O buffers. <br>• Prefault and lock are unwired; no decommit; Windows may commit the whole arena. <br>• **Port:** `budget.rs`, handle/slab/segmented (add wrap refusal), rings (make `split` consuming), loom bounds. |
| `wire` | **Fork and extend** (depending as-is is possible but not advised) | • Small and self-contained: it depends only on `machine`, for `derived!`, and on the derive. <br>• Sound canonical-encoding discipline. <br>• But no skew-tolerant evolution (any change is a `SchemaMismatch`), copying decode, single-lane CRC32C, and a linear kind lookup. <br>• A multi-year on-disk format and rolling fleet upgrades need per-kind versions, borrowed bulk decode and a faster checksum. |

### 10.3 What mantle must build new (no slates code to start from)

1. **Storage hardware detection** per data directory: device class, block sizes, write cache and flush/FUA, queue depth, and the filesystem.
2. **Write-based calibration:** fsync/flush latency and sequential/random throughput.
3. **Durable write path:**
   - preallocation;
   - aligned direct I/O where the device supports it;
   - group commit;
   - per-OS media-durability policy (Linux `fdatasync`/`fsync`; macOS `F_FULLFSYNC`; Windows `FlushFileBuffers` / write-through);
   - on-disk checksums with read-back scrubbing.
4. **Completion-based file I/O:** io_uring ops with registered buffers and opcode probing; IOCP overlapped file I/O; a macOS thread pool.
5. **Networking and the front end:** IPv6 and Windows TCP; an HTTP/1.1 (and later HTTP/2 or HTTP/3) server for the S3 API; standard QUIC if HTTP/3 is wanted, since slates' dialect is not interoperable.
6. **A persistent, sharded metadata key-value store** (Tectonic's name/file/block layers) with range scans, incremental checkpoints and schema migration.
7. **A crash-consistency harness that models power loss:** drop unsynced data, reorder and tear writes, and run a real-disk lane.

### 10.4 Practical constraints on a git dependency

- **Toolchain:** slates pins **Rust 1.98.0 exactly** and edition 2024 (`rust-toolchain.toml:6`; `Cargo.toml:9, 11`). A git dependency forces mantle onto a compatible toolchain.
- **License:** MIT, "Copyright (c) 2026 Hyperlight" (`LICENSE`), so porting is unrestricted with attribution.
- **Access:** the remote is `git@github.com:hyper-light/slates.git`. Whether mantle's CI can read it was not verified.
- **Churn:** slates is version 0.1.0 with 206 bug records in 24 days. A pinned revision would freeze a fast-moving research codebase, and each bump would pull in unrelated changes (for example `machine`'s serde and blake3 into `wire`).

**Recommendation:** port selected parts instead of depending on them, and carry the provenance (the slates commit and file) in each ported module's `//!` header. This follows slates' own "module docs state the design and its evidence" rule.

---

## Appendix A — Doc-versus-code discrepancies found

| Where | Claim | Reality |
|---|---|---|
| `CLAUDE.md:47` | `indexing_slicing`, `string_slice`, `panic_in_result_fn`, `unwrap_in_result` are in the `Cargo.toml` lint wall | Only `crates/transport/src/lib.rs:36-47` enables them |
| `CLAUDE.md:96` | "CI gates on instruction counts (iai-callgrind)" | No regression thresholds configured; no baseline (`TBD_FIXES.md:323-342`) |
| `SLATES_DESIGN.md:3649-3650` | Sparse ranges preserved; large files preallocated | Whole file read into a zeroed `Vec`, one `pwrite`; no preallocation (`land/src/engine.rs:1497-1511, 901-903`) |
| `crates/mem/src/lib.rs:5-20`; `SLATES_DESIGN.md:516-519` | Regions pre-faulted and locked at start | Lazy map, not locked (`server/src/config.rs:327-339`); prefault/lock have no callers |
| `crates/mem/src/handle.rs:8-10` | 24-bit generation alias "counted, not silent" | No counter; generations wrap silently (`slab.rs:274, 296`) |
| `crates/mem/src/ring.rs:25-26` | Cache line "checked at boot" | Hard-coded 128 B; no check |
| `crates/rt/src/registry.rs:84-87`; `parking.rs:3-5` | A message to a spinning shard costs no syscall | Pair-ring sends kick unconditionally (`shard.rs:552`; `driver.rs:137-158`) |
| `crates/rt/src/driver.rs:5-7` | Drivers carry file and socket operations | Only `Nop` and readiness `PollAdd` (`uring.rs:161-177, 216, 350`) |
| `crates/machine/src/facts.rs:66` | `address_bits` is measured | It is `usize::BITS` (`facts.rs:420, 727, 983`) |
| `SLATES_DESIGN.md:789` | Windows topology via `GetLogicalProcessorInformationEx` | Non-`Ex` variant (`facts.rs:865, 898`) |
| `SLATES_DESIGN.md:821` | Shards = physical performance cores | Logical cores (`rt/src/runtime.rs:154-184`) |
| `crates/transport/Cargo.toml` description | "congestion, and header protection owed" | Copa and header protection are implemented (`congestion/mod.rs`; `endpoint.rs:7-10`) |
| `crates/db/src/replay.rs:310` | Mentions `Op::Batch` | No such variant |
| `unsafe-budget.toml:130-131` | Winsock `SO_RCVBUF` listed under `slates-land` | Site is in `crates/rt/src/netsys.rs:321` |
| `AGENTS.md:47, 63` vs `CLAUDE.md:47, 63` | Same rules | `AGENTS.md` still permits `expect` with a message |

## Appendix B — slates' cross-platform constraint list, rows relevant to mantle

From `docs/wip/SLATES_DESIGN.md:5673-5696`. This is design knowledge the owner already gathered, and mantle should start from it:

- **Linux:**
  - 5.10 baseline through epoll;
  - io_uring needs synchronous cancellation (6.0), with single-issuer deferred work on 6.1;
  - `MADV_POPULATE` 5.14; `futex_waitv` 5.16;
  - containers may block io_uring through seccomp, so "epoll fallback is first-class";
  - `mlock` may be limited: "probed and reported".
- **Linux landing:**
  - `O_TMPFILE` (3.11; ext4, tmpfs, XFS 3.15, Btrfs 3.16, F2FS 3.16);
  - `renameat2 RENAME_EXCHANGE` (3.15), with EINVAL where a filesystem lacks it;
  - `FICLONE` (4.5);
  - `openat2 RESOLVE_BENEATH` (5.6).
- **macOS:**
  - 16 KiB pages and no superpages;
  - thread affinity is only a hint;
  - `renamex_np RENAME_SWAP` gated by `VOL_CAP_INT_RENAME_SWAP`;
  - `clonefile` by `VOL_CAP_INT_CLONE`;
  - **`F_BARRIERFSYNC` for ordering, `F_FULLFSYNC` for media**;
  - **no asynchronous file I/O (use a pool)**.
- **Windows:**
  - `FileRenameInfoEx` with `FILE_RENAME_POSIX_SEMANTICS` (Windows 10 1607+, NTFS);
  - share modes are real locks;
  - block cloning (`FSCTL_DUPLICATE_EXTENTS_TO_FILE`) on ReFS only;
  - large pages need `SeLockMemoryPrivilege`;
  - `WaitOnAddress` is process-local.
- **i686:** 32-bit `usize` means `try_from` on every mapped size; 64-bit atomics need `target_has_atomic` guards.
